/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Implementation of RDMA Naos
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */

#include "jvm_naos_rdma.hpp"
#include "jvm_naos.hpp"
#include "jvm_naos_klass_service.hpp"
#include "utilities/rdma_passive_connection.h"
#include "utilities/rdma_active_connection.h"
#include "runtime/vmThread.hpp"
#include "runtime/vm_operations.hpp"
#include "runtime/jniHandles.inline.hpp"
#include "memory/oopFactory.hpp"


inline struct ibv_mr* get_mr_for_region(ActiveVerbsEP* ep, ShenandoahHeap* heap, size_t region_index){
    ShenandoahHeapRegion* reg = heap->get_region(region_index);
    struct ibv_mr * mr = ep->reg_mem((char*)reg->bottom(), reg->region_size_bytes() , 0 );
    if(mr == NULL){
      printf("faield to register heap. Check memlock!!! \n"); fflush(stdout);  
    }
    
    return mr;
}
 
struct metadata_header_t {
  // number of data bytes
  uint32_t bytes_data;
  // number of objects contained in this message
  uint32_t objects_data;
  // number of truncate regions piggybacked
  uint32_t truncate_counter;

  // for specialized array send
  uint32_t array_size;
  // for specialized array send
  uint64_t array_klass;
};

const uint32_t prefix = sizeof(metadata_header_t);


long send_graph_rdma(void* rdmaep, jobject object, const bool blocking, const int array_len){
  ActiveVerbsEP* ep = (ActiveVerbsEP*)rdmaep;
//  MeasurementsSend msend((uint64_t)rdmaep);
 
  JavaThread* THREAD = JavaThread::current();
  oop obj = JNIHandles::resolve(object);
  Handle initial_object(THREAD, obj);

  GrowableArray<uint8_t>* topin_nonblock = NULL; // for nonblocking call 
  if(!blocking){
    topin_nonblock = ep->get_nonblocking_handle();
  }
  

  /*if(ep->get_total_remote_heap_size( ) == 0){ // it means that we do not have memory at all
    log_debug(naos) ("[RdmaSender%p] No memory, Try to request memory from the remote side.\n",ep);
    if(!ep->has_pending_heap_request())  
      ep->send_heap_request(ep->balance_remote_buffers( )); 
  }*/
 
  if(array_len > 0){
     log_debug(naos) ("[RdmaSender] Iteretive send detected"); 
  }

  uint64_t array_klass = 0;
  uint32_t array_size= 0;

  if(array_len < 0){
    array_klass = (uint64_t)(void*)obj->klass();
    objArrayOop array = objArrayOop(obj);
    array_size = array->length();
    log_debug(naos) ("[RdmaSender] Array send detected"); 
  }

  ep->init_traversal(initial_object,array_len); 
  //ep->send_buf.Reset();
  
  log_debug(naos) ("[RdmaSender] rdmaep=%p object=%p type=%s\n", rdmaep, object, blocking ? "blocking" : "nonblocking");

  bool done = false;

  uint32_t total_datalength = 0;
  uint32_t total_metadatalength = 0;

  const uint32_t segmentsize = ep->default_remote_segment_size();
 // const uint32_t default_buffer_length = 4096 * 2; //8KiB. todo should be tuned
  const uint32_t low_traverse_size = NaosPipelineSize ;
  const uint32_t high_send_size = 2*1024*1024; //2 MiB. upper bound on the message size. TODO unused
  const uint32_t low_send_size = 4096; //4KiB, the message is at least 4KiB
  const uint32_t copy_threshold = 256; // in bytes. it decides whether Copy or send from the heap? 

  ShenandoahHeap* heap = ((ShenandoahHeap*)Universe::heap()); 

  uint32_t token = 100;
  //uint64_t wr_id_to_wait = 0;
  ep->flush(false); // it will help to make room in send buffer


  region_t send_buffer = ep->get_send_buffer(0);  // empty send buffer
  uint32_t offset = 0; // offset in send_buffer

  uint32_t truncate_metadata[64]; 
  uint32_t truncate_counter = 0; 
  
  while(!done){
    uint32_t actually_sent_bytes = 0;
    ep->reset();

   // msend.add_start_traversal();

    done = ep->send_op.doit(low_traverse_size,high_send_size);
   // msend.add_end_traversal();
    // 1. send data.      
    // note that I can send less data at the step for optimization

/*    uint32_t need_to_write = ep->sendtree.get_current_length();
    uint32_t upper_bound = (uint32_t)(need_to_write*(1.3));

    uint32_t total_remote_heap = ep->get_total_remote_heap_size( );
 
    if(upper_bound > total_remote_heap){

        uint32_t in_blocks = (upper_bound - total_remote_heap + segmentsize - 1) / segmentsize;
        log_debug(naos) ("[RdmaSender] Expect need for memory. Try to request %u \n",in_blocks);
        bool was_sent = ep->send_heap_request(in_blocks + 1);
        if(!was_sent){
          log_debug(naos) ("[RdmaSender] But it was not sent\n");
        }
    }*/

    QueueIterator<SendNode> it = ep->sendtree.get_sendlist_iterator();
 
    const SendNode *elem  = it.next();

    while(elem != NULL){
      uint32_t current_to_send_bytes = elem->length; 
      bool is_last_ever = false;

      // check whether we should copy to buffer
      if(current_to_send_bytes < copy_threshold){ // copy to buffer
        ep->flush(false); // it will help to make room in send buffer
        bool was_extended = ep->extend_send_buffer(send_buffer,offset + current_to_send_bytes);
        
        if(!was_extended && offset ==0){
          log_debug(naos) ("[RdmaSender] wrap around the send buffer. "); 
          do{
            ep->flush(false);
            send_buffer = ep->get_send_buffer(current_to_send_bytes); 
            log_debug(naos) ("[RdmaSender] spin as the send buffer is extrememly small."); 
          } while (send_buffer.length == 0); 
          was_extended = true;
        }

        if(!was_extended){
          log_debug(naos) ("[RdmaSender] cannot fit it in the buffer. Send later"); 
        } else{
          log_debug(naos) ("[RdmaSender] extend send_buffer with new len %u",send_buffer.length);
          //log_debug(naos) ("[RdmaSender] copy to buffer %u bytes, ",current_to_send_bytes);
          memcpy((void*)(send_buffer.vaddr + offset), (void*)elem->addr, current_to_send_bytes);
          offset+=current_to_send_bytes; 

          elem = it.next(); // go to the next

          is_last_ever = done && (elem == NULL);
          if(offset < low_send_size && !is_last_ever){
            continue; // we will send it later
          }
        }
      } 
    
      if(offset){ // do we send from buffer? 
        current_to_send_bytes = offset; // send whole buffer
      }

      region_t remote_region;
      bool have_region = false;
      // todo. in theory instead of spinning I could opt for traversing
      // for that I would need to reset sendlist partially and do not reset metadata
      while(!have_region){
        if(!ep->has_region()){ 
          if(!ep->has_pending_heap_request()){
              ep->send_heap_request(1);
              log_debug(naos) ("[RdmaSender] failed to predict need for memory. Try to request 1 \n");
          }
        
          ep->flush(false);
          if(ep->can_repost_receives() > 8){
            uint32_t to_commit = ep->repost_receives();
            log_debug(naos) ("[RdmaSender1] we can repost %u receives",to_commit);
           // ep->send_commit_receives(to_commit);
          } 
       //   ep->flush(false);
       //   printf("spin1");
        //  fflush(stdout);
          continue;
        }
        // we have a region, but lets check its size
        remote_region = ep->get_region(current_to_send_bytes);
        if(remote_region.length < current_to_send_bytes){
          log_debug(naos) ("[RdmaSender] Cannot fit %u in the current segment %u ",current_to_send_bytes,remote_region.length );
          // here we can send truncate size.
          assert(truncate_counter < 64, "truncate_counter is too big.");
          truncate_metadata[truncate_counter] = remote_region.length;
          truncate_counter++;
          
          log_debug(naos) ("[RdmaSender] appended a heap_truncate. it will be sent later ");
        } else { 
          have_region = true;
        }
      }
      assert(current_to_send_bytes == remote_region.length, "Buffers must be equal\n");
      
      actually_sent_bytes += current_to_send_bytes;
      if(offset){ // do we send from buf? 
        uint32_t lkey = send_buffer.rkey;
        uint64_t send_from = send_buffer.vaddr;

        log_debug(naos) ("[RdmaSender] Write from buffer %lx to %lx with len %u",send_from,remote_region.vaddr, remote_region.length);  
        ep->write_to_remote_heap_from_sendbuf((char*)send_from, remote_region.length, lkey, 
                                              remote_region, (uint64_t)(is_last_ever) * (uint64_t)topin_nonblock); // the last param to rememner wrid compl
        send_buffer.vaddr += offset;
        send_buffer.length -= offset;
        offset=0;
        continue; // next iteration
      } 

      uint64_t send_from=elem->addr;
      elem  = it.next();
      is_last_ever = done && (elem == NULL);

      if(ep->withodp){
          struct ibv_mr * mr = ep->get_odp_mr();
          uint32_t lkey = mr->lkey; 
          log_debug(naos) ("[RdmaSender] Write from heap %lx to %lx with len %u",
            send_from,remote_region.vaddr, current_to_send_bytes);  
          ep->write_to_remote_heap((char*)send_from, current_to_send_bytes, lkey, 
                                    remote_region,(uint64_t)(is_last_ever) * (uint64_t)topin_nonblock);  // the last param to rememner wrid compl
          
          if(!blocking){ // if it is not blocking
              size_t index = heap->heap_region_index_containing((void*)send_from);
              int ind = (int)index; // Warning. narrowing. 
              if(index > 1024){
                printf("Ind to big for efficiency val: %lu\n",index);
                fflush(stdout);
              }
              topin_nonblock->at_put_grow(ind+4,(uint8_t)1,(uint8_t)0); // put one, for new put zero.
          }
      } else {
 
        uint32_t need_to_send = current_to_send_bytes;
        // we loop to cover the case with spanning over multiple HeapRegions
        do{
          size_t index = heap->heap_region_index_containing((void*)send_from);
          struct ibv_mr * mr = ep->get_mr_for_region_index(index);
          //register if region is not known
          if(mr == NULL){
            mr = get_mr_for_region(ep, heap, index);
            log_debug(naos) ("[RdmaSender] registered %lx with len %lu and key %u \n",
              (uint64_t)mr->addr, mr->length, mr->lkey );
            ep->set_mr_for_region_index(index,mr);
          }

          if(!blocking){ // if it is not blocking
              int ind = (int)index; // Warning. narrowing. 
              if(index > 1024){
                printf("Ind to big for efficiency val: %lu\n",index);
                fflush(stdout);
              }
              topin_nonblock->at_put_grow(ind+4,(uint8_t)1,(uint8_t)0); // put one, for new put zero.
          }
          
          // check if the element spans over multiple regions with different lkeys. 
          uint64_t end_of_mr  = ((uint64_t)mr->addr) + mr->length;
          if(send_from + need_to_send > end_of_mr ){
            log_debug(naos) ("[RdmaSender] span over many regions. \n");
            current_to_send_bytes = (end_of_mr - send_from);
            // truncate the send element. we will send the rest later. 
            log_debug(naos) ("[RdmaSender] span over many regions. trim the first part %u \n",current_to_send_bytes);
          } else {
            // we send all 
            current_to_send_bytes = need_to_send;
          }
          
          uint32_t lkey = mr->lkey; 
          log_debug(naos) ("[RdmaSender] Write from heap %lx to %lx with len %u",
            send_from,remote_region.vaddr, current_to_send_bytes);  
     
          ep->write_to_remote_heap((char*)send_from, current_to_send_bytes, lkey, remote_region, (uint64_t)(is_last_ever) * (uint64_t)topin_nonblock);
          need_to_send-=current_to_send_bytes;
          send_from+=current_to_send_bytes;
          remote_region.vaddr += current_to_send_bytes;
        } while(need_to_send!=0);
      }

    }
    

    // 2. send metadata
    uint32_t bytes_ooffset_list = ep->metadata.size_bytes();
    const uint32_t truncate_size = truncate_counter*sizeof(uint32_t);

    uint32_t total_metadata_size = bytes_ooffset_list + prefix + truncate_size;
    char* metadata_buf = ep->get_send_metadata_buffer(total_metadata_size);
    assert(metadata_buf!=NULL, "no memory to send metadata.... TODO");
    
    {
      metadata_header_t* header = (metadata_header_t*)metadata_buf;
      header->bytes_data = actually_sent_bytes;
      header->objects_data = ep->sendtree.get_current_objects();
      header->truncate_counter = truncate_counter;
      header->array_size = array_size;
      header->array_klass = array_klass; 
      log_debug(naos) ("[RdmaSender] send ooffset_list: %u bytes", bytes_ooffset_list);
      log_debug(naos) ("[RdmaSender] Send metadata[%u] with: len %u with objects %u \n",token,header->bytes_data,header->objects_data);  
    }
 
    
    if(truncate_size) memcpy(metadata_buf+prefix,truncate_metadata, truncate_size);
    // that is ok as metadata is small
    if(bytes_ooffset_list) memcpy(metadata_buf+ prefix + truncate_size,ep->metadata.metadata(), bytes_ooffset_list);


    ep->write_metadata(metadata_buf,total_metadata_size, token++, done);
 
    if(ep->can_repost_receives() > 8 ){ //|| done
      uint32_t to_commit = ep->repost_receives();
      log_debug(naos) ("[RdmaSender2] we can repost %u receives",to_commit);
     // ep->send_commit_receives(to_commit);
    }
 
    ep->flush(done); // force to push it. it will spin inside

    total_datalength+= actually_sent_bytes; 
    total_metadatalength+= bytes_ooffset_list;
    truncate_counter = 0; 
  }

  assert(total_datalength == ep->sendtree.get_total_length(), "we sent wrong number of bytes" );

 /* uint32_t total_remote_heap = ep->get_total_remote_heap_size( );
  uint32_t in_blocks = (total_remote_heap + segmentsize - 1) / segmentsize;
  uint32_t expect_to_have_remote = ep->balance_remote_buffers( );
  if(expect_to_have_remote > in_blocks){
      ep->send_heap_request(expect_to_have_remote - in_blocks);
  }*/

  if(blocking){
    while(ep->needtowait_send_completion()){ // we do not spin if it is not blocking
      ep->flush(false);
    } 
  } else {
    log_info(naos) ("[RdmaSender] Pinning affected regions of array %p.",topin_nonblock);
    uint32_t len = topin_nonblock->length()-4; // 4 = sizeof(uint32_t)
    uint8_t* data = topin_nonblock->adr_at(4);
    for(uint32_t i=0; i<len; i++){
      if(data[i]){
        log_debug(naos) ("\tPinning region %u\n",i);
        heap->get_region(i)->record_pin(); 
      }
    }
  }

  return (long)(void*)topin_nonblock;
}


void wait_rdma(void* rdmaep, long handle){
  ActiveVerbsEP* ep = (ActiveVerbsEP*)rdmaep;
  GrowableArray<uint8_t>* array = ( GrowableArray<uint8_t>* )(void*)handle; // for nonblocking call 
  uint32_t* val = (uint32_t*)array->adr_at(0);
  while(*val != 0){
    ep->process_completions();
  }
}


bool test_rdma(void* rdmaep, long handle){
  ActiveVerbsEP* ep = (ActiveVerbsEP*)rdmaep;
  GrowableArray<uint8_t>* array = ( GrowableArray<uint8_t>* )(void*)handle; // for nonblocking call 
  uint32_t* val = (uint32_t*)array->adr_at(0);
  return (*val == 0);
}
 
 
jobject receive_graph_rdma(void* rdmaep){
  JavaThread* THREAD = JavaThread::current();
  HandleMark hm(THREAD);

//  MeasurementsRecv mrecv((uint64_t)rdmaep);
  PassiveVerbsEP* ep = (PassiveVerbsEP*)rdmaep;
   
  bool done = false;
  uint32_t total_data_bytes = 0;
  uint32_t total_metadata_bytes = 0;
 
 
  ep->metadata.hard_reset();
  ep->recv_op.hard_reset();


  // for specialized array send
  objArrayOop array = NULL;
  uint32_t array_counter = 0; 
  int array_total_length = 0; 

  while(!done){
    while(!ep->is_readable()){
      ep->process_completions();

      if(ep->can_repost_receives() > 16){
        uint32_t to_commit = ep->repost_receives();
        log_debug(naos) ("[RdmaRecv1] we can repost %u receives",to_commit);
        ep->send_commit_receives(to_commit);
      }

      if(ep->has_heap_request()){
        log_debug(naos) ("[RdmaRecv]  -----Send new heap\n");
        int nums =  ep->has_heap_request();
        region_t reg[64]; 
   //     assert(nums <= 64, "too many buffer requested");
        for(int i =0; i < nums; i++){
          uint64_t heap_buffer = 0;
          jobject obj = allocate_and_pin_buffer(ep->default_segment_size(), &heap_buffer);

          struct ibv_mr * mr = ep->reg_mem((char*)heap_buffer, ep->default_segment_size() );
          ep->push_receive_heap_region(mr,obj);
          reg[i].vaddr = heap_buffer;
          reg[i].rkey = mr->rkey;
          reg[i].length=ep->default_segment_size(); 
        }
        ep->send_heap_reply(&reg[0],nums);
      }
 //     log_debug(naos) ("[RdmaRecv] spin is not readable" );      
    }
  //  mrecv.add_start_recv();
    
  //  log_debug(naos) ("[RdmaRecv] is readable" );    
  //  assert(ep->is_readable(), "must be readable");
    uint32_t bytes_data = 0;
    if(ep->has_cached_receive()){
      cached_receive_t receive = ep->pop_cached_receive();
      bytes_data = receive.bytes_data;
      done = receive.last; 
    } else {
      receive_t receive = ep->pop_receive();
   
      done = receive.last; 

      char* metadata_buf = receive.metadata_buf;
      metadata_header_t* header = (metadata_header_t*)metadata_buf;

      uint32_t token = receive.token;

 


      bytes_data = header->bytes_data ; // need. must be correct
      uint32_t total_objects = header->objects_data;  // unused. can be used fo debugging
      uint32_t truncate_counter = header->truncate_counter;  // truncate my heap

      if(header->array_klass != 0){
        array_total_length = (int)header->array_size ;
        Klass* local_klass = (Klass*)(void*)ep->recv_op.resolve_klass(header->array_klass);
        log_debug(naos) ("[RdmaRecv] array[%d] receive detected klass %p", array_total_length,local_klass);
        //array = oopFactory::new_objArray(local_klass, array_total_length, THREAD); // incorrect. 
        array = ((ObjArrayKlass*)local_klass)->allocate(array_total_length, THREAD);
      }
 
      for(uint32_t i=0; i < truncate_counter; i ++ ){
        ep->truncate_my_heap( ((uint32_t*)(metadata_buf+prefix))[i]   )  ;
      }

      //log_debug(naos) ("[RdmaRecv] Get a receive: token: %u; medatalen: %u ",token, receive.metadata_len );
      //log_debug(naos) ("[RdmaRecv] Buffer %p; Received %u bytes and %u objects ",metadata_buf,bytes_data,total_objects );
   
      // copy metadata
      uint32_t metadata_bytes =  receive.metadata_len - prefix - truncate_counter*sizeof(uint32_t);
      uint8_t* metadata_buffer = ep->metadata.add_metadata(metadata_bytes);
      // memcpy is fast, as it is always small 
      memcpy(metadata_buffer, metadata_buf+prefix+truncate_counter*sizeof(uint32_t), metadata_bytes);
   
      ep->total_real_metadata_bytes+=receive.metadata_len;
      // it is not neccessary to commit each iteration.
      if(ep->total_real_metadata_bytes > 4096){
        uint32_t offset_to_commit = ep->get_offset_to_commit(metadata_buf + receive.metadata_len );
       // log_debug(naos) ("offset_to_commit %u ",offset_to_commit);
        ep->send_commit_offset(offset_to_commit);
        ep->total_real_metadata_bytes = 0;
      }
      total_metadata_bytes+=metadata_bytes;
    }

   // mrecv.add_end_recv();

    uint32_t processed_bytes = 0; 
    region_t myregion = ep->get_my_heap(bytes_data);
    while(processed_bytes < bytes_data){
      
      log_debug(naos) ("[RdmaRecv] do region %lx with len %u ",myregion.vaddr, myregion.length);    
      if(myregion.length == 0){
        myregion = ep->get_my_heap(bytes_data - processed_bytes);
        continue;
      }

      range_t recv_list(myregion.vaddr,myregion.length);
      uint32_t cur_processed = (array==NULL) ?  ep->recv_op.do_region(recv_list) :
                                              ep->recv_op.do_region_array(recv_list, &array_counter, array); // fast path for array


      processed_bytes += cur_processed; 

      log_debug(naos) ("[RdmaRecv] cur_processed %u, stack %s \n",cur_processed, ep->recv_op.is_empty() ? "is empty" : "still has elements");    


      if(!ep->recv_op.is_empty()){
        log_debug(naos) ("[RdmaRecv] go to processing next heap region ");  
        if(bytes_data - processed_bytes > 0){ // konst: this "if" seems to be redundant
          myregion = ep->get_my_heap(bytes_data - processed_bytes);  
        }
        continue;
      } 

      // recv_op is empty now
      if(processed_bytes!=bytes_data){
        log_debug(naos) ("[RdmaRecv] iterative or array receive detected "); 
        myregion.vaddr  +=   cur_processed;
        myregion.length -=   cur_processed;
 
        if(array != NULL){ 
          // It is a slow path. Should never been used with fast path.
          log_debug(naos) ("[RdmaRecv] Warning. we receive array[%u] ",array_counter);
          array->obj_at_put(array_counter++, (oop)ep->recv_op.finalize());

          //ep->recvmetadata.hard_reset();
          ep->recv_op.hard_reset();
        } else {
          ep->cache_iterative_recv(bytes_data-processed_bytes, done, myregion);
          log_debug(naos) ("[RdmaRecv] iterative receive break loops ");
          done = true; // to break outer while loop
          break; // iterative receive
        }
      }

    }

    total_data_bytes+=processed_bytes;
    

    if(ep->can_repost_receives() > 16 ){
      uint32_t to_commit = ep->repost_receives();
      //   log_debug(naos) ("[RdmaRecv2] we can repost %u receives",to_commit);
      ep->send_commit_receives(to_commit);
    }
 
  //  mrecv.add_end_traversal();
  //  mrecv.add_data_bytes(bytes_data);
  //  mrecv.add_metadata_bytes(metadata_bytes);
  }
 
 // log_debug(naos) ("[RdmaRecv] references are fixed \n");
 

  if(array != NULL){
//    if(array_counter < array_total_length) { 
        // It is a slow path. Should never been used with fast path.
      //log_debug(naos) ("[RdmaRecv] Warning. last element of array[%u] total len: %d",array_counter,array_total_length);
      //array->obj_at_put(array_counter++,  (oop)ep->recv_op.finalize());
  //  }
    return JNIHandles::make_local(THREAD, array);
  } else{

    return JNIHandles::make_local(THREAD, (oop)ep->recv_op.finalize());
  }
 
}


// TODO. crete dor passive active
void close_rdma_ep(void* rdmaep){
  //VerbsEP* ep = (VerbsEP*)rdmaep;

  log_debug(naos) ("close rdma \n"); 
  // TODO proper close
}
