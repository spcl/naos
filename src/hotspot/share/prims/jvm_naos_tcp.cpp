/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Implementation of TCP Naos
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */

#include <runtime/biasedLocking.hpp>
#include "jvm_naos_tcp.hpp"
#include "jvm_naos.hpp"
#include "jvm_naos_klass_service.hpp"
#include "oops/typeArrayOop.inline.hpp"
#include "runtime/vmThread.hpp"
#include "runtime/vm_operations.hpp"
#include "runtime/jniHandles.inline.hpp"
#include "runtime/globals.hpp"
#include "memory/oopFactory.hpp"
 

// TODO - rbruno - properly document this constants
const uint32_t default_buffer_length = 4096 * 2; //8KiB. todo should be tuned
const uint32_t low_send_size = NaosPipelineSize; //4KiB, the message is at lest
const uint32_t high_send_size = 2*1024*1024; //2 MiB. upper bound on the message size
const uint32_t copy_threshold = 256; // in bytes

struct metadata_header_t {
  // address of the klass service
  uint64_t klass_service_address;
  // just a boolea to indicate if this is the last piece of data
  uint32_t done;
  // number of metadata bytes
  uint32_t bytes_offset_list;
  // number of data bytes
  uint32_t bytes_data;
  // number of objects contained in this message
  uint32_t objects_data;

  // nspecialized array send
  uint64_t array_klass;
};

inline size_t read_payload(int fd, uint8_t* buffer, size_t size) {
  size_t curr = 0;
  while (curr < size) {
    curr += read(fd, buffer + curr,  size - curr);
  }
  return size;
}
 
inline size_t write_payload(int fd, uint8_t* buffer, size_t size) {
  size_t curr = 0;
  while (curr < size) {
    curr += write(fd, buffer + curr,  size - curr);
  }
  return curr;
}


inline void print_metadata_header(int fd, struct metadata_header_t* metadata_header) {
  log_trace(naos) ("[naos-tcp] fd=%d metadata: klass_service.{ip,port} = {%u,%u}; done = %u ooffset_list= %u bytes; data= %u bytes; %u objects",
                   fd,
                   (uint32_t)(metadata_header->klass_service_address >> 32),
                   (uint16_t)(metadata_header->klass_service_address & 0xFFFF),
                   metadata_header->done,
                   metadata_header->bytes_offset_list,
                   metadata_header->bytes_data,
                   metadata_header->objects_data);
}


class NaosTcpEP: public CHeapObj<mtInternal>  {
public:
  const int fd;

  SendTree sendtree;
  OffsetSendMetadata metadata; 
  TraverseObject send_op;    // traverse graph and build send and traverse lists
 
  OffsetReceiveMetadata recvmetadata;
  ReceiveGraph recv_op;

  struct metadata_header_t received_metadata_header; // for receiving 
  typeArrayOop protected_mem; // for receiving 

  objArrayOop array;
  uint32_t array_counter; // 0 

  uint32_t processed;

  uint8_t * buffer; // buffer for batching small writes

  uint64_t klass_service_address; // my local address


  uint32_t total_datalength;
  uint32_t total_metadatalength;

  uint32_t array_total_length;


  NaosTcpEP(int fd): fd(fd), sendtree(), metadata(sizeof(struct metadata_header_t)),
  send_op(metadata, sendtree), recvmetadata(), recv_op(fd, recvmetadata), protected_mem(NULL), array(NULL), array_counter(0), processed(0)
  {
    buffer = (uint8_t*)os::malloc(default_buffer_length, mtInternal);
    klass_service_address = get_or_create_klass_service_address(0, fd);  

    memset(&received_metadata_header,0,sizeof(received_metadata_header));
  };


  inline void init_traversal(Handle initobj, int array_len){
    total_datalength = 0;
    total_metadatalength = 0;
    send_op.init(initobj,abs(array_len));
    sendtree.hard_reset();
  }
  
  inline void init_recv( ){
    recvmetadata.hard_reset();
    recv_op.hard_reset();
    uint64_t klass_service_address = received_metadata_header.klass_service_address;
    uint32_t sender_encoded_ip = (uint32_t)(klass_service_address >> 32);
    uint16_t sender_klass_service_port = (uint16_t)(klass_service_address & 0xFFFF);
    log_debug(naos) ("[naos-tcp] fd=%d  sender_ip=%X sender_klass_service_port=%d", fd, sender_encoded_ip,sender_klass_service_port);
    recv_op.set_resolver(sender_encoded_ip, sender_klass_service_port);
  }

  inline void reset(){
    metadata.reset();
    sendtree.reset();
  }

  // for sending
  inline void write_header(MeasurementsSend &msend, bool done, uint32_t array_len, uint64_t array_klass){
    // Warning: we don't save a ptr to the header because it might move due to the growable array used to store it.
  
    struct metadata_header_t* metadata_header = (struct metadata_header_t*)metadata.metadata();
    metadata_header->klass_service_address = klass_service_address;  
    metadata_header->done = (uint32_t)done;
    metadata_header->bytes_offset_list = metadata.offsets_size_bytes();
    metadata_header->bytes_data = sendtree.get_current_length();    // number of bytes

    metadata_header->objects_data = (array_klass == 0) ? sendtree.get_current_objects() : array_len ;  
    metadata_header->array_klass = array_klass;

    print_metadata_header(fd, metadata_header);

    msend.add_data_bytes(metadata_header->bytes_data);
    msend.add_metadata_bytes(metadata_header->bytes_offset_list);

    total_datalength += metadata_header->bytes_data;
    total_metadatalength += metadata_header->bytes_offset_list;


    int ret = write_payload(fd, (uint8_t*)metadata_header, metadata.size_bytes());
    assert((size_t)ret == metadata.size_bytes(), "sent bytes don't match metadata bytes");
  }

  inline bool doit(){
    return send_op.doit(NaosPipelineSize,high_send_size);
  }

  inline QueueIterator<SendNode> get_sendlist_iterator(){
    return sendtree.get_sendlist_iterator();
  }


  inline bool has_cached_receive(){
    return (processed != 0) && processed != received_metadata_header.bytes_data; 
  }

  inline uint8_t* get_heap_buffer_and_read_data(JavaThread* THREAD){ // todo
    if(this->protected_mem==NULL){ // normal receive. it is used for iterative receive
      processed = 0;
      uint32_t metadata_bytes = received_metadata_header.bytes_offset_list;
      uint32_t bytes_data = received_metadata_header.bytes_data;
      uint32_t total_objects = received_metadata_header.objects_data;


      this->protected_mem = TypeArrayKlass::cast(Universe::byteArrayKlassObj())->allocate_common(bytes_data, false, THREAD);  
      uint8_t* heap_buffer = (uint8_t*)(this->protected_mem)->byte_at_addr(0);
          // read metadata
      this->read_metadata(metadata_bytes);
      log_debug(naos) ("[naos-tcp] fd=%d receive ooffset_list: %u bytes ", fd, metadata_bytes );
     
      // read data
      size_t ret = read_payload(fd, heap_buffer, bytes_data);
      assert((size_t)ret == bytes_data, "failed to read data");

      uint64_t array_klass = received_metadata_header.array_klass;
      if(array_klass != 0){
        array_counter = 0;
        array_total_length = (int)total_objects;

        Klass* local_klass = (Klass*)(void*)recv_op.resolve_klass(array_klass);
        log_debug(naos) ("[naos-tcp] fd=%d array[%d] receive detected klass %p, %s", fd,array_total_length,local_klass, local_klass->signature_name());
        //array = oopFactory::new_objArray(local_klass, length, THREAD); // it is incorrect. Otherwise we get array of arrays
        array = ((ObjArrayKlass*)local_klass)->allocate(array_total_length, THREAD);

      } 

      log_debug(naos) ("[naos-tcp] fd=%d receive data: %u bytes", fd, bytes_data);
    } else{
      log_debug(naos) ("[naos-tcp] fd=%d iterative receive. continue fixing", fd);
    }

    return (uint8_t*)(this->protected_mem)->byte_at_addr(processed);;
  }

  inline uint32_t get_bytes_data(){
    return received_metadata_header.bytes_data - processed;
  }

  inline void read_metadata(uint32_t metadata_bytes){

    if(metadata_bytes){
      uint8_t* metadata_buffer = recvmetadata.add_metadata(metadata_bytes);
      int ret = read_payload(fd, metadata_buffer, metadata_bytes);
      assert((size_t)ret == metadata_bytes, "failed to read ooffset_list list");
    }
  }

  inline uint32_t do_region(range_t &recv_list){
    uint32_t cur_processed = recv_op.do_region(recv_list); // in segment code it is called in the loop
    processed+=cur_processed;
    return cur_processed;
  }
  
  // it is for fast path of array recovery
  inline uint32_t do_region_array(range_t &recv_list){
    uint32_t cur_processed = recv_op.do_region_array(recv_list, &(this->array_counter), this->array); // in segment code it is called in the loop
    processed+=cur_processed;
    return cur_processed; 
  }

  

  inline char* finalize(){
    return recv_op.finalize();
  }

 
  inline void end_traversal( ){
    log_debug(naos) ("[naos-tcp] fd=%d send data: %u bytes, metadata %u", fd, total_datalength,total_metadatalength );
  }

  ~NaosTcpEP(){
    os::free(buffer);
  }

};

void*  create_naos_tcp(int fd){
  NaosTcpEP* ep = new NaosTcpEP(fd);
  return  (void*)ep;
}


void test_f11(jobject object){
  JavaThread* THREAD = JavaThread::current();
  MutexLocker ml(Heap_lock);  
 
  oop obj = JNIHandles::resolve(object);
  Handle initial_object(THREAD, obj);
}
 
void send_naos_tcp(uint64_t naostcp_ptr, jobject object, const int array_len) {
  int ret;
  NaosTcpEP* ep = (NaosTcpEP*)(void*)naostcp_ptr;
  int fd = ep->fd;
 
  MeasurementsSend msend = MeasurementsSend(fd);
  JavaThread* THREAD = JavaThread::current();
  MutexLocker ml(Heap_lock); // konst: do we need it? 
 
  oop obj = JNIHandles::resolve(object);
  Handle initial_object(THREAD, obj);

  log_debug(naos) ("[naos-tcp] fd=%d object=%p pipeline_size = %d humongous_threshold_bytes=%zu", fd, object, 
                    NaosPipelineSize, ShenandoahHeapRegion::humongous_threshold_bytes()); // konst: TCP **could** work with other GC. 


 
  uint64_t array_klass = 0; 
  uint32_t size = 0; // total array size in arraySend

  if(array_len < 0){
    array_klass = (uint64_t)(void*)obj->klass(); // obj->is_objArray()
    objArrayOop array = objArrayOop(obj);
    size = array->length();
    log_debug(naos) ("[naos-tcp] specialized send array is detected %p, len: %u, name: %s",obj->klass(),size,obj->klass()->signature_name());
  }


  ep->init_traversal(initial_object,array_len); // array_len helps to send only objects. when array_len = 0. it is normal send. 
  uint8_t * buffer = ep->buffer;  // copy buffer for small writes

  bool done = false;

  while(!done){
    ep->reset();

    msend.add_start_traversal();
    done = ep->doit(); // it does a single traversal iteration
    msend.add_end_traversal();

    ep->write_header(msend, done, size, array_klass); 
    array_klass = 0; // write array klass only in the first part
 
    uint32_t offset_counter = 0;
    QueueIterator<SendNode> it = ep->get_sendlist_iterator();
    const SendNode *e  = it.next();
    while(e != NULL){
      if(e->length > copy_threshold){ // send directly from heap
        if(offset_counter) { // do we have data buffered to send?
          ret = write_payload(fd, buffer, offset_counter);
          assert((size_t)ret == offset_counter, "sent bytes don't match sendlist bytes");
          offset_counter = 0;
        } 
        ret = write_payload(fd, (uint8_t *)(void*)e->addr, e->length);
        assert((size_t)ret == e->length, "sent bytes don't match sendlist bytes");
      } else { // copy to buffer
        if(offset_counter + e->length >  default_buffer_length){ // can we fit in the buffer? 
          ret = write_payload(fd, buffer, offset_counter);
          assert((size_t)ret == offset_counter, "sent bytes don't match sendlist bytes");
          offset_counter = 0;
        }
        memcpy(buffer+offset_counter,(void*)e->addr,e->length);
        offset_counter+=e->length;
      }
      e = it.next();
    }

    if(offset_counter){ // do we have data buffered to send?
      ret = write_payload(fd, buffer, offset_counter);
      assert((size_t)ret == offset_counter, "sent bytes don't match sendlist bytes");
    }

    log_debug(naos) ("[naos-tcp] fd=%d send data %s", fd, done ? "last" : "middle");

    msend.add_end_send();
  }

  ep->end_traversal();
}



jobject receive_naos_tcp(uint64_t naostcp_ptr, uint64_t was_iterable_ptr) {
  int ret;
  int *was_iterable = (int*)(void*)was_iterable_ptr;
  if(was_iterable_ptr != 0) *was_iterable = 0;

  NaosTcpEP* ep = (NaosTcpEP*)(void*)naostcp_ptr;
  int fd = ep->fd;

//  MeasurementsRecv mrecv(fd);

  JavaThread* THREAD = JavaThread::current();
  size_t bytes_data = 0;
  size_t metadata_bytes = 0;
  uint32_t total_objects = 0;

  bool continue_fixing_iterable = ep->has_cached_receive(); 

  if(!continue_fixing_iterable){
    log_debug(naos) ("[naos-tcp] try to read metadata ");
    ret = read(fd, &(ep->received_metadata_header), sizeof(struct metadata_header_t));
    if (ret == 0){
      log_debug(naos) ("[naos-tcp] fd=%d is close_tcp_fd", fd );
      return NULL;
    }

    assert( ret == sizeof(struct metadata_header_t), "failed to read metadata header" );
    print_metadata_header(fd, &(ep->received_metadata_header));
  }

  ep->init_recv();
 
  bool done = false;
  uint32_t total_processed = 0;
  uint32_t total_data_bytes = 0;
  uint32_t total_metadata_bytes = 0;

  while(!done){
   // mrecv.add_start_recv();

    done = (ep->received_metadata_header.done != 0);
    metadata_bytes = ep->received_metadata_header.bytes_offset_list;
 
    // allocate or get existing on-heap buffer 
    uint8_t* heap_buffer = ep->get_heap_buffer_and_read_data(THREAD); 
    bytes_data = ep->get_bytes_data(); 
    //mrecv.add_end_recv();
 
    range_t recv_list((uint64_t)heap_buffer,(uint32_t)bytes_data);


 
    uint32_t processed = ep->array == NULL ?  ep->do_region(recv_list) : 
                                              ep->do_region_array(recv_list); // fast path for array


   // mrecv.add_end_traversal();
    log_debug(naos) ("[naos-tcp] fd=%d receive data: %zu bytes, processed %u, %s", fd, bytes_data, processed,  done ?  "last" : "middle");
 
    total_processed+=processed;
    total_data_bytes+=bytes_data;
    total_metadata_bytes+=metadata_bytes;
    // unpin buffer (we need to make sure it does not move while we receive data)

   // mrecv.add_data_bytes(processed);
   // mrecv.add_metadata_bytes(metadata_bytes);
    
    bool should_break_because_of_iterable = (processed != bytes_data);

    if(false && should_break_because_of_iterable){
      if(ep->array != NULL){
        log_debug(naos) ("[naos-tcp] Warning. we receive array[%u] ",ep->array_counter);
        ep->array->obj_at_put(ep->array_counter++, (oop)ep->finalize());

        //ep->recvmetadata.hard_reset();
        ep->recv_op.hard_reset();
        done=false;
        continue;
      }
      log_debug(naos) ("[naos-tcp] Warning. we received iterable array ");
      if(was_iterable_ptr != 0) *was_iterable = 1;
      if(metadata_bytes!=0){
        printf("Critical error. Iterable cannot have back-references!\n");
        fflush(stdout);
      }
      break;
    } 
  
    ep->protected_mem->set_length(0);  // that is the memory where we received current slice ob objects
    ep->protected_mem = NULL;
    ep->processed = 0; // it is offset in that memory
    
    if(!done){
      ret = read(fd, &(ep->received_metadata_header), sizeof(struct metadata_header_t));
      assert( ret == sizeof(struct metadata_header_t), "failed to read metadata header" );
      print_metadata_header(fd, &(ep->received_metadata_header));
    }
  }


  log_debug(naos) ("[naos-tcp] fd=%d received data: %u bytes, metadata %u, processed %u", fd, total_data_bytes,total_metadata_bytes,total_processed );
  if(ep->has_cached_receive()){
      log_debug(naos) ("[naos-tcp] Warning. we still have some data to fix later ");
  }
 

  if(ep->array != NULL){
  //  if(ep->array_counter < ep->array_total_length){
    // slow path. is depricated
  //    log_debug(naos) ("[receivegraph] Warning. last element of array[%u] ",ep->array_counter);
   //   ep->array->obj_at_put(ep->array_counter++,  (oop)ep->finalize());
  //  }

    oop toret = ep->array;
    ep->array = NULL;
    return JNIHandles::make_local(THREAD, toret);
  } else{

    return JNIHandles::make_local(THREAD, (oop)ep->finalize());
  }
}

// konst: here we can manage connection close
void close_tcp_fd(uint64_t naostcp_ptr) {
  NaosTcpEP* ep = (NaosTcpEP*)(void*)naostcp_ptr;
  int fd = ep->fd;
  log_debug(naos) ("[close_tcp_fd] fd = %d", fd);
  clear_state_fd(fd);

  delete ep;
  // TODO - rbruno - we may have to here for this on the sender side before closing our klass service thread.
}
 

