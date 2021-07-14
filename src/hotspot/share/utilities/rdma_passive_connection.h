/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * A passive side of an RDMA connection
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */

#ifndef JDK11_RDMA_RDMA_PASSIVE_CONNECTION_H
#define JDK11_RDMA_RDMA_PASSIVE_CONNECTION_H


#include <poll.h>


#include <stdio.h>
#include <stdlib.h>
#include <rdma/rdma_cma.h>
//#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>
//#include <list>

//#include "jvm_rdma.h"
#include "rdma_helpers.h"
#include "rdma_protocol.h"
#include "queue.hpp"
#include "growableArray.hpp"

#include "jni.h"
#include "logging/log.hpp"


#include "../prims/jvm_naos.hpp"

#define UNSIGNAL_THRESHOLD (16)
#define MAX_POST_RECV (32)
 

#define MIN(a,b) (((a)<(b))?(a):(b))

 
class PassiveVerbsEP: public CHeapObj<mtInternal>{ 
  struct rdma_cm_id  *const id; //

  struct ibv_qp *const qp;
  const int fd;
  struct ibv_pd *const pd;
  const uint32_t max_inline_data;
  const uint32_t max_send_size;
  const uint32_t max_recv_size;

  struct ibv_cq* const cq;
 
  SendBuffers send_metadata_buf;

  uint32_t can_be_dereged;

  QueueImpl<receive_t, ResourceObj::C_HEAP, mtInternal> receives; // should be queue
  QueueImpl<uint32_t, ResourceObj::C_HEAP, mtInternal>  received_ints; // should be queue

  uint32_t can_post_recv; // how many receives we can post and acknowledge


  uint32_t can_send;          // the number of sends we can send
  uint32_t unsignaled_sends; // how many sends was sent un-signaled after the last signaled send. 
 
 
  int has_incoming_heap_request; 

 // remote klass recovery
  uint32_t klass_service_ip;
  uint16_t klass_service_port;

  QueueImpl<region_t, ResourceObj::C_HEAP, mtInternal>  my_heap; // my heap regions   // should be queue
  QueueImpl<uint32_t, ResourceObj::C_HEAP, mtInternal>  finished_heap_writes; // should be queue


  
  struct heap_desc{
    struct ibv_mr * mr;
    jobject obj; // jobject
  };

  QueueImpl<heap_desc, ResourceObj::C_HEAP, mtInternal>  my_heap_mr;  


  uint32_t to_truncate_ind;
  uint32_t current_offset_in_head_region; // current offset in the current local heap region


  RecvBuffers recv_metadata_buf;

  
  const uint32_t segment_size;
  const uint32_t balance;

  RemoteBufferTracker remote_buffer;   // remote metadata receive buffer
 
  struct ibv_recv_wr rwr[MAX_POST_RECV];

  const bool withodp;
  struct ibv_mr * odpmr;
public:
  uint32_t total_real_metadata_bytes;


  // cached structures
  OffsetReceiveMetadata metadata;
  ReceiveGraph recv_op;

  cached_receive_t cached_receive;
  region_t cached_region;

  PassiveVerbsEP(struct rdma_cm_id  *id, uint32_t max_inline_data, uint32_t max_send_size, uint32_t max_recv_size, uint32_t buffersize, uint32_t nums, bool withodp) :
          id(id), qp(id->qp), fd(id->channel->fd), pd(id->qp->pd), max_inline_data(0), max_send_size(max_send_size),
          max_recv_size(max_recv_size), cq(id->qp->send_cq), send_metadata_buf(1024*1024,pd),
          can_post_recv(0),
          can_send(max_send_size), unsignaled_sends(0), 
          has_incoming_heap_request(0), current_offset_in_head_region(0),
          recv_metadata_buf(2*1024*1024,pd),
          segment_size(buffersize), balance(nums),withodp(withodp),odpmr(NULL),total_real_metadata_bytes(0),
          metadata(),recv_op(fd,metadata)
  {
      can_be_dereged = 0;
      to_truncate_ind = 0;
      cached_receive.bytes_data = 0;
      cached_region.vaddr = 0;
      cached_region.rkey = 0;
  }

  ~PassiveVerbsEP() {
    // TODO 
  }

  // the initial exchange happens in 2 steps. We post receives, and in second step we exchange messages and install the rest
  void prepost_recv_for_exchange(){
    struct ibv_sge sge;

    sge.addr = (uint64_t)send_metadata_buf.get_baseaddr();
    sge.lkey = send_metadata_buf.get_lkey();
    sge.length = 2048;

    struct ibv_recv_wr wr, *bad;
    wr.wr_id = 0;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    int ret = ibv_post_recv(this->qp, &wr, &bad);
    assert(ret == 0, "post recv failed");


    for(uint32_t i= 0; i < MAX_POST_RECV-1; i++){
      rwr[i].wr_id = 0;
      rwr[i].next = &rwr[i+1];
      rwr[i].sg_list = NULL;
      rwr[i].num_sge = 0;
    }
    rwr[MAX_POST_RECV-1].wr_id = 0;
    rwr[MAX_POST_RECV-1].next = NULL;
    rwr[MAX_POST_RECV-1].sg_list = NULL;
    rwr[MAX_POST_RECV-1].num_sge = 0;

    post_empty_recv(max_recv_size-1);
    
  }

  inline void post_empty_recv(uint32_t to_post){
    struct ibv_recv_wr  *bad;
    
    int first_post = to_post % MAX_POST_RECV;
    if(first_post > 0){
      rwr[first_post-1].next=NULL;
      ibv_post_recv(this->qp, &rwr[0], &bad);  
      rwr[first_post-1].next=&rwr[first_post];
    }

    for(uint32_t i=0; i < to_post /MAX_POST_RECV; i++ ){
      ibv_post_recv(this->qp, &rwr[0], &bad);  
    }
    return;
  }
  
  uint32_t default_segment_size() const {
    return segment_size; // 2 MiB 2*1024*1024
  }
 
  void push_receive_heap_region(struct ibv_mr * mr, jobject obj){
    heap_desc elem = {.mr = mr, .obj = obj };
    my_heap_mr.enqueue(elem);
  };

  bool deregister_old_region(jobject* obj){
    if(can_be_dereged){
      heap_desc elem =  my_heap_mr.dequeue();
      struct ibv_mr* mr = elem.mr;
      *obj = elem.obj;
      if(!withodp){
        ibv_dereg_mr(mr);  
      }
      can_be_dereged--;
      return true;
    }    

    return false;
  }



 
  struct ibv_mr * reg_mem(char* buffer, size_t length, int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE ){
    if(withodp){
      if(odpmr == NULL){
        this->odpmr = ibv_reg_mr(this->pd, 0, SIZE_MAX, access | IBV_ACCESS_ON_DEMAND );
      }

      struct ibv_sge sge;
      sge.addr = (uint64_t)buffer;
      sge.length = length;
      sge.lkey = odpmr->lkey;
      int ret = ibv_advise_mr(this->pd, IBV_ADVISE_MR_ADVICE_PREFETCH_WRITE, IBV_ADVISE_MR_FLAG_FLUSH, &sge,1);
      if(ret){
        printf("failed prefetch %d\n",ret);
        fflush(stdout);
      }
      return odpmr;
    }
    struct ibv_mr * mr = ibv_reg_mr(this->pd, buffer, length, access );
    return mr;
  }

  void block_on_event(int timeout){
    if(process_completions()){
      return; 
    }
    ibv_req_notify_cq(this->cq, 0);
    struct pollfd my_pollfd;
    my_pollfd.fd      = cq->channel->fd;
    my_pollfd.events  = POLLIN;
    my_pollfd.revents = 0;

    int rc = poll(&my_pollfd, 1, timeout);
    if(rc>0){
      struct ibv_cq *ev_cq;
      void* ev_ctx;
      ibv_get_cq_event(cq->channel, &ev_cq, &ev_ctx);

       // Ack the event
      ibv_ack_cq_events(cq, 1);
    }

  }


  // repost all possible receives and returns how many was reposted. 
  // if the returned value > 0, then you should notify the remote side about it.
  inline uint32_t repost_receives(){
      uint32_t toreturn = can_post_recv;

      post_empty_recv(can_post_recv);
      
      can_post_recv = 0;
      return toreturn;
  }


  inline uint32_t can_repost_receives() const {
      return can_post_recv;
  }

  // returns true if we polled at least one metadata receive
  // it does not progress the protocol. To progress call process_completions()
  inline bool is_readable(){
    return !receives.is_empty() || has_cached_receive();
  }

  inline bool is_readable_int(){
    return !received_ints.is_empty();
  }

  inline int pop_int(){
    return received_ints.dequeue();
  }


  inline receive_t pop_receive(){
    return receives.dequeue();
  }

 
  // this call checks the completion queues and progress the protocol
  // if does not block and returns true if at least one completion was polled
  bool process_completions(){
    struct ibv_wc wc;
    bool processed = false;

    while( ibv_poll_cq(this->cq, 1, &wc) > 0 ){
      if (wc.status != IBV_WC_SUCCESS) {
        printf("Failed status %s (%d) for opcode %d\n", ibv_wc_status_str(wc.status), wc.status, (int)wc.opcode);  
        fflush(stdout);
        assert(false,"failed completion op");
      }
      processed = true;
      if(wc.opcode & IBV_WC_RECV){ // is recv
        process_recv(&wc);
      }else{  // is send completion
        process_send(&wc);
      }
    }
    return processed;
  }
 

 
  uint32_t get_remote_klass_service_ip() const{
    return klass_service_ip;
  }

  uint16_t get_remote_klass_service_port() const{
    return klass_service_port;
  }

  inline int get_my_fd() const {
    return fd;
  }


  inline void cache_iterative_recv(uint32_t cached_bytes, bool done, region_t cached_region){
      assert(cached_bytes!=0, "Trying to cache 0 bytes");
      cached_receive.bytes_data = cached_bytes;
      cached_receive.last = done;
      this->cached_region = cached_region;
  }

  inline bool has_cached_receive(){
    return this->cached_receive.bytes_data != 0; 
  }

  inline cached_receive_t pop_cached_receive(){
    cached_receive_t toreturn = cached_receive;
    this->cached_receive.bytes_data = 0; 
    return toreturn;
  }
 
  inline region_t get_my_heap(uint32_t bytes){
    if(this->cached_region.vaddr!=0){ // I use rkey to identify cached regions
      // get cached region
      assert(bytes == this->cached_region.length, "cached region has unexpected length");
      region_t toreturn = cached_region;
      this->cached_region.vaddr = 0; 
      return toreturn;
    }
    region_t reg = my_heap.front(); // must be a copy

    uint32_t free_bytes_in_top = reg.length - current_offset_in_head_region;
    uint32_t get_from_top = MIN(free_bytes_in_top,bytes);

    reg.vaddr += current_offset_in_head_region;
    this->current_offset_in_head_region+=get_from_top;

    if(reg.length == current_offset_in_head_region){
      my_heap.dequeue();
      can_be_dereged++;
      assert(to_truncate_ind!=0, "region must be truncated first");
      to_truncate_ind--;
      current_offset_in_head_region = 0;
    }

    reg.length = get_from_top;  // "get_from_top" can be smaller than "bytes"
    return reg;
  } 

  void truncate_my_heap(uint32_t truncate){
    region_t *reg = my_heap.adr_at(to_truncate_ind++); // must be a pointer
    assert(truncate < reg->length, "trying to truncate to much");
    reg->length  -=  truncate;
  } 

  // check whether the remote side waits for a new heap region
  inline int has_heap_request(){
    return has_incoming_heap_request;
  }

  inline uint32_t get_offset_to_commit(char* addr){
    return recv_metadata_buf.get_offset(addr);
  }

  // notify the remote side that you have reposted the receives.  Returns true if it was sent
  bool send_commit_receives(uint32_t receives){
    while(can_send == 0){ // send now
      process_completions();
    } 
    can_send--;

    wr_id_t wr_id = {.comp = {.type = WR_EMPTY_TYPE, .outstanding_sends = (unsignaled_sends + 1), .token_or_offset = 0 } }  ;
    imm_data_t imm_data = { .comp = {.type = CONTROL_COMMIT_RECEIVES_TYPE, .token = receives } }; 
    struct ibv_send_wr wr, *bad;

    wr.wr_id = wr_id.whole;
    wr.next = NULL;
    wr.sg_list = NULL;
    wr.num_sge = 0;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;

    bool signaled = (unsignaled_sends == UNSIGNAL_THRESHOLD);
    if(signaled){
      unsignaled_sends = 0;
      wr.send_flags = IBV_SEND_SIGNALED;
    } else {
      unsignaled_sends++;
      wr.send_flags = 0; 
    }

    wr.imm_data = imm_data.whole;
    wr.wr.rdma.remote_addr = 0;
    wr.wr.rdma.rkey        = 0;
 
 
    int ret = ibv_post_send(this->qp, &wr, &bad);
    assert(ret == 0, "faield ibv_post_send");
    return true;
  }

  // send heap memory info to the remote side. Returns true if it was sent
  bool send_heap_reply(region_t *reg, uint32_t nums){
   
    while(can_send == 0){ // send now
      process_completions();
    } 
    can_send--;

    const uint32_t size = sizeof(region_t)*nums;

    for(uint32_t i = 0; i < nums; i++){
        my_heap.enqueue(reg[i]);  
    }

    has_incoming_heap_request-=nums;

    if(has_incoming_heap_request < 0){
      has_incoming_heap_request = 0;
    } 

    char* source = send_metadata_buf.Alloc(size);
    memcpy(source,reg,size);
   //get_baseaddr
    uint32_t offset = send_metadata_buf.get_offset(source + size);

    uint64_t dest = remote_buffer.Alloc(size); 
    remote_buffer.Commit(remote_buffer.get_current_offset()); // lazy commit. as the receiver expects data.

    wr_id_t wr_id = {.comp = {.type = WR_COMMIT_OFFSET_TYPE, 
          .outstanding_sends = (unsignaled_sends + 1), .token_or_offset = offset } }  ;

    struct ibv_send_wr wr, *bad;
    struct ibv_sge sge;

    sge.addr = (uint64_t)source;
    sge.length = size;
    sge.lkey = send_metadata_buf.get_lkey();

    wr.wr_id = wr_id.whole;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;

    wr.send_flags = IBV_SEND_SIGNALED;

    imm_data_t imm_data = { .comp = {.type = CONTROL_HEAP_REPLY_TYPE, .token = nums } }; 

    wr.imm_data =  imm_data.whole;
    wr.wr.rdma.remote_addr = dest;
    wr.wr.rdma.rkey        = remote_buffer.get_rkey();
    unsignaled_sends = 0;
    ibv_post_send(this->qp, &wr, &bad);
    return true;
  }
 
  // notify the remote side that you have read the data until offset.  Returns true if it was sent
  bool send_commit_offset(uint32_t offset){
    while(can_send == 0){ // send now
      process_completions();
    } 
    can_send--;

    wr_id_t wr_id = {.comp = {.type = WR_EMPTY_TYPE, .outstanding_sends = (unsignaled_sends + 1), .token_or_offset = 0 } }  ;
    imm_data_t imm_data = { .comp = {.type = CONTROL_COMMIT_OFFSET_TYPE, .token = offset } }; 
    struct ibv_send_wr wr, *bad;
 
    wr.wr_id = wr_id.whole;
    wr.next = NULL;
    wr.sg_list = NULL;
    wr.num_sge = 0;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = imm_data.whole;
    wr.wr.rdma.remote_addr = 0;
    wr.wr.rdma.rkey        = 0;
    unsignaled_sends       = 0;
 
    ibv_post_send(this->qp, &wr, &bad);
    return true;
  }

  void exchange_with_active(region_t *reg, uint32_t nums){
    for(uint32_t i = 0; i < nums; i++){
        my_heap.enqueue(reg[i]);  
    }

    uint32_t tota_len = sizeof(exchange_data_t) + nums*sizeof(region_t);
    exchange_data_t* data = (exchange_data_t* )(send_metadata_buf.get_baseaddr() + 2048);
    data->receives = max_recv_size;
    data->heapnums = nums;
    data->metadata_buf.length = recv_metadata_buf.get_length();
    data->metadata_buf.rkey = recv_metadata_buf.get_rkey();
    data->metadata_buf.vaddr = (uint64_t)recv_metadata_buf.get_baseaddr();
 
    memcpy(data + 1, reg, sizeof(region_t)*nums);
 
    struct ibv_sge send_sge;
    send_sge.addr = (uint64_t)(void*)(data);
    send_sge.lkey = send_metadata_buf.get_lkey();
    send_sge.length = tota_len;
 
    struct ibv_send_wr wr, *bad;
    wr.wr_id = 0;
    wr.next = NULL;
    wr.sg_list = &send_sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;  

    int ret = ibv_post_send(this->qp, &wr, &bad);
    if(ret){
      log_debug(naos)("error ibv_post_send\n");
      assert(0,"failed ibv_post_send");
    }
    for(uint32_t i =0; i<2; i++){ // get completion for send and receive
      struct ibv_wc wc;
      // get send or recv
      while(ibv_poll_cq(this->cq, 1, &wc) == 0){

      }
      if(wc.status !=0){
        printf("error wc %u \n", wc.status );
      }

      if(wc.opcode & IBV_WC_RECV){ // is recv
          exchange_data_t* remotedata = (exchange_data_t* )(send_metadata_buf.get_baseaddr());
 
          this->remote_buffer.init(remotedata->metadata_buf.vaddr, 
                                    remotedata->metadata_buf.rkey , remotedata->metadata_buf.length);
          this->klass_service_ip = remotedata->klass_service_ip;
          this->klass_service_port = remotedata->klass_service_port;
          recv_op.set_resolver(this->klass_service_ip,this->klass_service_port);
      }else{  // is send completion
        //nothing
      }
    }
    post_empty_recv(1);
  }
 
private:
  void process_send(struct ibv_wc *wc){
    wr_id_t wr_id = {.whole = wc->wr_id};
    can_send += (uint32_t)wr_id.comp.outstanding_sends;
 
    if(wr_id.comp.type == WR_COMMIT_OFFSET_TYPE){
      uint32_t offset = (uint32_t)(uint64_t)wr_id.comp.token_or_offset;
      send_metadata_buf.Commit(offset);
      return;
    } 
  }
 

  void process_recv(struct ibv_wc *wc){
    can_post_recv++;
    uint32_t  byte_len = wc->byte_len;
    if(wc->opcode == IBV_WC_RECV && (wc->wc_flags & IBV_WC_WITH_IMM)){
      // it is the data path for ints
      this->received_ints.enqueue(wc->imm_data);
      return;
    }

    if(wc->wc_flags & IBV_WC_WITH_IMM){
      imm_data_t data = {.whole = wc->imm_data};
      switch(data.comp.type){
        case DATA_HEAPDATA:
          {
            finished_heap_writes.enqueue( data.comp.token );
            break;   
          }
        case DATA_METADATA:
        case DATA_METADATA_LAST:
          {
            uint32_t last = (data.comp.type == DATA_METADATA_LAST) ? 1 : 0;
            receive_t temp = {.metadata_buf = recv_metadata_buf.GetNext(byte_len), .metadata_len=byte_len, .last = last, .token = data.comp.token  };
            receives.enqueue( temp );
            log_debug(naos) ("[Rdma] get metadata receive" ); 
            break;     
          }
        case CONTROL_HEAP_REQUEST_TYPE:
          {
           // printf("CONTROL_HEAP_REQUEST_TYPE %u buffers\n", data.comp.token );
            this->has_incoming_heap_request +=  data.comp.token ;
            break;
          }
      /*  case CONTROL_HEAP_TRUNCATE_TYPE: /// depricated
          {
            uint32_t truncate = data.comp.token;
           // printf("CONTROL_HEAP_TRUNCATE_TYPE %u \n",truncate);
            truncate_my_heap(truncate);
            break;
          }*/
        default:
            assert(0,"uknown imm data type");
      }
    }else{
      assert(0,"Error received data with no imm data\n");
    }
  }
 


};





#endif //JDK11_RDMA_RDMA_CONNECTION_H
