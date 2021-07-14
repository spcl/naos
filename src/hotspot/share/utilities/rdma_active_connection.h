/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * An active side of an RDMA connection
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */

#ifndef JDK11_RDMA_RDMA_ACTIVE_CONNECTION_H
#define JDK11_RDMA_RDMA_ACTIVE_CONNECTION_H


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
#define MAX_WRS (64)
#define MAX_POST_RECV (32)

#define MIN(a,b) (((a)<(b))?(a):(b))


class RemoteEP: public CHeapObj<mtInternal>  {
  uint32_t remote_receives;   // we count how many posted receives on remote side to avoid overflow
 // std::list<region_t> direct_regions; // remote heap regions //should be queue
  QueueImpl<region_t, ResourceObj::C_HEAP, mtInternal> direct_regions;
  uint32_t current_offset_in_head_region;
  RemoteBufferTracker remote_buffer;   // remote metadata receive buffer

  uint32_t remote_heap_total;

public:
  RemoteEP():
  remote_receives(0), remote_buffer()
  {
    this->current_offset_in_head_region = 0;
    this->remote_heap_total =  0; 
  };

  void init(uint32_t remote_receives, region_t* rembuffer)
  {
    this->remote_receives = remote_receives;
    this->remote_buffer.init(rembuffer->vaddr, rembuffer->rkey, rembuffer->length);
    this->current_offset_in_head_region = 0;
    this->remote_heap_total =  rembuffer->length; 
  };


//---------------------------------------RECEIVES----------
  // remote side allowed us to send
  void commit_receives(uint32_t num){
    remote_receives += num;
  }

  void consume_receives(uint32_t num){
    //assert(remote_receives >= num && "we don't have so many receives");
    remote_receives-=num;
  }


  //check whether the remote side has place in its receive queue.
  uint32_t get_receives() const {
    return remote_receives;
  }


//---------------------------------------METADATA----------
  
  uint64_t get_metadata(uint32_t bytes){
    return remote_buffer.Alloc(bytes);
  }

  bool can_get_metadata(uint32_t bytes){
    return remote_buffer.CanAlloc(bytes);
  }


  uint32_t get_metadata_rkey() const {
    return remote_buffer.get_rkey();
  }

  void commit_metadata(uint32_t offset){
    remote_buffer.Commit(offset);
  }


//---------------------------------------REMOTE HEAP----------
 

  inline bool has_region(){
    return !direct_regions.is_empty();
  }

  // returns the available bytes in the top region
  inline uint32_t get_contig_remote_heap_size(){
    if(direct_regions.is_empty()){
      return 0;
    }
    return  (direct_regions.front().length - this->current_offset_in_head_region);
  }

  inline uint32_t get_total_remote_heap_size(){
    return  this->remote_heap_total;
  }
 
  void add_region(region_t* rembuffer){
    log_debug(naos)("add region %u\n", rembuffer->length);
    direct_regions.enqueue(*rembuffer);
    this->remote_heap_total += rembuffer->length;
  }

  // it returns as much memory as it can from the top region.
  // so it can return smaller remote buffer than requested
  inline region_t get_region(uint32_t bytes){
    
    uint32_t free_bytes_in_top = direct_regions.front().length - current_offset_in_head_region;
    uint32_t get_from_top = MIN(free_bytes_in_top,bytes);
    
    region_t reg = direct_regions.front(); // must be a copy
    reg.vaddr += current_offset_in_head_region;
    reg.length = get_from_top;  // "get_from_top" can be smaller than "bytes"

    this->remote_heap_total -= get_from_top;
    this->current_offset_in_head_region+=get_from_top;

    if(get_from_top < bytes){ // now it pops a region only if the request fails
      direct_regions.dequeue();
      this->current_offset_in_head_region = 0;
    }

    return reg;
  } 

};



class ActiveVerbsEP: public CHeapObj<mtInternal>{ 
  struct rdma_cm_id  *const id; //

  struct ibv_qp *const qp;
  const int fd;
  struct ibv_pd *const pd;
  const uint32_t max_inline_data;
  const uint32_t max_send_size;
  const uint32_t max_recv_size;

  struct ibv_cq* const cq;
 
  SendBuffers send_metadata_buf;


  uint32_t unsignaled_sends; // how many sends was sent un-signaled after the last signaled send. 
 
  uint32_t total_wr; 
  uint32_t current_wr; 
  struct ibv_send_wr wrs[MAX_WRS];
  struct ibv_sge sges[MAX_WRS];

  RemoteEP rem_ep;
  uint32_t can_send;          // the number of sends we can send

  uint32_t can_post_recv; // how many receives we can post and acknowledge


  RecvBuffers recv_metadata_buf; // for heap replies
public:
  SendBuffers send_buf; // for debugging and initial release
private:
  bool has_outstanding_heap_request; 


  GrowableArray<struct ibv_mr*>  *mem_list;
  
  bool needtowait;

  uint64_t wrid_to_wait;

  const uint32_t segment_size;
  const uint32_t balance;
  uint32_t order_token;
  
  struct ibv_recv_wr rwr[MAX_POST_RECV];

  Stack<uint64_t,mtInternal> _cached_arrays;
  uint32_t expected_array_size; 


public:
  const bool withodp;
  struct ibv_mr * odpmr;

  // cached strcuctures for traversal
  OffsetSendMetadata metadata;
  SendTree sendtree;  
  TraverseObject send_op;
 
  ActiveVerbsEP(struct rdma_cm_id  *id, uint32_t max_inline_data, uint32_t max_send_size, uint32_t max_recv_size, uint32_t buffersize, uint32_t nums, bool withodp) :
          id(id), qp(id->qp), fd(id->channel->fd), pd(id->qp->pd), max_inline_data(0), max_send_size(max_send_size),
          max_recv_size(max_recv_size), cq(id->qp->send_cq) , send_metadata_buf(1024*1024,pd),unsignaled_sends(0),
          total_wr(0), current_wr(0), can_send(max_send_size),  can_post_recv(0), recv_metadata_buf(1024*1024,pd), 
          send_buf(1024*1024,pd), has_outstanding_heap_request(false),needtowait(false),wrid_to_wait(0),
          segment_size(buffersize),balance(nums), order_token(1), expected_array_size(128), withodp(withodp),odpmr(NULL),metadata(0,false),
          sendtree(),send_op(metadata,sendtree)
  {
    mem_list = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<struct ibv_mr*>(16, true);
  }

  ~ActiveVerbsEP() {
    // TODO
  }

  inline void init_traversal(Handle obj,int array_len){
    send_op.init(obj,abs(array_len)); 
    sendtree.hard_reset();
  }

  inline void reset(){
    metadata.reset();
    sendtree.reset();
  }

  inline GrowableArray<uint8_t> * get_nonblocking_handle(){
    GrowableArray<uint8_t> * array = NULL;
    if(_cached_arrays.is_empty()){
      array = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<uint8_t>(expected_array_size, true);
    } else {
      array = (GrowableArray<uint8_t> *)(void*)_cached_arrays.pop();
    }

    array->trunc_to(4);

    uint32_t* val = (uint32_t*)array->adr_at(0);
    *val = (uint32_t)0xFFFFFFFF; 
    return array;
  }
 

  // get ip of the active endpoint
  uint32_t get_my_ip() {
    struct sockaddr * sa = rdma_get_local_addr (this->id);
    if (sa->sa_family == AF_INET) {
      struct sockaddr_in *s = (struct sockaddr_in *)sa;
      return s->sin_addr.s_addr;
        //printf("Peer ipv4 is %s \n",inet_ntop(AF_INET, &s->sin_addr, ipstr, sizeof ipstr));
    } else if(sa->sa_family == AF_INET6) {
      struct sockaddr_in6 *s = (struct sockaddr_in6 *)sa;
      // here we will truncate it to ipv4
      uint8_t *bytes = s->sin6_addr.s6_addr;
      bytes+=12;
      uint32_t ipv4  = *(uint32_t *)bytes;
      return ipv4;
        //printf("Peer ipv6 is %s \n",inet_ntop(AF_INET6, &s->sin6_addr, ipstr, sizeof ipstr));
    }
    return 0;
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

  // fast optimized post recv. It posts receives in batches of MAX_POST_RECV
  void post_empty_recv(uint32_t to_post){
    log_debug(naos)("post_empty_recv %u\n", to_post);
    struct ibv_recv_wr  *bad;
    
    int first_post = to_post % MAX_POST_RECV;
    if(first_post > 0){
      rwr[first_post-1].next=NULL;
      ibv_post_recv(this->qp, &rwr[0], &bad);  
      rwr[first_post-1].next=&rwr[first_post];
    }

    for(uint32_t i=0; i < to_post/MAX_POST_RECV; i++ ){
      ibv_post_recv(this->qp, &rwr[0], &bad);  
    }
    
    return;
  }

  void exchange_with_passive(uint32_t klass_service_ip, uint16_t klass_service_port){
 
    uint32_t tota_len = sizeof(exchange_data_t);
    exchange_data_t* data = (exchange_data_t* )(send_metadata_buf.get_baseaddr() + 2048);
 
    data->metadata_buf.length = recv_metadata_buf.get_length();
    data->metadata_buf.rkey = recv_metadata_buf.get_rkey();
    data->metadata_buf.vaddr = (uint64_t)recv_metadata_buf.get_baseaddr();
    data->klass_service_ip = klass_service_ip;
    data->klass_service_port = klass_service_port;
 
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
          this->rem_ep.init(remotedata->receives, &(remotedata->metadata_buf));
          region_t *regs = (region_t*)(void*)(remotedata + 1);
       //   printf("[%p] Received %u buffers \n", this, remotedata->heapnums );
          for(uint32_t j=0; j < remotedata->heapnums; j++){
            this->rem_ep.add_region(regs + j);  
          }
      }else{  // is send completion
        //nothing
      }
    }
    post_empty_recv(1);
  }


  bool send_int(int val){
    assert(total_wr == 0 , "I do not expect that it will be used in parallel");

    wr_id_t wr_id = {.comp = {.type = WR_EMPTY_TYPE, .outstanding_sends = (unsignaled_sends + 1), .token_or_offset = 0 } }  ;
    struct ibv_send_wr *bad;

    wrs[current_wr].wr_id = wr_id.whole;
    wrs[current_wr].next = NULL;
    wrs[current_wr].sg_list = NULL;
    wrs[current_wr].num_sge = 0;
    wrs[current_wr].opcode = IBV_WR_SEND_WITH_IMM;
    wrs[current_wr].imm_data = val;
 
    bool signaled = (unsignaled_sends == UNSIGNAL_THRESHOLD);
    if(signaled){
      unsignaled_sends = 0;
      wrs[current_wr].send_flags = IBV_SEND_SIGNALED;
    } else {
      unsignaled_sends++;
      wrs[current_wr].send_flags = 0; 
    }

    if(can_send > 0 && rem_ep.get_receives()>0 && total_wr == 0){ // send now
      rem_ep.consume_receives(1);
      can_send--;
      ibv_post_send(this->qp, &wrs[current_wr], &bad);
      return true;
    }
    // saved, not send
    log_debug(naos)("Delay[%u] delay send_int\n",total_wr);
    current_wr = (current_wr + 1) % MAX_WRS;
    total_wr++;
    return false;
  }


 
  // this call checks the completion queues and progress the protocol
  // if does not block and returns true if at least one completion was polled
 
  inline bool process_completions(){
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
        log_debug(naos)("Get recv;\n");
        process_recv(&wc);
      }else{  // is send completion
        process_send(&wc);
      }
    }
    return processed;
  }
 
  inline uint32_t can_repost_receives() const {
      return can_post_recv;
  }


  // repost all possible receives and returns how many was reposted. 
  // if the returned value > 0, then you should notify the remote side about it.
  inline uint32_t repost_receives(){
      uint32_t toreturn = can_post_recv;

     
      post_empty_recv(can_post_recv);
      
      can_post_recv = 0;
      return toreturn;
  }
 
  void process_recv(struct ibv_wc *wc){
    can_post_recv++;
    uint32_t  byte_len = wc->byte_len;
 
    if(wc->wc_flags & IBV_WC_WITH_IMM){
      imm_data_t data = {.whole = wc->imm_data};
      switch(data.comp.type){
        case CONTROL_HEAP_REPLY_TYPE:
          {
            region_t* heap_buf = (region_t* )recv_metadata_buf.GetNext(byte_len);
            log_debug(naos)("-------------heap reply %lx : %u : %u\n",heap_buf->vaddr,heap_buf->length,heap_buf->rkey);
            has_outstanding_heap_request = false;
            uint32_t nums = byte_len / sizeof(region_t);
            assert(nums == data.comp.token, "error on heap reply processing ");
            for(uint32_t i =0 ; i < nums; i++){
                this->rem_ep.add_region(heap_buf + i);
            }
            break;
          }
        case CONTROL_COMMIT_OFFSET_TYPE:
          {
            uint32_t offset = data.comp.token;
            this->rem_ep.commit_metadata(offset);
            break;
          }
        case CONTROL_COMMIT_RECEIVES_TYPE:
          {
            uint32_t ack_receives = data.comp.token;
            log_debug(naos)("commit %u receives\n",ack_receives);
            this->rem_ep.commit_receives(ack_receives);
            break;
          }
        default:
          assert(0,"uknown imm data type");
      }
    }else{
      assert(0,"Error received data with no imm data\n");
    }
  }


  struct ibv_mr* get_mr_for_region_index(size_t index){
      assert(index == (size_t)(int)index, "Index of mem region does not fit in int");
      return mem_list->at_grow((int)index, NULL);
  }

  void set_mr_for_region_index(size_t index, struct ibv_mr* mr ){
      assert(index == (size_t)(int)index, "Index of mem region does not fit in int");
      mem_list->at_put_grow((int)index, mr, NULL);
  }


  // gets as much as possible from send buffer
  inline region_t get_send_buffer(uint32_t atleast_bytes){
    region_t reg; 
    reg.rkey = send_buf.get_lkey();
    reg.length = 0;
    if(send_buf.CanAlloc(atleast_bytes)){
      reg.vaddr = (uint64_t)send_buf.Alloc(atleast_bytes);
      reg.length = atleast_bytes;
    }
    return reg;
  }

    // gets as much as possible from send buffer
  inline bool extend_send_buffer(region_t &reg,uint32_t atleast_bytes){
    if(reg.length >= atleast_bytes){
      return true;
    }
    uint32_t need = atleast_bytes - reg.length;
    if(send_buf.CanAlloc(need,false)){
      uint64_t addr = (uint64_t)send_buf.Alloc(need);
      assert(addr == reg.vaddr + reg.length , "wrong implementation of can alloc no wrap");
      reg.length = atleast_bytes;
      return true;
    }
    return false;
  }


  struct ibv_mr * reg_mem(char* buffer, size_t length, int access = 0 ){
    struct ibv_mr * mr = ibv_reg_mr(this->pd, buffer, length, access );
    return mr;
  }

  struct ibv_mr * get_odp_mr(int access = 0){    
    if(odpmr == NULL){
      this->odpmr = ibv_reg_mr(this->pd, 0, SIZE_MAX, access | IBV_ACCESS_ON_DEMAND );
    }
    return odpmr;
  }
 

  inline void flush(bool blocking){
    do{
      process_completions();
      try_to_send();
    } while(blocking && total_wr != 0);
 
  }

  // this call checks the completion queues and progress the protocol, and also waits for specific send wr_id
  // The call is supposed to be used after writing data to remote heap. 
  // It helps to release locks from heap after the data was sent.
  // returns true on success
  inline bool needtowait_send_completion( ){
    return needtowait;
  }

  // sends data to remote heap. It will use write, so the remote side will be unaware.
  // it will not check the remote heap boundary, and it assumes that it was checked before this call
  // it will register the local heap on demand if it is not registered
  bool write_to_remote_heap(char* buf, uint32_t length, uint32_t lkey, region_t  &reg, uint64_t wrid);

  // as above, but buffer is from internal send buffer
  bool write_to_remote_heap_from_sendbuf(char* buf, uint32_t length, uint32_t lkey, region_t  &reg, uint64_t wrid);
  
 
  // check whether I have outstanding heap request
  inline bool has_pending_heap_request(){
    return has_outstanding_heap_request;
  }

  // send heap request. Returns true if it was sent
  bool send_heap_request(uint32_t num = 1);

  // send request to inform the other side how much data is not used in the current heap.
  //bool send_heap_truncate(uint32_t truncate); depricated

  inline uint32_t get_total_remote_heap_size(){
    return rem_ep.get_total_remote_heap_size();
  }

  inline uint32_t default_remote_segment_size() const {
    return segment_size; // 2 MiB 2*1024*1024
  }

  inline uint32_t get_offset_to_commit(char* addr){
    return recv_metadata_buf.get_offset(addr);
  }

  inline region_t get_region(uint32_t bytes){
    return rem_ep.get_region(bytes);
  }

  inline bool has_region(){
    return rem_ep.has_region();
  }

  inline uint32_t balance_remote_buffers() const{
    return balance;
  }
 
  // sends metadata to remote side. It assumes that the buf is from send_buffer
  // the token is 30 bit specified by user  
  bool write_metadata(char* buf, uint32_t length, uint32_t token, bool last); 

 
  inline char* get_send_metadata_buffer(uint32_t size){
    if( send_metadata_buf.CanAlloc(size)) {
      return send_metadata_buf.Alloc(size);
    }
    return NULL;
  }

  inline int get_my_fd() const {
    return fd;
  }

 
private:
  inline void try_to_send(){
    struct ibv_send_wr *bad;
    uint32_t can_send_now = 0;
    uint32_t from = (MAX_WRS + current_wr - total_wr) % MAX_WRS; 

    for(uint32_t i = 0; i < total_wr; i++){
      if(can_send == 0)
        break;

      if(wrs[(from+i) % MAX_WRS].opcode == IBV_WR_RDMA_WRITE_WITH_IMM){
        if(rem_ep.get_receives()>0){
          rem_ep.consume_receives(1);
        } else {
          break;
        }
      }  
      can_send_now++;
      can_send--;
      wrs[(from+i) % MAX_WRS].next = &wrs[(from+i+1) % MAX_WRS];
    }

    if(can_send_now){
      log_debug(naos)("Pushed cached sends %u from %u\n",can_send_now,from);
      this->total_wr-=can_send_now;
      wrs[(from+can_send_now-1) % MAX_WRS].next = NULL;
      int ret = ibv_post_send(this->qp, &wrs[from], &bad); 
      assert(ret == 0, "ibv_post_send failed");
      //assert(this->current_wr == 0, "I am stupid");
    } else {
      if(total_wr) {
        log_debug(naos)("Failed since %u %u\n",can_send, rem_ep.get_receives());
      }
    }
  }


  inline void process_send(struct ibv_wc *wc){
    wr_id_t wr_id = {.whole = wc->wr_id};
    can_send += (uint32_t)wr_id.comp.outstanding_sends;

    if(this->needtowait && this->wrid_to_wait == wr_id.whole){
        needtowait = false;
    }

    if(wr_id.comp.type == WR_COMMIT_OFFSET_TYPE){
      uint32_t offset = (uint32_t)(uint64_t)wr_id.comp.token_or_offset;
      send_metadata_buf.Commit(offset);
      return;
    } 
    else if(wr_id.comp.type == WR_INTERNAL_BUFFER){
      uint32_t offset = (uint32_t)(uint64_t)wr_id.comp.token_or_offset;
      send_buf.Commit(offset);
    }
    else if(wr_id.comp.type == WR_WAIT_TOCKEN){
      log_info(naos)("Found WR_WAIT_TOCKEN %p\n",(void*)(uint64_t)wr_id.comp.token_or_offset);
      GrowableArray<uint8_t> * array = (GrowableArray<uint8_t> *)(void*)(uint64_t)wr_id.comp.token_or_offset;
      uint32_t* val = (uint32_t*)array->adr_at(0);
      uint32_t offset = *val;
      if(offset != 0xFFFFFFFF){
        log_debug(naos)("Commit offset %u\n",offset);
        send_buf.Commit(offset);
      }
      *val = 0;
      uint32_t len = array->length()-4; // 4 = sizeof(uint32_t)
      uint8_t* data = array->adr_at(4);

      ShenandoahHeap* heap = ((ShenandoahHeap*)Universe::heap()); 
      for(uint32_t i=0; i<len; i++){
        if(data[i]){
          heap->get_region(i)->record_unpin(); 
          log_debug(naos)("\trecord_unpin %u\n",i);
          data[i]=0;
        }
      }
      _cached_arrays.push((uint64_t)(void*)array);
      expected_array_size = (((uint32_t)array->max_length()) + 64 - 1) & 0xFFFFFF40; //  64 byte allignment
      log_debug(naos)("expected_array_size %u\n",expected_array_size);
    }
  }
  
};


















































#endif //JDK11_RDMA_RDMA_ACTIVE_CONNECTION_H
