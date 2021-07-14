/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Implementation of RDMA connections
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
#include "rdma_active_connection.h"
 


bool ActiveVerbsEP::send_heap_request(uint32_t nums){
    if(has_outstanding_heap_request){
      return false;
    }

    has_outstanding_heap_request = true;

    while(total_wr == MAX_WRS){
      process_completions();
      try_to_send();
    }

    wr_id_t wr_id = {.comp = {.type = WR_EMPTY_TYPE, .outstanding_sends = (unsignaled_sends + 1), .token_or_offset = 0 } }  ;
 
    struct ibv_send_wr *bad;

    wrs[current_wr].wr_id = wr_id.whole;
    wrs[current_wr].next = NULL;
    wrs[current_wr].sg_list = NULL;
    wrs[current_wr].num_sge = 0;
    wrs[current_wr].opcode = IBV_WR_RDMA_WRITE_WITH_IMM;

    bool signaled = (unsignaled_sends == UNSIGNAL_THRESHOLD);
    if(signaled){
      unsignaled_sends = 0;
      wrs[current_wr].send_flags = IBV_SEND_SIGNALED;
    } else {
      unsignaled_sends++;
      wrs[current_wr].send_flags = 0; 
    }

    imm_data_t imm_data = { .comp = {.type = CONTROL_HEAP_REQUEST_TYPE, .token = nums } }; 
      
    wrs[current_wr].imm_data = imm_data.whole;

    wrs[current_wr].wr.rdma.remote_addr = 0;
    wrs[current_wr].wr.rdma.rkey        = 0;
 
    if(can_send > 0 && rem_ep.get_receives()>0 && total_wr == 0){ // send now
      log_debug(naos)("send_heap_request\n");
      rem_ep.consume_receives(1);
      can_send--;
      ibv_post_send(this->qp, &wrs[current_wr], &bad);
      return true;
    }
    // saved, not send
    log_debug(naos)("Delay[%u] delay send_heap_request\n",total_wr);
    current_wr = (current_wr + 1) % MAX_WRS;
    total_wr++;
    return false;
  }


  // sends metadata to remote side. It assumes that the buf is from send_buffer
  bool ActiveVerbsEP::write_metadata(char* source, uint32_t size, uint32_t token, bool last){
    while(!rem_ep.can_get_metadata(size) || total_wr == MAX_WRS ){
      log_debug(naos)("spin write_metadata because of %d %d \n",rem_ep.can_get_metadata(size),total_wr == MAX_WRS);
      process_completions();
      try_to_send();
    }
 
    uint32_t offset = send_metadata_buf.get_offset(source + size);
    uint64_t dest = rem_ep.get_metadata(size);

    wr_id_t wr_id = {.comp = {.type = WR_COMMIT_OFFSET_TYPE, 
      .outstanding_sends = (unsignaled_sends + 1), .token_or_offset = offset } }  ;
    imm_data_t imm_data = { .comp = {.type = last ? DATA_METADATA_LAST : DATA_METADATA,  .token = token} };

    if(last){
     // assert(!needtowait, "we cannot wait for two completions");
      wrid_to_wait = wr_id.whole;
      needtowait = true;  
    }

    struct ibv_send_wr *bad;
    sges[current_wr].addr = (uint64_t)source;
    sges[current_wr].length = size;
    sges[current_wr].lkey = send_metadata_buf.get_lkey();
      
    wrs[current_wr].wr_id = wr_id.whole;
    wrs[current_wr].next = NULL;
    wrs[current_wr].sg_list = &sges[current_wr];
    wrs[current_wr].num_sge = 1;
    wrs[current_wr].opcode = IBV_WR_RDMA_WRITE_WITH_IMM;

    wrs[current_wr].send_flags = IBV_SEND_SIGNALED; 
    wrs[current_wr].imm_data = imm_data.whole;

    wrs[current_wr].wr.rdma.remote_addr = dest;
    wrs[current_wr].wr.rdma.rkey        = rem_ep.get_metadata_rkey();

    unsignaled_sends = 0;
 
    if(can_send > 0 && rem_ep.get_receives()>0 && total_wr == 0){
      log_debug(naos)("Send metadata from %lx to %lx  len: %u\n",(uint64_t)source,dest, size);
      rem_ep.consume_receives(1);
      can_send--;
      int ret = ibv_post_send(this->qp, &wrs[current_wr], &bad);
      assert(ret == 0, "ibv_post_send failed");
      return true;
    }
    log_debug(naos)("Delay[%u] Send metadata from %lx to %lx  len: %u\n",total_wr,(uint64_t)source,dest, size);
    
    current_wr = (current_wr + 1) % MAX_WRS;
    total_wr++;
    return false;
  }

  bool ActiveVerbsEP::write_to_remote_heap(char* buf, uint32_t length, uint32_t lkey,  region_t  &reg, uint64_t wrid){
    while(total_wr == MAX_WRS){
      log_debug(naos)("heap spin as the sendlist is full \n");
      process_completions();
      try_to_send();
    }

    // if wrid is 0, then the type is EMPTY and token is 0.
    wr_id_t wr_id = {.comp = {.type = wrid == 0 ? WR_EMPTY_TYPE : WR_WAIT_TOCKEN , .outstanding_sends = (unsignaled_sends + 1), .token_or_offset = wrid } }  ;
    
    if(wrid){
       log_debug(naos)("To remote heap with %lu %lu \n",wr_id.comp.type,wr_id.comp.token_or_offset);
    }

    struct ibv_send_wr *bad;
    sges[current_wr].addr = (uint64_t)buf;
    sges[current_wr].length = length;
    sges[current_wr].lkey = lkey;
      
    wrs[current_wr].wr_id = wr_id.whole;
    wrs[current_wr].next = NULL;
    wrs[current_wr].sg_list = &sges[current_wr];
    wrs[current_wr].num_sge = 1;
    wrs[current_wr].opcode = IBV_WR_RDMA_WRITE;

    bool signaled = (unsignaled_sends == UNSIGNAL_THRESHOLD || wrid != 0);
    if(signaled){
      unsignaled_sends = 0;
      wrs[current_wr].send_flags = IBV_SEND_SIGNALED;
    } else {
      unsignaled_sends++;
      wrs[current_wr].send_flags = 0; 
    }

    wrs[current_wr].imm_data = 0;

    wrs[current_wr].wr.rdma.remote_addr = reg.vaddr;
    wrs[current_wr].wr.rdma.rkey        = reg.rkey;

    if(can_send > 0 && total_wr == 0){ // send now
      can_send--;
      ibv_post_send(this->qp, &wrs[current_wr], &bad);
      return true;
    }
    // saved, not send
    log_debug(naos)("Delay[%u] write_to_remote_heap \n",total_wr);
    current_wr = (current_wr + 1) % MAX_WRS;
    total_wr++;
    return false;
  }

  bool ActiveVerbsEP::write_to_remote_heap_from_sendbuf(char* buf, uint32_t length, uint32_t lkey, region_t  &reg, uint64_t wrid){
    while(total_wr == MAX_WRS){
      process_completions();
      try_to_send();
    }

    uint32_t offset_to_commit = send_buf.get_offset(buf+length);

    wr_id_t wr_id =  
     {.comp = {.type = (wrid == 0 ? WR_INTERNAL_BUFFER : WR_WAIT_TOCKEN), .outstanding_sends = (unsignaled_sends + 1), .token_or_offset = (wrid == 0 ? offset_to_commit : wrid)} } ;

    if(wrid){
      GrowableArray<uint8_t> * array = (GrowableArray<uint8_t> *)(void*)wrid;
      uint32_t* val = (uint32_t*)array->adr_at(0);
      *val = offset_to_commit;
      assert(offset_to_commit!=0, "offset cannot be zero");
    }

    struct ibv_send_wr *bad;
    sges[current_wr].addr = (uint64_t)buf;
    sges[current_wr].length = length;
    sges[current_wr].lkey = lkey;
      
    wrs[current_wr].wr_id = wr_id.whole;
    wrs[current_wr].next = NULL;
    wrs[current_wr].sg_list = &sges[current_wr];
    wrs[current_wr].num_sge = 1;
    wrs[current_wr].opcode = IBV_WR_RDMA_WRITE;

    wrs[current_wr].send_flags = IBV_SEND_SIGNALED; 
    wrs[current_wr].imm_data = 0;

    wrs[current_wr].wr.rdma.remote_addr = reg.vaddr;
    wrs[current_wr].wr.rdma.rkey        = reg.rkey;

    unsignaled_sends = 0;

    if(can_send > 0 && total_wr == 0){
        can_send--;
        // it must be signalled as we need offset from the completion.    
        ibv_post_send(this->qp, &wrs[current_wr], &bad);
        return true;
    }
    log_debug(naos)("Delay[%u] remote_heap_from_sendbuf\n",total_wr);
    // saved, not send
    current_wr = (current_wr + 1) % MAX_WRS;
    total_wr++;
    return false;
  }

 