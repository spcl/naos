/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Data types for RDMA connections
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */

#ifndef JDK11_RDMA_RDMA_PROTOCOL_H
#define JDK11_RDMA_RDMA_PROTOCOL_H

struct region_t{
  uint64_t vaddr;
  uint32_t rkey;
  uint32_t length;
};

struct cached_receive_t{
  uint32_t bytes_data;
  bool last;
};

struct receive_t{
  char* metadata_buf;
  uint32_t metadata_len;
  uint32_t last : 1;
  uint32_t token : 31;
};

const uint32_t DATA_HEAPDATA = 0;
const uint32_t DATA_METADATA = 1;
const uint32_t DATA_METADATA_LAST = 2;
const uint32_t CONTROL_HEAP_REPLY_TYPE = 3;
const uint32_t CONTROL_HEAP_REQUEST_TYPE = 4;
const uint32_t CONTROL_COMMIT_OFFSET_TYPE = 5;
const uint32_t CONTROL_COMMIT_RECEIVES_TYPE = 6;
const uint32_t CONTROL_HEAP_TRUNCATE_TYPE = 7;




const uint32_t WR_EMPTY_TYPE = 0;
const uint32_t WR_COMMIT_OFFSET_TYPE = 1;
const uint32_t WR_WAIT_TOCKEN = 2;
const uint32_t WR_INTERNAL_BUFFER = 3;

// encoding of imm_data, that is 32 bit wide. 
typedef union {
    uint32_t whole;
    struct {
        uint32_t type               : 3;
        uint32_t token              : 29;
    } comp;
} imm_data_t;  


// encoding of wr_id, that is 64 bit wide.
typedef union {
    uint64_t whole;
    struct {
        uint64_t type : 2;  // if 1, then the value is offset see WR_ above
        uint64_t outstanding_sends : 14;
        uint64_t token_or_offset  : 48;  
    } comp;
} wr_id_t;  


struct exchange_data_t{
  region_t metadata_buf;
  uint32_t receives;
  uint32_t heapnums;
  uint32_t klass_service_ip;
  uint16_t klass_service_port;
  uint16_t pading1;
};





#endif //JDK11_RDMA_RDMA_PROTOCOL_H
