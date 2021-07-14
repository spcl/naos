/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Tools for RDMA connections
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */

#ifndef JDK11_RDMA_RDMA_HELPERS_H
#define JDK11_RDMA_RDMA_HELPERS_H
#include <infiniband/verbs.h>

// it allocates buffers of any size.
// but it assumes short lifetime of objects.
struct SendBuffers{
  char *buffer;
  uint32_t const length;
  uint32_t lkey;
  struct ibv_mr * mr;

  uint32_t current_offset;
  uint32_t free_space;
  uint32_t committed_offset; // only need for free

 
  SendBuffers(size_t length, struct ibv_pd *pd):
          length(length), current_offset(0), free_space(length), committed_offset(length)
  {
    this->buffer = (char*)aligned_alloc(64, length);
 //   assert(buffer != NULL && "error memory allocation");

    this->mr = ibv_reg_mr(pd, buffer, length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if(mr==NULL){
      printf("Error reg mr\n");
    }
    this->lkey = mr->lkey;
  }

  uint32_t get_lkey() const {
    return lkey;
  }

  char* get_baseaddr() const {
    return buffer;
  }

  uint32_t get_offset(char* addr) const {
    return (uint32_t)(addr-buffer);
  }
  
  bool CanAlloc(uint32_t size, bool canwrap = true) const {
    uint32_t to_allocate = size;
    if(to_allocate>free_space){
      return false;
    }
    if(current_offset+to_allocate > length){
      // wrap around
      if(!canwrap || to_allocate > (free_space - (length-current_offset) )){ // check again as we could crop the space we need
        return false;
      }
    }
    return true;
  }

  uint32_t GetMaxAllocSize() const {
    if(current_offset + free_space > length){
      uint32_t till_the_end = length - current_offset;
      uint32_t from_beg = committed_offset;
      return  till_the_end > from_beg ? till_the_end : from_beg;
    }else {
      return free_space;
    }
  }

  char* Alloc(uint32_t size) {
    uint32_t to_allocate = size;
    if(to_allocate>free_space){
      printf("return don't have memory\n" );
      return NULL;
    }
 
    if(current_offset+to_allocate > length){
      // wrap around
      free_space-=(length-current_offset); // we crop the last bytes
      current_offset = 0;
      if(to_allocate>free_space){ // check again as we could crop the space we need
        printf("return don't have memory\n" );
        return NULL;
      }
    }

    free_space-=to_allocate;
    uint32_t return_offset = current_offset;
    current_offset+=to_allocate;
    return buffer + return_offset;
  }
 
  void Reset(){
    committed_offset = 0;
    current_offset = 0;
    free_space = length;
  }

  void Commit(uint32_t offset) {
     if(offset == committed_offset && free_space == 0){ // free_space == 0 condition is to avoid the case when we commit the same offset twice
      free_space = length;
      printf("committed all data\n" );
      return;
    }

    uint32_t to_free = (offset + length - committed_offset) % length;
    free_space+=to_free;
    committed_offset = offset;
  }

  ~SendBuffers(){
    if(mr)
      ibv_dereg_mr(mr);
    if(buffer)
      free(buffer);
  }
};


// it allocates buffers of any size.
// but it assumes short lifetime of objects.
struct RemoteBufferTracker{
  uint64_t vaddr;
  uint32_t rkey;
  uint32_t length;

  uint32_t current_offset;   // only need for alloc
  uint32_t committed_offset; // only need for free
  uint32_t free_space;


  RemoteBufferTracker( )
  {
    // empty
  }

  void init(uint64_t vaddr, uint32_t rkey, uint32_t length){
    this->vaddr = vaddr;
    this->rkey = rkey;
    this->length = length;
    this->current_offset = 0;
    this->committed_offset = length;
    this->free_space = length;
  }

  uint32_t get_current_offset() const {
    return current_offset;
  }

  uint32_t get_rkey() const {
    return rkey;
  }

  bool CanAlloc(uint32_t size) const {
    uint32_t to_allocate = size;
    if(to_allocate>free_space){
      return false;
    }
    if(current_offset+to_allocate > length){
      // wrap around
      if(to_allocate > (free_space - (length-current_offset) )){ // check again as we could crop the space we need
        return false;
      }
    }
    return true;
  }

  uint64_t Alloc(uint32_t size) {
    uint32_t to_allocate = size;
    if(to_allocate>free_space){
      printf("return don't have memory\n" );
      return 0ULL;
    }
 
    if(current_offset+to_allocate > length){
      // wrap around
      free_space-=(length-current_offset); // we crop the last bytes
      current_offset = 0;
      if(to_allocate>free_space){ // check again as we could crop the space we need
        printf("return don't have memory\n" );
        return 0ULL;
      }
    }

    free_space-=to_allocate;
    size_t return_offset = current_offset;
    current_offset+=to_allocate;
    return vaddr + return_offset;
  }

  void Commit(uint32_t offset) {

    if(offset == committed_offset && free_space == 0){ // free_space == 0 condition is to avoid the case when we commit the same offset twice
      free_space = length;
      printf("committed all data\n" );
      return;
    }

    uint32_t to_free = (offset + length - committed_offset) % length;
    free_space+=to_free;
    committed_offset = offset;
  }

  bool is_full() const {
    return (free_space==0);
  }

  ~RemoteBufferTracker(){
    //empty
  }
};



// it allocates buffers of any size.
// but it assumes short lifetime of objects.
struct RecvBuffers{
  char *buffer;
  struct ibv_mr * mr;
  uint32_t const length;
  uint32_t current_offset;   
   
  //const uint32_t Allignment = 64;

  RecvBuffers(uint32_t length, struct ibv_pd *pd):
          length(length), current_offset(0) //, first_notfree(total_size)
  {
    this->buffer = (char*)aligned_alloc(64, length);
   // assert(buffer != NULL && "error memory allocation");

    this->mr = ibv_reg_mr(pd, buffer, length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if(mr==NULL){
      printf("[RecvBuffers] Error reg mr\n");
    }
  }

  uint32_t get_rkey() const {
    return mr->rkey;
  }

  uint32_t get_length() const {
    return length;
  }

  char* get_baseaddr() const {
    return buffer;
  }

  inline uint32_t get_offset(char* addr) const {
    return (uint32_t)(addr-buffer);
  }

  char* GetNext(uint32_t size) {
 
    if(current_offset+size > length){
      // wrap around
      current_offset = 0;
    }
 
    size_t return_offset = current_offset;
    current_offset+=size;
    return buffer + return_offset;
  }
 
  ~RecvBuffers(){
    if(mr)
      ibv_dereg_mr(mr);
    if(buffer)
      free(buffer);
  }
};




#endif //JDK11_RDMA_RDMA_HELPERS_H
