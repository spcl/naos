/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Naos's interval tree for fast cycle detections
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
#ifndef JDK11_RDMA_JVM_NAOS_TRAVERSE_HPP
#define JDK11_RDMA_JVM_NAOS_TRAVERSE_HPP

#include <stdint.h>

#include "memory/allocation.inline.hpp"
#include "memory/allocation.hpp"
#include "memory/resourceArea.hpp"
#include "utilities/c-rbtree.hpp"
#include "utilities/queue.hpp"

struct SendNode {

  // addr of the buffer (addr is also used for sorting in the rbtree)
  uint64_t addr;
  // number of bytes in this node
  uint32_t length;
  // global offset. This offset can be recomputed if you traverse the rbtree in order and sum all lengths.
  uint32_t offset;

  // hook for the rbtree, must be 8 byte alligned
  CRBNode rb;

} ;

class SendTree {

  // Note: the tree only contains metadata that points to SendNodes. The actual nodes are stored in a Queue.
  CRBTree _tree;

  // Storage for actual tree nodes. One of the queues keeps nodes that have still to be sent.
  // TODO- rbruno - we could have only one list. The other list could be replaced by pointer to where we should send from.
  QueueImpl<SendNode, ResourceObj::C_HEAP, mtInternal>  _send_list;
  QueueImpl<SendNode, ResourceObj::C_HEAP, mtInternal>  _sent_send_list;

  // These are helpers to make the non-cold path faster.
  SendNode* _prev;
  SendNode* _curr;
  SendNode* _next;

  // This is the global offset (see SendNode.offset).
  uint32_t _offset;

  // Helper counters.
  uint32_t _total_objects;
  uint32_t _num_segments;
  uint32_t _current_sendlist_length; 
  uint32_t _current_sendlist_objects;

public:

  inline SendTree(): _offset(0), _total_objects(0), _num_segments(0), _current_sendlist_length(0),_current_sendlist_objects(0) {
    c_rbtree_init(&_tree);
    _prev = NULL;
    _curr = NULL;
    _next = NULL;
  }

  ~SendTree() {
    // TODO - rbruno - do we need to do anything w.r.t. the tree? I don't think so...
  }

  inline uint32_t get_total_objects() const{
    return _total_objects;
  }

  inline uint32_t get_total_length() const{
    return _offset;
  }

  inline int get_num_segments() const{
    return _num_segments;
  }

  inline void hard_reset(){
    c_rbtree_init(&_tree);
    _sent_send_list.clear();
    _send_list.clear();

    _prev = NULL;
    _curr = NULL;
    _next = NULL;
    
    _offset=0;
    _total_objects = 0;
    _num_segments = 0; 
    _current_sendlist_length = 0;
    _current_sendlist_objects = 0;

  }

  inline void  reset() {
    _sent_send_list.move(&_send_list);
    _current_sendlist_length = 0;
    _current_sendlist_objects = 0;
    _curr = NULL;
    _prev = NULL;
    _next = NULL;
  }

  inline uint32_t get_current_length() const{
    return _current_sendlist_length; 
  }

  inline uint32_t get_current_objects() const{
    return _current_sendlist_objects;  
  }
  

  // TODO - change find to return the SendItem that contains the address.

private:
  // Traverses the tree until it finds a node containing this 'addr'. Returns NULL is no SendNode contains 'addr'.
  inline SendNode* tree_find(uint64_t addr);

  // Addes the node to the tree. If there is a node with the same key, no insertion is performed and node with the same key is returned.
  inline SendNode* tree_add(SendNode *node);

  // Returns the next & prev node according to the three ordering
  inline SendNode* tree_find_next(SendNode* node);
  inline SendNode* tree_find_prev(SendNode* node);

  inline bool tree_add_next(SendNode* existing_node, SendNode* new_node);
  inline bool tree_add_prev(SendNode* existing_node, SendNode* new_node);

  // slow path of appending an element
  inline bool add_slow_path(uint64_t addr, uint32_t size, uint32_t *offset);
  inline void update_counters(uint32_t size);

public:

  // Adds this data to the tree. Returns true is this 'addr' is already in the tree or false otherwise.
  // if it returns true, offset indicates when the object was visited;
  // konst: to make it inline, the code must be in the header.
  bool handle_data(uint64_t addr, uint32_t size, uint32_t *offset);
  bool handle_data_no_backpts(uint64_t addr, uint32_t size, uint32_t *offset);

  // Debug print of the tree contents
  void tree_print();
  void state_print(const char* tag, uint64_t addr, uint32_t size);


  inline QueueIterator<SendNode> get_sendlist_iterator(){
    QueueIterator<SendNode> it(_send_list.head());
    return it;
  }

  // TODO - rbruno - this method is not used. Delete?
  inline QueueImpl<SendNode, ResourceObj::C_HEAP, mtInternal> * get_sendlist() {
    return &_send_list;
  }

  inline uint8_t *compact() {
    uint8_t * buffer = (uint8_t*)os::malloc(get_total_length(), mtInternal);
    compact_into(buffer);
    return buffer;
  }

  inline uint8_t* compact_into(uint8_t* buffer) {
    QueueIterator<SendNode> it(_send_list.head());
    size_t copied = 0;

    const SendNode* e = it.next();

    // compact memory ranges into a single one
    while(e != NULL){
      memcpy(buffer + copied, (void*)e->addr, e->length);
      copied += e->length;
      e = it.next();
    }
    return buffer;
  }
};

#endif //JDK11_RDMA_JVM_NAOS_TRAVERSE_HPP
