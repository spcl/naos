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
#include "jvm_naos_traverse.hpp"
#include "logging/log.hpp"

inline SendNode* SendTree::tree_find(uint64_t addr) {
  CRBNode* i = _tree.root;
  SendNode* entry;

  while (i) {
    entry = c_rbnode_entry(i, SendNode, rb);

    if (addr < entry->addr)
      i = i->left;
    else if (addr >= (entry->addr + entry->length))
      i = i->right;
    else
      return entry;
  }

  return NULL;
}
 

inline SendNode* SendTree::tree_add(SendNode *node) {
  CRBNode *parent, **i;
  SendNode *entry;

  parent = NULL;
  i = &_tree.root;

  while (*i) {
    parent = *i;
    entry = c_rbnode_entry(*i, SendNode, rb);

    if (node->addr < entry->addr)
      i = &parent->left;
    else if (node->addr >= (entry->addr + entry->length))
      i = &parent->right;
    else
      return entry;
  }

  c_rbtree_add(&_tree, parent, i, &node->rb);
  return node;
}

inline bool SendTree::tree_add_next(SendNode* existing_node, SendNode* new_node) {
  CRBNode *parent, **i;
  SendNode *entry;

  parent = &existing_node->rb;
  i = &existing_node->rb.right;

  while (*i) {
    parent = *i;
    entry = c_rbnode_entry(*i, SendNode, rb);

    if (new_node->addr < entry->addr)
      i = &parent->left;
    else if (new_node->addr > entry->addr)
      i = &parent->right;
    else
      return false;
  }

  c_rbtree_add(&_tree, parent, i, &new_node->rb);
  return true;
}

inline bool SendTree::tree_add_prev(SendNode* existing_node, SendNode* new_node) {
  CRBNode *parent, **i;
  SendNode *entry;

  parent = &existing_node->rb;
  i = &existing_node->rb.left;

  while (*i) {
    parent = *i;
    entry = c_rbnode_entry(*i, SendNode, rb);

    if (new_node->addr < entry->addr)
      i = &parent->left;
    else if (new_node->addr > entry->addr)
      i = &parent->right;
    else
      return false;
  }

  c_rbtree_add(&_tree, parent, i, &new_node->rb);
  return true;
}

inline SendNode* SendTree::tree_find_next(SendNode* node) {
  return c_rbnode_entry(c_rbnode_next(&node->rb), SendNode, rb);
}

inline SendNode* SendTree::tree_find_prev(SendNode* node) {
  return c_rbnode_entry(c_rbnode_prev(&node->rb), SendNode, rb);
}

void SendTree::tree_print() {
  SendNode* i;
  c_rbtree_for_each_entry(i, &_tree, rb) {
    log_debug(naos) ("SendNode addr = %lu ; len = %u ; offset = %u",i->addr, i->length, i->offset);
  }
}

inline void SendTree::update_counters(uint32_t size) {
  _offset += size;
  _current_sendlist_length += size;
  _total_objects++;
  _current_sendlist_objects++;
}

inline bool SendTree::add_slow_path(uint64_t addr, uint32_t size, uint32_t *offset) {
  SendNode node = {.addr = addr, .length = size, .offset = _offset};
  QueueNode<SendNode>* qnode = _send_list.new_node(node);
  SendNode* newnode = qnode->data_ref();
  SendNode* oldnode = tree_add(newnode);
  // try insertion, if returns newnode, then success, otherwise, repeated node
  if (oldnode == newnode) {
    _send_list.enqueue(qnode);
    _curr = newnode;
    _next = tree_find_next(_curr);
    _prev = tree_find_prev(_curr);
    _num_segments++;
    update_counters(size);
    return false;
  } else {
    _send_list.delete_node(qnode);
    // return offset to the visited object
    *offset = oldnode->offset + (uint32_t)(oldnode->addr - addr);
    return true;
  }
}

void SendTree::state_print(const char* tag, uint64_t addr, uint32_t size) {
  void* addrptr = (void*) addr;
  log_trace(naos) ("%s %p prev : (addr=%p/size=%u) curr : (addr=%p/size=%u) next : (addr=%p/size=%u)",
          tag,
          addrptr,
          _prev == NULL ? NULL : (void*)_prev->addr, _prev == NULL ? 0 : _prev->length,
          _curr == NULL ? NULL : (void*)_curr->addr, _curr == NULL ? 0 : _curr->length,
          _next == NULL ? NULL : (void*)_next->addr, _next == NULL ? 0 : _next->length);
}

bool SendTree::handle_data_no_backpts(uint64_t addr, uint32_t size, uint32_t *offset) {
  // increase the size of the current segment or create a new one
  if (_curr != NULL && addr == _curr->addr + _curr->length) {
    _curr->length += size;
  } else {
    SendNode node = {.addr = addr, .length = size, .offset = _offset};
    _send_list.enqueue(node);
    _curr = _send_list.adr_tail();
    _num_segments++;
  }
  update_counters(size);
  return false;
}

bool SendTree::handle_data(uint64_t addr, uint32_t size, uint32_t *offset) {

  // first element
  if(_curr == NULL) {
    return add_slow_path(addr, size, offset);
  }

  assert(_curr == _send_list.adr_tail(), "curr does not match list.tail!");
  assert(_next == tree_find_next(_curr), "next does not match curr.next!");
  assert(_prev == tree_find_prev(_curr), "prev does not match curr.prev!");

  // fast path, just increase length
  if (addr == _curr->addr + _curr->length && (_next == NULL || addr != _next->addr)) {
    _curr->length += size;
    update_counters(size);
    return false;
  }

  // fast paths, add included in curr, prev, or next
  if (addr >= _curr->addr && addr < _curr->addr + _curr->length) {
    *offset = _curr->offset + (uint32_t)(_curr->addr - addr);
    return true;
  }
  if (_prev != NULL && addr >= _prev->addr && addr < _prev->addr + _prev->length) {
    *offset = _prev->offset + (uint32_t)(_prev->addr - addr);
    return true;
  }
  if (_next != NULL && addr >= _next->addr && addr < _next->addr + _next->length) {
    *offset = _next->offset + (uint32_t)(_next->addr - addr);
    return true;
  }

  // warm path, add right between curr and next
  if (addr > _curr->addr && (_next == NULL || addr < _next->addr)) {
    SendNode node = {.addr = addr, .length = size, .offset = _offset};
    _send_list.enqueue(node);
    SendNode* newcurr = _send_list.adr_tail();
    tree_add_next(_curr, newcurr);
    assert(tree_find_prev(newcurr) == _curr, "cur.prev does not match old curr");
    assert(tree_find_next(newcurr) == _next, "cur.next does not match old _next");
    _prev = _curr;
    _curr = newcurr;
    _num_segments++;
    update_counters(size);
    return false;
  }
  // warm path, add right between prev and curr
  if (addr < _curr->addr && (_prev == NULL || addr > _prev->addr)) {
    SendNode node = {.addr = addr, .length = size, .offset = _offset};
    _send_list.enqueue(node);
    SendNode* newcurr = _send_list.adr_tail();
    tree_add_prev(_curr, newcurr);
    assert(tree_find_next(newcurr) == _curr, "cur.next does not match old curr");
    assert(tree_find_prev(newcurr) == _prev, "cur.prev does not match old _prev");
    _next = _curr;
    _curr = newcurr;
    _num_segments++;
    update_counters(size);
    return false;
  }

  return add_slow_path(addr,size, offset);
}
