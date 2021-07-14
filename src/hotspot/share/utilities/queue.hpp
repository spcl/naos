
#ifndef SHARE_VM_UTILITIES_QUEUE_HPP
#define SHARE_VM_UTILITIES_QUEUE_HPP

#include "memory/allocation.hpp"

/*
 * The implementation of a queue based on a linked list, which uses various
 * backing storages, such as C heap, arena and resource, etc.
 */

template <class E> class QueueNode : public ResourceObj {
 private:
  E                  _data;  // embedded content
  QueueNode<E>*      _next;  // next entry

 public:
  QueueNode() : _next(NULL) { }


  QueueNode(const E e): _data(e), _next(NULL) { }

  inline void set_next(QueueNode<E>* node) { _next = node; }
  inline QueueNode<E> * next() const       { return _next; }

  E  data() { return _data; }

  E*  data_ref() { return &_data; }
};


// A Queue implementation.
// The Queue can be allocated in various type of memory: C heap, arena and resource area, etc.
template <class E, ResourceObj::allocation_type T = ResourceObj::C_HEAP,
  MEMFLAGS F = mtNMT, AllocFailType alloc_failmode = AllocFailStrategy::RETURN_NULL>
  class QueueImpl : public ResourceObj {
 protected:
  uint32_t _size;
  Arena*                 _arena;
  QueueNode<E>*    _head;
  QueueNode<E>*    _tail;

  inline void set_head(QueueNode<E>* h) { _head = h; }
  inline void set_tail(QueueNode<E>* h) { _tail = h; }

 public:
  QueueImpl() :  _arena(NULL),_head(NULL),_tail(NULL) { }
  QueueImpl(Arena* a) : _arena(a),_head(NULL),_tail(NULL) { }

  inline QueueNode<E>* head() const     { return _head; }
  inline QueueNode<E>* tail() const     { return _tail; }
  inline bool is_empty()           const     { return _head == NULL; }

  E front(){
    return this->head()->data();
  }

  E* adr_front(){
    return this->head()->data_ref();
  }

  E* adr_tail(){
    return this->tail()->data_ref();
  }

  E* adr_at(uint32_t ind){
    assert(_size > ind , "out of range");
    QueueNode<E>* p = this->head();
    for(uint32_t i=0; i < ind; i++){
      p = p->next();
    }
    return p->data_ref();
  }

  ~QueueImpl() {
    clear();
  }

  void clear() {
    QueueNode<E>* p = this->head();
    this->_size = 0;
    this->set_head(NULL);
    this->set_tail(NULL);
    while (p != NULL) {
      QueueNode<E>* to_delete = p;
      p = p->next();
      delete_node(to_delete);
    }
  }

  E dequeue() {
    QueueNode<E>* h = this->head();
    assert(h != NULL, "Queue is empty");
    this->_size--;
    this->set_head(h->next());
    E e = h->data();
    if(this->tail() == h){
      this->set_tail(NULL);
    }
    delete_node(h);
    return e;
  }

  void move(QueueImpl<E,T,F>* list) {
    assert(list->storage_type() == this->storage_type(), "Different storage type");
    QueueNode<E>* node = list->head();
    if(node == NULL){
      return;
    }

    if(this->tail() == NULL){
      assert(this->head() == NULL, "must be also NULL");
      this->set_head(node);
    } else {
      this->tail()->set_next(node);
    }
    this->set_tail(list->tail());

    this->_size+=list->_size;
    // All entries are moved
    list->_size = 0;
    list->set_head(NULL);
    list->set_tail(NULL);
  }


  bool enqueue(const E e)  {
    return enqueue(this->new_node(e));
  }

  bool enqueue(QueueNode<E>* node)  {
    if (node == NULL) {
      return false;
    }
    this->_size++;
    if(this->tail() == NULL){
      assert(this->head() == NULL, "must be also NULL");
      this->set_head(node);
      this->set_tail(node);
    } else {
      this->tail()->set_next(node);
      this->set_tail(node);
    }

    return true;
  }

  E* enqueue_empty()  {
    QueueNode<E>* node = this->new_node();
    if (node == NULL) {
      return NULL;
    } 
    this->_size++;
    if(this->tail() == NULL){
      assert(this->head() == NULL, "must be also NULL");
      this->set_head(node);
      this->set_tail(node);
    } else {
      this->tail()->set_next(node);
      this->set_tail(node);
    }
    return node->data_ref();
  }

  DEBUG_ONLY(ResourceObj::allocation_type storage_type() { return T; })

  // Create new queue node object in specified storage
  QueueNode<E>* new_node(const E e) const {
     switch(T) {
       case ResourceObj::ARENA: {
         assert(_arena != NULL, "Arena not set");
         return new(_arena) QueueNode<E>(e);
       }
       case ResourceObj::RESOURCE_AREA:
       case ResourceObj::C_HEAP: {
         if (alloc_failmode == AllocFailStrategy::RETURN_NULL) {
           return new(std::nothrow, T, F) QueueNode<E>(e);
         } else {
           return new(T, F) QueueNode<E>(e);
         }
       }
       default:
         ShouldNotReachHere();
     }
     return NULL;
  }

  QueueNode<E>* new_node( ) const {
     switch(T) {
       case ResourceObj::ARENA: {
         assert(_arena != NULL, "Arena not set");
         return new(_arena) QueueNode<E>( );
       }
       case ResourceObj::RESOURCE_AREA:
       case ResourceObj::C_HEAP: {
         if (alloc_failmode == AllocFailStrategy::RETURN_NULL) {
           return new(std::nothrow, T, F) QueueNode<E>( );
         } else {
           return new(T, F) QueueNode<E>( );
         }
       }
       default:
         ShouldNotReachHere();
     }
     return NULL;
  }

  // Delete queue node object
  void delete_node(QueueNode<E>* node) {
    if (T == ResourceObj::C_HEAP) {
      delete node;
    }
  }
};

 // Iterates all entries in the list
template <class E> class QueueIterator : public StackObj {
 private:
  QueueNode<E>* _p;
  bool               _is_empty;
 public:
  QueueIterator(QueueNode<E>* head) : _p(head) {
    _is_empty = (head == NULL);
  }

  bool is_empty() const { return _is_empty; }

  const E* next() {
    if (_p == NULL) return NULL;
    const E* e = _p->data_ref();
    _p = _p->next();
    return e;
  }
};

#endif
