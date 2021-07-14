/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Naos' graph traversal and pointer recovery
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */

#ifndef SHARE_VM_PRIMS_JVM_NAOS_HPP
#define SHARE_VM_PRIMS_JVM_NAOS_HPP

#include "memory/allocation.inline.hpp"
#include "memory/allocation.hpp"
#include "memory/resourceArea.hpp"
#include "runtime/vm_operations.hpp"

#include "runtime/vm_operations.hpp"
#include "jvm_naos_utils.hpp"
#include "jvm_naos_traverse.hpp"
#include "jvm_naos_klass_service.hpp"
#include "memory/oopFactory.hpp"
#include "utilities/hashtable.hpp"


// rdma naos entry points
jobject allocate_and_pin_buffer(unsigned long long size, uint64_t *heap_buffer);


jlong naos_get_size(jobject object, bool bfs);


void reset_and_unpin_buffer(jobject arrobj);

 
 

class SendList{

 public:
 
  GrowableArray<range_t> * const _range_send_list;

  const uint32_t _first_segment_limit;
  const uint32_t _segment_limit; // TODO is not implemented
  
  volatile bool _is_done;

  size_t _total_length; // it is the total number of bytes in the send list

  uint32_t _max_segment_length; // max length of region in the sendlist
  uint32_t _total_objects;

  SendList(): 
  _range_send_list(new (ResourceObj::C_HEAP, mtInternal) GrowableArray<range_t>(32, true)), 
  _first_segment_limit(0), _segment_limit(0) ,_is_done(false)
  {
    _total_length = 0;
    _max_segment_length = 0;
    _total_objects = 0;

  }

  // constructor used for RDMA 
  SendList(uint32_t first_segment_limit, uint32_t segment_limit):
  _range_send_list(new (ResourceObj::C_HEAP, mtInternal) GrowableArray<range_t>(32, true)), 
  _first_segment_limit(first_segment_limit), _segment_limit(segment_limit),_is_done(false)
  {
    _total_length = 0;
    _max_segment_length = 0;
    _total_objects = 0;
  }

  ~SendList() {
    delete _range_send_list;
  }

  void reset(){
    _total_length = 0;
    _max_segment_length = 0;
    _total_objects = 0;
    _range_send_list->clear();
  }

  void handle(oop object);

  uint8_t* compact();
  uint8_t* compact_into(uint8_t* buffer);
  
  uint32_t size() const { return _range_send_list->length(); }

  static int sort_f(range_t *e1,  range_t* e2){
    if(e1->addr < e2->addr){
      return -1;
    } else {
      assert(e1->addr > e2->addr, "should not be equal as we never copy the same region twice");
      return 1;
    }
  }

  // <debug> 
  // the call checks whether we could reduce the list by sorting it.
  void debug_optimize();
  // </debug>

  void print();


  void set_is_done() {
    // nothing
    Atomic::store(true, &_is_done);
  };

  bool get_is_done(){
    return Atomic::load(&_is_done);
  }

};


class OffsetReceiveMetadata {

  // offsets for back pointers
  GrowableArray<ref_offset_t>* _backoffsets;
  // size in bytes and elems of the back pointers array
  size_t _backoffsets_size_bytes;
  size_t _backoffsets_list_size;
  // helper counters
  size_t _backoffsets_list_curr;

public:
  OffsetReceiveMetadata(size_t backoffsets_size_bytes) {
    _backoffsets_size_bytes = backoffsets_size_bytes;
    _backoffsets_list_size = _backoffsets_size_bytes / sizeof(ref_offset_t);
    _backoffsets_list_curr = 0;
    _backoffsets = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<ref_offset_t>(_backoffsets_list_size, true);
  }

  OffsetReceiveMetadata() {
    _backoffsets = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<ref_offset_t>(128, true);
    _backoffsets_size_bytes = 0;
    _backoffsets_list_size = 0;
    _backoffsets_list_curr = 0;
  }

  OffsetReceiveMetadata(size_t backoffsets_size_bytes, uint8_t*  buffer) {
    _backoffsets_size_bytes = backoffsets_size_bytes;
    _backoffsets_list_size = _backoffsets_size_bytes / sizeof(ref_offset_t);
    _backoffsets_list_curr = 0;
    _backoffsets = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<ref_offset_t>(_backoffsets_list_size, true);
    memcpy(_backoffsets->data_addr(), buffer, backoffsets_size_bytes);
  }

  ~OffsetReceiveMetadata() {
    delete _backoffsets;
  }

  inline void hard_reset(){
    _backoffsets_size_bytes = 0;
    _backoffsets_list_size = 0;
    _backoffsets_list_curr = 0;
  }
 
  uint8_t * add_metadata(uint32_t size){ //  uint32_t objects
    if(size){
      uint32_t future_size = _backoffsets_list_size + (size / sizeof(ref_offset_t));
      _backoffsets->at_grow(future_size-1);
      uint8_t *p = (uint8_t *)_backoffsets->adr_at(_backoffsets_list_size);

      _backoffsets_size_bytes += size;
      _backoffsets_list_size = future_size;
      return p;
    } else {
      return NULL;
    }
  } 

  inline ref_offset_t* metadata() {
      return _backoffsets->data_addr();
  }

  inline bool is_trivial_oop(uint32_t ref_number) {
    return _backoffsets_list_curr >= _backoffsets_list_size || _backoffsets->at(_backoffsets_list_curr).ref_number != ref_number;
  }

  inline  oop fixed_oop(GrowableArray<range_t> * recv_list) {
    // TODO add sum prefix list and binary serach
    uint32_t offset = 0;
    range_t range;
    oop fixed;

    for (int i = 0; i < recv_list->length(); i++) {
      if (_backoffsets->at(_backoffsets_list_curr).offset >= offset + recv_list->at(i).length) {
        offset += recv_list->at(i).length;
      }  else {
        range = recv_list->at(i);
        break;
      }
    }

    fixed = (oop)(void*)(range.addr + _backoffsets->at(_backoffsets_list_curr).offset - offset);

    log_trace(naos) ("[OffsetReceiveMetadata] fix oop: %p (%d bytes, num %zu, offset %d) of type %p (%s)",
                     static_cast<void*>(fixed), fixed->size() * BytesPerWord, _backoffsets_list_curr, _backoffsets->at(_backoffsets_list_curr).offset, fixed->klass(), fixed->klass()->signature_name());
    _backoffsets_list_curr++;
    return fixed;
  }

};
 
class OffsetSendMetadata {
  // array of non trivial object refs
  GrowableArray<ref_offset_t>*  _traverse_list;
  // map of types used in the graph
  KlassTable* _ktable;
  // size of the header
  size_t _header_size;

public:

  OffsetSendMetadata(size_t header_bytes = 0, bool track_klasses = false) : _header_size(header_bytes / sizeof(ref_offset_t)) {
    assert(header_bytes % sizeof(ref_offset_t) == 0, "header size must be multiple of sizeof(ref_offset_t)");
    _traverse_list = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<ref_offset_t>(100, true);
    _ktable = track_klasses ? new KlassTable() : NULL;

    // reserve space for the header in the metadata buffer
    for(size_t i = 0; i < _header_size; i++){
      ref_offset_t elem(0, 0);
      _traverse_list->push(elem);
    }
  }

  ~OffsetSendMetadata() {
    if(_ktable == NULL){
      delete _ktable;
    }
    if(_traverse_list == NULL){
      delete _traverse_list;
    }
  }

  // is not required. but could be used later by buffered version
  inline void hard_reset(){
    delete _ktable;
    _ktable = new KlassTable();
  }

  inline void reset(){
    _traverse_list->trunc_to(_header_size);
  }

  inline void add_oop(uint32_t ref_number, uint32_t offset, oop o) {
    ref_offset_t elem(ref_number, offset);
    _traverse_list->push(elem);
    log_trace(naos) ("[OffsetSendMetadata] add oop: %p (%d bytes, num %d, offset %d) of type %p (%s)",
                     static_cast<void*>(o), o->size() * BytesPerWord, _traverse_list->length(), offset, o->klass(), o->klass()->signature_name());
  }

  inline void track_klass(Klass* klass) {
    if (_ktable != NULL && _ktable->lookup((int)(uintptr_t)klass) == NULL) {
      _ktable->add((int)(uintptr_t)klass, klass);
    }
  };

  inline size_t size_bytes() {
    return _traverse_list->length() * sizeof(ref_offset_t);
  }

  inline size_t offsets_size_bytes() {
    return (_traverse_list->length() - _header_size) * sizeof(ref_offset_t);
  }

  inline uint8_t* metadata() {
    return (uint8_t *)_traverse_list->data_addr();
  }

  inline KlassTable* klasses() {
    return _ktable;
  }
};

 
  


template<class T> class OopVisitStructure {
private:
#ifdef TRAVERSE_BFS
  QueueImpl<T>* _data;
#else
  GrowableArray<T>* _data;
#endif

public:
  OopVisitStructure() {
#ifdef TRAVERSE_BFS
    _data = new (ResourceObj::C_HEAP, mtInternal) QueueImpl<T>();
#else
    _data = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<T>(1024, true);
#endif
  }

  ~OopVisitStructure() {
    delete _data;
  }

  inline void push(T o) {
#ifdef TRAVERSE_BFS
    _data->enqueue(o);
#else
    _data->push(o);
#endif
  }

  inline T pop() {
#ifdef TRAVERSE_BFS
    return _data->dequeue();
#else
    return _data->pop();
#endif
  }

  inline T top() {
#ifdef TRAVERSE_BFS
    return _data->front();
#else
    return _data->top();
#endif
  }

  inline bool is_empty() { return _data->is_empty(); }

};
 
struct cached_klass_metadata_t {
  // klass pointer in the receiver (valid pointer)
  Klass* local_klass;
  // cached sender klass. This field is used for speculation.
  Klass* remote_klass;
  // size of the object pointed by this field, receiver_klass->size()
  int size;
  // number of fields;
  int num_fields;
  // offset for each field
  int* field_offsets;
  // metadata for each field
  cached_klass_metadata_t** field_metadatas;
};

struct oop_metadata_t {
  oop* obj_ptr;
  cached_klass_metadata_t** klass_metadata;
};

#include "prims/jvmtiTagMap_tools.hpp"  
class ReceiveGraph {
 private:

  KlassResolver _klassresolver;

  OopVisitStructure<oop_metadata_t> _visit; // TODO - rbruno - we need a data structure that does not move nodes.

  //Stack<oop_reference,mtInternal> _visit; // konst: we can test later to use Stack here
  
  GrowableArray<range_t> *_recv_list;
  OffsetReceiveMetadata &_recv_metadata;
  
  struct offset_range_t{
    uint32_t offset;
    uint32_t range;
  };

  uint32_t _offset;
  int _range;
  uint32_t _ref_number;
  uint32_t _obj_number;

  uint32_t _processed;

  cached_klass_metadata_t* array_hint = NULL;
  
  static int sort_f(const cached_klass_metadata_t &e1, const cached_klass_metadata_t& e2){
    if(e1.local_klass == e2.local_klass){
      return 0;
    }

    if(e1.local_klass < e2.local_klass){
      return -1;
    } else {
      return 1;
    }
  }

  GrowableArray<cached_klass_metadata_t> *_cache_field_map;

 public:
  ReceiveGraph(int fd, OffsetReceiveMetadata &metadata ) 
  : _klassresolver(fd), _visit(), _recv_metadata(metadata) 
  {
    _recv_list = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<range_t>(32, true);
    _offset = 0;
    _range = 0;
    _ref_number = 1; // count from 1 as 0 is reserved
    _obj_number = 0;
    _processed = 0;

    _cache_field_map = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<cached_klass_metadata_t>(16, true);
  }


  // for buffer version
  ReceiveGraph(KlassTable* ktable, OffsetReceiveMetadata &metadata ) 
  : _klassresolver(ktable), _visit(), _recv_metadata(metadata) 
  {
    _recv_list = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<range_t>(32, true);
    _offset = 0;
    _range = 0;
    _ref_number = 1; // count from 1 as 0 is reserved
    _obj_number = 0;
    _processed = 0;

    _cache_field_map = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<cached_klass_metadata_t>(16, true);
  }


  ~ReceiveGraph(){
    for (int i=0; i<_cache_field_map->length(); i++) {
      cached_klass_metadata_t* cf = _cache_field_map->adr_at(i);
      os::free(cf->field_offsets);
      os::free(cf->field_metadatas);
    }
    delete _recv_list;
    delete _cache_field_map;
  }

  void set_resolver(uint32_t ip, uint16_t port){
    _klassresolver.set_resolver(ip,port);
  }

  uintptr_t resolve_klass(uintptr_t klass_in_sender){
    return _klassresolver.resolve_klass(klass_in_sender);
  }

  inline void  hard_reset(){
    _recv_list->clear();
    _offset = 0;
    _range = 0;
    _ref_number = 1; // count from 1 as 0 is reserved
    _obj_number = 0;
    _processed = 0;
  }

  char* finalize();
  uint32_t do_region(range_t &range);
  uint32_t do_region_array(range_t &range, uint32_t *ind, objArrayOop array);

  inline bool is_empty() { return _visit.is_empty();}
  inline void print(uint32_t obj_num);
  inline bool fix_ref(oop_metadata_t &oop_metadata);
  inline void do_obj(oop o, cached_klass_metadata_t**);
  inline cached_klass_metadata_t* get_class_metadata(Klass* k, oop o, bool is_instance_klass);
};
 

struct visit_t{
  uint32_t order;
  uint32_t upper_bits;
};

// it is depriceted. Replaced by SendTree
class TraverseTable : public Hashtable<uint64_t,mtClass> {

  inline uint32_t hash_addr(uint64_t addr){
    return ((uint32_t)addr)>>3; // the address is always 8 bytes aligned
  };

public:
  TraverseTable() : Hashtable<uint64_t,mtClass>(1987, sizeof(HashtableEntry<uint64_t, mtClass>)) { }
 
  visit_t* add(uint64_t addr){ // it returns pointer to the entry.
    uint32_t hash = hash_addr(addr);
    uint32_t upper_bits = (uint32_t)(addr>>32);
    int index = hash_to_index(hash);

    for (HashtableEntry<uint64_t,mtClass>* e = bucket(index); e != NULL; e = e->next()) {
      if (e->hash() == hash && ((visit_t*)e->literal_addr())->upper_bits == upper_bits) {
        // found
        return (visit_t*)e->literal_addr();
      }
    }
    visit_t temp = {.order = 0, .upper_bits = upper_bits};
    HashtableEntry<uint64_t,mtClass>* e = this->new_entry(hash,(uint64_t)0);
    *((visit_t*)e->literal_addr()) = temp;
    add_entry(index, e);
    return (visit_t*)e->literal_addr();
  }

  bool contains(uint64_t addr){ // it returns pointer to the entry.
    uint32_t hash = hash_addr(addr);
    uint32_t upper_bits = (uint32_t)(addr>>32);
    int index = hash_to_index(hash);

    for (HashtableEntry<uint64_t,mtClass>* e = bucket(index); e != NULL; e = e->next()) {
      if (e->hash() == hash && ((visit_t*)e->literal_addr())->upper_bits == upper_bits) {
        // found
        return true;
      }
    }
    return false;
  }
};


class SkywayTable {
  GrowableArray<uint64_t> *id_to_klass;
  GrowableArray<int> *sorted_ind; // it is sorted index for binary search
public:
  SkywayTable() { 
    id_to_klass = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<uint64_t>(16, true);
    sorted_ind = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<int>(16, true);
  }

  ~SkywayTable(){
    delete id_to_klass;
    delete sorted_ind;
  }


  void register_class(int id, Klass* k){
    uint64_t toinsert = (uint64_t)(void*)k;

    id_to_klass->at_put_grow(id, toinsert, 0); 
    int length = id_to_klass->length();
    for(int id =0; id< length; id++){
      sorted_ind->at_put_grow(id, id, 0); 
    }

    // simple buble sort. Register class is not a bottleneck
    for (int i = 0; i < length; i++){
      for (int j = i+1; j < length; j++){
        int fake_indi = sorted_ind->at(i);
        int fake_indj = sorted_ind->at(j);
        if (id_to_klass->at(fake_indi) > id_to_klass->at(fake_indj)) {
            sorted_ind->at_put(i,fake_indj);
            sorted_ind->at_put(j,fake_indi);
        }
      }
    }

  };


  Klass* get_class(int id){
    return (Klass*)(void*)id_to_klass->at(id);
  } 
  
  int get_id(Klass* k){
    // we use binary search with sorted index
    uint64_t tofind = (uint64_t)(void*)k;
    int min = 0;
    int max = id_to_klass->length() - 1;

    while (max >= min) {
      int mid = (int)(((uint)max + min) / 2);
      int real_mid = sorted_ind->at(mid);
      uint64_t value = id_to_klass->at(real_mid);
       
      if (tofind > value) {
        min = mid + 1;
      } else if (tofind < value) {
        max = mid - 1;
      } else {
        return real_mid;
      }
    }
    const char* name = ((Klass*) k)->name()->as_C_string();
    printf("Is not registered klass %p %s\n",k,name);
    fflush(stdout);
    return -1; // not found
  } 
   
};


class TraverseObject {
 private:
 
  struct cached_field_t{
    Klass *k;
    ClassFieldMap *field_map; 
  };
  
  static int sort_f(const cached_field_t &e1, const cached_field_t& e2){
    if(e1.k == e2.k){
      return 0;
    }

    if(e1.k < e2.k){
      return -1;
    } else {
      return 1;
    }
  }

  uint32_t ref_number;
  order_t obj_number;
 
  OffsetSendMetadata &_metadata;
  SendTree &_sendtree;
 // GrowableArray<oop> *_visit_stack;
  Stack<oop,mtInternal> _visit_stack;

  QueueImpl<oop, ResourceObj::C_HEAP, mtInternal> q; // for bfs

  // TODO - rbruno - do we have a way to avoid this? This is linear search for every object...
  GrowableArray<cached_field_t> *_cache_field_map;
 public:
  TraverseObject(Handle initial_object, OffsetSendMetadata &metadata, SendTree& sendtree ) :
   ref_number(0), obj_number(1), _metadata(metadata), _sendtree(sendtree), _visit_stack()
  {
 
    _cache_field_map = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<cached_field_t>(16, true);

    _visit_stack.push(initial_object());
  }

  TraverseObject(OffsetSendMetadata &metadata, SendTree& sendtree ) :
   ref_number(0), obj_number(1), _metadata(metadata), _sendtree(sendtree), _visit_stack()
  {
    _cache_field_map = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<cached_field_t>(16, true);
  }

  void init(Handle initial_object, int array_len){

    _visit_stack.clear();
    ref_number = 0;
    obj_number = 1;

    oop obj = initial_object();
    if(array_len!=0 && obj->is_objArray()){
      log_debug(naos) ("[TraverseObject] Init interable traverse with %d elems",array_len);
      objArrayOop array = objArrayOop(obj);
      for (int index = array_len - 1; index >= 0; index--) { //inverse pass has better locality of stored objects
        oop elem = array->obj_at(index);
        if (elem != NULL) { // should be never null actually. 
          _visit_stack.push(elem);
        }
      }
    } else {
      _visit_stack.push(obj); 
    }
  }



  void visit(oop o);
 
  bool doit(uint32_t low = 0, uint32_t high = 0);

  bool doitBFS(); // only for statistics
  
  ~TraverseObject(){
    for (int i=0; i<_cache_field_map->length(); i++) {
      cached_field_t* cf = _cache_field_map->adr_at(i);
      delete cf->field_map;
    }
    delete _cache_field_map;
  }
};


// skysay use BFS as was said in the paper.
// BFS achieved via processing objects in the same order they are written to the buffer array
class SkywayTraverseObject {
 private:
  struct cached_field_t{
    Klass *k;
    ClassFieldMap *field_map; 
  };
  
  static int sort_f(const cached_field_t &e1, const cached_field_t& e2){
    if(e1.k == e2.k){
      return 0;
    }

    if(e1.k < e2.k){
      return -1;
    } else {
      return 1;
    }
  }

  SendTree &_sendtree;
  SkywayTable &_classes;
  typeArrayOop _array;

  uint32_t metadata_offset; // is required to support current sendtree
  uint32_t array_processed_offset; //  
  uint32_t array_written_offset; // 
  uint32_t array_length; // current maximum array size

 
  GrowableArray<cached_field_t> *_cache_field_map;
  JavaThread* THREAD; 
 
 public:
  SkywayTraverseObject(Handle initial_object,  SendTree& sendtree, SkywayTable &classes, typeArrayOop array, uint32_t offset) :
     _sendtree(sendtree), _classes(classes),   THREAD(NULL)
  { 
    _cache_field_map = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<cached_field_t>(16, true);
    
    init(initial_object, array,offset);
  }

  SkywayTraverseObject(SendTree& sendtree, SkywayTable &classes) :
    _sendtree(sendtree), _classes(classes),  THREAD(NULL)
  { 
    _cache_field_map = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<cached_field_t>(16, true);
  }

  void init(Handle initial_object,  typeArrayOop array, uint32_t offset){
    _array = array;
    array_length = _array->length();
    metadata_offset = offset;
    array_processed_offset = offset;
    array_written_offset = offset;

    oop obj = initial_object();
    uint64_t addr = (uint64_t) static_cast<void*>(obj); 
    uint32_t size = obj->size() * BytesPerWord;

    if(NaosDisableCycles){
      _sendtree.handle_data_no_backpts(addr,size,&offset) ;
    }
    else {
      _sendtree.handle_data(addr,size,&offset);
    }
 
    append_object_to_buffer((char*)addr,size);
  }

  uint32_t append_object_to_buffer(char* addr, uint32_t size){
    
    while(size + array_written_offset > array_length){
      // need to grow
      log_debug(naos)("[skyway] Doubled array\n");
      uint32_t new_array_length = array_length * 2;
      typeArrayOop newarray = oopFactory::new_byteArray(new_array_length, THREAD);
      memcpy(newarray->byte_at_addr(0), _array->byte_at_addr(0) , array_length);
      array_length = new_array_length;
      _array = newarray;
    }
 
    log_debug(naos)("[skyway] Copy object  %p to buffer with size %u\n",addr,size);
    memcpy(_array->byte_at_addr(0) + array_written_offset, addr, size);
    uint32_t current_offset = array_written_offset;
    array_written_offset += size;
    return current_offset;
  }

  uint32_t get_total_length(){
    assert(array_processed_offset == array_written_offset, "buffer is not fully processed");
    return array_processed_offset;
  }

 
 
  void doit(JavaThread* THREAD){
    this->THREAD = THREAD; // for memory allocation

    while (array_processed_offset < array_written_offset) {
      oop obj_in_buffer = (oop)(((char*)_array->byte_at_addr(0)) + array_processed_offset);
      uint32_t size = obj_in_buffer->size()* BytesPerWord;
      visit(obj_in_buffer);
      array_processed_offset+=size;
    }

    return;
  }

  inline void set_field(oop* obj_ptr, uint32_t offset){
    if (UseCompressedOops) {
      *(narrowOop*)obj_ptr = (narrowOop)offset;
    } else {
      *obj_ptr = (oop)(void*)(uintptr_t)offset;
    }
  }
 

  inline void process_obj_ptr(oop* obj_ptr){
    oop fld_o = (UseCompressedOops) ? CompressedOops::decode(*(narrowOop*)obj_ptr) : *obj_ptr ;
    if (fld_o != 0){
      uint32_t offset = 0;
      uint64_t addr = (uint64_t) static_cast<void*>(fld_o);
      uint32_t size = fld_o->size() * BytesPerWord;
      bool was_marked = NaosDisableCycles ? _sendtree.handle_data_no_backpts(addr,size,&offset) : _sendtree.handle_data(addr,size,&offset);
      if(was_marked){
        log_debug(naos)("[Skyway] Object was already visited at %u\n",offset);
        set_field(obj_ptr,offset + metadata_offset);
      }else{
        uint32_t offset_temp = append_object_to_buffer((char*)addr, size);
        log_debug(naos)("[Skyway] Set offset to %u\n",offset_temp);
        set_field(obj_ptr,offset_temp);
      }
    }
  }

  inline void visit(oop obj_in_buffer){

    Klass* k = obj_in_buffer->klass();
 
    if (obj_in_buffer->is_instance() && k != SystemDictionary::Class_klass()) {

      cached_field_t temp = {.k = k, .field_map = NULL};
      bool found = false;
      int location = _cache_field_map->find_sorted<cached_field_t, &sort_f>(temp, found);
      ClassFieldMap* field_map  = NULL;
      if(found){
        // take from cache
        field_map = _cache_field_map->adr_at(location)->field_map;
      }else{
        // allocate and add to cache
        field_map = ClassFieldMap::create_map_of_instance_obj_fields(obj_in_buffer);
        temp.field_map = field_map;
        _cache_field_map->insert_sorted<&sort_f>(temp);
      }

      int total = field_map->field_count();
      for (int i = 0; i < total; i++) {
        ClassFieldDescriptor* field = field_map->field_at(i);
        oop* obj_ptr = obj_in_buffer->obj_field_addr_raw<oop>(field->field_offset());
        process_obj_ptr(obj_ptr);
      }
    }
    // object array
    else if (obj_in_buffer->is_objArray()) {
      objArrayOop array = objArrayOop(obj_in_buffer);
      if(UseCompressedOops) {
        narrowOop *obj_ptr = ((narrowOop*) array->base_raw()) ;
        for (int index = 0; index < array->length(); index++, obj_ptr++) {
           process_obj_ptr((oop*)obj_ptr);
        }
      } else{
        oop *obj_ptr = ((oop*) array->base_raw());
        for (int index = 0; index < array->length(); index++, obj_ptr++) {
           process_obj_ptr(obj_ptr);
        }
      }
    } 
     // fix class in the buffer. TODO: narrowops
    { 
      int id = _classes.get_id(k);
      log_debug(naos)("[skyway] Set class %p to id %u\n",k,id);
      if (UseCompressedClassPointers) {
        *(obj_in_buffer->compressed_klass_addr()) = (narrowKlass)id; 
      } else {
        *(obj_in_buffer->klass_addr()) = (Klass*)(void*)(uint64_t)id; 
      }
    }
  }


  typeArrayOop get_buffer(){
    return _array;
  }
  
  ~SkywayTraverseObject(){
    for (int i=0; i<_cache_field_map->length(); i++) {
      cached_field_t* cf = _cache_field_map->adr_at(i);
      delete cf->field_map;
    }
    delete _cache_field_map;
  }
};



class SkywayReceiveGraph{
  SkywayTable &_classes;

  struct cached_field_t{
    Klass *k;
    ClassFieldMap *field_map; 
  };
  
  static int sort_f(const cached_field_t &e1, const cached_field_t& e2){
    if(e1.k == e2.k){
      return 0;
    }

    if(e1.k < e2.k){
      return -1;
    } else {
      return 1;
    }
  }

   GrowableArray<cached_field_t> *_cache_field_map;

  inline void fix_obj_ptr_raw(oop* obj_addr, char* base) {
    if(UseCompressedOops){
      uint32_t encoded_offset = *(narrowOop*)(obj_addr);
      if(encoded_offset != 0){
        oop value = (oop)(base + encoded_offset);
        log_debug(naos)("[skyway]Fix pointer from %u to %p\n",encoded_offset,(void*)value);
        *(narrowOop*)obj_addr = CompressedOops::encode_not_null(value);
      }
    } else {
      uint32_t encoded_offset = (uint32_t)(uintptr_t)(oop)(*obj_addr);
      if(encoded_offset != 0){
        oop value = (oop)(base + encoded_offset);
        log_debug(naos)("[skyway]Fix pointer from %u to %p\n",encoded_offset,(void*)value);
        *(oop*)obj_addr = value;
      }
    }
  }

  public:
    SkywayReceiveGraph(SkywayTable &classes): 
    _classes(classes)
    {
      _cache_field_map = new (ResourceObj::C_HEAP, mtInternal) GrowableArray<cached_field_t>(16, true);
    }

    ~SkywayReceiveGraph(){
      for (int i=0; i<_cache_field_map->length(); i++) {
        cached_field_t* cf = _cache_field_map->adr_at(i);
        delete cf->field_map;
      }
      delete _cache_field_map;
    }

    oop doit(char * buffer, uint32_t total_size){
      uint32_t offset = 0;
      log_debug(naos)("[skyway] Start recovery of buffer %p",buffer);
      while(offset < total_size){
        oop o =  (oop)(void*)(buffer + offset);
        // fix class

        int klass_id = UseCompressedClassPointers ? *(o->compressed_klass_addr()) : 
                                                        (int)(uintptr_t)o->klass();
 
        Klass* k = _classes.get_class(klass_id);
        o->set_klass(k);
        log_debug(naos)("[skyway] Received object with klass id %d set to %p\n", klass_id,k);

        // fix all pointers
        fix_pointers(o, buffer - 8); // 8 is the shift due to header

        offset+=o->size()*BytesPerWord;

      }
      return (oop)(void*)(buffer);
    }

    void fix_pointers(oop o, char* base){
      Klass* k = o->klass();

      if (o->is_instance() && k != SystemDictionary::Class_klass()) {
        cached_field_t temp = {.k = k, .field_map = NULL};
        bool found = false;
        int location = _cache_field_map->find_sorted<cached_field_t, &sort_f>(temp, found);
        ClassFieldMap* field_map  = NULL;
        if(found){
          // take from cache
          field_map = _cache_field_map->adr_at(location)->field_map;
        }else{
          // allocate and add to cache
          field_map = ClassFieldMap::create_map_of_instance_obj_fields(o);
          temp.field_map = field_map;
          _cache_field_map->insert_sorted<&sort_f>(temp);
        }
        int total = field_map->field_count();
   
        for (int i = 0; i < total; i++) {
          ClassFieldDescriptor* field = field_map->field_at(i);
          oop* ptr = o->obj_field_addr_raw<oop>(field->field_offset());
          fix_obj_ptr_raw(ptr, base);
        }
      }
      // object array
      else if (o->is_objArray()) {
        objArrayOop array = objArrayOop(o);
        if(UseCompressedOops) {
          narrowOop *ptr = ((narrowOop *) array->base_raw()) ;
          for (int index = 0; index < array->length(); index++, ptr++) {
            fix_obj_ptr_raw((oop*)ptr, base);
          }
        } else{
          oop *ptr = ((oop *) array->base_raw()) ;
          for (int index = 0; index < array->length(); index++, ptr++) {
            fix_obj_ptr_raw(ptr, base);
          }
        }
      }
     
    }
};


#endif // SHARE_VM_PRIMS_JVM_NAOS_HPP
