/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Implementation of graph traversal and pointer recovery in Naos
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
#include "precompiled.hpp"
#include "classfile/systemDictionary.hpp"
#include "classfile/vmSymbols.hpp"
#include "jvmtifiles/jvmtiEnv.hpp"
#include "logging/log.hpp"
#include "memory/resourceArea.hpp"
#include "oops/objArrayKlass.hpp"
#include "prims/jvmtiEventController.hpp"
#include "prims/jvmtiExport.hpp"
#include "prims/jvmtiImpl.hpp"
#include "prims/jvmtiTagMap.hpp"
#include "runtime/biasedLocking.hpp"
#include "runtime/jniHandles.inline.hpp"
#include "runtime/mutexLocker.hpp"
#include "runtime/vmThread.hpp"
#include "runtime/vm_operations.hpp"
#include "utilities/macros.hpp"
#include "utilities/exceptions.hpp"
#include "utilities/queue.hpp"
#include "utilities/globalDefinitions.hpp"
#include "classfile/systemDictionaryShared.hpp"
#include "prims/jvm_naos.hpp"
#include "prims/jvmtiTagMap_tools.hpp"

#include <string.h>



static int alive_regions = 0;

jobject allocate_and_pin_buffer(unsigned long long size, uint64_t *heap_buffer){

  log_debug(naos) ("[allocate_and_pin_buffer] %llu us", size);

  JavaThread* THREAD = JavaThread::current();
  // allocate on-heap buffer
  //typeArrayOop array = oopFactory::new_byteArray(size, THREAD);
  typeArrayOop array = TypeArrayKlass::cast(Universe::byteArrayKlassObj  ())->allocate_common(size, false, THREAD);


  // make local, destroy_local
  jobject arrobj = JNIHandles::make_global(Handle(THREAD, array), AllocFailStrategy::RETURN_NULL);
  //jobject arrobj = JNIHandles::make_weak_global(Handle(THREAD, array), AllocFailStrategy::RETURN_NULL);
  //jobject arrobj = JNIHandles::make_local(THREAD, array);
  if(arrobj == NULL){
    printf("failed to make global after %d regions\n",alive_regions);
    fflush(stdout);
    return NULL;
  }
  alive_regions++;

  log_debug(naos) ("[allocate_and_pin_buffer] try to pin %p", array);
  Universe::heap()->pin_object(THREAD, array);

  *heap_buffer = (uint64_t)(char*)array->byte_at_addr(0);

  return arrobj;
}


// function is not finished yet.
void reset_and_unpin_buffer(jobject arrobj){
  JavaThread* THREAD = JavaThread::current();
  log_debug(naos) ("[reset_and_unpin_buffer] %p\n",arrobj);
  // allocate on-heap buffer

  //int header_size = arrayOopDesc::header_size(T_BYTE) * HeapWordSize;
  //typeArrayOop array = (typeArrayOop)(void*)(address - (uint32_t)header_size); //

  typeArrayOop array = (typeArrayOop)JNIHandles::resolve(arrobj);
  Universe::heap()->unpin_object(THREAD, array);
//  printf("Length was %d\n",array->length());
  array->set_length(0);
  JNIHandles::destroy_global(arrobj);

  alive_regions--;
  // konst: I think we have to check how to correctly reset buffer and memory
}


jlong naos_get_size(jobject object, bool bfs){
  JavaThread* THREAD = JavaThread::current();
  //MutexLocker ml(Heap_lock); // konst: do we need it? 
 
  oop obj = JNIHandles::resolve(object);
  Handle initial_object(THREAD, obj);

  SendTree sendtree;
  OffsetSendMetadata metadata; 
  TraverseObject send_op(initial_object, metadata, sendtree); 
  
  uint64_t time = getTimeMicroseconds();

  if(bfs)
    send_op.doitBFS(); // used only for traversal
  else
    send_op.doit();

  uint64_t ttime = getTimeMicroseconds();
  printf("[getsize] traversal %s result object=%p, length=%u, numobjects=%u, send segments=%d, metadata=%zu, time=%zu\n", 
    bfs? "bfs" : "dfs",  object,sendtree.get_total_length(), sendtree.get_total_objects(), 
    sendtree.get_num_segments(), metadata.size_bytes(), ttime - time);
  
  return sendtree.get_total_length(); 
}



void SendList::handle(oop o) {
  uint64_t addr = (uint64_t) static_cast<void*>(o);
  size_t len = o->size() * BytesPerWord;
  _total_length += len;
  _total_objects++;

  // TODO. add region limit.
  if(!_range_send_list->is_empty()
 //    &&  !( _first_segment_limit < _total_length && _first_segment_limit >= (_total_length - len) ) // we cannot merge as it will break _first_segment_limit
     )
  {
    // try to merge
    range_t *last = _range_send_list->adr_at(_range_send_list->length() - 1);
    // TODO [rbruno] - wouldn't this be the place to add the tradeoff of sending unnecessary data?
    if(last->addr + last->length == addr){
      last->length+=len;
      if(_max_segment_length < last->length){
        _max_segment_length = last->length;
      }
      return;
    }
  }


  if(_max_segment_length < len){
    _max_segment_length = len;
  }

  range_t r(addr, len);
  _range_send_list->push(r );
  return;
}

uint8_t *SendList::compact() {
  uint8_t * buffer = (uint8_t*)os::malloc(_total_length, mtInternal);
  compact_into(buffer);
  return buffer;
}

uint8_t* SendList::compact_into(uint8_t* buffer)
{
  size_t copied = 0;

  // compact memory ranges into a single one
  for(int i = 0; i < _range_send_list->length(); i++ ){
    range_t *e = _range_send_list->adr_at(i);
    memcpy(buffer + copied, (void*)e->addr, e->length);
    copied += e->length;
  }
  // clear the ranges
  _range_send_list->clear();

  return buffer;
}

// warining. it sorts the list which changes the order of buffers
void SendList::debug_optimize() {
  _range_send_list->sort(&sort_f);
  uint32_t merged = 0;
  for(int i = 0; i < _range_send_list->length() - 1; i++ ){
    range_t *e1 = _range_send_list->adr_at(i);
    range_t *e2 = _range_send_list->adr_at(i + 1);
    if(e1->addr+e1->length == e2->addr){
      merged++;
    }
  }
  if(merged>0){
    log_debug(naos) ("We could optimize the list and make it shorter by %u elems",merged);
  }
}

void SendList::print() {
  int len =  _range_send_list->length();
  log_debug(naos) ("naos send list has %d elements",len);
  log_debug(naos) ("The longest segment is %u",_max_segment_length);
  for(int i = 0; i < len; i++ ){
    range_t *e = _range_send_list->adr_at(i);
    log_debug(naos) ("[%d] start %lx, length %u\n",i,e->addr,e->length);
  }
}



bool TraverseObject::doit(uint32_t low , uint32_t high ) { // Konst: high is unused
  // visit each object until all reachable objects have been visited
  while (!_visit_stack.is_empty() && (low==0 || _sendtree.get_current_length() < low) ) {

    //if(ref_number % 100000 == 0 || obj_number % 100000 ==0){
    //  log_debug(naos) ("[TraverseObject] visited %u %u \n",ref_number, obj_number);
    //}
    oop o = _visit_stack.pop();
    uint32_t offset = 0; // will be fetched from _sendtree.
    uint64_t addr = (uint64_t) static_cast<void*>(o);
    uint32_t size = o->size() * BytesPerWord;
    bool was_marked = NaosDisableCycles ? _sendtree.handle_data_no_backpts(addr,size,&offset) : _sendtree.handle_data(addr,size,&offset);

    ref_number++;

    if (!was_marked) {
     // printf("New object %lx, len %u ObjId[%u] RefId[%u] \n",addr,size,obj_number,ref_number);
      visit(o);
      obj_number++;
    } else{
     // printf("%lx was already visited at %u. RefId[%u]\n",addr,offset,ref_number);
      _metadata.add_oop(ref_number, offset, o);
    }

    log_trace(naos) ("[TraverseObject] %s oop: %p (%d bytes, num %d) of type %p (%s)",
                     was_marked ? "marked" : "unmarked", static_cast<void*>(o), o->size() * BytesPerWord, ref_number, o->klass(), o->klass()->signature_name());

  }

  log_debug(naos) ("[TraverseObject] So far discovered references %d (%d of which are backedges), %d mem segments, %u bytes, %u objects",
   ref_number, ref_number - _sendtree.get_total_objects(), _sendtree.get_num_segments(), _sendtree.get_total_length(),obj_number);

  return (_visit_stack.is_empty());
}



void TraverseObject::visit(oop o) {
  // instance (ignoring java.lang.Class)
  if (o->is_instance() && o->klass() != SystemDictionary::Class_klass()) {

    Klass* k = o->klass();
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
      oop fld_o = o->obj_field(field->field_offset());
      if (fld_o != NULL) {
        assert(Universe::heap()->is_in_reserved(fld_o), "oop is outsite the heap?!");
        _visit_stack.push(fld_o);
      }
    }
  }
  // object array
  else if (o->is_objArray()) {
    objArrayOop array = objArrayOop(o);
    for (int index = array->length() - 1; index >= 0; index--) { //inverse pass has better locality of stored objects
      oop elem = array->obj_at(index);
      if (elem != NULL) {
        _visit_stack.push(elem);
      }
    }
  }
  // type array
  else if (o->is_typeArray()) {
    log_trace(naos) ("[TraverseObject] ignoring %s type artay oop: %p (%d bytes) of type %p (%s)",
                     o->mark()->is_marked() ? "marked" : "unmarked", static_cast<void*>(o), o->size() * BytesPerWord, o->klass(), o->klass()->signature_name());
  }
}



uint32_t ReceiveGraph::do_region(range_t &range) {
  _processed = 0;

  // if it is the first range
  if(_recv_list->length() == 0){
    oop o =  (oop)(void*)(range.addr);

    cached_klass_metadata_t* metadata = NULL;
    do_obj(o, &metadata);

    uint32_t temp = (o)->size() * BytesPerWord;
    _offset += temp;
    _processed+= temp;
    if(_offset >= range.length){
      _range += 1;
      _offset = 0;
    }
    _obj_number++;
    log_debug(naos) ("[ReceiveGraph] %s oop: %p (%d bytes, num %d) of type %p (%s)",
                     "unmarked", static_cast<void*>(o), o->size() * BytesPerWord, 
                     _ref_number, metadata == NULL ? NULL : metadata->local_klass, o->klass()->signature_name());
  }

  _recv_list->push(range);

  while (!_visit.is_empty()) {
    oop_metadata_t opp_metadata = _visit.pop();
    if (!fix_ref(opp_metadata)) {
      //log_debug(naos) ("[ReceiveGraph] reference was not fixed as we don't have required range");
      _visit.push(opp_metadata);
      return _processed;
    }
  }
  return _processed;
}

uint32_t ReceiveGraph::do_region_array(range_t &range, uint32_t *ind, objArrayOop array){
  //return do_region(range); // no implementation case

  _processed = 0;
  _recv_list->push(range);


  while(_processed < range.length || !_visit.is_empty()){
 
    while (!_visit.is_empty()) {
      oop_metadata_t opp_metadata = _visit.pop();
      if (!fix_ref(opp_metadata)) {
        log_debug(naos) ("[ReceiveGraph] reference was not fixed as we don't have required range");
        _visit.push(opp_metadata);
        return _processed;
      }
    }
    // it means array is empty. we recovered   
    
    if (_range != _recv_list->length()){ // it means can push unprocessed object
      log_debug(naos) ("[ReceiveGraphArray] push next element array[%d]",*ind); 

      oop o = (oop)(void*)(_recv_list->adr_at(_range)->addr + _offset);
      do_obj(o, &array_hint);

      array->obj_at_put((*ind)++, o); // for array 

      uint32_t temp = (o)->size() * BytesPerWord;
      _offset += temp;
      _processed+= temp;
      if(_offset >= range.length){
        _range += 1;
        _offset = 0;
      }
      _obj_number++;
    } else {
      log_debug(naos) ("[ReceiveGraphArray] reached end of segment"); 
    }  
  }

  return _processed;
}


char* ReceiveGraph::finalize() {
  log_debug(naos) ("[ReceiveGraph] recovered %u objects out of %u with %u references ", _obj_number,_obj_number,_ref_number );
  return (char*)_recv_list->adr_at(0)->addr;
}

inline void ReceiveGraph::print(uint32_t obj_num) {
  range_t* cur_range = _recv_list->adr_at(0);
  uint32_t current_offset = 0;
  for(uint32_t i=0;i<obj_num;i++){
    oop obj = (oop)(void*)(cur_range->addr + current_offset);
    obj->print();
    current_offset+=obj->size() * BytesPerWord;
    if(current_offset >= cur_range->length ){
      current_offset = 0;
      cur_range+=1;
    }
  }
}

inline void store_oop_raw(oop* obj_addr, oop value) {
  if (UseCompressedOops) {
    *(narrowOop*)obj_addr = CompressedOops::encode_not_null(value);
  } else {
    *(oop*)obj_addr = (oop)value;
  }
}

inline bool ReceiveGraph::fix_ref(oop_metadata_t &oop_metadata) {
  oop fixed_ref = NULL;
  bool trivial_oop = _recv_metadata.is_trivial_oop(++_ref_number);

  if (trivial_oop) {

    if (_range == _recv_list->length()) { // do we have required buffer?
      _ref_number--;
      return false;
    }

    fixed_ref = (oop)(void*)(_recv_list->adr_at(_range)->addr + _offset);

    do_obj(fixed_ref, oop_metadata.klass_metadata);

    uint32_t temp = fixed_ref->size() * BytesPerWord;
    _offset += temp;
    _processed+=temp;
    if (_offset >= _recv_list->adr_at(_range)->length) {
      _range += 1;
      _offset = 0;
    }
    _obj_number++;
  } else{
    fixed_ref = _recv_metadata.fixed_oop(_recv_list);
  }

  store_oop_raw(oop_metadata.obj_ptr, fixed_ref);
 
  log_trace(naos) ("[ReceiveGraph] %s oop: %p (%d bytes, num %d) of type %p (%s)",
                  trivial_oop ? "unmarked" : "marked", static_cast<void*>(fixed_ref), fixed_ref->size() * BytesPerWord, _ref_number, fixed_ref->klass(), fixed_ref->klass()->signature_name());
  //fixed_ref->print(); // If you want more details about the object, uncomment the following line.
  return true;
}

inline cached_klass_metadata_t* ReceiveGraph::get_class_metadata(Klass* k, oop o, bool is_instance_klass) {
  cached_klass_metadata_t temp = { .local_klass = k, .remote_klass = NULL };
  cached_klass_metadata_t* metadata  = NULL;
  bool found = false;
  int location = _cache_field_map->find_sorted<cached_klass_metadata_t, &sort_f>(temp, found);

  if (found) {
    metadata = _cache_field_map->adr_at(location);
  } else if (is_instance_klass) {
    ClassFieldMap* field_map = ClassFieldMap::create_map_of_instance_obj_fields(o);
    temp.num_fields = field_map->field_count();
    temp.field_offsets = (int*)os::malloc(temp.num_fields * sizeof(int), mtInternal);
    temp.field_metadatas = (cached_klass_metadata_t**)os::malloc(temp.num_fields * sizeof(cached_klass_metadata_t*), mtInternal);
    for (int i = 0; i < temp.num_fields; i++) {
      temp.field_offsets[i] = field_map->field_at(i)->field_offset();
      temp.field_metadatas[i] = NULL;
    }
    // TODO - this part could be optimized, we are paying 2x O(logn) instead of just 1x
    _cache_field_map->insert_sorted<&sort_f>(temp);
    location = _cache_field_map->find_sorted<cached_klass_metadata_t, &sort_f>(temp, found);
    metadata = _cache_field_map->adr_at(location);
  } else {
    temp.num_fields = 1;
    temp.field_offsets = (int*)os::malloc(sizeof(int), mtInternal);
    temp.field_metadatas = (cached_klass_metadata_t**)os::malloc(sizeof(cached_klass_metadata_t*), mtInternal);
    temp.field_offsets[0] = 0;
    temp.field_metadatas[0] = NULL;
    // TODO - this part could be optimized, we are paying 2x O(logn) instead of just 1x
    _cache_field_map->insert_sorted<&sort_f>(temp);
    location = _cache_field_map->find_sorted<cached_klass_metadata_t, &sort_f>(temp, found);
    metadata = _cache_field_map->adr_at(location);
  }

  assert(found, "didn't find klass metadata?");
  return metadata;
}

inline void ReceiveGraph::do_obj(oop o, cached_klass_metadata_t** metadata_ptr) {

  cached_klass_metadata_t* metadata = *metadata_ptr;
  // fix class of new object
  uintptr_t klass_in_sender = (uintptr_t)o->klass();

  if (metadata != NULL && metadata->remote_klass == (Klass*) klass_in_sender) {
    o->set_klass(metadata->local_klass);
  } else {
    uintptr_t klass = _klassresolver.resolve_klass(klass_in_sender);
    o->set_klass((Klass*)klass);
    if (metadata != NULL) {
      metadata->remote_klass = (Klass*) klass_in_sender;
    }
  }

  Klass* k = o->klass();

  if (o->is_instance() && k != SystemDictionary::Class_klass()) {

    // check if we have the correct metadata, otherwise, fetch new one and cache it
    if (metadata == NULL || metadata->local_klass != k) {
      *metadata_ptr = metadata = get_class_metadata(k, o, true);
    }

    if (UseCompressedOops) {
      for (int i = 0; i < metadata->num_fields; i++) {
        narrowOop * ptr = o->obj_field_addr_raw<narrowOop>(metadata->field_offsets[i]);
        if (*ptr != 0){
          oop_metadata_t tmp = { .obj_ptr = (oop*)ptr, .klass_metadata = &metadata->field_metadatas[i]};
          _visit.push(tmp);
        }
      }
    } else {
      for (int i = 0; i < metadata->num_fields; i++) {
        oop* ptr = o->obj_field_addr_raw<oop>(metadata->field_offsets[i]);
        if (*ptr != NULL){
          oop_metadata_t tmp = { .obj_ptr = (oop*)ptr, .klass_metadata = &metadata->field_metadatas[i]};
          _visit.push(tmp);
        }
      }
    }
  }

  // object array
  if (o->is_objArray()) {
    objArrayOop array = objArrayOop(o);

    // check if we have the correct metadata, otherwise, fetch new one and cache it
    // Note: we use the same field metadata for all entries of an array
    if (metadata == NULL || metadata->local_klass != k) {
      *metadata_ptr = metadata = get_class_metadata(k, o, false);
    }

    if(UseCompressedOops) {
      narrowOop *ptr = ((narrowOop *) array->base_raw()) + array->length() - 1;
      for (int index = array->length() - 1; index >= 0; index--, ptr--) {
        if (*ptr != 0) {
          oop_metadata_t tmp = { .obj_ptr = (oop*)ptr, .klass_metadata = &metadata->field_metadatas[0]};
          _visit.push(tmp);
        }
      }
    } else {
      oop *ptr = ((oop *) array->base_raw()) + array->length() - 1;
      for (int index = array->length() - 1; index >= 0; index--, ptr--) {
        if (*ptr != NULL) {
          oop_metadata_t tmp = { .obj_ptr = (oop*)ptr, .klass_metadata = &metadata->field_metadatas[0]};
          _visit.push(tmp);
        }
      }
    }
  } // if (o->is_objArray())
} // do_obj



bool TraverseObject::doitBFS( ) { // for statistics
 
  
  q.enqueue(_visit_stack.pop());

  while (!q.is_empty()) {
    oop o = q.dequeue();
    uint32_t offset = 0; // will be fetched from _sendtree.
    uint64_t addr = (uint64_t) static_cast<void*>(o);
    uint32_t size = o->size() * BytesPerWord;
    bool was_marked = NaosDisableCycles ? _sendtree.handle_data_no_backpts(addr,size,&offset) : _sendtree.handle_data(addr,size,&offset);

    ref_number++;

    if (!was_marked) {
     // printf("New object %lx, len %u ObjId[%u] RefId[%u] \n",addr,size,obj_number,ref_number);
        if (o->is_instance() && o->klass() != SystemDictionary::Class_klass()) {

          Klass* k = o->klass();
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
            oop fld_o = o->obj_field(field->field_offset());
            if (fld_o != NULL) {
              assert(Universe::heap()->is_in_reserved(fld_o), "oop is outsite the heap?!");
              q.enqueue(fld_o);
            }
          }
        }
        else if (o->is_objArray()) {         // object array
          objArrayOop array = objArrayOop(o);
          for (int index = 0; index < array->length(); index++) { //inverse pass has better locality of stored objects
            oop elem = array->obj_at(index);
            if (elem != NULL) {
              q.enqueue(elem);
            }
          }
        }

      obj_number++;
    } else {
     // printf("%lx was already visited at %u. RefId[%u]\n",addr,offset,ref_number);
      _metadata.add_oop(ref_number, offset, o);
    }

  }
 
  return true;
}

