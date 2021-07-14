/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Our implementation of Skyway. Note that we do not implement cycle detection.
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
#include "jvm_skyway.hpp"

#include <runtime/biasedLocking.hpp>
#include "jvm_naos.hpp"
#include "jvm_naos_klass_service.hpp"
#include "oops/typeArrayOop.inline.hpp"
#include "runtime/vmThread.hpp"
#include "runtime/vm_operations.hpp"
#include "runtime/jniHandles.inline.hpp"
#include "memory/oopFactory.hpp"
#include <sys/uio.h>

 


class SkywayEP: public CHeapObj<mtInternal>  {
public:
  SendTree sendtree;
  SkywayTable classes;
  SkywayTraverseObject skyway_op;
  SkywayReceiveGraph skyway_recv; 


  SkywayEP(): sendtree(), classes(), skyway_op(sendtree,classes), skyway_recv(classes)
  {

  };

  void register_class(Klass* k,int id){
    classes.register_class(id,k);
  }

  void init(Handle initial_object,  typeArrayOop array, uint32_t offset){
    sendtree.hard_reset();
    skyway_op.init(initial_object, array, offset);
  }

  void doit(JavaThread* THREAD){
    skyway_op.doit(THREAD);
  }

  oop recover(char* buf, uint32_t size){
    return skyway_recv.doit(buf,size);
  }
  
  typeArrayOop get_buffer(){
    return skyway_op.get_buffer();
  }

  ~SkywayEP(){
    
  }

};


jlong create_skyway(){
  SkywayEP* ep = new SkywayEP();
  return  (jlong)(void*)ep;
}

void register_class_skyway(jlong ptr, void* k, int id){
  SkywayEP* ep = (SkywayEP*)(void*)ptr;
  ep->register_class((Klass*)k,id);
}



struct skyway_header_t {
  uint32_t bytes_data;
  uint32_t total_objects;
};

// for creating a buffer. returns a pointer to buffer.
jobject send_graph_skyway(jlong ptr, jobject object, jint init_size) { // Bytes can be NULL
  JavaThread* THREAD = JavaThread::current();
 
  oop obj = JNIHandles::resolve(object);
  Handle initial_object(THREAD, obj);
 
  uint32_t offset = sizeof(skyway_header_t);
 
  typeArrayOop array = oopFactory::new_byteArray(init_size, THREAD);
 
  SkywayEP* ep = (SkywayEP*)(void*)ptr;
  ep->init(initial_object,array, offset);
  ep->doit(THREAD);

  //SkywayTraverseObject skyway_op(initial_object, sendtree, classes, array, offset); 
  //skyway_op.doit(THREAD);

  // allocate output (on-heap) buffer
  typeArrayOop buffer = ep->get_buffer();
 
  uint32_t total_length = sizeof(skyway_header_t) + ep->sendtree.get_total_length();
  log_debug(naos) ("[send_graph_skyway] sizes %u %u \n",total_length,ep->skyway_op.get_total_length());
  buffer->set_length(total_length); // the rest becomes heap. Note memory should be zeroed
  
  skyway_header_t *header = (skyway_header_t *)array->byte_at_addr(0);
  header->bytes_data = ep->sendtree.get_total_length();
  header->total_objects = ep->sendtree.get_total_objects();

  return JNIHandles::make_local(JavaThread::current(), buffer);
}

// for writing to exisitng buffer.returns how much written
jint send_graph_skyway_to_buf(jlong ptr, jobject object, jobject bytes) {  
  JavaThread* THREAD = JavaThread::current();
 
  oop obj = JNIHandles::resolve(object);
  Handle initial_object(THREAD, obj);

  oop old_buffer = JNIHandles::resolve(bytes);
  assert(old_buffer->is_typeArray(), "object should be a byte array");
 
  uint32_t offset = sizeof(skyway_header_t);
 
  SkywayEP* ep = (SkywayEP*)(void*)ptr;
  ep->init(initial_object, (typeArrayOop)old_buffer, offset);
  ep->doit(THREAD);

 
  // allocate output (on-heap) buffer
  typeArrayOop buffer = ep->get_buffer();
  assert( buffer == old_buffer, "buffer overflow");

  uint32_t total_length = sizeof(skyway_header_t) + ep->sendtree.get_total_length();
  log_debug(naos) ("[send_graph_skyway] sizes %u %u \n",total_length,ep->skyway_op.get_total_length());
  //buffer->set_length(total_length); // the rest becomes heap. Note memory should be zeroed
  
  skyway_header_t *header = (skyway_header_t *)buffer->byte_at_addr(0);
  header->bytes_data = ep->sendtree.get_total_length();
  header->total_objects = ep->sendtree.get_total_objects();

  return (jint)total_length;
}


jobject receive_graph_skyway(jlong ptr, jobject bytes) {
  JavaThread* THREAD = JavaThread::current();
  oop obj = JNIHandles::resolve(bytes);
  Handle initial_object(THREAD, obj);
 

  assert(obj->is_typeArray(), "object should be a byte array");
  typeArrayOop array = (typeArrayOop) obj;

  log_debug(naos) ("[receive_graph_skyway] recover from buffer of size %u \n",array->length());

  char * heap_buffer = (char *)array->byte_at_addr(0);
  skyway_header_t header = *(skyway_header_t*) heap_buffer;
  log_debug(naos) ("[receive_graph_skyway] metadata %u bytes %u objects \n",header.bytes_data, header.total_objects);


  SkywayEP* ep = (SkywayEP*)(void*)ptr;
  oop first_object = ep->recover(heap_buffer+sizeof(skyway_header_t), header.bytes_data);

  // unpin and return the revived object
  array->set_length(sizeof(skyway_header_t)); // the rest becomes heap


  return JNIHandles::make_local(JavaThread::current(), first_object);
}

