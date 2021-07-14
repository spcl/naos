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
#ifndef JDK11_RDMA_JVM_SKYWAY_HPP
#define JDK11_RDMA_JVM_SKYWAY_HPP

#include "jni.h"


jlong create_skyway();
void register_class_skyway(jlong ptr, void* k, int id);

jobject send_graph_skyway(jlong ptr, jobject initial_object, jint init_size);
jint send_graph_skyway_to_buf(jlong ptr, jobject initial_object, jobject bytes);

jobject receive_graph_skyway(jlong ptr, jobject bytes);



#endif //JDK11_RDMA_JVM_SKYWAY_HPP
