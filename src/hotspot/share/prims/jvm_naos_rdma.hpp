/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * RDMA Naos: jni-accessible functions
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
#ifndef JDK11_RDMA_JVM_NAOS_RDMA_HPP
#define JDK11_RDMA_JVM_NAOS_RDMA_HPP

#include "jni.h"

long send_graph_rdma(void* rdmaep, jobject object, const bool blocking, const int array_len);
jobject receive_graph_rdma(void* rdmaep);

void close_rdma_ep(void* rdmaep);


void wait_rdma(void* rdmaep, long handle);
bool test_rdma(void* rdmaep, long handle);

#endif //JDK11_RDMA_JVM_NAOS_RDMA_HPP
