/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * TCP Naos: jni-accessible functions
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
#ifndef JDK11_RDMA_JVM_NAOS_TCP_H
#define JDK11_RDMA_JVM_NAOS_TCP_H

#include "jni.h"

void send_naos_tcp(uint64_t naostcp_ptr, jobject object, const int array_len = 0);
jobject receive_naos_tcp(uint64_t naostcp_ptr, uint64_t was_iterable);
void close_tcp_fd(uint64_t naostcp_ptr);


void* create_naos_tcp(int fd);
void test_f11(jobject object);


#endif //JDK11_RDMA_JVM_NAOS_TCP_H
