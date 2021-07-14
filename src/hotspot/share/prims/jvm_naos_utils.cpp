/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Tools for measuring time of Naos's pipelining. Has been used for debugging.
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
#include "jvm_naos_utils.hpp"

#include <sys/time.h>

uint64_t getTimeMicroseconds() {
  struct timeval time;
  gettimeofday(&time, 0);
  return time.tv_sec*1000000 + time.tv_usec;
}