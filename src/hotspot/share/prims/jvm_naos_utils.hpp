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
#ifndef JDK11_RDMA_JVM_NAOS_UTILS_H
#define JDK11_RDMA_JVM_NAOS_UTILS_H

#include <stdint.h>
#include "utilities/queue.hpp"
#include "logging/log.hpp"

//#define TRAVERSE_BFS


typedef uint32_t order_t;


uint64_t getTimeMicroseconds();

struct ref_offset_t {

  ref_offset_t() : ref_number(0), offset(0) { }
  ref_offset_t(uint32_t ref_number, uint32_t offset): ref_number(ref_number), offset(offset) { }

  uint32_t ref_number;
  uint32_t offset;
};

struct range_t {

  range_t() : addr(0), length(0) { }
  range_t(uint64_t addr, uint32_t length): addr(addr), length(length) { }

  // start address
  uint64_t addr;

  // length in bytes
  uint32_t length; 
};



struct send_step_t{
  uint64_t start_trav;
  uint64_t end_trav;
  uint64_t end_send;
  uint32_t data_bytes;
  uint32_t metadata_bytes;
};

struct send_times_t{
  uint64_t start;
  uint64_t end;
  QueueImpl<send_step_t> *pipeline;
};

struct recv_step_t{
  uint64_t start_recv;
  uint64_t end_recv;
  uint64_t end_trav;
  uint32_t data_bytes;
  uint32_t metadata_bytes;
};

struct recv_times_t{
  uint64_t start;
  uint64_t end;
  QueueImpl<recv_step_t> *pipeline;
};

class MeasurementsSend {

  QueueImpl<send_times_t> _measurements;
  uint64_t _id;

public:

  MeasurementsSend(uint64_t id) : _id(id) {
    add_start();
  }

  ~MeasurementsSend() {
    add_end();
    print();
  }

  inline void add_start() {
    send_times_t *temp = _measurements.enqueue_empty();
    temp->pipeline = new (ResourceObj::C_HEAP, mtClass) QueueImpl<send_step_t>();
    temp->start = getTimeMicroseconds();
  };

  inline void add_start_traversal() {
    send_step_t* temp = _measurements.adr_tail()->pipeline->enqueue_empty();
    temp->start_trav = getTimeMicroseconds();
  };

  inline void add_end_traversal() {
    send_step_t *temp = _measurements.adr_tail()->pipeline->adr_tail();
    temp->end_trav = getTimeMicroseconds();
  };


  inline void add_end_send() {
    send_step_t *temp = _measurements.adr_tail()->pipeline->adr_tail();
    temp->end_send = getTimeMicroseconds();
  };

  inline void add_data_bytes(uint32_t val) {
    send_step_t *temp = _measurements.adr_tail()->pipeline->adr_tail();
    temp->data_bytes = val;
  };

  inline void add_metadata_bytes(uint32_t val) {
    send_step_t *temp = _measurements.adr_tail()->pipeline->adr_tail();
    temp->metadata_bytes = val;
  };

  inline void add_end() {
    send_times_t *temp = _measurements.adr_tail();
    temp->end = getTimeMicroseconds();
  };

  void print() {

    log_info(naos)("[MeasurementsSend id=%zu] times", _id);
    while(!_measurements.is_empty()) {
      send_times_t m = _measurements.dequeue();
      uint64_t prev_time = m.start;

      log_info(naos)("[MeasurementsSend id=%zu] measurement:", _id);
      while(m.pipeline != NULL && !m.pipeline->is_empty()) {
        send_step_t step = m.pipeline->dequeue();

        log_info(naos)("\t[SendStep id=%zu] waited to traverse: %zu us; traversal: %zu us; send: %zu us; send data: %u bytes; metadata: %u bytes",
              _id,
              step.start_trav - prev_time,
              step.end_trav - step.start_trav,
              step.end_send - step.end_trav,
              step.data_bytes, step.metadata_bytes);
        prev_time = step.end_send;
      }

      log_info(naos)("\t[MeasurementsSend id=%zu] total took %zu us", _id, m.end - m.start);

      if(m.pipeline) {
        delete m.pipeline;
      }
    }
  }
};


class MeasurementsRecv {

  QueueImpl<recv_times_t> _measurements;
  uint64_t _id;

public:

  MeasurementsRecv(uint64_t id) : _id(id) {
    add_start();
  }

  ~MeasurementsRecv() {
    add_end();
    print();
  }

  inline void add_start() {
    recv_times_t *temp = _measurements.enqueue_empty();
    temp->pipeline = new (ResourceObj::C_HEAP, mtClass) QueueImpl<recv_step_t>();
    temp->start = getTimeMicroseconds();
  };

  inline void add_start_recv() {
    recv_step_t* temp = _measurements.adr_tail()->pipeline->enqueue_empty();
    temp->start_recv = getTimeMicroseconds();
  };

  inline void add_end_recv() {
    recv_step_t *temp = _measurements.adr_tail()->pipeline->adr_tail();
    temp->end_recv = getTimeMicroseconds();
  };

  inline void add_end_traversal() {
    recv_step_t *temp = _measurements.adr_tail()->pipeline->adr_tail();
    temp->end_trav = getTimeMicroseconds();
  };

  inline void add_data_bytes(uint32_t val) {
    recv_step_t *temp = _measurements.adr_tail()->pipeline->adr_tail();
    temp->data_bytes = val;
  };

  inline void add_metadata_bytes(uint32_t val) {
    recv_step_t *temp = _measurements.adr_tail()->pipeline->adr_tail();
    temp->metadata_bytes = val;
  };

  inline void add_end() {
    recv_times_t *temp = _measurements.adr_tail();
    temp->end = getTimeMicroseconds();
  };

  void print() {

    log_info(naos)("[MeasurementsRecv id=%zu] times", _id);
    while(!_measurements.is_empty()){
      recv_times_t m = _measurements.dequeue();
      uint64_t prev_time = m.start;

      log_info(naos)("[MeasurementsRecv id=%zu]: measurement", _id);
      while(m.pipeline != NULL && !m.pipeline->is_empty()){
        recv_step_t step = m.pipeline->dequeue();
        log_info(naos)("\t[ReceiveStep id=%zu] waited to receive: %zu us; receive: %zu us; traversal: %zu us; receive data: %u bytes; metadata: %u bytes",
              _id,
              step.start_recv - prev_time,
              step.end_recv - step.start_recv,
              step.end_trav - step.end_recv,
             step.data_bytes, step.metadata_bytes);
        prev_time = step.end_trav;
      }

      log_info(naos)("\t[MeasurementsRecv id=%zu] total took %zu us", _id, m.end - m.start);

      if(m.pipeline) {
        delete m.pipeline;
      }
    }
  }
};

#endif // JDK11_RDMA_JVM_NAOS_UTILS_H