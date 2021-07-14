/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Naos' class service that allows sending objects without type registration
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
#ifndef JDK11_RDMA_JVM_NAOS_KLASS_SERVICE_H
#define JDK11_RDMA_JVM_NAOS_KLASS_SERVICE_H

#include <stdint.h>

#include "oops/oop.inline.hpp"
#include "oops/klass.inline.hpp"
#include "jvm_naos_utils.hpp"

// Look up from Klass ID -> Klass*
class KlassTable : public Hashtable<Klass*, mtClass> {
public:
  KlassTable() : Hashtable<Klass*, mtClass>(1987, sizeof(HashtableEntry<Klass*, mtClass>)) { }
  void add(int id, Klass* klass);
  Klass* lookup(int id);

  // Serializes all entries in table to buffer. Return number of writen bytes.
  size_t serialize_to_buffer(uint8_t* buffer, size_t max_buf_size);
  // Deserializes all bytes from buffer and inserts into table. Returns number of inserted entries.
  size_t deserialize_from_buffer(Klass* caller, uint8_t* buffer, size_t bytes_buffer);

};

class KlassResolver {
	KlassTable* ktable;
	uint32_t ip;
	uint16_t port;
	int sockfd;
	Klass* caller;
	uintptr_t klass_service_request(uintptr_t klass_ptr);
public:
	KlassResolver(int fd,uint32_t ip = 0,uint16_t port = 0);
    KlassResolver(KlassTable* ktable);
	uintptr_t resolve_klass(uintptr_t klass_ptr);

	void set_resolver(uint32_t ip, uint16_t port){
		if(this->ip == 0){
			this->ip = ip;
			this->port = port;
		}
	}

	~KlassResolver();
};
 

// Takes ktable and writes all entries to buffer. Number of bytes in buffer are returned.
size_t serialize_klass_table(KlassTable* ktable, uint8_t* buffer, size_t buf_size);

// Takes a buffer containing a number of entries and inserts each of them in a newly created table.sss
KlassTable* deserialize_klass_table(uint8_t* buffer, size_t bytes_buffer);

uint64_t get_or_create_klass_service_address(uint32_t ip, int fd);

void* klass_service(void * args);

 
void clear_state_fd(int fd);

#endif //JDK11_RDMA_JVM_NAOS_KLASS_SERVICE_H
