/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Implementation of klass service in Naos
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */

#include "jvm_naos_klass_service.hpp"

#include "utilities/hashtable.hpp"
#include "runtime/thread.inline.hpp"
#include "classfile/systemDictionaryShared.hpp"
#include "runtime/vframe.inline.hpp"
#include "runtime/handles.inline.hpp"

#include <netdb.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <classfile/symbolTable.hpp>


// local ip in network encoded format
volatile uint32_t encoded_local_ip = 0;

// local klass service port
volatile uint16_t klass_service_port = 0;

// Array indexed using a fd gives a node klass table. This array is only used on the received side.
KlassTable* volatile fd_to_ktable[1024];

// identifier for the klass service thread
volatile pthread_t klass_service_thread = 0;

// it will try to get my ip from fd
uint32_t get_encoded_local_ip(int fd) {
  int ret;
  if(fd!= -1){
    struct sockaddr_storage sa;
    unsigned int sa_len = sizeof(sa);
    int retval = getsockname(fd,(struct sockaddr*)&sa, &sa_len);

    if(retval == -1){
      log_debug(naos) ("getsockname error!\n");
    }
    else{
      if (sa.ss_family == AF_INET) {
        struct sockaddr_in *s = (struct sockaddr_in *)&sa;
        return s->sin_addr.s_addr;
        //printf("Peer ipv4 is %s \n",inet_ntop(AF_INET, &s->sin_addr, ipstr, sizeof ipstr));
      } else if(sa.ss_family == AF_INET6) {
        struct sockaddr_in6 *s = (struct sockaddr_in6 *)&sa;
        // here we will truncate it to ipv4
        uint8_t *bytes = s->sin6_addr.s6_addr;
        bytes+=12;
        uint32_t ipv4  = *(uint32_t *)bytes;
        return ipv4;
        //printf("Peer ipv6 is %s \n",inet_ntop(AF_INET6, &s->sin6_addr, ipstr, sizeof ipstr));
      }
    }
    log_debug(naos) ("Failed get the ip from fd. Use gethostbyname method \n");
  }

  char hostbuffer[256];
  struct in_addr ip_addr;

  // get hostname
  ret = gethostname(hostbuffer, sizeof(hostbuffer));
  assert( ret == 0, "failed to get host name");

  // setup host data structure
  struct hostent* host_entry = gethostbyname(hostbuffer);

  // get an ip string representation
  char * ip_buff = inet_ntoa(*((struct in_addr*) host_entry->h_addr_list[0]));

  // encoded the ip string representation as an integer
  ret = inet_aton(ip_buff, &ip_addr);
  assert( ret != 0, "failed to get enconded ip");

  return ip_addr.s_addr;
}


// create the server using the ip.
// if ip == 0, the ip will be retrived from fd
uint64_t get_or_create_klass_service_address(uint32_t ip, int fd){
  // start the klass resultion service if not started yet.
  if (klass_service_thread == 0) {
    pthread_t unused;
    if(ip == 0){
      ip = get_encoded_local_ip(fd);
    }
    if (pthread_create(&unused, NULL, klass_service, (void*)(uint64_t)ip)) {
      assert(false, "failed to create klass service thread");
    }
  }
  while(klass_service_port==0){
    usleep(100);
  }
  uint64_t addr = encoded_local_ip;
  addr = addr << 32;
  addr += klass_service_port;
  return addr;
}

// create a thread which replies to klass translation requests
// it mapes virtual addresses to full class names
void* klass_service(void * args) {
  struct sockaddr_in servaddr, cliaddr;
  socklen_t cliaddr_sz = sizeof(sockaddr_in);
  socklen_t servaddr_sz = sizeof(sockaddr_in);
  pthread_t tid = pthread_self();
  uintptr_t klass_ptr = 0;
  int sockfd;
  int ret;
  char klass_name[1024];

  uint32_t ipv4 = (uint32_t)(uint64_t)args;

  // if some other thread already started, quit...
  if (Atomic::cmpxchg(tid, &klass_service_thread, (pthread_t)0) != 0) {
    return NULL;
  }

  // create udp socket
  sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  assert(sockfd >= 0, "klass service socket creation failed");


  // init data structures
  memset(&servaddr, 0, servaddr_sz);
  memset(&cliaddr, 0, cliaddr_sz);
  servaddr.sin_family = AF_INET; // ipv4
  servaddr.sin_addr.s_addr = ipv4;
  servaddr.sin_port = 0;

  encoded_local_ip = servaddr.sin_addr.s_addr;

  // bind the socket with the server address
  ret = bind(sockfd, (const struct sockaddr *)&servaddr, servaddr_sz);
  assert( ret >= 0, "klass service socket bind failed");

  // get the info about the socket
  ret = getsockname(sockfd, (struct sockaddr *)&servaddr, &servaddr_sz);
  assert( ret == 0, "failed to get socket port");

  // save the port picked by bind
  klass_service_port = servaddr.sin_port; //ntohs

  log_debug(naos) ("[klass_service] Started at ip: %X port: %u",encoded_local_ip, klass_service_port);


  while (true) {
    // receive klass ptr to resolve
    ret = recvfrom(sockfd, &klass_ptr, sizeof(uintptr_t), MSG_WAITALL, (struct sockaddr*) &cliaddr, &cliaddr_sz);
    assert( ret == sizeof(uintptr_t), "failed to read class ptr");

    // resolve klass ptr to klass name
    const char* name = ((Klass*) klass_ptr)->name()->as_C_string(klass_name, 1024);
    size_t name_size = (strlen(name) + 1) * sizeof(char); // also include the null character
    size_t curr = 0;

    // send size of klass name
    ret = sendto(sockfd, &name_size, sizeof(size_t), MSG_CONFIRM, (const struct sockaddr*) &cliaddr, cliaddr_sz);
    assert( ret == sizeof(size_t), "failed to send klass name size");

    // send klass name
    while (curr < name_size) {
      curr += sendto(sockfd, name + curr, name_size - curr, MSG_CONFIRM, (const struct sockaddr*) &cliaddr, cliaddr_sz);
    }

    log_trace(naos) ("[klass_service] klass_ptr = %p -> %s", (void*)klass_ptr, name);
  }
}

Klass* get_caller_klass(int depth) {
  JavaThread* THREAD = JavaThread::current();
  vframeStream vfst(THREAD);
  vfst.security_get_caller_frame(depth);

  if (vfst.at_end()) {
    THROW_MSG_0(vmSymbols::java_lang_InternalError(), "no caller?");
  }

  return vfst.method()->method_holder();
}


Klass* get_klass_from_caller(Klass* caller, char* name) {
  JavaThread* THREAD = JavaThread::current();
  Handle class_loader = Handle(THREAD, caller->class_loader());
  Handle protection_domain = Handle(THREAD, caller->protection_domain());
  TempNewSymbol symbol = SymbolTable::new_symbol(name, CHECK_NULL);
  Klass* klass = SystemDictionary::resolve_or_null(symbol, class_loader, protection_domain, CHECK_NULL);

  if (klass != NULL) {
    klass->initialize(THREAD);
  }

  return klass;
}

KlassResolver::KlassResolver(int fd,uint32_t ip,uint16_t port)
: ip(ip), port(port), sockfd(-1)
{
  this->caller =  get_caller_klass(2);
  KlassTable* ktable = fd_to_ktable[fd];

  // get table for this sender
  if (ktable == NULL) {
    ktable = new KlassTable();
    fd_to_ktable[fd] = ktable;
  }
  this->ktable = ktable;
}
 

KlassResolver::KlassResolver(KlassTable* ktable)
: ip(0), port(0), sockfd(-1)
{
  this->caller =  get_caller_klass(2);
  this->ktable = ktable;
}

KlassResolver::~KlassResolver(){
  // TODO - rbruno - shoundn't we free ktable?
  if(sockfd >= 0){
    close(sockfd);
  }
}

uintptr_t KlassResolver::resolve_klass(uintptr_t klass_ptr){
  assert(klass_ptr!=((uintptr_t)0), "klass_ptr cannot be null");

  Klass* klass = ktable->lookup((int)klass_ptr);
  if (klass == NULL) {
    // request resolution
    klass = (Klass*) klass_service_request(klass_ptr);
    // cache klass resolution
    ktable->add((int)klass_ptr, klass);
  }

  return (uintptr_t)klass;
}

uintptr_t KlassResolver::klass_service_request(uintptr_t klass_ptr){
  struct sockaddr_in servaddr;
  socklen_t servaddr_sz;
  int ret;
  char klass_name[1024];
  size_t name_size;
  size_t curr = 0;
 
  // create udp socket
  if(sockfd < 0){
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  } 
  assert(sockfd >= 0, "klass service request socket creation failed");

  // init data structures and retrieve sender ip
  servaddr_sz = sizeof(servaddr);
  memset(&servaddr, 0, servaddr_sz);
  servaddr.sin_family = AF_INET;
  servaddr.sin_port = port;
  servaddr.sin_addr.s_addr = ip;

  log_trace(naos) ("[klass_service_request] (%X:%u) klass_ptr = %p", ip, port, (void*)klass_ptr);

  // send klass ptr to resolve
  ret = sendto(sockfd, &klass_ptr, sizeof(uintptr_t), MSG_CONFIRM, (const struct sockaddr *) &servaddr, servaddr_sz);
  assert(ret == sizeof(uintptr_t), "failed to send klass ptr");

  // read klass name size
  ret = recvfrom(sockfd, &name_size, sizeof(size_t), MSG_WAITALL, (struct sockaddr *) &servaddr, &servaddr_sz);
  assert(ret == sizeof(size_t), "failed to read klass name size");

  // read klass name
  while (curr < name_size) {
    curr += recvfrom(sockfd, klass_name + curr, name_size - curr, MSG_WAITALL, (struct sockaddr *) &servaddr, &servaddr_sz);
  }
 
  // resolve klass ptr to klass name
  return (uintptr_t) get_klass_from_caller(caller, klass_name);
}

size_t serialize_klass_table(KlassTable* ktable, uint8_t* buffer, size_t max_buf_size) {
  return ktable->serialize_to_buffer(buffer, max_buf_size);
}

// Takes a buffer containing a number of entries and inserts each of them in a newly created table.sss
KlassTable* deserialize_klass_table(uint8_t* buffer, size_t bytes_buffer) {
  KlassTable * ktable = new KlassTable();
  ktable->deserialize_from_buffer(get_caller_klass(2), buffer, bytes_buffer);
  return ktable;
}

 
void clear_state_fd(int fd) {
  KlassTable* ktable = fd_to_ktable[fd];
  if(ktable!=NULL){
    delete ktable;
    fd_to_ktable[fd] = NULL;
  }
}

void KlassTable::add(int id, Klass* klass) {
  unsigned int hash = (unsigned int)id;
  HashtableEntry<Klass*, mtClass>* entry = new_entry(hash, klass);
  add_entry(hash_to_index(hash), entry);
}


Klass* KlassTable::lookup(int id) { // konst: Is int enough? ptr is 8 bytes, but int is 4
  unsigned int hash = (unsigned int)id;
  int index = hash_to_index(id);
  for (HashtableEntry<Klass*, mtClass>* e = bucket(index); e != NULL; e = e->next()) {
    if (e->hash() == hash) {
      return e->literal();
    }
  }
  return NULL;
}


size_t KlassTable::serialize_to_buffer(uint8_t* buffer, size_t max_buf_size) {
  size_t writen = 0;

  for (int i = 0; i < table_size(); i++) {
    for (HashtableEntry<Klass*, mtClass>* e = bucket(i); e != NULL; e = e->next()) {
      Klass* klass = e->literal();

      // write klass ptr and advance buffer
      *((uintptr_t *)buffer) = (uintptr_t) klass;
      writen += sizeof(uintptr_t);
      buffer += sizeof(uintptr_t);

      // write klass name and advance buffer
      klass->name()->as_C_string((char*)buffer, max_buf_size - writen);
      size_t name_size = (strlen((char*)buffer) + 1) * sizeof(char); // also include the null character

      log_trace(naos) ("[KlassTable::serialize_to_buffer] klass_ptr = %p -> %s", (void*)klass, buffer);

      writen += name_size;
      buffer += name_size;
    }
  }

  return writen;
}


size_t KlassTable::deserialize_from_buffer(Klass* caller, uint8_t* buffer, size_t bytes_buffer) {
  size_t read = 0;

  while (read < bytes_buffer) {

    // read klass ptr and advance buffer
    uintptr_t klass_ptr = *((uintptr_t*)buffer);
    read += sizeof(uintptr_t);
    buffer += sizeof(uintptr_t);

    // read klass name and advance buffer
    Klass* klass = get_klass_from_caller(caller, (char*)buffer);
    size_t name_size = (strlen((char*)buffer) + 1) * sizeof(char); // also include the null character

    log_trace(naos) ("[KlassTable::deserialize_from_buffer] klass_ptr = %p -> %s", (void*)klass, buffer);

    read += name_size;
    buffer += name_size;

    // add entry
    add((int)klass_ptr, klass);
  }

  return read;
}