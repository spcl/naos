/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * JNI bindings for Naos
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
#include "ch_ethz_naos_jni_NativeDispatcher.h"
#include <jvmti.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <netinet/in.h>
#include <sys/poll.h>
#include <unistd.h>
#include <fcntl.h>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <infiniband/verbs.h>


struct connect_data{
    int buffer_size;
    int buffer_num;
    int connectid;
    int reserved;
};


/**
 * Throw a Java exception by name. Similar to SignalError.
 */
JNIEXPORT void JNICALL JNU_ThrowByName(JNIEnv *env, const char *name, const char *msg) {
  jclass cls = env->FindClass(name);

  if (cls != 0) /* Otherwise an exception has already been thrown */
  {
    env->ThrowNew(cls, msg);
  }
}

/*
 * Throw an IOException, using provided message string.
 */
void JNU_ThrowIOException(JNIEnv *env, const char *msg) {
  // TODO - rbruno - why is this commented out?
 // JNU_ThrowByName(env, "java/io/IOException", msg);
}

static unsigned long long createObjectId(void *obj) {
  unsigned long long obj_id = (unsigned long long)obj;
  return obj_id;
}


JNIEXPORT jlong JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1createServer(JNIEnv *env, jclass calling_class, jlong address) {
  unsigned long long obj_id = -1;
  struct rdma_cm_id *listen_id = NULL;
  int ret;
  struct sockaddr *s_addr = (struct sockaddr *)address;

  struct sockaddr_in *sp = (struct sockaddr_in *)s_addr;
  //printf("ip address %d %u %u\n",sp->sin_family, sp->sin_port, sp->sin_addr.s_addr);
  struct rdma_event_channel * cm_channel = rdma_create_event_channel();
  if (cm_channel == NULL) {
    JNU_ThrowIOException(env, "rdma_create_event_channel cm_channel null\n");
  }

  ret = rdma_create_id(cm_channel, &listen_id, NULL, RDMA_PS_TCP);
  if (ret) {
    JNU_ThrowIOException(env, "rdma_create_id listen_id null\n");
  }

  ret = rdma_bind_addr(listen_id, s_addr);
  if (ret) {
    JNU_ThrowIOException(env, "Failed to bind RDMA CM address on the server.");
  }

  ret = rdma_listen(listen_id, 2);
  if (ret) {
    JNU_ThrowIOException(env, "rdma listen failed");
  }

  //printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1createServer\n");
  obj_id = createObjectId(listen_id);
  return obj_id;
}


JNIEXPORT jlong JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1createClient(JNIEnv *env, jclass calling_class, jlong address) {
  unsigned long long obj_id = -1;
  struct rdma_cm_id *connect_id = NULL;
  struct rdma_cm_event *event;
  int ret;

  struct sockaddr *s_addr = (struct sockaddr *)address;
  //printf("%d %u %u\n",s_addr->sin_family, s_addr->sin_port, s_addr->sin_addr.s_addr);
  struct rdma_event_channel * cm_channel = rdma_create_event_channel();
  if (cm_channel == NULL) {
    JNU_ThrowIOException(env, "rdma_create_event_channel cm_channel null\n");
  }

  ret = rdma_create_id(cm_channel, &connect_id, NULL, RDMA_PS_TCP);
  if (ret) {
    JNU_ThrowIOException(env, "rdma_create_id connect_id null\n");
  }

  ret = rdma_resolve_addr(connect_id, NULL, s_addr, 2000);
  if (ret) {
    JNU_ThrowIOException(env, "rdma_resolve_addr connect_id null\n");
  }

  ret = rdma_get_cm_event(cm_channel, &event);
  if(event->event != RDMA_CM_EVENT_ADDR_RESOLVED){
    JNU_ThrowIOException(env, "event is not RDMA_CM_EVENT_ADDR_RESOLVED\n");
  }
  connect_id = event->id;
  rdma_ack_cm_event(event);

  ret = rdma_resolve_route(connect_id, 2000);
  if (ret) {
    JNU_ThrowIOException(env, "rdma_resolve_route null\n");
  }
  ret = rdma_get_cm_event(cm_channel, &event);
  if(event->event != RDMA_CM_EVENT_ROUTE_RESOLVED){
    JNU_ThrowIOException(env, "event is not RDMA_CM_EVENT_ROUTE_RESOLVED\n");
  }
  connect_id = event->id;
  rdma_ack_cm_event(event);

  //printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1createClient\n");
  obj_id = createObjectId(connect_id);
  return obj_id;
}



int create_qp(JNIEnv *env, struct rdma_cm_id *id, uint32_t inlinesize, uint32_t sendsize, uint32_t recvsize ){
  int ret;

  struct ibv_comp_channel *event_channel = ibv_create_comp_channel(id->verbs);

  struct ibv_cq *cq = ibv_create_cq(id->verbs, 5*(sendsize+recvsize), NULL,event_channel,0);
  cq->channel = event_channel;

  int flags = fcntl(event_channel->fd, F_GETFL);
  ret = fcntl(event_channel->fd, F_SETFL, flags | O_NONBLOCK);
  if (ret < 0) {
      fprintf(stderr, "Failed to change file descriptor of completion event channel\n");
      return -1;
  }

  struct ibv_qp_init_attr attr;
  memset(&attr, 0, sizeof(attr));
  attr.send_cq = cq;
  attr.recv_cq = cq;
  attr.cap.max_send_wr = sendsize;
  attr.cap.max_recv_wr = recvsize;
  attr.cap.max_send_sge = 1;
  attr.cap.max_recv_sge = 1;
  attr.cap.max_inline_data = inlinesize;
  attr.qp_type = IBV_QPT_RC;

  ret = rdma_create_qp(id, NULL, &attr);
  if (ret) {
    printf("error rdma_create_qp\n");
    return -1;
  }
 // printf("Cq pointers:  %p %p %p \n", id->qp->send_cq, id->send_cq, cq);
  id->qp->send_cq = cq;
  return 0;
}
 

JNIEXPORT jlong JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1acceptServer(JNIEnv *env, jclass calling_class, jlong rdmaserver, jlong params) {
  struct rdma_cm_id *listen_id = ( struct rdma_cm_id * )rdmaserver;

  int* connect_params = (int*)(void*)params;

  struct rdma_cm_event *event;
  struct rdma_event_channel * cm_channel =  listen_id->channel;

  while(true){
    int ret = rdma_get_cm_event(cm_channel, &event);
    if(event->event == RDMA_CM_EVENT_DISCONNECTED){
//      printf("Jni client disconnected \n");
      rdma_ack_cm_event(event);
    } else {
      break;
    }
  }

  if(event->event != RDMA_CM_EVENT_CONNECT_REQUEST){
    JNU_ThrowIOException(env, "event is not RDMA_CM_EVENT_CONNECT_REQUEST\n");
  }

  struct rdma_cm_id * id = event->id;

//  printf("Connect request get event with data len %d \n",event->param.conn.private_data_len);

  int buffer_size = ((connect_data*)event->param.conn.private_data)->buffer_size;
  int buffer_num = ((connect_data*)event->param.conn.private_data)->buffer_num;
  int connectid = ((connect_data*)event->param.conn.private_data)->connectid;

 // printf("Connect request with %d %d %d \n",buffer_size, buffer_num,connectid );


  connect_params[0] = buffer_size;
  connect_params[1] = buffer_num;
  connect_params[2] = connectid;

  rdma_ack_cm_event(event);

  id->channel = cm_channel;

 // printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1acceptServer\n");

  return   createObjectId(id);
}


JNIEXPORT jlong JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1connectActive(JNIEnv *env, jclass calling_class, jlong rdmaid, jlong params) {
  struct rdma_cm_id *id = (struct rdma_cm_id *)rdmaid;

  int* connect_params = (int*)(void*)params;
  int connectid = connect_params[2];

//  printf("\tJava_ch_ethz_naos_jni_NativeDispatcher__1connectActive\n");
  uint32_t inlinesize = 0;
  uint32_t sendsize = 256;
  uint32_t recvsize = 1024;

  int ret = create_qp(env,id, inlinesize, sendsize, recvsize );

  if(ret < 0){
    JNU_ThrowIOException(env, "failed to create QP\n");
  }

  struct rdma_conn_param conn_param;
  memset(&conn_param, 0 , sizeof(conn_param));

  connect_data data;
  data.buffer_size = -1;
  data.buffer_num = -1;
  data.connectid = connectid;

  conn_param.private_data =  &data;
  conn_param.private_data_len = sizeof(connect_data);

  conn_param.responder_resources = 0; // no rdma reads
  conn_param.initiator_depth = 0; // no rdma reads
  conn_param.retry_count = 3;
  conn_param.rnr_retry_count = 3;

  struct rdma_cm_event *event;

  ret = rdma_connect(id, &conn_param);
  if (ret) {
    printf("error rdma_connect\n");
    return 0;
  }

  struct rdma_event_channel * cm_channel =  id->channel;

  ret = rdma_get_cm_event(cm_channel, &event);
  if(event->event != RDMA_CM_EVENT_ESTABLISHED){
    return 0;
  }

 // printf("\tActive Client get event with data len %d \n",event->param.conn.private_data_len);

  int buffer_size = ((connect_data*)event->param.conn.private_data)->buffer_size;
  int buffer_num = ((connect_data*)event->param.conn.private_data)->buffer_num;
  connect_params[0] = buffer_size;
  connect_params[1] = buffer_num;


 // printf("\tActive Client get event with  %d %d \n",buffer_size,buffer_num );


  rdma_ack_cm_event(event);

  void* ep =env->CreateActiveRdmaEP((void*)id,(jint)inlinesize,
                                    (jint)sendsize,(jint)recvsize, (jint)buffer_size, (jint)buffer_num);

  return  createObjectId(ep);
}

JNIEXPORT jlong JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1connectPassive(JNIEnv *env, jclass calling_class, jlong rdmaid, jlong params) {
  struct rdma_cm_id *id = (struct rdma_cm_id *)rdmaid;

  int* connect_params = (int*)(void*)params;

  int buffer_size = connect_params[0];
  int buffer_num = connect_params[1];
  int connectid = connect_params[2];

 // printf("\tJava_ch_ethz_naos_jni_NativeDispatcher__1connectPassive\n");
  uint32_t inlinesize = 0;
  uint32_t sendsize = 256;
  uint32_t recvsize = 4096;

  int ret = create_qp(env,id, inlinesize, sendsize, recvsize );

  if(ret < 0){
    JNU_ThrowIOException(env, "failed to create QP\n");
  }

  struct rdma_conn_param conn_param;
  memset(&conn_param, 0 , sizeof(conn_param));

  connect_data data;
  data.buffer_size = buffer_size;
  data.buffer_num = buffer_num;
  data.connectid = connectid;

  conn_param.private_data =  &data;
  conn_param.private_data_len = sizeof(connect_data);

  conn_param.responder_resources = 0; // no rdma reads
  conn_param.initiator_depth = 0; // no rdma reads
  conn_param.retry_count = 3;
  conn_param.rnr_retry_count = 3;

  struct rdma_cm_event *event;

  ret = rdma_connect(id, &conn_param);
  if (ret) {
    printf("error rdma_connect\n");
    return 0;
  }

  struct rdma_event_channel * cm_channel =  id->channel;

  ret = rdma_get_cm_event(cm_channel, &event);
  if(event->event != RDMA_CM_EVENT_ESTABLISHED){
    return 0;
  }

  //printf("\tPassive Client get event with data len %d \n",event->param.conn.private_data_len);

  rdma_ack_cm_event(event);

  void* ep =env->CreatePassiveRdmaEP((void*)id,(jint)inlinesize,
                                      (jint)sendsize,(jint)recvsize, (jint)buffer_size, (jint)buffer_num);

  return  createObjectId(ep);
}


JNIEXPORT jlong JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1acceptActive(JNIEnv *env, jclass calling_class, jlong rdmaid, jlong params) {
  struct rdma_cm_id *id = (struct rdma_cm_id *)rdmaid;

  uint32_t* connect_params = (uint32_t*)(void*)params;

  uint32_t buffer_size = connect_params[0];
  uint32_t buffer_num = connect_params[1];

 // printf("Java_ch_ethz_naos_jni_NativeDispatcher__1acceptActive\n");
  uint32_t inlinesize = 0;
  uint32_t sendsize = 256;
  uint32_t recvsize = 1024;

  int ret = create_qp(env,id, inlinesize, sendsize, recvsize );

  if(ret < 0){
    JNU_ThrowIOException(env, "failed to create QP\n");
  }

  struct rdma_conn_param conn_param;
  memset(&conn_param, 0 , sizeof(conn_param));

  conn_param.responder_resources = 0; // no rdma reads
  conn_param.initiator_depth = 0; // no rdma reads
  conn_param.retry_count = 3;
  conn_param.rnr_retry_count = 3;

  struct rdma_cm_event *event;

  ret = rdma_accept(id, &conn_param);
  if (ret) {
    printf("error rdma_connect\n");
    return 0;
  }

  struct rdma_event_channel * cm_channel =  id->channel;

  ret = rdma_get_cm_event(cm_channel, &event);
  if(event->event != RDMA_CM_EVENT_ESTABLISHED){
    return 0;
  }

//  printf("Active Accept get event with data len %d \n",event->param.conn.private_data_len);

  rdma_ack_cm_event(event);

  void* ep =env->CreateActiveRdmaEP((void*)id,(jint)inlinesize,
                                      (jint)sendsize,(jint)recvsize, (jint)buffer_size, (jint)buffer_num);

  return  createObjectId(ep);
}



JNIEXPORT jlong JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1acceptPassive(JNIEnv *env, jclass calling_class, jlong rdmaid, jlong params) {
  struct rdma_cm_id *id = (struct rdma_cm_id *)rdmaid;

  uint32_t* connect_params = (uint32_t*)(void*)params;

  uint32_t buffer_size = connect_params[0];
  uint32_t buffer_num = connect_params[1];

 // printf("Java_ch_ethz_naos_jni_NativeDispatcher__1acceptPassive\n");
  uint32_t inlinesize = 0;
  uint32_t sendsize = 256;
  uint32_t recvsize = 2048;
  int ret = create_qp(env,id, inlinesize, sendsize, recvsize );

  if(ret < 0){
    JNU_ThrowIOException(env, "failed to create QP\n");
  }

  struct rdma_conn_param conn_param;
  memset(&conn_param, 0 , sizeof(conn_param));

  connect_data data;
  data.buffer_size = buffer_size;
  data.buffer_num = buffer_num;

  conn_param.private_data =  &data;
  conn_param.private_data_len = sizeof(connect_data);

  conn_param.responder_resources = 0; // no rdma reads
  conn_param.initiator_depth = 0; // no rdma reads
  conn_param.retry_count = 3;
  conn_param.rnr_retry_count = 3;

  struct rdma_cm_event *event;

  ret = rdma_accept(id, &conn_param);
  if (ret) {
    printf("error rdma_connect\n");
    return 0;
  }

  struct rdma_event_channel * cm_channel =  id->channel;

  ret = rdma_get_cm_event(cm_channel, &event);
  if(event->event != RDMA_CM_EVENT_ESTABLISHED){
    return 0;
  }

  rdma_ack_cm_event(event);

  void* ep =env->CreatePassiveRdmaEP((void*)id,(jint)inlinesize,
                                      (jint)sendsize,(jint)recvsize, (jint)buffer_size, (jint)buffer_num);

  return  createObjectId(ep);
}




JNIEXPORT void JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1closeClient(JNIEnv *env, jclass calling_class, jlong clientid) {
  struct rdma_cm_id *id = (struct rdma_cm_id *)clientid;
  struct rdma_event_channel * cm_channel =  id->channel;
  rdma_destroy_event_channel(cm_channel);
 // printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1closeClient\n");
}


JNIEXPORT void JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1closeServer(JNIEnv *env, jclass calling_class, jlong listenid) {
  struct rdma_cm_id *listen_id = (struct rdma_cm_id *)listenid;
  struct rdma_event_channel * cm_channel =  listen_id->channel;
  rdma_destroy_id(listen_id);
  rdma_destroy_event_channel(cm_channel);
 // printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1closeServer\n");
}

uint64_t count = 0;

JNIEXPORT void JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1test5(JNIEnv *env, jclass calling_class) {
  count++;  
}

JNIEXPORT void JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1test6(JNIEnv *env, jclass calling_class) {
  env->Test_f6();
}

JNIEXPORT void JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1test7(JNIEnv *env, jclass calling_class) {
  env->Test_f7();
}

JNIEXPORT void JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1waitRdma(JNIEnv *env, jclass calling_class, jlong rdmaep, jlong handle) {
  env->WaitRdma(rdmaep,handle);
}
JNIEXPORT jboolean JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1testRdma(JNIEnv *env, jclass calling_class, jlong rdmaep, jlong handle) {
  return env->TestRdma(rdmaep,handle);
}

JNIEXPORT jlong JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1createNaosTcp(JNIEnv *env, jclass calling_class, jint fd) {
  return createObjectId(env->CreateNaosTcp(fd));
}

JNIEXPORT void JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1test11(JNIEnv *env, jclass calling_class, jobject obj) {
  env->Test_f11(obj);
}


JNIEXPORT void JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1writeObj(JNIEnv *env, jclass calling_class, jlong rdmaep, jobject obj, jint array_len) {
 // printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1writeObj\n");

  env->RdmaWriteObj(rdmaep,obj,array_len);
}

JNIEXPORT jlong JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1writeObjAsync(JNIEnv *env, jclass calling_class, jlong rdmaep, jobject obj, jint array_len) {
 // printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1writeObj\n");

  return env->AsyncRdmaWriteObj(rdmaep,obj,array_len);
}

JNIEXPORT jobject JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1readObj(JNIEnv *env, jclass calling_class, jlong rdmaep) {
  //printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1readObj\n");

  return env->RdmaReadObj(rdmaep);
}

JNIEXPORT void JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1writeInt(JNIEnv *env, jclass calling_class, jlong rdmaep, jint val) {
 // printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1writeObj\n");
  env->RdmaWriteInt(rdmaep,val);
}

JNIEXPORT jint JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1readInt(JNIEnv *env, jclass calling_class, jlong rdmaep) {
  //printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1readObj\n");

  return env->RdmaReadInt(rdmaep);
}

JNIEXPORT jint JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1isReadable(JNIEnv *env, jclass calling_class, jlong rdmaep, jint timeout) {
  return env->PollRdmaEP((void*)rdmaep,timeout);
}


JNIEXPORT void JNICALL
        Java_ch_ethz_naos_jni_NativeDispatcher__1closeEP(JNIEnv *env, jclass calling_class, jlong rdmaep) {
  //printf("Java_ch_ethz_binaryrdma_NativeDispatcher__1closeEP\n");
  env->RdmaCloseEP(rdmaep);
}

JNIEXPORT jobject JNICALL
Java_ch_ethz_naos_jni_NativeDispatcher__1readObjFD(JNIEnv *env, jclass calling_class, jlong naostcp, jlong params) {
  jobject obj = env->ReceiveNaosTcp(naostcp,params);
  return obj;
}

JNIEXPORT void JNICALL
Java_ch_ethz_naos_jni_NativeDispatcher__1writeObjFD(JNIEnv *env, jclass calling_class, jlong naostcp, jobject obj, jint array_len) {
  env->SendNaosTcp(naostcp, obj, array_len);
}


JNIEXPORT jint JNICALL
Java_ch_ethz_naos_jni_NativeDispatcher__1readableFD(JNIEnv *env, jclass calling_class, jint fd, jint timeout) {
  
//  int timeout = 10'000; // in millisec
  //int timeout = -1; // infinite

  struct pollfd pfd;
  pfd.fd = fd;
  pfd.events = POLLIN;

  int ret = poll(&pfd, 1, timeout);

  if(ret < 0){
    JNU_ThrowIOException(env, "poll failed in _readableFD\n");  
  }
  return ret;
}


// skyway calls

JNIEXPORT jobject JNICALL
Java_ch_ethz_naos_jni_NativeDispatcher__1readObjSkyway(JNIEnv *env, jclass calling_class, jlong skyway, jobject bytes) {
  return env->ReceiveSkyway(skyway, bytes);
}

JNIEXPORT jobject JNICALL
Java_ch_ethz_naos_jni_NativeDispatcher__1writeObjSkyway(JNIEnv *env, jclass calling_class, jlong skyway, jobject obj, jint initsize) {
  return env->SendSkyway(skyway, obj,initsize);
}

JNIEXPORT jint JNICALL
Java_ch_ethz_naos_jni_NativeDispatcher__1writeObjSkywayBuf(JNIEnv *env, jclass calling_class, jlong skyway, jobject obj, jobject bytes) {
  return env->SendSkywayBuf(skyway, obj,bytes);
}


JNIEXPORT jlong JNICALL
Java_ch_ethz_naos_jni_NativeDispatcher__1createSkyway(JNIEnv *env, jclass calling_class) {
  return env->CreateSkyway();
}

JNIEXPORT void JNICALL
Java_ch_ethz_naos_jni_NativeDispatcher__1registerSkywayClass(JNIEnv *env, jclass calling_class, jlong skyway, jobject _class, jint id) {
  env->RegisterSkywayClass(skyway, (jclass)_class, id);
}
 

JNIEXPORT jlong JNICALL
Java_ch_ethz_naos_jni_NativeDispatcher__1sizeof(JNIEnv *env, jclass calling_class, jobject obj, jboolean bfs) {
  return env->GetSize(obj,bfs);
}