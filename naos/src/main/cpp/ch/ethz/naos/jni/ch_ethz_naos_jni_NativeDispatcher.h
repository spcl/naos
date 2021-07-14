/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class ch_ethz_naos_jni_NativeDispatcher */

#ifndef _Included_ch_ethz_naos_jni_NativeDispatcher
#define _Included_ch_ethz_naos_jni_NativeDispatcher
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _test5
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1test5
  (JNIEnv *, jclass);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _test6
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1test6
  (JNIEnv *, jclass);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _test7
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1test7
  (JNIEnv *, jclass);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _test11
 * Signature: (Ljava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1test11
  (JNIEnv *, jclass, jobject);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _createServer
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1createServer
  (JNIEnv *, jclass, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _createClient
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1createClient
  (JNIEnv *, jclass, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _acceptServer
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1acceptServer
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _acceptPassive
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1acceptPassive
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _acceptActive
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1acceptActive
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _connectPassive
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1connectPassive
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _connectActive
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1connectActive
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _closeServer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1closeServer
  (JNIEnv *, jclass, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _closeClient
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1closeClient
  (JNIEnv *, jclass, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _waitRdma
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1waitRdma
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _testRdma
 * Signature: (JJ)Z
 */
JNIEXPORT jboolean JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1testRdma
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _writeObj
 * Signature: (JLjava/lang/Object;I)V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1writeObj
  (JNIEnv *, jclass, jlong, jobject, jint);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _writeObjAsync
 * Signature: (JLjava/lang/Object;I)J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1writeObjAsync
  (JNIEnv *, jclass, jlong, jobject, jint);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _readObj
 * Signature: (J)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1readObj
  (JNIEnv *, jclass, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _writeInt
 * Signature: (JI)V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1writeInt
  (JNIEnv *, jclass, jlong, jint);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _readInt
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1readInt
  (JNIEnv *, jclass, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _isReadable
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1isReadable
  (JNIEnv *, jclass, jlong, jint);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _closeEP
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1closeEP
  (JNIEnv *, jclass, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _createNaosTcp
 * Signature: (I)J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1createNaosTcp
  (JNIEnv *, jclass, jint);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _readObjFD
 * Signature: (JJ)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1readObjFD
  (JNIEnv *, jclass, jlong, jlong);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _writeObjFD
 * Signature: (JLjava/lang/Object;I)V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1writeObjFD
  (JNIEnv *, jclass, jlong, jobject, jint);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _readableFD
 * Signature: (II)I
 */
JNIEXPORT jint JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1readableFD
  (JNIEnv *, jclass, jint, jint);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _readObjSkyway
 * Signature: (JLjava/lang/Object;)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1readObjSkyway
  (JNIEnv *, jclass, jlong, jobject);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _writeObjSkyway
 * Signature: (JLjava/lang/Object;I)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1writeObjSkyway
  (JNIEnv *, jclass, jlong, jobject, jint);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _writeObjSkywayBuf
 * Signature: (JLjava/lang/Object;Ljava/lang/Object;)I
 */
JNIEXPORT jint JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1writeObjSkywayBuf
  (JNIEnv *, jclass, jlong, jobject, jobject);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _createSkyway
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1createSkyway
  (JNIEnv *, jclass);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _registerSkywayClass
 * Signature: (JLjava/lang/Object;I)V
 */
JNIEXPORT void JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1registerSkywayClass
  (JNIEnv *, jclass, jlong, jobject, jint);

/*
 * Class:     ch_ethz_naos_jni_NativeDispatcher
 * Method:    _sizeof
 * Signature: (Ljava/lang/Object;Z)J
 */
JNIEXPORT jlong JNICALL Java_ch_ethz_naos_jni_NativeDispatcher__1sizeof
  (JNIEnv *, jclass, jobject, jboolean);

#ifdef __cplusplus
}
#endif
#endif
