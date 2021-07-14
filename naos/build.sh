#!/bin/bash

read -p "Flavour (release, fastdebug, slowdebug)? " flavour

export JAVA_HOME=../build/linux-x86_64-normal-server-$flavour/images/jdk/

JAVA_SRC=src/main/java
JAVA_BIN=target
CPP_SRC=src/main/cpp
CPP_BIN=res

mvn package

mkdir -p $CPP_BIN

$JAVA_HOME/bin/javac \
    -cp $JAVA_BIN/classes:. \
    -h $CPP_SRC/ch/ethz/naos/jni \
    $JAVA_SRC/ch/ethz/naos/jni/NativeDispatcher.java

g++ -c \
    -fPIC \
    -g \
    -I"$JAVA_HOME/include" \
    -I"$JAVA_HOME/include/linux" \
    -o $CPP_BIN/jni_rdma.o $CPP_SRC/ch/ethz/naos/jni/jni_rdma.cpp

g++ -fPIC \
    -shared \
    -g \
    -o $CPP_BIN/libjni_rdma.so \
    $CPP_BIN/jni_rdma.o -libverbs -lrdmacm

mvn assembly:assembly install
