#!/bin/bash
                                                                                    
# Naos: Serialization-free RDMA networking in Java
# 
# Simple script to debug and to test Naos.
#
# Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
# 

build="release"
#build="fastdebug"
#build="slowdebug"

ulimit -c unlimited

export JAVA_HOME=../build/linux-x86_64-normal-server-$build/images/jdk/
java=$JAVA_HOME/bin/java

JVM_OPTS="$JVM_OPTS -XX:+UnlockDiagnosticVMOptions"
JVM_OPTS="$JVM_OPTS -XX:+UnlockExperimentalVMOptions"
JVM_OPTS="$JVM_OPTS -XX:+UseShenandoahGC -XX:ShenandoahGCMode=passive"
JVM_OPTS="$JVM_OPTS -XX:-UseBiasedLocking"
JVM_OPTS="$JVM_OPTS -Xms2g -Xmx2g"
JVM_OPTS="$JVM_OPTS -XX:+AlwaysPreTouch"
JVM_OPTS="$JVM_OPTS -XX:NaosPipelineSize=2000000"
JVM_OPTS="$JVM_OPTS -XX:ShenandoahHeapRegionSize=32m"
JVM_OPTS="$JVM_OPTS -XX:+UseCompressedOops"

# Debug options
#JVM_OPTS="$JVM_OPTS -Xlog:naos=info"
#JVM_OPTS="$JVM_OPTS -Xlog:naos=debug"
#JVM_OPTS="$JVM_OPTS -Xlog:naos=trace"
#JVM_OPTS="$JVM_OPTS -Xlog:gc=trace"

if [ "$build" = "slowdebug" ]; then
    # This is necessary for the buffer mode (we are resolving klass from onheap
    # strings and the vm does not like that. I am confident that it is safe for us.
    JVM_OPTS="$JVM_OPTS -XX:SuppressErrorAt=/symbolTable.cpp:458"
    # Experimental!
    #JVM_OPTS="$JVM_OPTS -XX:SuppressErrorAt=/jvmtiTagMap_tools.cpp:140"
    #JVM_OPTS="$JVM_OPTS -XX:SuppressErrorAt=/jvmtiTagMap_tools.cpp:141"
fi


if [ -z "$1" ]; then
hostname="192.168.1.10"
else
hostname=$1
fi


port="9999"

# Selecting the data structure
#datastructure="hashmap"
#datastructure="array"
datastructure="floats"

# Number of nodes
export nodes_list=128

# Serializers to benchmark
export networks="tcp rdma"
export serializers_list="java kryo naos"  #rdma

main_recver="ch.ethz.microperf.ReceiverNIO"
main_sender="ch.ethz.microperf.SenderNIO"

jar="microperf-0.0.1-SNAPSHOT-jar-with-dependencies.jar"


function run {
    $java $JVM_OPTS -cp target/$jar -ea  $main_recver $args_recver & #> run-recver-$tag.log &
    pid_recver=$!

    sleep 1

    $java $JVM_OPTS -cp target/$jar -ea  $main_sender $args_sender & #> run-sender-$tag.log &
    pid_sender=$!

    wait $pid_sender
    wait $pid_recver

    sleep 1

}

for nodes in $nodes_list; do
for network in $networks; do
for serializer in $serializers_list;    do
        tag="$serializer-$nodes-$network"
        args_recver="-d ${datastructure} -e ${nodes} -h ${hostname}  -i 10 -n ${network} -p ${port} -s ${serializer}"
        args_sender="-d ${datastructure} -e ${nodes} -h ${hostname}  -i 10 -n ${network} -p ${port} -s ${serializer}"
        echo "running $tag..."
        run
        echo "running $tag... done!"
done
done
done
printf "\n"

