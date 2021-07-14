#!/bin/bash

# Naos: Serialization-free RDMA networking in Java
# 
# Simple script to run all pipeline tests.
#
# Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
# 

trap 'echo -ne "Stop tests..." && exit 1' INT

server=r630-09,192.168.1.79:9999
client=r630-10
libpath="/home/ktaranov/tools_r630/lib/" # for Disni.so


server=euler06,192.168.0.16:9999
client=euler05

size=$((1024*1024)) # we send large objects

########################################################### Kryo/Java TESTS  ##########################
tests=("array" "kvp" "map")
networks=("tcp" "rdma" ) 
serializers=("kryo" "java")

for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do
   ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test}
done
done
done

########################################################### Skyway TESTS  ##########################
tests=("array" "kvp" "map")
networks=("tcp") 
serializers=("skyway")
 
for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do
   ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test} 
   ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test} --naosnocycles 
done
done
done

#exit 1
########################################################### Naos TESTS  ##########################
tests=("array" "kvp" "map")
networks=("tcp" "rdma" ) #tcp rdma
serializers=("naos")

min_power=11
max_power=18


for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do
    ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test} --naosnocycles 
    ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test} --naosnocycles --array
    ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test} --array
    ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test} 
 
	for (( i=$min_power; i<=$max_power; i++ ))
	do
	    psize=$((2**$i))
 ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test} --pipeline=${psize} --naosnocycles
 ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test} --pipeline=${psize} --naosnocycles --array
 ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test} --pipeline=${psize} --array
 ./naos-pipeline-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=110  --libpath=${libpath} --size=${size}  --data=${test} --pipeline=${psize}

	done

done
done
done
 











