#!/bin/bash

# Naos: Serialization-free RDMA networking in Java
# 
# Simple script to run all latency tests.
#
# Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
# 

trap 'echo -ne "Stop tests..." && exit 1' INT

min_power=2
max_power=16
server=r630-09,192.168.1.79:9999
client=r630-10
server=euler06,192.168.0.16:9999
client=euler05
libpath="/home/ktaranov/tools_r630/lib/"  # for Disni.so

########################################################### NAOS TESTS ##########################
tests=("floats" "array" "kvp" "map") #"floats"
networks=("tcp" "rdma") #tcp rdma
serializers=("naos")


for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do

for (( i=$min_power; i<=$max_power; i++ ))
do
    size=$((2**$i))
   ./naos-latency-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=1900  --libpath=${libpath} --size=${size}  --data=${test} --naosnocycles
   ./naos-latency-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=1900  --libpath=${libpath} --size=${size}  --data=${test} 
done

done
done
done



########################################################### NAOS TESTS ##########################
tests=("array" "kvp" "map") #"floats"
networks=("tcp" "rdma") #tcp rdma
serializers=("naos")


for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do

for (( i=$min_power; i<=$max_power; i++ ))
do
    size=$((2**$i))
   ./naos-latency-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=1900  --libpath=${libpath} --size=${size}  --data=${test} --naosnocycles --array
   ./naos-latency-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=1900  --libpath=${libpath} --size=${size}  --data=${test} --array
done

done
done
done



########################################################### Kryo/Java TESTS  ##########################
tests=("floats" "array" "kvp" "map")
networks=("tcp" "rdma") #tcp rdma
serializers=("kryo" "java")


for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do
for (( i=$min_power; i<=$max_power; i++ ))
do
    size=$((2**$i))
   ./naos-latency-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=1900  --libpath=${libpath} --size=${size}  --data=${test}
   ./naos-latency-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=1900  --libpath=${libpath} --size=${size}  --data=${test} --offheap
done

done
done
done




########################################################### Skyway TESTS ##########################

tests=("floats" "array" "map" "kvp")
networks=("tcp") #tcp
serializers=("skyway")


for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do

for (( i=$min_power; i<=$max_power; i++ ))
do
    size=$((2**$i))
   ./naos-latency-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=1900  --libpath=${libpath} --size=${size}  --data=${test} 
   ./naos-latency-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=1900  --libpath=${libpath} --size=${size}  --data=${test} --naosnocycles
done


done
done
done



########################################################### NAOS TESTS with batch ##########################
##   Iterative naos it bottlenecked by interface of the receiver. We do not evaluate it.

tests=("floats" "array" "kvp" "map")
networks=("tcp" "rdma" ) #tcp rdma
serializers=("naos")


for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do

for (( i=$min_power; i<=$max_power; i++ ))
do
    size=$((2**$i))
   #./naos-latency-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --iter=1800  --libpath=${libpath} --size=${size}  --data=${test} --naosnocycles
done

done
done
done
