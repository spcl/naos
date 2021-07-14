#!/bin/bash
                                                                                                    
# Naos: Serialization-free RDMA networking in Java
# 
# Simple script to run all throughput tests.
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
libpath="/home/ktaranov/tools_r630/lib/" # for Disni.so

server=euler06,192.168.0.16:9999
client=euler05

########################################################### NAOS TESTS ##########################

tests=("floats" "array" "kvp" "map")
networks=("tcp" "rdma" ) 
serializers=("naos")


for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do

for (( i=$min_power; i<=$max_power; i++ ))
do
    size=$((2**$i))

   ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test}
   ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test} --naosnocycles
   
    if [ ${net} == "rdma" ]; then # async send
      ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test} --offheap
      ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test} --offheap --naosnocycles
    fi

    if [ ${ser} != "floats" ]; then # array send does not make sense for floats.
     ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test} --naosnocycles --array
     ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test} --array
      if [ ${net} == "rdma" ]; then # async send
       ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test} --offheap --array
       ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test} --offheap --naosnocycles --array
      fi
    fi

done

done
done
done





########################################################### Kryo/Java TESTS  ##########################

tests=("floats" "array" "kvp" "map")
networks=("tcp" )  
serializers=("kryo" "java")


for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do

for (( i=$min_power; i<=$max_power; i++ ))
do
    size=$((2**$i))
#    continue
    ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test}
    ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test} --offheap
done

done
done
done



########################################################### Skyway TESTS  ##########################

 

tests=("floats" "array" "kvp" "map")
networks=("tcp")  
serializers=("skyway")


for test in "${tests[@]}"; do
for network in "${networks[@]}"; do
for ser in "${serializers[@]}"; do

for (( i=$min_power; i<=$max_power; i++ ))
do
    size=$((2**$i))
    ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test}  --naosnocycles
    ./naos-throughput-test.sh --server=${server} --client=${client} --network=${network} --serializer=${ser}  --time=110  --libpath=${libpath} --size=${size}  --data=${test}
done
done
done
done

 