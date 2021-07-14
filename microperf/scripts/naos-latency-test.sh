#!/bin/bash
# Naos: Serialization-free RDMA networking in Java
# 
# Simple script to run a latency test
#
# Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
# 
source core.sh
trap 'echo -ne "Stop all servers..." && killAllProcesses && echo "done" && exit 1' INT

define HELP <<'EOF'

Script for starting Naos latency test.
usage  : $0 [options]
options: --server=HOST,DEVICEIP:PORT   # IP address of the server
         --client=HOST   # IP address of the client
         --network=NAME # type of the network: tcp|rdma 
         --serializer=NAME # type of the serializer: java|kryo|naos|skyway
         --data=NAME       # data stucture: floats|array|map|kvp
         --offheap         # use offheap memory on the tests
         --size=INT # the size of experiment e.g., the number of elements in an array
         --iter=INT # the number of tests
         --dir=PATH #absolute path to working dir on remote machines
         --java=PATH #absolute path to java
         --libpath=PATH # to add library path to shared libs
         --output=FILE #name of the output file 
         --naosnocycles
         --batch=INT # to enable iterative naos send
         --array
EOF

usage () {
    echo -e "$HELP"
}

JAVADIR="/mnt/scratch/${USER}/jdk/bin"
jar="microperf-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
WORKDIR="/mnt/scratch/ktaranov/"

WORKDIR="/home/ktaranov/naostest/"
JAVADIR="/home/ktaranov/naostest/jdk/bin"
#JAVADIR="/mnt/ntfs/Documents/jdk11-rdma/build/linux-x86_64-normal-server-release/images/jdk/bin"

#this preload was used to allow us to compile java on one machine and run it on another with older glibc
PRELOAD="LD_PRELOAD=/home/ktaranov/tools/lib/libm.so.6 "

size=128
iter=100
network="tcp"
serializer="java"
data="floats"
server=""
client="" 
libpath=""
offheap=""
array=""
naosnocycles=""
output=""
iterative_batch=""

main_recver="ch.ethz.microperf.ReceiverNIO"
main_sender="ch.ethz.microperf.SenderNIO"

for arg in "$@"
do
    case ${arg} in
    --help|-help|-h)
        usage
        exit 1
        ;;
    --server=*)
        server=`echo $arg | sed -e 's/--server=//'`
        server=`eval echo ${server}`    # tilde and variable expansion
        ;;
    --client=*)
        client=`echo $arg | sed -e 's/--client=//'`
        client=`eval echo ${client}`    # tilde and variable expansion
        ;;
    --network=*)
        network=`echo $arg | sed -e 's/--network=//'`
        network=`eval echo ${network}`    # tilde and variable expansion
        ;;
    --data=*)
        data=`echo $arg | sed -e 's/--data=//'`
        data=`eval echo ${data}`    # tilde and variable expansion
        ;;
    --serializer=*)
        serializer=`echo $arg | sed -e 's/--serializer=//'`
        serializer=`eval echo ${serializer}`    # tilde and variable expansion
        ;;
    --size=*)
        size=`echo $arg | sed -e 's/--size=//'`
        size=`eval echo ${size}`    # tilde and variable expansion
        ;;
    --iter=*)
        iter=`echo $arg | sed -e 's/--iter=//'`
        iter=`eval echo ${iter}`    # tilde and variable expansion
        ;;
    --batch=*)
        iterative_batch=`echo $arg | sed -e 's/--batch=//'`
        iterative_batch=`eval echo ${iterative_batch}`    # tilde and variable expansion
        ;;
    --offheap)
        offheap="--offheap"
        ;;
    --array)
        array="--array"
        ;;
    --naosnocycles)
        naosnocycles="-XX:+NaosDisableCycles"
        ;;
    --libpath=*)
        libpath=`echo $arg | sed -e 's/--libpath=//'`
        libpath=`eval echo ${libpath}`    # tilde and variable expansion
        libpath="-Djava.library.path=${libpath}"
        ;;
    --dir=*)
        WORKDIR=`echo $arg | sed -e 's/--dir=//'`
        WORKDIR=`eval echo ${WORKDIR}`    # tilde and variable expansion
        ;;
    --java=*)
        JAVADIR=`echo $arg | sed -e 's/--java=//'`
        JAVADIR=`eval echo ${JAVADIR}`    # tilde and variable expansion
        ;;
    --output=*)
        output=`echo $arg | sed -e 's/--output=//'`
        output=`eval echo ${output}`    # tilde and variable expansion
        ;;
    esac
done


if [ -z "$server" ]
then
      echo "server is empty"
      usage 
      exit 1
fi

if [ -z "$client" ]
then
      echo "client is empty"
      usage 
      exit 1
fi

if [ -z "$output" ]; then

output="latency_${network}_${serializer}_${data}_${size}_${iter}"

if [ ! -z "${offheap}" ]; then 
output="${output}_offheap"
fi

if [ ! -z "${array}" ]; then 
output="${output}_array"
fi


if [ ! -z "${naosnocycles}" ]; then 
output="${output}_nocycles"
fi

if [ ! -z "${iterative_batch}" ]; then 
output="${output}_batch${iterative_batch}"
fi

output="${output}.txt"

echo "Output file is ${output}"
fi

if [ -z "${iterative_batch}" ]; then 
iterative_batch="0"
fi


serverhost=$( echo $server | cut -d, -f1)
serverdev=$( echo $server | cut -d: -f1 | cut -d, -f2)
serverport=$( echo $server | cut -d: -f2)
echo "Start latency test at server $serverhost $serverdev $serverport"
echo "Start latency test at client $client"

#scpFileTo $serverhost #dubbo-remoting-naostcp.jar
#-XX:NaosPipelineSize=1047552  -Xlog:naos=info -Xlog:naos=debug
jvmparams=" ${libpath}  -XX:+UnlockDiagnosticVMOptions -XX:+UnlockExperimentalVMOptions \
 -XX:+UseShenandoahGC -XX:ShenandoahGCMode=passive -XX:-UseBiasedLocking -Xms2g \
-Xmx2g -XX:+AlwaysPreTouch -XX:ShenandoahHeapRegionSize=32m -XX:+UseCompressedOops ${naosnocycles}" # -XX:+NaosDisableCycles
codeparams="-d ${data} -e ${size} -h ${serverdev}  -i ${iter} -n ${network} -p ${serverport} -s ${serializer} -b ${iterative_batch} ${offheap} ${array}"

recvcommand="${PRELOAD} timeout -s 9 46s ${JAVADIR}/java $jvmparams -cp ${WORKDIR}/${jar} -ea  $main_recver $codeparams"
sendcommand="${PRELOAD} timeout -s 9 46s ${JAVADIR}/java $jvmparams -cp ${WORKDIR}/${jar} -ea  $main_sender $codeparams"

debug "The command $sendcommand"
debug "The command $recvcommand"

#exit 1
sshCommandSyncUnblock ${serverhost} "${recvcommand}"
sleep 0.5
sshCommandSync ${client} "${sendcommand}" "${WORKDIR}/$output"


echo "The experiment is done"

scpFileFrom ${client} "${output}"

killAllProcesses

sshCommandSync ${client} "pkill -9 -u ${USER} java"
sshCommandSync ${serverhost} "pkill -9 -u ${USER} java"


echo "Results in ${PWD}/${output}"

echo "----------Done--------------"
