#!/bin/bash
# Naos: Serialization-free RDMA networking in Java
# 
# Simple script to run an ODP test
#
# Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
# 
source core.sh
trap 'echo -ne "Stop all servers..." && killAllProcesses && echo "done" && exit 1' INT

define HELP <<'EOF'

Script for starting Naos odp test.
usage  : $0 [options]
options: --server=HOST,DEVICEIP:PORT   # IP address of the server
         --client=HOST   # IP address of the client
         --size=INT # the size of experiment e.g., the number of elements in an array
         --iter=INT # the number of tests
         --dir=PATH #absolute path to working dir on remote machines
         --java=PATH #absolute path to java
         --libpath=PATH # to add library path to shared libs
         --output=FILE #name of the output file 
         --odp
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
size=128
iter=100
network="rdma"
serializer="naos"
data="floats"
withodp=""
server=""
client="" 
libpath=""
output=""

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
    --size=*)
        size=`echo $arg | sed -e 's/--size=//'`
        size=`eval echo ${size}`    # tilde and variable expansion
        ;;
    --iter=*)
        iter=`echo $arg | sed -e 's/--iter=//'`
        iter=`eval echo ${iter}`    # tilde and variable expansion
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
    --odp)
        withodp="-XX:+NaosUseODP"
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

if [ -z "$output" ]
then

if [ -z "${withodp}" ]; then
  output="odplatency_${network}_${serializer}_${data}_${size}_${iter}.txt"
else 
  output="odplatency_${network}_${serializer}_${data}_${size}_${iter}_odp.txt"
fi

echo "Output file is ${output}"
fi


serverhost=$( echo $server | cut -d, -f1)
serverdev=$( echo $server | cut -d: -f1 | cut -d, -f2)
serverport=$( echo $server | cut -d: -f2)
echo "Start latency test at server $serverhost $serverdev $serverport"
echo "Start latency test at client $client"

#scpFileTo $serverhost #dubbo-remoting-naostcp.jar
#-XX:NaosPipelineSize=1047552  -Xlog:naos=info -Xlog:naos=debug
jvmparams=" ${libpath} -XX:+UnlockDiagnosticVMOptions -XX:+UnlockExperimentalVMOptions \
 -XX:+UseShenandoahGC -XX:ShenandoahGCMode=passive -XX:-UseBiasedLocking -Xms2g \
-Xmx2g -XX:+AlwaysPreTouch -XX:ShenandoahHeapRegionSize=32m -XX:+UseCompressedOops ${withodp}" # -XX:+NaosDisableCycles
datasize=$((${size}*4+64))
codeparams="-d ${data} -e ${size} -h ${serverdev}  -i ${iter} -n ${network} -p ${serverport} -s ${serializer} -a ${datasize}"

recvcommand="timeout -s 9 10s ${JAVADIR}/java $jvmparams -cp ${WORKDIR}/${jar} -ea  $main_recver $codeparams"
sendcommand="timeout -s 9 9s ${JAVADIR}/java $jvmparams -cp ${WORKDIR}/${jar} -ea  $main_sender $codeparams"

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
