#!/bin/bash
# Naos: Serialization-free RDMA networking in Java
# 
# Simple script to scp java and naos to a remote machine
#
# Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
# 

define(){ IFS='\n' read -r -d '' ${1} || true; }

define HELP <<'EOF'


Script for copying naos to remote machine.
usage  : $0 [options]
options: --server=HOST  # IP address of the client
         --dir=PATH #absolute path to working dir on remote machine
         --project=PATH #absolute path to java naos
         --only # to send only jars
EOF

usage () {
    echo -e "$HELP"
}

ruser="$USER"
WORKDIR="$HOME/naostest/"
PROJECTDIR="$PWD/../../"
server=""
onlyjars=""
data=""

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
    --dir=*)
        WORKDIR=`echo $arg | sed -e 's/--dir=//'`
        WORKDIR=`eval echo ${WORKDIR}`    # tilde and variable expansion
        ;;
    --project=*)
        PROJECTDIR=`echo $arg | sed -e 's/--project=//'`
        PROJECTDIR=`eval echo ${PROJECTDIR}`    # tilde and variable expansion
        ;;
    --only)
        onlyjars="true"
        ;;
    esac
done

if [ -z "$server" ]
then
      echo "server is empty"
      usage 
      exit 1
fi

origin_dir="$PROJECTDIR/build/linux-x86_64-normal-server-release/images/jdk/"
 
#ssh $ruser@$destination_host "rm -rf $destination_dir/jdk"
if [ -z "$onlyjars" ]
then
scp -r $origin_dir $ruser@$server:$WORKDIR
fi

scp $PROJECTDIR/microperf/target/microperf-0.0.1-SNAPSHOT-jar-with-dependencies.jar $ruser@$server:$WORKDIR


