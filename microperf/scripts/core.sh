#!/bin/bash
# Naos: Serialization-free RDMA networking in Java
# 
# Core file for working with Naos scripts 
#
# Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
# 
# Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
#            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
# 

define(){ IFS='\n' read -r -d '' ${1} || true; }
redirection=( "> out" "2> err" "< /dev/null" )

declare -a remotePids
declare -a localPids

__VERBOSE=2

function log () {
    if [[ $__VERBOSE -ge 1 ]]; then
        echo -e "$@"  >&2
    fi
}

function debug () {
    if [[ $__VERBOSE -ge 2 ]]; then
        echo -e "$@" >&2
    fi
}


scpFileTo(){
    local server="$1"
    local filename="$2"
    local cmd=( "scp" "$2" "$USER@$server:${WORKDIR}/" )
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

scpFileFrom(){
    local server="$1"
    local filename="$2"
    local cmd=("scp" "$USER@$server:${WORKDIR}/$2" ./)
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

sshCommandAsync() {
    local server=$1
    local command=$2
    local valredirect="${redirection[@]}"
    if ! [[ -z $3 ]]
    then
        valredirect="> "$3" 2>/dev/null"
    fi
    local cmd=( "ssh" "-oStrictHostKeyChecking=no" "$USER@$server" "nohup" "$command" "$valredirect" "&" "echo \$!" )
    debug "\t\tExecuting Async: ${cmd[@]}"
    local pid=$("${cmd[@]}")
    remotePids+=("${server},$pid")
    debug "\t\tAppended to remote: ${server},$pid"
}

sshCommandSync() {
    local server="$1"
    local command="$2"
    local valredirect="${redirection[@]}"
    if ! [[ -z $3 ]]
    then
        valredirect="&> "$3" "
    fi
    local cmd=( "ssh" "-oStrictHostKeyChecking=no" "$USER@$server" "$command" "$valredirect" )
    debug "\t\tExecuting sync: ${cmd[@]}"
    $("${cmd[@]}")
}


sshCommandSyncUnblock() {
    local server="$1"
    local command="$2"
    local valredirect="${redirection[@]}"
    if ! [[ -z $3 ]]
    then
        valredirect="&> "$3" "
    fi
    local cmd=( "ssh" "-oStrictHostKeyChecking=no" "$USER@$server" "$command" "$valredirect")
    debug "\t\tExecuting syncUnblock: ${cmd[@]}"
    local pid=$("${cmd[@]}") & pid=$! 
    localPids+=("$pid")
    debug "\t\tAppended to local: $pid"
}

sshKillCommand() {
    local server=$1
    local pid=$2
    cmd=( "ssh" "$USER@$server" "kill -9" "${pid}" )
    debug "\t\tExecuting: ${cmd[@]}"
    $("${cmd[@]}")
}

waitForAllSSH() {
	local timeout=$1
	debug "\t\tWaiting for $timeout"
	echo "Is not implemneted"
}

killAllProcesses(){
    debug "Killing remote processes"
    for id in "${!remotePids[@]}"
    do  
        local temp=${remotePids[$id]}
        local servername=$( echo $temp | cut -d, -f1)
        local pid=$( echo $temp | cut -d, -f2)
        sshKillCommand $servername $pid
        log "\tProcess is killed at $servername"
    done

    debug "Killing local ssh processes"
    for pid in "${localPids[@]}"
    do  
        debug "kill ssh $pid"
        kill -9 $pid
        log "\tSSH Process is killed with PID$pid"
    done
}

CleanUP() {
    rm -f *.log
}
 



