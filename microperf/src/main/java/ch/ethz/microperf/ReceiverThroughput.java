/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Usage example. 
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.microperf;

import ch.ethz.microperf.benchmarks.BenchmarkNaos;
import ch.ethz.microperf.benchmarks.BenchmarkRDMA;
import ch.ethz.microperf.benchmarks.BenchmarkTCP;
import ch.ethz.naos.rdma.*;
import ch.ethz.rdma.RdmaChannel;
import ch.ethz.serializers.Serializer;
import ch.ethz.serializers.SerializerManager;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class ReceiverThroughput extends ExperimentEntryPoint {

    public static void main(String[] args) throws Exception {
    	parseArgs("ReceiverThroughput", args);

        boolean isrdma = network.equals("rdma");
        boolean isnaos = serializer.equals("naos");

        System.out.println(String.format("########## Receiver %s %s %s (%d nodes) %s ##########", network, serializer, datastructure, nodes, isdirect ?  "off-heap" : "in-heap" ));

        Serializer serializerImpl = SerializerManager.getSerializer(serializer);

        SocketChannel channel = null; // for tcp
        RdmaChannel rdmachannel =  null; // for disni
        EndpointRDMA rdmaep = null; // for naos
        Socket socket = null;

        // setting up connections...
        System.out.println("[ReceiverNIO] connecting... ");
        if(isrdma){
            if(isnaos){
               ConnectParamsRDMA params = new ConnectParamsRDMA(30*1024*1024,8);
               ServerRDMA server = new ServerRDMA(host,port);
               ActiveEndpointRDMA aep = null;
               PassiveEndpointRDMA pep = null;

               RawEndpointRDMA t = server.accept();
               if(t.isActive()){
                   aep = t.acceptActive();
               } else{
                   pep = t.acceptPassive(params);
               }
               t = server.accept();
               if(t.isActive()){
                   aep = t.acceptActive();
               } else {
                   pep = t.acceptPassive(params);
               }
               rdmaep = new EndpointRDMA(aep,pep);
               System.out.printf("Recv: Info about by connection: r:%d l:%d \n", rdmaep.getRemoteBufferSize(), rdmaep.getLocalBufferSize());

            } else {
                ch.ethz.rdma.RdmaServer server = new ch.ethz.rdma.RdmaServer(host, port);
                rdmachannel = server.lazy_accept(256);
            }
        } else {
            if(isnaos){
                ServerSocket s = new ServerSocket(port);
                socket = s.accept();
                //s.close();
            }else {
                ServerSocketChannel s = ServerSocketChannel.open();
                s.socket().bind(new InetSocketAddress(host, port));
                channel = s.accept();
                //s.close();
            }
        }
        System.out.println("[ReceiverNIO] connecting... done!");

        if(isnaos) {
            if(isrdma) {
                BenchmarkNaos.runThroughputTestReceiver(rdmaep, serializerImpl);
            } else {
                BenchmarkNaos.runThroughputTestReceiver(socket, serializerImpl);
            }
        } else {
            if(isrdma) {
                BenchmarkRDMA.runThroughputTestReceiver(rdmachannel, serializerImpl);
            } else {
                BenchmarkTCP.runThroughputTestReceiver(channel, isdirect, serializerImpl);
            }
        }

        if (channel!=null)
            channel.close();

        if (rdmachannel!=null)
            rdmachannel.close();

     //   if (rdmaep != null)
         //   rdmaep.close();

        if (socket != null)
            socket.close();
    }
}
