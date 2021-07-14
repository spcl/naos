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

import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import ch.ethz.microperf.benchmarks.BenchmarkNaos;
import ch.ethz.microperf.benchmarks.BenchmarkRDMA;
import ch.ethz.microperf.benchmarks.BenchmarkTCP;
import ch.ethz.naos.rdma.*;
import ch.ethz.rdma.RdmaChannel;
import ch.ethz.serializers.Serializer;
import ch.ethz.serializers.SerializerManager;

public class ReceiverNIO extends ExperimentEntryPoint {

    public static void main(String[] args) throws Exception {
    	parseArgs("ReceiverNIO", args);
    	
        boolean isrdma = network.equals("rdma");
        boolean isnaos = serializer.equals("naos");

        System.out.println(String.format("########## Receiver %s %s %s (%d nodes) %s ##########", network, serializer, datastructure, nodes, isdirect ?  "off-heap" : "in-heap" ));

        Serializer serializerImpl = SerializerManager.getSerializer(serializer);

        SocketChannel channel = null; // for tcp
        RdmaChannel rdmachannel =  null; // for dini
        EndpointRDMA rdmaep = null; // for naos
        Socket socket = null;

        // setting up connections...
        System.out.println("[ReceiverNIO] connecting... ");
        if(isrdma){
            if(isnaos){
                ConnectParamsRDMA params = null;
                if(specialized_reg_size == 0)
                    params = new ConnectParamsRDMA(30*1024*1024,8);
                else
                    params = new ConnectParamsRDMA(specialized_reg_size,1);
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
                rdmachannel = server.lazy_accept();
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
            if(batch_size == 0) {
                if (isrdma) {
                    BenchmarkNaos.runLatencyTestReceiver(rdmaep, serializerImpl, iters);
                } else {
                    BenchmarkNaos.runLatencyTestReceiver(socket, serializerImpl, iters);
                }
            } else {
                if (isrdma) {
                    BenchmarkNaos.runLatencyTestReceiver(rdmaep, serializerImpl, iters, nodes);
                } else {
                    BenchmarkNaos.runLatencyTestReceiver(socket, serializerImpl, iters, nodes);
                }
            }
        } else {
            if(isrdma) {
                BenchmarkRDMA.runLatencyTestReceiver(rdmachannel, serializerImpl, iters);
            } else {
                BenchmarkTCP.runLatencyTestReceiver(channel, isdirect, serializerImpl, iters);
            }
        }

        if (channel!=null)
            channel.close();

        if (rdmachannel!=null)
            rdmachannel.close();
        
        if (rdmaep != null)
        	rdmaep.close();
        
        if (socket != null)
        	socket.close();
    }
}
