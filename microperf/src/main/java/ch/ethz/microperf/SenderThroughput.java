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
import ch.ethz.microperf.datastructures.DataStructureManager;
import ch.ethz.naos.rdma.*;
import ch.ethz.rdma.RdmaChannel;
import ch.ethz.serializers.Serializer;
import ch.ethz.serializers.SerializerManager;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class SenderThroughput extends ExperimentEntryPoint {
    private static long warmupinms = 200; // 200 ms

    public static void main(String[] args) throws Exception {
    	parseArgs("SenderThroughput", args);
    	
        boolean isrdma = network.equals("rdma");
        boolean isnaos = serializer.equals("naos");

        System.out.println(String.format("########## Sender NIO %s %s %s (%d nodes) %s ##########", network, serializer, datastructure, nodes, isdirect ?  "off-heap" : "in-heap" ));

        Serializer serializerImpl = SerializerManager.getSerializer(serializer);

        Object obj = null;
        Iterable it = null;
        if(batch_size == 0) {
            obj = DataStructureManager.setupObject(datastructure, nodes, naosarray && isnaos);
        } else {
            it = DataStructureManager.setupObjectIterable(datastructure, nodes);
            if(it == null){
                System.out.println(datastructure + "is not iterable");
                return;
            }
        }

        SocketChannel channel = null; // for tcp
        RdmaChannel rdmachannel =  null; // for dini
        EndpointRDMA rdmaep = null; // for naos
        Socket socket = null; // for naos tcp

        // setting up connections...
        System.out.println("[SenderNIO] connecting... ");
        if(isrdma){
            if(isnaos){
                ConnectParamsRDMA params = new ConnectParamsRDMA(0,0);

                PassiveEndpointRDMA pep = new ClientRDMA(host,port).connectPassive(params);
                ActiveEndpointRDMA aep = new ClientRDMA(host,port).connectActive();

                rdmaep = new EndpointRDMA(aep,pep);

                System.out.printf("Sender: Info about by connection: r:%d l:%d \n", rdmaep.getRemoteBufferSize(), rdmaep.getLocalBufferSize());
            } else {
                //Thread.sleep(1000);
                rdmachannel = new ch.ethz.rdma.RdmaClient(host, port).lazy_connect(256);
            }
        } else {
            if(isnaos){
                socket = new Socket(host,port);
            } else {
                channel = SocketChannel.open();
                channel.connect(new InetSocketAddress(host, port));
            }
        }
        System.out.println("[SenderNIO] connecting... done!");


        ThroughputHandler th = new ThroughputHandler(target_throughput,warmupinms,timeinms);

        // running test...
        if(isnaos) {
            if(batch_size == 0) { // no iterative
                if(naosarray){ // naos array send
                    if (isrdma) {
                        if (isdirect) // for simplicity, I treat it as Async
                            BenchmarkNaos.runThroughputTestAsyncSenderArray(rdmaep, obj, serializerImpl, th, nodes);
                        else
                            BenchmarkNaos.runThroughputTestSenderArray(rdmaep, obj, serializerImpl, th, nodes);
                    } else {
                        BenchmarkNaos.runThroughputTestSenderArray(socket, obj, serializerImpl, th, nodes);
                    } 
                } else {
                    if (isrdma) {
                        if (isdirect) // for simplicity, I treat it as Async
                            BenchmarkNaos.runThroughputTestAsyncSender(rdmaep, obj, serializerImpl, th);
                        else
                            BenchmarkNaos.runThroughputTestSender(rdmaep, obj, serializerImpl, th);
                    } else {
                        BenchmarkNaos.runThroughputTestSender(socket, obj, serializerImpl, th);
                    }
                }
            } else { // iterative 
                if (isrdma) {
                    if (isdirect) // for simplicity, I treat it as Async
                        BenchmarkNaos.runThroughputTestAsyncSender(rdmaep, it, serializerImpl, th, batch_size);
                    else
                        BenchmarkNaos.runThroughputTestSender(rdmaep, it, serializerImpl, th, batch_size);
                } else {
                    BenchmarkNaos.runThroughputTestSender(socket, it, serializerImpl, th, batch_size);
                }
            }
        } else { // no naos
            if (isrdma) {
                BenchmarkRDMA.runThroughputTestSender(rdmachannel, obj, serializerImpl, th);
            } else {
                BenchmarkTCP.runThroughputTestSender(channel, obj, isdirect, serializerImpl, th );
            }
        }

        if (channel!=null)
            channel.close();

        if (rdmachannel!=null)
            rdmachannel.close();

    //    if (rdmaep != null)
    //        rdmaep.close();

        if (socket != null)
            socket.close();
    }
}
