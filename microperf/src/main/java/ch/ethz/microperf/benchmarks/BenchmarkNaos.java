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
package ch.ethz.microperf.benchmarks;

import ch.ethz.microperf.ThroughputHandler;
import ch.ethz.microperf.Utils;
import ch.ethz.naos.fd.NaosSocket;
import ch.ethz.naos.rdma.EndpointRDMA;
import ch.ethz.serializers.Serializer;
import ch.ethz.naos.fd.NaosSocketChannel;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
//import java.util.Iterator;

import ch.ethz.microperf.datastructures.PointFloat;
import ch.ethz.microperf.datastructures.Pair;
import java.util.ArrayList;
import java.util.Iterator;

public class BenchmarkNaos {

    public static double measureLatencyRDMA(EndpointRDMA ep, int to_send) throws Exception {
        // warm up
        for(int i = 0; i < 500; i ++) {
            ep.writeInt(to_send);
            int val = ep.readInt();
           // assert val == to_send : "wrong int value";
        }
        // the test warm up
        long time = Utils.getTimeMicroseconds();
        for(int i = 0; i < 600; i ++) {
            ep.writeInt(to_send);
            int val = ep.readInt();
        }
        long total =  Utils.getTimeMicroseconds() - time;
        return (total) / 600.0;
    }

    public static int getDataSizeRDMA(EndpointRDMA ep) throws Exception {

        int val = 0;
        for(int i = 0; i < 1100; i ++) {
            val = ep.readInt();
            ep.writeInt(val);
        }

        return val;
    }

    public static void runLatencyTestReceiver(SocketChannel channel, Serializer serializerImpl, int iters) throws Exception{
        BenchmarkTCP.getDataSizeTCP(channel); // to perform latency test // it is used to warm up
        channel.configureBlocking(false);
        Selector selector = Selector.open();
        SelectionKey all_keys = channel.register(selector, SelectionKey.OP_READ,null);

        ByteBuffer pong = ByteBuffer.allocate(4);

        NaosSocketChannel naos = new NaosSocketChannel(channel);

        for (int i = 0; i < iters; i++) {
            selector.select();
            Iterator selectedKeys = selector.selectedKeys().iterator();
            while (selectedKeys.hasNext()) {
                SelectionKey key = (SelectionKey) selectedKeys.next();
                selectedKeys.remove();

                if (!key.isValid()) {
                    continue;
                }
                if (key.isReadable()) {
                    //SocketChannel chan = (SocketChannel) key.channel();
                    long time1 = Utils.getTimeMicroseconds();
                    Object o = naos.readObject();
                    long time2 = Utils.getTimeMicroseconds();
                    pong.clear();
                    pong.putInt((int)(time2 - time1));
                    pong.flip();
                    int writeb = channel.write(pong);
                    assert writeb == 4 : "error on send pong";
                } else {
                    assert false : "unexpected selector key event";
                }
            }
        }
    }

    public static void runLatencyTestSender(SocketChannel channel, Object obj, Serializer serializerImpl, int iters) throws Exception{
        ByteBuffer pong = ByteBuffer.allocate(4);

        double latency = BenchmarkTCP.measureLatencyTCP(channel, 0);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
        NaosSocketChannel naos = new NaosSocketChannel(channel);
 
        for (int i = 0; i < iters; i++) {
            long time1 = Utils.getTimeMicroseconds();

            naos.writeObject(obj);
  
            long time2 = Utils.getTimeMicroseconds();
            pong.clear();
            int readb = channel.read(pong);;
            assert readb == 4 : "error on pong";
            long time3 = Utils.getTimeMicroseconds();
            pong.position(0);
            long receive = pong.getInt();

            System.out.println(String.format("[Sender] serialization took 0 us "));
            System.out.println(String.format("[Sender] send took %d us ", time2 - time1));
            System.out.println(String.format("[Receiver] receive took %d us ", receive));
            System.out.println(String.format("[Receiver] deserialization took 0 us "));
            System.out.println(String.format("[Total] total time %d us\n", (time3 - time1)));
        }
    }

    //array
    public static void runLatencyTestSenderArray(SocketChannel channel, Object obj, Serializer serializerImpl, int iters, int array_size) throws Exception{
        ByteBuffer pong = ByteBuffer.allocate(4);

        double latency = BenchmarkTCP.measureLatencyTCP(channel, 0);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
        NaosSocketChannel naos = new NaosSocketChannel(channel);

        for (int i = 0; i < iters; i++) {
            long time1 = Utils.getTimeMicroseconds();
 
            System.out.println("send array");
            naos.writeArray(obj,array_size);

            long time2 = Utils.getTimeMicroseconds();
            pong.clear();
            int readb = channel.read(pong);;
            assert readb == 4 : "error on pong";
            long time3 = Utils.getTimeMicroseconds();
            pong.position(0);
            long receive = pong.getInt();

            System.out.println(String.format("[Sender] serialization took 0 us "));
            System.out.println(String.format("[Sender] send took %d us ", time2 - time1));
            System.out.println(String.format("[Receiver] receive took %d us ", receive));
            System.out.println(String.format("[Receiver] deserialization took 0 us "));
            System.out.println(String.format("[Total] total time %d us\n", (time3 - time1)));
        }
    }

    // for iterative send
    public static void runLatencyTestSender(Socket socket, Iterable it, Serializer serializerImpl, int iters, int batch) throws Exception{
        socket.setTcpNoDelay(true);
        double latency = BenchmarkTCP.measureLatencyTCP(socket, 0);
        System.out.println( "[SenderSocketIterator] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
        NaosSocket naos = new NaosSocket(socket);

        byte[] bytes = new byte[4];
        InputStream input = socket.getInputStream(); // it used to read int from socket

        for (int i = 0; i < iters; i++) {
            long time1 = Utils.getTimeMicroseconds();
            int sent_objects = naos.writeIterable(it.iterator(),batch);
            //System.out.println( "send iterator " + sent_objects + " with batch " +batch);
            long time2 = Utils.getTimeMicroseconds();
            int ret = input.read(bytes);
            assert ret == 4 : "read the wrong number of bytes";
            long time3 = Utils.getTimeMicroseconds();
            int pong = ByteBuffer.wrap(bytes).getInt();
            long receive = pong;

            System.out.println(String.format("[Sender] serialization took 0 us "));
            System.out.println(String.format("[Sender] send took %d us ", time2 - time1));
            System.out.println(String.format("[Receiver] receive took %d us ", receive));
            System.out.println(String.format("[Receiver] deserialization took 0 us "));
            System.out.println(String.format("[Total] total time %d us\n", (time3 - time1)));
        }
    }



    // for blocking sockets
    public static void runLatencyTestSender(Socket socket, Object obj, Serializer serializerImpl, int iters) throws Exception{
        socket.setTcpNoDelay(true);
        double latency = BenchmarkTCP.measureLatencyTCP(socket, 0);
        System.out.println( "[SenderSocket] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
        NaosSocket naos = new NaosSocket(socket);

        byte[] bytes = new byte[4];
        InputStream input = socket.getInputStream(); // it used to read int from socket
 
        for (int i = 0; i < iters; i++) {
            long time1 = Utils.getTimeMicroseconds();
 
            naos.writeObject(obj);

            long time2 = Utils.getTimeMicroseconds();
            int ret = input.read(bytes);
            assert ret == 4 : "read the wrong number of bytes";
            long time3 = Utils.getTimeMicroseconds();
            int pong = ByteBuffer.wrap(bytes).getInt();
            long receive = pong;

            System.out.println(String.format("[Sender] serialization took 0 us "));
            System.out.println(String.format("[Sender] send took %d us ", time2 - time1));
            System.out.println(String.format("[Receiver] receive took %d us ", receive));
            System.out.println(String.format("[Receiver] deserialization took 0 us "));
            System.out.println(String.format("[Total] total time %d us\n", (time3 - time1) ));
        }

    }
    // for array send
    public static void runLatencyTestSenderArray(Socket socket, Object obj, Serializer serializerImpl, int iters, int array_size) throws Exception{
        socket.setTcpNoDelay(true);
        double latency = BenchmarkTCP.measureLatencyTCP(socket, 0);
        System.out.println( "[SenderSocketArray] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
        NaosSocket naos = new NaosSocket(socket);

        byte[] bytes = new byte[4];
        InputStream input = socket.getInputStream(); // it used to read int from socket

        for (int i = 0; i < iters; i++) {
            long time1 = Utils.getTimeMicroseconds();
            
            naos.writeArray(obj,array_size);
            
            //naos.writeObject(obj);
            long time2 = Utils.getTimeMicroseconds();
            int ret = input.read(bytes);
            assert ret == 4 : "read the wrong number of bytes";
            long time3 = Utils.getTimeMicroseconds();
            int pong = ByteBuffer.wrap(bytes).getInt();
            long receive = pong;

            System.out.println(String.format("[Sender] serialization took 0 us "));
            System.out.println(String.format("[Sender] send took %d us ", time2 - time1));
            System.out.println(String.format("[Receiver] receive took %d us ", receive));
            System.out.println(String.format("[Receiver] deserialization took 0 us "));
            System.out.println(String.format("[Total] total time %d us\n", (time3 - time1) ));
        }

    }


    // for testing iterable naos
    public static void runLatencyTestReceiver(Socket socket, Serializer serializerImpl, int iters, int size) throws Exception{
        socket.setTcpNoDelay(true);
        Object[] arr  = new Object[size];

        BenchmarkTCP.getDataSizeTCP(socket); // to perform latency test

        NaosSocket naos = new NaosSocket(socket);
        OutputStream output = socket.getOutputStream();

        byte[] bytes = new byte[4];


        for (int i = 0; i < iters; i++) {
            int isread = naos.isReadable(-1);
            long time1 = Utils.getTimeMicroseconds();
            for(int j=0; j<size; j++){
                arr[j] = naos.readObject();
            }
            long time2 = Utils.getTimeMicroseconds();
            int pong = ((int)(time2 - time1));
            ByteBuffer.wrap(bytes).putInt(pong);
            output.write(bytes);
            output.flush();
        }

    }

    public static void runLatencyTestReceiver(Socket socket, Serializer serializerImpl, int iters) throws Exception{
        socket.setTcpNoDelay(true);
        BenchmarkTCP.getDataSizeTCP(socket); // to perform latency test
 

        NaosSocket naos = new NaosSocket(socket);
        OutputStream output = socket.getOutputStream();

        byte[] bytes = new byte[4];

        for (int i = 0; i < iters; i++) {
            int isread = naos.isReadable(-1);
            long time1 = Utils.getTimeMicroseconds();
            Object o = naos.readObject();
            long time2 = Utils.getTimeMicroseconds();
            int pong = ((int)(time2 - time1));
            ByteBuffer.wrap(bytes).putInt(pong);
            output.write(bytes);
            output.flush();
        }

    }

///////////////////////////////////////////// RDMA //////////////////////////////////////////////



// for iterative receive
    public static void runLatencyTestReceiver(EndpointRDMA ep, Serializer serializerImpl, int iters, int size) throws Exception{
        Object[] arr  = new Object[size];
        int val = getDataSizeRDMA(ep);
        //  System.out.println( "[recv] get : " + val + " value");
        //  System.out.println( "-------------------------------------------\n")

        for (int i = 0; i < iters; i++) {
            int stat = 0;
            while(stat == 0){
                stat = ep.isReadable(1000); // blocking
            }

            long time1 = Utils.getTimeMicroseconds();
            for(int j=0; j<size; j++){
                arr[j] = ep.readObject();
            }
            long time2 = Utils.getTimeMicroseconds();
            ep.writeInt((int)(time2-time1));
        }
    }

    public static void runLatencyTestReceiver(EndpointRDMA ep, Serializer serializerImpl, int iters) throws Exception{
        int val = getDataSizeRDMA(ep);
      //  System.out.println( "[recv] get : " + val + " value");
      //  System.out.println( "-------------------------------------------\n");
       for (int i = 0; i < iters; i++) {
            int stat = 0;
            while(stat == 0){
                stat = ep.isReadable(1000); // blocking
            }

            long time1 = Utils.getTimeMicroseconds();
            Object o = ep.readObject();
            long time2 = Utils.getTimeMicroseconds();
            ep.writeInt((int)(time2-time1));
       }
    }

    // iterative send
    public static void runLatencyTestSender(EndpointRDMA ep,  Iterable it,  Serializer serializerImpl, int iters,int batch) throws Exception{

        double latency = measureLatencyRDMA(ep,666);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");

        for (int i = 0; i < iters; i++) {
            long time1 = Utils.getTimeMicroseconds();
            ep.writeIterable(it.iterator(),batch);
            long time2 = Utils.getTimeMicroseconds();
            int receive = ep.readInt(); // blocking
            long time3 = Utils.getTimeMicroseconds();

            System.out.println(String.format("[Sender] serialization took 0 us "));
            System.out.println(String.format("[Sender] send took %d us ", time2 - time1));
            System.out.println(String.format("[Receiver] receive took %d us ", receive));
            System.out.println(String.format("[Receiver] deserialization took 0 us "));
            System.out.println(String.format("[Total] total time %d us\n", (time3 - time1)));
        }

    }

    // normal send
    public static void runLatencyTestSender(EndpointRDMA ep,  Object obj,  Serializer serializerImpl, int iters) throws Exception{

        double latency = measureLatencyRDMA(ep,666);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
 
        for (int i = 0; i < iters; i++) {
            long time1 = Utils.getTimeMicroseconds();
 
            ep.writeObject(obj);
 
            long time2 = Utils.getTimeMicroseconds();
            int receive = ep.readInt(); // blocking
            long time3 = Utils.getTimeMicroseconds();

            System.out.println(String.format("[Sender] serialization took 0 us "));
            System.out.println(String.format("[Sender] send took %d us ", time2 - time1));
            System.out.println(String.format("[Receiver] receive took %d us ", receive));
            System.out.println(String.format("[Receiver] deserialization took 0 us "));
            System.out.println(String.format("[Total] total time %d us\n", (time3 - time1) ));
        }
    }

    // array send
    public static void runLatencyTestSenderArray(EndpointRDMA ep,  Object obj,  Serializer serializerImpl, int iters, int array_size) throws Exception{

        double latency = measureLatencyRDMA(ep,666);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");

        for (int i = 0; i < iters; i++) {
            long time1 = Utils.getTimeMicroseconds();

            ep.writeArray(obj,array_size);
        
            long time2 = Utils.getTimeMicroseconds();
            int receive = ep.readInt(); // blocking
            long time3 = Utils.getTimeMicroseconds();

            System.out.println(String.format("[Sender] serialization took 0 us "));
            System.out.println(String.format("[Sender] send took %d us ", time2 - time1));
            System.out.println(String.format("[Receiver] receive took %d us ", receive));
            System.out.println(String.format("[Receiver] deserialization took 0 us "));
            System.out.println(String.format("[Total] total time %d us\n", (time3 - time1) ));
        }
    }

    // iterative 
    public static void runThroughputTestAsyncSender(EndpointRDMA ep, Iterable it, Serializer serializerImpl, ThroughputHandler th, int batch_size) throws Exception {
        double latency = measureLatencyRDMA(ep,666);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------Async-----------------\n");

        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;
        while(endtime > currentNs){
            ep.writeIterableAsync(it.iterator(),batch_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs();
        iter = 0;
        long handle = 0;
        while(endtime > currentNs){
            handle = ep.writeIterableAsync(it.iterator(),batch_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,iter));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (iter*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        System.out.printf("Wait for handle %d \n",handle);
        ep.waitHandle(handle);

        ep.writeInt(1); // to close
    }

    public static void runThroughputTestAsyncSender(EndpointRDMA ep, Object obj, Serializer serializerImpl, ThroughputHandler th) throws Exception {
        double latency = measureLatencyRDMA(ep,666);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------Async-----------------\n");

        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;
        while(endtime > currentNs){
            ep.writeObjectAsync(obj);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs();
        iter = 0;
        long handle = 0;
        while(endtime > currentNs){
            handle = ep.writeObjectAsync(obj);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,iter));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (iter*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        System.out.printf("Wait for handle %d \n",handle);
        ep.waitHandle(handle);

        ep.writeInt(1); // to close
    }

    public static void runThroughputTestAsyncSenderArray(EndpointRDMA ep, Object obj, Serializer serializerImpl, ThroughputHandler th, int array_size) throws Exception {
        double latency = measureLatencyRDMA(ep,666);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------Async-----------------\n");

        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;
        while(endtime > currentNs){
            ep.writeArrayAsync(obj,array_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs();
        iter = 0;
        long handle = 0;
        while(endtime > currentNs){
            handle = ep.writeArrayAsync(obj,array_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,iter));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (iter*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        System.out.printf("Wait for handle %d \n",handle);
        ep.waitHandle(handle);

        ep.writeInt(1); // to close
    }

    public static void runThroughputTestSender(EndpointRDMA ep, Iterable it, Serializer serializerImpl, ThroughputHandler th, int batch_size) throws Exception {
        double latency = measureLatencyRDMA(ep,666);

        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");

        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;
        while(endtime > currentNs){
            ep.writeIterable(it.iterator(),batch_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs();
        iter = 0;
        while(endtime > currentNs){
            ep.writeIterable(it.iterator(),batch_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,iter));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (iter*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        ep.writeInt(1); // to close
    }

    public static void runThroughputTestSender(EndpointRDMA ep, Object obj, Serializer serializerImpl, ThroughputHandler th) throws Exception {
        double latency = measureLatencyRDMA(ep,666);

        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");

        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;
        while(endtime > currentNs){
            ep.writeObject(obj);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs();
        iter = 0;
        while(endtime > currentNs){
            ep.writeObject(obj);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,iter));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (iter*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        ep.writeInt(1); // to close
    }

    public static void runThroughputTestSenderArray(EndpointRDMA ep, Object obj, Serializer serializerImpl, ThroughputHandler th, int array_size) throws Exception {
        double latency = measureLatencyRDMA(ep,666);

        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");

        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;
        while(endtime > currentNs){
            ep.writeArray(obj,array_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs();
        iter = 0;
        while(endtime > currentNs){
            ep.writeArray(obj,array_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,iter));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (iter*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        ep.writeInt(1); // to close
    }

    public static void runThroughputTestReceiver(EndpointRDMA ep, Serializer serializerImpl) throws Exception {
        int val = getDataSizeRDMA(ep);

        while(true){
            int stat = 0;
            while(stat == 0){
                stat = ep.isReadable(0); // blocking
            }
            if(stat == 2){
                // end of test. we have readable int.
                System.out.println("Received finish int!");
                break;
            }
            Object o = ep.readObject();
        }
    }

    // iterative
    public static void runThroughputTestSender(Socket socket, Iterable it, Serializer serializerImpl, ThroughputHandler th, int batch_size) throws Exception {
        double latency = BenchmarkTCP.measureLatencyTCP(socket, 0);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
        NaosSocket naos = new NaosSocket(socket);

        InputStream input = socket.getInputStream();

        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;
        while(endtime > currentNs){
            naos.writeIterable(it.iterator(),batch_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs();
        iter = 0;
        while(endtime > currentNs){
            naos.writeIterable(it.iterator(),batch_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,iter));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (iter*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        // do nothing. It will be closed because of empty read on the receiver
    }


    // normal test
    public static void runThroughputTestSender(Socket socket, Object obj, Serializer serializerImpl, ThroughputHandler th) throws Exception {
        double latency = BenchmarkTCP.measureLatencyTCP(socket, 0);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
        NaosSocket naos = new NaosSocket(socket);

        InputStream input = socket.getInputStream();

        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;
        while(endtime > currentNs){
            naos.writeObject(obj);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs();
        iter = 0;
        while(endtime > currentNs){
            naos.writeObject(obj);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,iter));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (iter*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        // do nothing. It will be closed because of empty read on the receiver
    }

    public static void runThroughputTestSenderArray(Socket socket, Object obj, Serializer serializerImpl, ThroughputHandler th, int array_size) throws Exception {
        double latency = BenchmarkTCP.measureLatencyTCP(socket, 0);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");
        NaosSocket naos = new NaosSocket(socket);

        InputStream input = socket.getInputStream();

        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;
        while(endtime > currentNs){
            naos.writeArray(obj,array_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs();
        iter = 0;
        while(endtime > currentNs){
            naos.writeArray(obj,array_size);
            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,iter));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (iter*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        // do nothing. It will be closed because of empty read on the receiver
    }

    public static void runThroughputTestReceiver(Socket socket, Serializer serializerImpl) throws Exception {
        BenchmarkTCP.getDataSizeTCP(socket); // to perform latency test
        NaosSocket naos = new NaosSocket(socket);

        while(true){
            int isreadable = naos.isReadable(-1);
            Object o = naos.readObject();
            if(o == null) {
                // the connection is closed by the other side
                System.out.println("Received finish!");
                break;
            }
        }
    }
}
