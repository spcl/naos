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
import ch.ethz.serializers.Serializer;
import ch.ethz.serializers.skyway.SkywaySerializer;

import javax.swing.plaf.synth.SynthTextAreaUI;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class BenchmarkTCP {
    public static double measureLatencyTCP(SocketChannel channel, int to_send) throws Exception {
        ByteBuffer init = ByteBuffer.allocate(4);
        // measure latency
        // warm up
        for(int i = 0; i < 500; i ++) {
            init.putInt(to_send);
            init.flip();
            channel.write(init);
            init.clear();
            int readb = channel.read(init);
            assert readb == 4 : "failed read";
            init.position(0);
        }
        // the test warm up
        long time = Utils.getTimeMicroseconds();
        for(int i = 0; i < 600; i ++) {
            init.putInt(to_send);
            init.flip();
            channel.write(init);
            init.clear();
            int readb = channel.read(init);
            assert readb == 4 : "failed read";
            init.position(0);
        }
        long total =  Utils.getTimeMicroseconds() - time;
        return (total) / 600.0;
    }

    public static int getDataSizeTCP(SocketChannel channel) throws Exception {
        ByteBuffer init = ByteBuffer.allocate(4);

        for(int i = 0; i < 1100; i ++) {
            init.clear();
            int readb = channel.read(init);
            assert readb == 4 : "failed read";
            init.position(0);
            int writeb = channel.write(init);
            assert writeb == 4 : "failed read";
        }
        init.position(0);
        return  init.getInt();
    }

    public static double measureLatencyTCP(Socket socket, int to_send) throws Exception {
        if(to_send > 255){
            System.out.printf("Error! Socket.write only send 0-255 values\n\n");
            throw new Exception("Error! Socket.write only send 0-255 values");
        }

        OutputStream output = socket.getOutputStream();
        InputStream input = socket.getInputStream();
        // measure latency
        // warm up
        for(int i = 0; i < 500; i ++) {
            output.write(to_send);
            output.flush();
            int val = input.read();
            assert val == to_send : "wrong pong value";
        }
        // the test warm up
        long time = Utils.getTimeMicroseconds();
        for(int i = 0; i < 600; i ++) {
            output.write(to_send);
            output.flush();
            int val = input.read();
            assert val == to_send : "wrong pong value";
        }
        long total =  Utils.getTimeMicroseconds() - time;
        return (total) / 600.0;
    }

    public static int getDataSizeTCP(Socket socket) throws Exception {
        OutputStream output = socket.getOutputStream();
        InputStream input = socket.getInputStream();
        int val = 0;
        for(int i = 0; i < 1100; i ++) {
            val = input.read();
            output.write(val);
            output.flush();
        }
        return  val;
    }


    public static void runLatencyTestReceiver(SocketChannel channel, boolean isdirect, Serializer serializerImpl, int iters) throws Exception{
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        int object_length = BenchmarkTCP.getDataSizeTCP(channel);
        System.out.println(String.format("[Receiver] receiving %d bytes ", object_length));
        ByteBuffer recv_buffer = null;
        if(isdirect)
            recv_buffer = ByteBuffer.allocateDirect(object_length);
        else
            recv_buffer = ByteBuffer.allocate(object_length);

        recv_buffer.clear();

        channel.configureBlocking(false);
        Selector selector = Selector.open();
        SelectionKey all_keys = channel.register(selector, SelectionKey.OP_READ,null);

        ByteBuffer pong = ByteBuffer.allocate(8);

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
                    int readb = 0 ;

                    recv_buffer.clear();
                    while(readb < object_length) {
                        readb += channel.read(recv_buffer);
                    }
                    recv_buffer.position(0);
                    long time2 = Utils.getTimeMicroseconds();
                    Object o = serializerImpl.deserializeObjectFromBuffer(recv_buffer);
                    long time3 = Utils.getTimeMicroseconds();
                    pong.clear();
                    pong.putInt((int)(time3 - time2));
                    pong.putInt((int)(time2 - time1));
                    pong.flip();
                    int writeb = channel.write(pong);
                    assert writeb == 8 : "error on send pong";
                } else {
                    assert false : "unexpected selector key event";
                }
            }

            // Note: this is needed because it unwraps the bytes into objects and you can't overwrite this memory.
            if (serializerImpl instanceof SkywaySerializer ) {
                recv_buffer = ByteBuffer.allocate(object_length);
            }
        }

        all_keys.cancel();
        selector.close();
    }

    public static void runLatencyTestSender(SocketChannel channel, Object obj, boolean isdirect, Serializer serializerImpl, int iters) throws Exception{
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        int object_length = serializerImpl.getObjectLength(obj);
        System.out.println(String.format("[SenderSocketChannel] sending %d bytes ", object_length));

        ByteBuffer send_buffer = null;
        if(isdirect)
            send_buffer = ByteBuffer.allocateDirect(object_length);
        else
            send_buffer = ByteBuffer.allocate(object_length);

        double latency = BenchmarkTCP.measureLatencyTCP(channel, object_length);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");

        // the sender is blocking
        channel.configureBlocking(true);

        ByteBuffer pong = ByteBuffer.allocate(8);

        for (int i = 0; i < iters; i++) {
            long time1 = Utils.getTimeMicroseconds();
 
            send_buffer.clear();
            serializerImpl.serializeObjectToBuffer(obj, send_buffer);
 
            send_buffer.position(0);
            long time2 = Utils.getTimeMicroseconds();
            int wbytes = 0;
            while (wbytes < object_length) {
                wbytes += channel.write(send_buffer);
            }
            long time3 = Utils.getTimeMicroseconds();
            pong.clear();
            int readb = channel.read(pong);
            assert readb == 8 : "error on pong";
            long time4 = Utils.getTimeMicroseconds();
            pong.position(0);
            long deserialization = pong.getInt();
            long receive = pong.getInt();

            System.out.println(String.format("[Sender] serialization took %d us ", time2 - time1));
            System.out.println(String.format("[Sender] send took %d us ", time3 - time2));
            System.out.println(String.format("[Receiver] receive took %d us ", receive));
            System.out.println(String.format("[Receiver] deserialization took %d us ", deserialization));
            System.out.println(String.format("[Total] total time %d us\n", (time4 - time1) ));
        }

    }

    public static void runThroughputTestSender(SocketChannel channel, Object obj, boolean isdirect, Serializer serializerImpl, ThroughputHandler th) throws Exception {
        int object_length = serializerImpl.getObjectLength(obj);
        System.out.println(String.format("[Sender] sending %d bytes ", object_length));

        ByteBuffer send_buffer = null;
        if(isdirect)
            send_buffer = ByteBuffer.allocateDirect(object_length);
        else
            send_buffer = ByteBuffer.allocate(object_length);


        double latency = BenchmarkTCP.measureLatencyTCP(channel, object_length);
        System.out.println( "[Sender] Round trip latency : " + latency + " us");
        System.out.println( "-------------------------------------------\n");

        // the sender is blocking
        channel.configureBlocking(true);
        // warm up
        long startNs = System.nanoTime();
        long currentNs = startNs;
        long endtime = startNs + th.getWarmupNs();
        long iter = 0;
        while(endtime > currentNs){

            send_buffer.clear();
            serializerImpl.serializeObjectToBuffer(obj, send_buffer);

            send_buffer.position(0);
            int wbytes = 0;
            while (wbytes < object_length) {
                wbytes += channel.write(send_buffer);
            }

            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        System.out.println(String.format("[Sender] Warmup is done. it took %d us", (currentNs - startNs) / 1000L ));

        startNs = System.nanoTime();
        currentNs = startNs;
        endtime = startNs + th.getTestNs(); 
        iter = 0;
        while(endtime > currentNs){
 
            send_buffer.clear();
            serializerImpl.serializeObjectToBuffer(obj, send_buffer);
     
    
            send_buffer.position(0);
            int wbytes = 0;
            while (wbytes < object_length) {
                wbytes += channel.write(send_buffer);
            }

            iter++;
            currentNs = th.sleepAndGetTime(iter,startNs);
        }
        currentNs = System.nanoTime();

        System.out.println(String.format("[Sender] Test is done. it took %d us, did %d iters", (currentNs - startNs) / 1000L ,iter));
        System.out.println(String.format("[Sender] achieved throughput is %.2f op/sec", (iter*1000.0F / ((currentNs - startNs) / 1_000_000.0F ) ) ));

        ByteBuffer close = ByteBuffer.allocate(1);
        close.put((byte)1);
        close.flip();
        channel.write(close);
        System.out.println("send close");
    }


    public static void runThroughputTestReceiver(SocketChannel channel, boolean isdirect, Serializer serializerImpl) throws Exception{
        int object_length = BenchmarkTCP.getDataSizeTCP(channel);
        System.out.println(String.format("[Receiver] receiving %d bytes ", object_length));
        ByteBuffer recv_buffer = null;
        if(isdirect)
            recv_buffer = ByteBuffer.allocateDirect(object_length);
        else
            recv_buffer = ByteBuffer.allocate(object_length);
        recv_buffer.clear();

        channel.configureBlocking(true);

        while (true) {
            int readb = 0 ;
            recv_buffer.clear();
            int stop_counter = 0;
            while(readb < object_length) {
                if(readb == 1){
                    if(stop_counter == 3){
                        System.out.println("Received finish byte!");
                        return;
                    }
                    stop_counter++;
                }
                int temp = channel.read(recv_buffer);
                if(temp < 0){
                    System.out.println("channel is closed");
                    return;
                }
                readb += temp;
            }
            recv_buffer.position(0);
            Object o = serializerImpl.deserializeObjectFromBuffer(recv_buffer);
            // Note: this is needed because it unwraps the bytes into objects and you can't overwrite this memory.
            if (serializerImpl instanceof SkywaySerializer) {
                recv_buffer = ByteBuffer.allocate(object_length);
            }
        }
    }

}
