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
package ch.ethz.benchmarks;

import ch.ethz.naos.fd.NaosSocket;
import ch.ethz.serializers.Serializer;
import ch.ethz.serializers.SerializerManager;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class NaosTCPNetwork extends Network {
    public BlockingQueue<Object> incoming;
    public BlockingQueue<Object>[] outgoing;
    public final boolean withArray; // with writeIterator

    class ReadWorker extends Thread {

        private Socket socket;
        private BlockingQueue<Object> queue;
        private NaosSocket naos;

        public ReadWorker(Socket socket, BlockingQueue<Object> queue) {
            this.socket = socket;
            this.queue = queue;
            this.naos = new NaosSocket(socket);
        }

        @Override
        public void run() {
            runWithoutIterator();
        }


        private void runWithoutIterator() {
            try {
                while (true) {
                    int val = naos.isReadable(1000);

                    if (socket.isClosed()) {
                        break;
                    }
                    if (val < 0) { // error
                        break;
                    }
                    if (val == 0) { // is not readable
                        continue;
                    }

                    Object o = naos.readObject();
                    if (o == null) {
                        break; // connection was closed by peer;
                    }
                    queue.add(o);
                }
            } catch (SocketException se) {
                // ignore, socket got closed
            } catch (EOFException eofe) {
                // ignore, socket got closed
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class WriteWorker extends Thread {

        private Socket socket;
        private BlockingQueue<Object> queue;
        private NaosSocket naos;
        private final int batch_size;        

        public WriteWorker(Socket socket, BlockingQueue<Object> queue, int batch_size) {
            this.socket = socket;
            this.queue = queue;
            this.naos = new NaosSocket(socket);
            this.batch_size = Math.abs(batch_size);
        }

        @Override
        public void run() {
            if(withArray)
                runWithArray();
            else
                runWithoutArray();

        }

        public void runWithArray(){
            try {
                while(true) {
                    // queue.poll()
                    Object o = queue.take();
                    naos.writeArray(o,batch_size);
                }
            } catch (SocketException se) {
                // ignore, socket got closed
            } catch (InterruptedException ie) {
                // ignore, we are shutting down
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void runWithoutArray(){
            try {
                while(true) {
                    // queue.poll()
                    Object o = queue.take();
                    naos.writeObject(o);
                }
            } catch (SocketException se) {
                // ignore, socket got closed
            } catch (InterruptedException ie) {
                // ignore, we are shutting down
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }



    private Socket incomingSocks[];
    private Socket outgoingSocks[];

    private ReadWorker[] readers;
    private WriteWorker[] writers;

    @Override
    public Object receiveObject() throws InterruptedException {
        return incoming.take();
    }

    @Override
    public boolean hasReceiveObject() throws InterruptedException {
        return !incoming.isEmpty();
    }


    public void sendObject(int target, Object obj){
        this.outgoing[target].add(obj);
    }
 

    @SuppressWarnings("unchecked")
    public NaosTCPNetwork(String serializer, int nodeid, Node[] nodes, int batch_size) throws IOException, InterruptedException {
        super(serializer,nodeid,nodes.length);
        this.withArray = (batch_size<0);
        if(this.withArray){
            System.out.println("Use NaosRDMANetwork specialized array send");
        }else{
            System.out.println("Use NaosRDMANetwork normal send");
        }

        this.incoming = new LinkedBlockingQueue<>( );
        this.outgoing = (BlockingQueue<Object>[]) new BlockingQueue[nodes.length];
        this.incomingSocks = new Socket[nodes.length];
        this.outgoingSocks = new Socket[nodes.length];
        this.readers = new ReadWorker[nodes.length];
        this.writers = new WriteWorker[nodes.length];


        ServerSocket server = new ServerSocket(nodes[nodeid].port);

        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                for (int i = 0; i < nodes.length; i++) {

                    if (i == nodeid) {
                        continue;
                    }

                    try {
                        Socket sock = server.accept();
                        sock.setTcpNoDelay(true);
                        int id = sock.getInputStream().read(); // warning this only supports up to 256 workers!

                        incomingSocks[id] = sock;
                        readers[id] = new ReadWorker(incomingSocks[id], incoming);

                        System.out.println(String.format("Received Connection from %s:%d", nodes[id].host, nodes[id].port));
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        t.start();

        Thread.sleep(4000);

        // initiate outgoing connections
        for(int i = 0; i < nodes.length; i++) {

            if (i == nodeid) {
                continue;
            }

            // initiate connection
            System.out.print(String.format("Connecting to %s:%d ... ", nodes[i].host, nodes[i].port));
            outgoingSocks[i] = new Socket(nodes[i].host, nodes[i].port);
            outgoingSocks[i].setTcpNoDelay(true);
            outgoingSocks[i].getOutputStream().write(nodeid); // warning this only supports up to 256 workers!
            System.out.println("connected!");

            // setup data structures

            outgoing[i] = new LinkedBlockingQueue<>();
            writers[i] = new WriteWorker(outgoingSocks[i], outgoing[i], batch_size);
        }

        t.join();


        server.close();
        // synchronization part.
        // 1. each worker sends a message to node 0
        // 2. once the node 0 received all requests, it replies to workers trigerring them to work
        if(nodeid == 0){
            for (int id = 1; id < nodes.length; id++) {
                outgoingSocks[id].getOutputStream().write(0);
            }

            for (int id = 1; id < nodes.length; id++) {
                int val = incomingSocks[id].getInputStream().read();
                assert val == 1 : "error on stage 1 of sync";
            }

            for (int id = 1; id < nodes.length; id++) {
                outgoingSocks[id].getOutputStream().write(2);
            }
 
        }else{
            int val = incomingSocks[0].getInputStream().read();
            assert val == 0 : "error on stage 0 of sync";
            outgoingSocks[0].getOutputStream().write(1);
            val = incomingSocks[0].getInputStream().read();
            assert val == 2 : "error on stage 2 of sync";
        }


        // start worker threads
        for (int id = 0; id < nodes.length; id++) {
            if (id != nodeid) {
                readers[id].start();
                writers[id].start();
            }
        }
    }

    public void shutdown() throws IOException, InterruptedException {
        for (int id = 0; id < outgoingSocks.length; id++) {
            if (outgoingSocks[id] == null) {
                // our id, skip everything
                continue;
            }
            outgoingSocks[id].close();
            incomingSocks[id].close();
            readers[id].interrupt();
            readers[id].join();
            writers[id].interrupt();
            writers[id].join();
        }
    }
}
