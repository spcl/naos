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

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.LinkedBlockingQueue;

import ch.ethz.naos.rdma.*;
import ch.ethz.serializers.Serializer;
import ch.ethz.serializers.SerializerManager;

public class NaosRDMANetwork extends Network {
	public BlockingQueue<Object> incoming;
	public BlockingQueue<Object>[] outgoing;
	private final boolean withArray;

	class ReadWorker extends Thread {

		private PassiveEndpointRDMA ep;
		private BlockingQueue<Object> queue;

		AtomicBoolean running;

		public ReadWorker(PassiveEndpointRDMA ep, BlockingQueue<Object> queue) {
			this.running = new AtomicBoolean(true);
			this.ep = ep;
			this.queue = queue;

		}

        public int GetToken() throws Exception{
        	return ep.readInt();
        }
		@Override
		public void run() {
			runWithoutIterator();
		}


		public void runWithoutIterator() {
			try {
				while(true) {
					int val = ep.isReadable(1000);
					if(val!=1) { // 2 and 3 means that there is an int to read
						continue;
					}

					Object obj = ep.readObject();
					if(obj == null){
						System.out.printf("Unexpected null receive\n");
						continue;
					}

					queue.add(obj);
					if(!running.get()){
						break;
					}
				}
			} catch (SocketException se) {
				// ignore, socket got closed
			} catch (EOFException eofe) {
				// ignore, socket got closed
			} catch (Exception e) {
				e.printStackTrace();
			}
		}


		void close(){
			running.set(false);
		}
	}
	
	class WriteWorker extends Thread {
		
		private ActiveEndpointRDMA ep;
		private BlockingQueue<Object> queue;
		private final boolean async;
		
		private final int batch_size;
	
		public WriteWorker(ActiveEndpointRDMA ep, BlockingQueue<Object> queue, boolean async, int batch_size) {
			this.ep = ep;
			this.queue = queue;
			this.async=async;

			this.batch_size = Math.abs(batch_size);
		}

		// send token can be used only during connection esteblishment
		public void SendToken(int token) throws Exception{
            ep.writeInt(token);
		}

		public void AppendToken(int token) {
			queue.add(new Integer(token));
		}
		
		@Override
		public void run() {
			if(withArray)
				runWithArraySend();
			else
				runWithoutArraySend();
		}
 

		public void runWithoutArraySend() {
			try {
				while (true) {
					Object obj = queue.take();
					if (async)
						ep.writeObjectAsync(obj); 
					else
						ep.writeObject(obj);
				}
			} catch (SocketException se) {
				// ignore, socket got closed
			} catch (InterruptedException ie) {
				// ignore, we are shutting down
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void runWithArraySend() {
			try {
				while (true) {
					Object obj = queue.take();
					if (async)
						ep.writeArrayAsync	(obj,batch_size);
					else
						ep.writeArray(obj,batch_size);
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
	public NaosRDMANetwork(String serializer, int nodeid, Node[] nodes, int batch_size) throws Exception, InterruptedException {
		super(serializer,nodeid,nodes.length);
		this.withArray = (batch_size<0);

        if(this.withArray){
            System.out.println("Use NaosRDMANetwork specialized array send");
        }else{
            System.out.println("Use NaosRDMANetwork normal send");
        }

		this.incoming = new LinkedBlockingQueue<>();
		this.outgoing = (BlockingQueue<Object>[]) new BlockingQueue[nodes.length];
		this.readers = new ReadWorker[nodes.length];
		this.writers = new WriteWorker[nodes.length];


		ServerRDMA server = new ServerRDMA(nodes[nodeid].host,nodes[nodeid].port);

		boolean async = serializer.contains("async");

        if(async){
            System.out.println("Use NaosRDMANetwork with async send");
        }else{
            System.out.println("Use NaosRDMANetwork without async send");
        }
		
		// accept connections with nodes with higher id.
		for(int j = 0; j < 2; j ++) { // accept 2 endpoints per node
			for (int i = nodeid + 1; i < nodes.length; i++) {
				// accept connection
				RawEndpointRDMA ep = server.accept();

				int id = ep.getConnectId();
				System.out.print(String.format("Get connect request with nodeid %d \n", id));
				if (ep.isActive()) {
					ActiveEndpointRDMA aep = ep.acceptActive();
					outgoing[id] = new LinkedBlockingQueue<>();
					writers[id] = new WriteWorker(aep, outgoing[id], async,batch_size);
					System.out.println(String.format("Received Active Connection from %s:%d", nodes[id].host, nodes[id].port));
				} else {
					ConnectParamsRDMA params = new ConnectParamsRDMA(30*1024*1024, 4);
					PassiveEndpointRDMA pep = ep.acceptPassive(params);
					readers[id] = new ReadWorker(pep, incoming);
					System.out.println(String.format("Received Passive Connection from %s:%d", nodes[id].host, nodes[id].port));
				}

			}
		}
		
		// Waiting for all nodes to open their server sockets.
		Thread.sleep(1000);
		
		// initiate connections with nodes with lower id.
		for(int id = 0; id < nodeid; id++) {
			// initiate connection
			System.out.print(String.format("Connecting to %s:%d ... with nodeid %d \n", nodes[id].host, nodes[id].port, nodeid));

			ConnectParamsRDMA params = new ConnectParamsRDMA(30*1024*1024, 4,nodeid);
			PassiveEndpointRDMA pep = (new ClientRDMA(nodes[id].host, nodes[id].port)).connectPassive(params);

			System.out.println("connected passive!");

			ActiveEndpointRDMA aep = (new ClientRDMA(nodes[id].host, nodes[id].port)).connectActive(nodeid);

			System.out.println("connected active!");
					
			// setup data structures
			outgoing[id] = new LinkedBlockingQueue<>();
			readers[id] = new ReadWorker(pep, incoming);
			writers[id] = new WriteWorker(aep, outgoing[id],async, batch_size);
		}
				
		server.close();

		// synchronization part.
		// 1. each worker sends a message to node 0
		// 2. once the node 0 received all requests, it replies to workers trigerring them to work
		if(nodeid == 0){
			for (int id = 1; id < nodes.length; id++) {
				writers[id].SendToken(0);
			}

			for (int id = 1; id < nodes.length; id++) {
				int val = readers[id].GetToken();
				assert val == 1 : "error on stage 1 of sync";
			}

			for (int id = 1; id < nodes.length; id++) {
				writers[id].SendToken(2);
			}
 
		}else{
			int val = readers[0].GetToken();
			assert val == 0 : "error on stage 0 of sync";
			writers[0].SendToken(1);
			val = readers[0].GetToken();
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
		for (int id = 0; id < readers.length; id++) {
			if (readers[id] != null) {
				readers[id].close();
				readers[id].interrupt();
				readers[id].join();
			}
			if (writers[id] != null) {
				writers[id].interrupt();
				writers[id].join();
			}
		}
	}
}
