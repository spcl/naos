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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import ch.ethz.serializers.Serializer;
import ch.ethz.serializers.SerializerManager;

public class TCPNetwork extends Network {
	public BlockingQueue<Object> incoming;
	public BlockingQueue<Object>[] outgoing;

	class SocketReadWorker extends Thread {
		
		private Socket socket;
		private BlockingQueue<Object> queue;
	
		public SocketReadWorker(Socket socket, BlockingQueue<Object> queue) {
			this.socket = socket;
			this.queue = queue;
		}
		
		@Override
		public void run() {
			try {
				Serializer ser = SerializerManager.getSerializer(TCPNetwork.this.serializer);

				while(true) {
					InputStream in = socket.getInputStream();
					Object o = ser.deserializeObjectFromStream(in);
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
	
	class SocketWriteWorker extends Thread {
		
		private Socket socket;
		private BlockingQueue<Object> queue;
		
	
		public SocketWriteWorker(Socket socket, BlockingQueue<Object> queue) {
			this.socket = socket;
			this.queue = queue;
		}
		
		@Override
		public void run() {
			try {
				Serializer ser = SerializerManager.getSerializer(TCPNetwork.this.serializer);
				while(true) {		
					Object o = queue.take();
					OutputStream out = socket.getOutputStream();
					ser.serializeObjectToStream(out, o);
					out.flush();
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
	
	private SocketReadWorker[] readers;
	private SocketWriteWorker[] writers;

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
	public TCPNetwork(String serializer, int nodeid, Node[] nodes) throws IOException, InterruptedException {
		super(serializer,nodeid,nodes.length);
		this.incoming = new LinkedBlockingQueue<>( );
		this.outgoing = (BlockingQueue<Object>[]) new BlockingQueue[nodes.length];
		this.incomingSocks = new Socket[nodes.length];
		this.outgoingSocks = new Socket[nodes.length];
		this.readers = new SocketReadWorker[nodes.length];
		this.writers = new SocketWriteWorker[nodes.length];
		
		
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
						readers[id] = new SocketReadWorker(incomingSocks[id], incoming);
	
						System.out.println(String.format("Received Connection from %s:%d", nodes[id].host, nodes[id].port));
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		});
		t.start();
				
		Thread.sleep(5000);
		
		// initiate outgoing connections
		for(int i = 0; i < nodes.length; i++) {
			
			if (i == nodeid) {
				continue;
			}
			
			// initiate connection
			System.out.print(String.format("Connecting to %s:%d ... ", nodes[i].host, nodes[i].port));
			outgoingSocks[i] = new Socket(nodes[i].host, nodes[i].port);
			outgoingSocks[i].setTcpNoDelay(true);
			outgoingSocks[i].getOutputStream().write(nodeid); // warning this only supports up to 256 workers! since it writes only 1 byte
			System.out.println("connected!");
					
			// setup data structures
			
			outgoing[i] = new LinkedBlockingQueue<>( );
			writers[i] = new SocketWriteWorker(outgoingSocks[i], outgoing[i]);
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

				// synchronization part.
		// 1. each worker sends a message to node 0
		// 2. once the node 0 received all requests, it replies to workers trigerring them to work
		if(nodeid == 0){
		/*	for (int id = 1; id < nodes.length; id++) {
				outgoingSocks[id].getOutputStream().write(0);
			}

			for (int id = 1; id < nodes.length; id++) {
				int val = incomingSocks[id].getInputStream().read();
				assert val == 1 : "error on stage 1 of sync";
			}

			for (int id = 1; id < nodes.length; id++) {
				outgoingSocks[id].getOutputStream().write(2);
			}*/
 
		}else{
			/*int val = incomingSocks[0].getInputStream().read();
			assert val == 0 : "error on stage 0 of sync";
			outgoingSocks[0].getOutputStream().write(1);
			val = incomingSocks[0].getInputStream().read();
			assert val == 2 : "error on stage 2 of sync";*/
		}

		for (int id = 0; id < outgoingSocks.length; id++) {
			if (outgoingSocks[id] == null) {
				// our id, skip everything
				continue;
			}

			while(outgoing[id].size()!=0){
				System.out.println("Closing with unsent objects");
				Thread.sleep(10000);
			}
			Thread.sleep(10000);

			outgoingSocks[id].close();
			incomingSocks[id].close();
			readers[id].interrupt();
			readers[id].join();
			writers[id].interrupt();
			writers[id].join();
		}
	}
}
