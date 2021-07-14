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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.LinkedBlockingQueue;

import ch.ethz.rdma.RdmaChannel;
import ch.ethz.rdma.RdmaClient;
import ch.ethz.rdma.RdmaEP;
import ch.ethz.rdma.RdmaServer;
import ch.ethz.serializers.Serializer;
import ch.ethz.serializers.SerializerManager;
import com.ibm.disni.verbs.*;


public class DisniRDMANetwork extends Network {
	public BlockingQueue<Object> incoming;
	public BlockingQueue<Object>[] outgoing;

	static int BUFFERSIZE = 64*1024*1024; // 64 MIB
	static int max_sends = 10;
	static int max_recvs = 10;
	
	class DisniReadWorker extends Thread {

		private RdmaChannel channel;
		private RdmaEP ep;
		private BlockingQueue<Object> queue;
		private Serializer ser;
		private ByteBuffer inputbuffers[];
		private IbvMr mrs[];
		private SVCPostRecv recvs[];

		SVCReqNotify regnotify;
		SVCPollCq pollrecvCqCall;
		IbvWC[] wcListrecv;
		IbvCompChannel comp;

		AtomicBoolean running;


		public DisniReadWorker(RdmaChannel channel, int max_recvs, BlockingQueue<Object> queue) throws IOException {
			running=new AtomicBoolean(true);

			this.ep = channel.ep;
			this.channel = channel;
			this.queue = queue;
			try {
				this.ser = SerializerManager.getSerializer(DisniRDMANetwork.this.serializer);
			} catch (Exception exp){
				System.out.println("no serializer " + DisniRDMANetwork.this.serializer);
			}
			inputbuffers = new ByteBuffer[max_recvs];
			mrs = new IbvMr[max_recvs];
			recvs = new SVCPostRecv[max_recvs];

			for(int i= 0; i <max_recvs; i++ ){
				inputbuffers[i] = ByteBuffer.allocateDirect(BUFFERSIZE);
				int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
				mrs[i] = ep.pd.regMr(inputbuffers[i],access).execute().free().getMr();

				LinkedList<IbvRecvWR> wrList_recv = new LinkedList<IbvRecvWR>();
				IbvSge sgeRecv = new IbvSge();
				sgeRecv.setAddr(mrs[i].getAddr());
				sgeRecv.setLength(mrs[i].getLength());
				sgeRecv.setLkey(mrs[i].getLkey());
				LinkedList<IbvSge> sgeListRecv = new LinkedList<IbvSge>();
				sgeListRecv.add(sgeRecv);
				IbvRecvWR recvWR = new IbvRecvWR();
				recvWR.setSg_list(sgeListRecv);
				recvWR.setWr_id(i);
				wrList_recv.add(recvWR);

				recvs[i] = ep.qp.postRecv(wrList_recv, null);
				recvs[i].execute();
			}

			// for blocking reads
			regnotify = channel.attr.getRecv_cq().reqNotification(false);

			wcListrecv = new IbvWC[1];
			wcListrecv[0] = new IbvWC();
			pollrecvCqCall = channel.attr.getRecv_cq().poll(wcListrecv, 1);
			comp = channel.attr.getRecv_cq().getChannel();

			regnotify.execute();

		}

		boolean read_recv() throws IOException {

			while (true) {
				int res = pollrecvCqCall.execute().getPolls();
				if (res < 0) {
					System.out.println("Receive error!!! ");
				}
				if (res > 0) {
					if ( wcListrecv[0].getStatus() == IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal() ){
						return false;
					}
					return true;
				}

				if(res < 0){
					return true;
				}

				if(!running.get()){
					return true;
				}
				boolean success = comp.getCqEvent(channel.attr.getRecv_cq(), 1000); // it blocks
				if (success) {
					channel.attr.getRecv_cq().ackEvents(1);
					regnotify.execute();
				}
			}
		}


		public int GetToken() throws Exception{
				boolean disconnect = read_recv(); // blocking
				if(disconnect){
					return -1;
				}

				int bufid = (int) wcListrecv[0].getWr_id();
				int size = wcListrecv[0].getByte_len();
				assert size == 0 : "Unexpected message size during sync";
				recvs[bufid].execute();// repost recv
				return wcListrecv[0].getImm_data();
		}


		@Override
		public void run() {
			try {
				while(true) {
					boolean disconnect = read_recv(); // blocking
					if(disconnect){
						break;
					}

					int bufid = (int) wcListrecv[0].getWr_id();
					int size = wcListrecv[0].getByte_len();

					// block on CQ.
					ByteBuffer buf = inputbuffers[bufid];
					buf.position(0);
					buf.limit(size);

					Object obj = ser.deserializeObjectFromBuffer(buf);
					queue.add(obj);

					recvs[bufid].execute();// repost recv

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
	
	class DisniWriteWorker extends Thread {

		private RdmaChannel channel;
		private RdmaEP ep;
		private BlockingQueue<Object> queue;
		private Serializer ser;
		ByteBuffer outputbufers[];
		IbvMr mrs[];

		LinkedList<Integer> available = new LinkedList<>();

		IbvWC[] wcListsend;
		SVCPollCq pollsendCqCall;

		AtomicBoolean running;

		public DisniWriteWorker(RdmaChannel channel, int max_sends,  BlockingQueue<Object> queue) throws IOException {
			this.running = new AtomicBoolean(true);
			this.ep = channel.ep;
			this.channel = channel;
			this.queue = queue;
			try {
				this.ser = SerializerManager.getSerializer(DisniRDMANetwork.this.serializer);
			} catch (Exception exp){
				System.out.println("no serializer " + DisniRDMANetwork.this.serializer);
			}
			outputbufers = new ByteBuffer[max_sends];
			mrs = new IbvMr[max_sends];


			for(int i= 0; i <max_sends; i++ ){
				outputbufers[i] = ByteBuffer.allocateDirect(BUFFERSIZE);
				int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
				mrs[i] = ep.pd.regMr(outputbufers[i],access).execute().free().getMr();
				available.push(i);
			}
			wcListsend = new IbvWC[1];
			wcListsend[0] = new IbvWC();
			pollsendCqCall = channel.attr.getSend_cq().poll(wcListsend, 1);
		}

		void poll_completions() throws IOException {
			while(pollsendCqCall.execute().getPolls() > 0){
				int id = (int)wcListsend[0].getWr_id(); 
				if(id != 1001)
					available.push(id);
			}
		}


		void SendToken(int token) throws Exception{
			IbvSge sgeSend = new IbvSge();
			LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
			sgeList.add(sgeSend);
			IbvSendWR sendWR = new IbvSendWR();
			sendWR.setWr_id(1001);
			sendWR.setSg_list(sgeList);
			sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
			sendWR.setImm_data(token);
			LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();
			wrList_send.add(sendWR);

			SVCPostSend send = ep.qp.postSend(wrList_send, null);
			send.execute().free();
			poll_completions();
		}

		
		@Override
		public void run() {
			try {
				while(true) {
					poll_completions(); // to get buffers
					Object obj = queue.take();
					while(available.isEmpty()){
						poll_completions(); // to get buffers
					}
					int bufid = available.pollFirst();
					outputbufers[bufid].position(0);
					ser.serializeObjectToBuffer(obj,outputbufers[bufid]);
					int size = outputbufers[bufid].position();

					LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();

					IbvSge sgeSend = new IbvSge();
					sgeSend.setAddr(mrs[bufid].getAddr());
					sgeSend.setLength(size);
					sgeSend.setLkey(mrs[bufid].getLkey());
					LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
					sgeList.add(sgeSend);
					IbvSendWR sendWR = new IbvSendWR();
					sendWR.setWr_id(bufid);
					sendWR.setSg_list(sgeList);
					sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
					sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
					wrList_send.add(sendWR);
					ep.qp.postSend(wrList_send, null).execute().free();
				}
			} catch (SocketException se) {
				// ignore, socket got closed
			} catch (InterruptedException ie) {
				// ignore, we are shutting down
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		void close(){
			running.set(false);
		}
	}
	

	private RdmaEP clients[];
	
	private DisniReadWorker[] readers;
	private DisniWriteWorker[] writers;
	
	
	@SuppressWarnings("unchecked")
	public DisniRDMANetwork(String serializer, int nodeid, Node[] nodes) throws Exception, InterruptedException {
		super(serializer,nodeid,nodes.length);


		this.incoming = new LinkedBlockingQueue<>(1024);
		this.outgoing = (BlockingQueue<Object>[]) new BlockingQueue[nodes.length];
		this.clients = new RdmaEP[nodes.length];
		this.readers = new DisniReadWorker[nodes.length];
		this.writers = new DisniWriteWorker[nodes.length];

		RdmaServer server = new RdmaServer( nodes[nodeid].host, nodes[nodeid].port);

		// accept connections with nodes with higher id.
		for (int i = nodeid + 1; i < nodes.length; i++) {
			// accept connection
			RdmaChannel channel = server.lazy_accept();
			System.out.println(String.format("accepted"));

			RdmaEP ep = channel.ep;

			LinkedList<IbvRecvWR> wrList_recv = new LinkedList<IbvRecvWR>();
			IbvSge sgeRecv = new IbvSge();
			LinkedList<IbvSge> sgeListRecv = new LinkedList<IbvSge>();
			sgeListRecv.add(sgeRecv);
			IbvRecvWR recvWR = new IbvRecvWR();
			recvWR.setSg_list(sgeListRecv);
			recvWR.setWr_id(1000);
			wrList_recv.add(recvWR);

			SVCPostRecv recv = ep.qp.postRecv(wrList_recv, null);
			recv.execute();
			System.out.println(String.format("posted recv request"));


			IbvWC[] wcListrecv = new IbvWC[1];
			wcListrecv[0] = new IbvWC();
			SVCPollCq pollrecvCqCall = channel.attr.getRecv_cq().poll(wcListrecv, 1);
			while(pollrecvCqCall.execute().getPolls()==0) {
				//empty
			}
			int id = wcListrecv[0].getImm_data();
			System.out.println("completion recv!" + wcListrecv[0].getStatus());
			pollrecvCqCall.free();

			clients[id] = ep;
			System.out.println(String.format("Received Connection from %s:%d", nodes[id].host, nodes[id].port));

			// setup data structures
			outgoing[id] = new LinkedBlockingQueue<>();
			readers[id] = new DisniReadWorker(channel, max_recvs, incoming);
			writers[id] = new DisniWriteWorker(channel, max_sends, outgoing[id]);
		}
		
		// Waiting for all nodes to open their server sockets.
		Thread.sleep(4000);
		
		// initiate connections with nodes with lower id.
		for(int id = 0; id < nodeid; id++) {
			// initiate connection
			RdmaClient client = new RdmaClient(nodes[id].host, nodes[id].port);
			System.out.print(String.format("Connecting to %s:%d ... ", nodes[id].host, nodes[id].port));
			RdmaChannel channel = client.lazy_connect();
			System.out.println(String.format("connected"));
			RdmaEP ep = channel.ep;
			clients[id] = ep;

			IbvSge sgeSend = new IbvSge();
			LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
			sgeList.add(sgeSend);
			IbvSendWR sendWR = new IbvSendWR();
			sendWR.setWr_id(1001);
			sendWR.setSg_list(sgeList);
			sendWR.setOpcode(IbvSendWR.IBV_WR_SEND_WITH_IMM);
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
			sendWR.setImm_data(nodeid);
			LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();
			wrList_send.add(sendWR);

			SVCPostSend send = ep.qp.postSend(wrList_send, null);

			Thread.sleep(50); // delay to be sure that the receiver posted memory
			send.execute().free();
			System.out.println(String.format("send request"));


			IbvWC[] wcListsend = new IbvWC[1];
			wcListsend[0] = new IbvWC();
			SVCPollCq pollsendCqCall = channel.attr.getSend_cq().poll(wcListsend, 1);
			while(pollsendCqCall.execute().getPolls()==0) {
				//empty
			}

			System.out.println("send completion connected!" + wcListsend[0].getStatus());
			pollsendCqCall.free();
					
			// setup data structures
			outgoing[id] = new LinkedBlockingQueue<>();
			readers[id] = new DisniReadWorker(channel, max_recvs, incoming);
			writers[id] = new DisniWriteWorker(channel, max_sends, outgoing[id]);
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

	@Override
	public Object receiveObject() throws InterruptedException {
		return incoming.take();
	}

	@Override
	public boolean hasReceiveObject() throws InterruptedException {
		return !incoming.isEmpty();
	}

 

	public void sendObject(int target, Object obj){
		// nothing
		this.outgoing[target].add(obj);
	}
 

	public void shutdown() throws IOException, InterruptedException {
		for (int id = 0; id < clients.length; id++) {
			if (clients[id] != null) {
				clients[id].close();
				readers[id].close();
				readers[id].interrupt();
				readers[id].join();
				writers[id].interrupt();
				writers[id].join();
			}
		}
	}
}
