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

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Network {

	public final int nodes;
	public final int nodeid;
	protected final String serializer;
	
	public Network(String serializer, int nodeid,  int nodes) {
		this.serializer = serializer;
		this.nodeid = nodeid;
		this.nodes = nodes;
	}

	public abstract Object receiveObject( ) throws InterruptedException;
	public abstract boolean hasReceiveObject( ) throws InterruptedException;
 
	public abstract void sendObject(int target, Object obj);
	
	//public abstract void sendArray(int target, Object obj, int len); 
	
	public abstract void shutdown() throws IOException, InterruptedException;


}
