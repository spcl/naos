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

public class Node {

	public String host;
	public int port;
	
	public Node(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	@Override
	public String toString() {
		return String.format("host %s port %d", host, port);
	}
}
