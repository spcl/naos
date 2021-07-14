/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.naos.rdma;
 
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ConnectParamsRDMA {
	private final static boolean nativeIsNetwork = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
	
	public static int CSIZE = 16;
	
	protected int buffersize;
	protected int to_preallocate; // num of buffers to preallcoate
	protected int connectid; // client can send data
	protected int reserved; // can be used later

	
	public ConnectParamsRDMA(int buffersize, int to_preallocate) {
		this.buffersize = buffersize;
		this.to_preallocate = to_preallocate;
		this.connectid = 0;
	}

	public ConnectParamsRDMA(int buffersize, int to_preallocate, int connectid) {
		this.buffersize = buffersize;
		this.to_preallocate = to_preallocate;
		this.connectid = connectid;
	}

	public void writeBack(ByteBuffer buffer) {
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		buffer.putInt(buffersize);
		buffer.putInt(to_preallocate);
		buffer.putInt(connectid);
		buffer.putInt(reserved);
		buffer.order(ByteOrder.nativeOrder());
	}
 
	public int size() {
		return CSIZE;
	}
 
}
