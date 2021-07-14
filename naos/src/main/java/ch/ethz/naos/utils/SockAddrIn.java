/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Tools for parsing SockAddr to C format.
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.naos.utils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SockAddrIn {
	private final static boolean nativeIsNetwork = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
	
	public static short AF_INET = 2;
	public static int CSIZE = 28;
	
	protected short sin_family;
	protected int sin_addr;
	protected short sin_port;
	
	public SockAddrIn(InetSocketAddress addr) {
		this.sin_family = AF_INET;
		this.sin_addr = getIntIPFromInetAddress(addr.getAddress());
		this.sin_port = hostToNetworkByteOrder((short)addr.getPort());
	}
 

	SockAddrIn(short sin_family, int sin_addr, short sin_port) {
		this.sin_family = sin_family;
		this.sin_addr = sin_addr;
		this.sin_port = sin_port;
	}
	
	public void writeBack(ByteBuffer buffer) {
		buffer.putShort(sin_family);
		buffer.putShort(sin_port);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		buffer.putInt(sin_addr);
		buffer.order(ByteOrder.nativeOrder());
	}
 
	public static int getIntIPFromInetAddress(InetAddress localHost) {
		byte[] addr = localHost.getAddress();
		ByteBuffer buffer = ByteBuffer.wrap(addr);
		buffer.order(ByteOrder.LITTLE_ENDIAN);
		buffer.clear();
		return buffer.getInt();
	}
	
	public static short hostToNetworkByteOrder(short x) {
		if (nativeIsNetwork){
			return x;
		}
		return swap(x);
	}
	
	public static short swap(short x) {
		return (short) ((x << 8) | ((char) x >>> 8));
	}

	public int size() {
		return CSIZE;
	}
 
}
