/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * RDMA Naos server endpoint that accepts connections.
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.naos.rdma;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import ch.ethz.naos.jni.NativeDispatcher;
import ch.ethz.naos.utils.SockAddrIn;

import java.net.InetAddress;
 

public class ServerRDMA {
	
	private final long objId; // it is pointer to rdma_id which listens to the port.
	 
	public ServerRDMA(InetSocketAddress addr) throws IOException{
 
		SockAddrIn dst = new SockAddrIn(addr);
		ByteBuffer dstBuf = ByteBuffer.allocateDirect(SockAddrIn.CSIZE).order(ByteOrder.nativeOrder());
        dst.writeBack(dstBuf);
        long ptr_addr = NativeDispatcher.getDirectBufferAddress(dstBuf);
      
        this.objId = NativeDispatcher._createServer(ptr_addr);
	}
	
	public ServerRDMA(String host, int port) throws IOException{
 	    InetAddress _this_addr = InetAddress.getByName(host);
		InetSocketAddress addr = new InetSocketAddress(_this_addr, port);

		SockAddrIn dst = new SockAddrIn(addr);
		ByteBuffer dstBuf = ByteBuffer.allocateDirect(SockAddrIn.CSIZE).order(ByteOrder.nativeOrder());
        dst.writeBack(dstBuf);
        long ptr_addr = NativeDispatcher.getDirectBufferAddress(dstBuf);
      
        this.objId = NativeDispatcher._createServer(ptr_addr);
	}
 

	public RawEndpointRDMA accept() throws IOException {
		ByteBuffer dstBuf = ByteBuffer.allocateDirect(ConnectParamsRDMA.CSIZE).order(ByteOrder.nativeOrder());
		long ptr_addr = NativeDispatcher.getDirectBufferAddress(dstBuf);
		long epObj = NativeDispatcher._acceptServer(objId,ptr_addr);

		dstBuf.position(0);
		dstBuf.order(ByteOrder.LITTLE_ENDIAN);
		int buffer_size = dstBuf.getInt(0);
		int buffer_num = dstBuf.getInt(4);
		int connectid = dstBuf.getInt(8);
		dstBuf.order(ByteOrder.nativeOrder());
		System.out.printf("Received connect request with size %d num %d, nad id %d \n", buffer_size,buffer_num,connectid );

		return new RawEndpointRDMA(epObj,buffer_size,buffer_num,connectid);
	}	

	public void close() throws IOException{
		NativeDispatcher._closeServer(objId);
	}
	
}
