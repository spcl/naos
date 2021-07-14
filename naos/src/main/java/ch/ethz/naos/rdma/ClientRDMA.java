/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Helper for connecting Naos
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

public class ClientRDMA {
	
	private long objId;
	
	public ClientRDMA(InetSocketAddress addr) throws IOException{
		SockAddrIn dst = new SockAddrIn(addr);
		ByteBuffer dstBuf = ByteBuffer.allocateDirect(SockAddrIn.CSIZE).order(ByteOrder.nativeOrder());;
        dst.writeBack(dstBuf);
        long ptr_addr = NativeDispatcher.getDirectBufferAddress(dstBuf);
        
        this.objId = NativeDispatcher._createClient(ptr_addr);
	}

	public ClientRDMA(String host, int port) throws IOException{
		InetAddress _this_addr = InetAddress.getByName(host);
		InetSocketAddress addr = new InetSocketAddress(_this_addr, port);

		SockAddrIn dst = new SockAddrIn(addr);
		ByteBuffer dstBuf = ByteBuffer.allocateDirect(SockAddrIn.CSIZE).order(ByteOrder.nativeOrder());;
        dst.writeBack(dstBuf);
        long ptr_addr = NativeDispatcher.getDirectBufferAddress(dstBuf);
        
        this.objId = NativeDispatcher._createClient(ptr_addr);
	}
	
	public PassiveEndpointRDMA connectPassive(ConnectParamsRDMA params) throws IOException {
		ByteBuffer dstBuf = ByteBuffer.allocateDirect(ConnectParamsRDMA.CSIZE).order(ByteOrder.nativeOrder());
		params.writeBack(dstBuf);
		long ptr_addr = NativeDispatcher.getDirectBufferAddress(dstBuf);

		long epObj = NativeDispatcher._connectPassive(objId,ptr_addr);
		System.out.println(String.format("connect passive 0x%08X", epObj));
		return new PassiveEndpointRDMA(epObj, params.buffersize, params.to_preallocate );
	}

	public ActiveEndpointRDMA connectActive() throws IOException {
		return connectActive(0);
	}

	public ActiveEndpointRDMA connectActive(int connectid) throws IOException {
		ByteBuffer dstBuf = ByteBuffer.allocateDirect(ConnectParamsRDMA.CSIZE).order(ByteOrder.nativeOrder());
		ConnectParamsRDMA params = new ConnectParamsRDMA(-1,-1,connectid);
		params.writeBack(dstBuf);

		long ptr_addr = NativeDispatcher.getDirectBufferAddress(dstBuf);

		// it will rewrite content of ptr_addr buffer
		long epObj = NativeDispatcher._connectActive(objId,ptr_addr);
		System.out.println(String.format("connect active 0x%08X", epObj));

		dstBuf.position(0);
		dstBuf.order(ByteOrder.LITTLE_ENDIAN);
		int buffer_size = dstBuf.getInt(0);
		int buffer_num = dstBuf.getInt(4);
		dstBuf.order(ByteOrder.nativeOrder());
		System.out.printf("\tconnectActive size %d num %d \n", buffer_size,buffer_num );

		return new ActiveEndpointRDMA(epObj,buffer_size,buffer_num);
	}

	
	public void close() throws IOException{
		NativeDispatcher._closeClient(objId);
	}
}
