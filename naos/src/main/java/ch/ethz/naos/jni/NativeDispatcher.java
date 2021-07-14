/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Loader of Naos' shared library
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.naos.jni;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import ch.ethz.naos.utils.NativeUtils;

// TODO - rbruno - make this class protected
public class NativeDispatcher {
	static {
		Method directBufferAddressMtd = null;
        try {
    		NativeUtils.loadLibraryFromJar("/libjni_rdma.so");
            directBufferAddressMtd = Class.forName("sun.nio.ch.DirectBuffer").getMethod("address");
        } catch (Exception e) {
            e.printStackTrace();
        }

        DIRECT_BUFFER_ADDRESS_MTD = directBufferAddressMtd;
	}
	
	private static final Method DIRECT_BUFFER_ADDRESS_MTD;
	
 
	
	public static long getDirectBufferAddress(ByteBuffer directBuffer) {
		assert directBuffer.isDirect();
		try {
			return (Long)DIRECT_BUFFER_ADDRESS_MTD.invoke(directBuffer);
	    }
	    catch (Exception e) {
	        throw new RuntimeException(e);
	    }
	}
	

	public static native void _test5( );
	public static native void _test6( );
	public static native void _test7( );

	public static native void _test11(Object obj);

	public static native long _createServer(long ipaddr) throws IOException;
	public static native long _createClient(long ipaddr) throws IOException;
	
	public static native long _acceptServer(long addr, long params) throws IOException;

	public static native long _acceptPassive(long addr, long params) throws IOException;
	public static native long _acceptActive(long addr, long params) throws IOException;
	public static native long _connectPassive(long addr, long params) throws IOException;
	public static native long _connectActive(long addr, long params) throws IOException;
	
	public static native void _closeServer(long addr) throws IOException;
	public static native void _closeClient(long addr) throws IOException;
	
	

	public static native void _waitRdma(long ep_addr, long handle );
	public static native boolean _testRdma(long ep_addr, long handle );

	public static native void   _writeObj(long ep_addr, Object Obj, int array_len) throws IOException;
	public static native long   _writeObjAsync(long ep_addr, Object Obj, int array_len) throws IOException;
	public static native Object _readObj (long ep_addr) throws IOException;
	public static native void   _writeInt(long ep_addr, int val) throws IOException;
	public static native int _readInt (long ep_addr) throws IOException;
	public static native int _isReadable(long ep_addr, int timeout) throws IOException;
	public static native void   _closeEP (long ep_addr) throws IOException;
	
	// support for file descriptor input/output streams
	public static native long _createNaosTcp(int fd);
	public static native Object _readObjFD(long naostcp,long params) throws IOException;
	public static native void _writeObjFD(long naostcp, Object obj, int array_len) throws IOException;
	public static native int _readableFD(int fd,int timeout) throws IOException;


	// support for byte buffer input/output streams
	public static native Object _readObjSkyway(long skyway, Object buffer) throws IOException;
	public static native Object _writeObjSkyway(long skyway, Object obj, int init_size) throws IOException;
	public static native int _writeObjSkywayBuf(long skyway, Object obj, Object buffer) throws IOException;
	
	public static native long _createSkyway( ) throws IOException;
	public static native void _registerSkywayClass(long skyway, Object type, int id) throws IOException;

	public static native long _sizeof(Object obj, boolean bfs) throws IOException;

}
