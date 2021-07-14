/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Naos over java.net.Socket
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.naos.fd;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.lang.reflect.Field;
import java.util.Iterator;

import java.io.FileDescriptor;
import java.net.*;

import ch.ethz.naos.jni.NativeDispatcher;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

public class NaosSocket {

	public final int fd;
	public final long naostcp;
    private Object[] objArray; 
    private int arrayLen; 
    private ByteBuffer is_readable; // because of iterable receive
    private long ptr_is_readable; // because of iterable receive

	public NaosSocket(Socket socket) {
		arrayLen = 128;
        objArray = new Object[arrayLen];
		is_readable = ByteBuffer.allocateDirect(4);
		ptr_is_readable = NativeDispatcher.getDirectBufferAddress(is_readable);
		is_readable.putInt(0,0);
		try {

			Method SocketGetImpl = Socket.class.getDeclaredMethod("getImpl");
			Method SocketImplGetFileDescriptor = SocketImpl.class.getDeclaredMethod("getFileDescriptor");
			Field privateFd=FileDescriptor.class.getDeclaredField("fd");
			SocketImplGetFileDescriptor.setAccessible(true);
			SocketGetImpl.setAccessible(true);
			privateFd.setAccessible(true);
 
			SocketImpl impl = (SocketImpl) SocketGetImpl.invoke(socket);
			FileDescriptor fd = (FileDescriptor) SocketImplGetFileDescriptor.invoke(impl);
		    this.fd = ((Integer) privateFd.get(fd)).intValue();
			SocketImplGetFileDescriptor.setAccessible(false);
			SocketGetImpl.setAccessible(false);
			privateFd.setAccessible(false);

            naostcp = NativeDispatcher._createNaosTcp(this.fd);


		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	// Should only be called if isReadable was called before
	public Object readObject() throws IOException {
		return NativeDispatcher._readObjFD(naostcp, ptr_is_readable);
	}

	public void writeObject(Object obj) throws IOException {
		NativeDispatcher._writeObjFD(naostcp,obj,0);
	}

	public void writeArray(Object objArray, int elements) throws IOException{
        NativeDispatcher._writeObjFD(naostcp,objArray,(-elements)); // i use negative to distinguish between iterative send
    }
 
	// the call send all the object of the iteratos and also for performance purposes it breakes it bacthes
	public int writeIterable(Iterator it, int batch_size) throws IOException {
        int c = 0;
        int total = 0;
        while (it.hasNext()) 
        { 
            if(c == arrayLen){ // then double array
                Object[] doubledObjArray = new Object[2*arrayLen];
                System.arraycopy(objArray, 0, doubledObjArray, 0, arrayLen);
                arrayLen = arrayLen*2;
                objArray = doubledObjArray;
            }
            objArray[c++] = it.next(); 
            total++;
            if(c==batch_size){
        		NativeDispatcher._writeObjFD(naostcp,objArray,batch_size);
        		c = 0;    	
            }
        } 
        if(c>0){
        	NativeDispatcher._writeObjFD(naostcp,objArray,c);
        }
        return total;
    }


	// Blocking call that unblocks when there is data to read
	/**
	 * @param timeout in ms. Negative values mean unlimited waiting time. 0 means unblocking.
	 * @return A value of 0  indicates that the call timed out. Positive values if it is readable. Negative values on error.
	 */
	public int isReadable(int timeout) throws IOException {
		if(is_readable.getInt(0) != 0) return 1;
		return NativeDispatcher._readableFD(fd,timeout);
	}

	public long sizeOfObject(Object obj) throws IOException {
		return NativeDispatcher._sizeof(obj,false);
	}
}
