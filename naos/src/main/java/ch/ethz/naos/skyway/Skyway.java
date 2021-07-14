/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Our implementation of Skyway.
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.naos.skyway;

import java.io.IOException;
import java.nio.ByteBuffer;

import ch.ethz.naos.jni.NativeDispatcher;

public class Skyway {

	private long skyway;

	public Skyway() throws IOException {
		this.skyway = NativeDispatcher._createSkyway();
	}
	
	public void registerClass(Class type, int id) throws IOException {
		NativeDispatcher._registerSkywayClass(this.skyway,type,id);
	}

	// Warining read object invalidates the buffer. A new buffer is required per OP. 
	public Object readObject(byte[] bb) throws IOException {
		return NativeDispatcher._readObjSkyway(this.skyway,bb);
	}

	public byte[] writeObject(Object obj) throws IOException {
		return writeObject(obj,1024);
	}

	public byte[] writeObject(Object obj, int init_size) throws IOException {
		return (byte[])NativeDispatcher._writeObjSkyway(this.skyway,obj,init_size);
	}

	public int writeObject(Object obj, byte[] bb) throws IOException {
		return NativeDispatcher._writeObjSkywayBuf(this.skyway,obj,bb);
	}

	public long sizeOfObject(Object obj) throws IOException {
		return NativeDispatcher._sizeof(obj,false) + 8; // 8 is the size of the header
	}

	public long sizeOfObjectTest(Object obj, boolean usebfs) throws IOException {
		return NativeDispatcher._sizeof(obj,usebfs) + 8; // 8 is the size of the header
	}
}
