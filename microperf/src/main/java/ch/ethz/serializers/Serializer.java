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
package ch.ethz.serializers;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface Serializer {
	
	public void serializeObjectToStream(OutputStream stream, Object obj) throws Exception;

	public void serializeObjectToBuffer(Object obj, ByteBuffer buf) throws Exception;

	public ByteBuffer serializeObjectToBuffer(Object obj) throws Exception;
	
	public Object deserializeObjectFromStream(InputStream stream) throws Exception;

	public Object deserializeObjectFromBuffer(ByteBuffer buf) throws Exception;
	
	public int getObjectLength(Object o) throws Exception;
	
	// helper methods
	default void sendBytes(OutputStream stream, byte[] bytes) throws Exception {
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(stream));
		dos.writeInt(bytes.length);
		dos.write(bytes);
		dos.flush();
	}
		// helper methods
	default void sendBytes(OutputStream stream, byte[] bytes, int offset, int len) throws Exception {
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(stream));
		dos.writeInt(len);
		dos.write(bytes,offset,len);
		dos.flush();
	}

	default byte[] recvBytes(InputStream stream) throws Exception {
		DataInputStream dis = new DataInputStream(new BufferedInputStream(stream));
		byte[] bytes = new byte[dis.readInt()];
		dis.readFully(bytes);
		return bytes;
	}
}
