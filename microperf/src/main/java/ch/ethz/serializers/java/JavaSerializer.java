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
package ch.ethz.serializers.java;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import ch.ethz.microperf.Utils;
import ch.ethz.serializers.Serializer;

import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;

public class JavaSerializer implements Serializer {
	
	private static byte[] serializeToArray(Object o) throws Exception {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = new ObjectOutputStream(bos);		
		out.writeObject(o);
		out.close();
		return bos.toByteArray();
	}
	
	private static Object deserializeFromArray(byte[] bytes) throws Exception {
		return new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
	}
	
	@Override
	public void serializeObjectToStream(OutputStream stream, Object o) throws Exception {
		//long time = Utils.getTimeMicroseconds();
		byte[] bytes = serializeToArray(o);
		//System.out.println(String.format("\t serialization took %d us", Utils.getTimeMicroseconds() - time));
		
		//time = Utils.getTimeMicroseconds();
		sendBytes(stream, bytes);
		//System.out.println(String.format("\t network sending %d bytes", bytes.length));
	}

	@Override
	public Object deserializeObjectFromStream(InputStream stream) throws Exception {
		//long time = Utils.getTimeMicroseconds();
		byte[] bytes = recvBytes(stream);
		//System.out.println(String.format("\t network receiving %d bytes took %d us", bytes.length, Utils.getTimeMicroseconds() - time));
		
		//time = Utils.getTimeMicroseconds();
		Object o = deserializeFromArray(bytes);
		//System.out.println(String.format("\t deserializaion took %d us", Utils.getTimeMicroseconds() - time));
		return o;
	}

	@Override
	public Object deserializeObjectFromBuffer(ByteBuffer buf) throws Exception {
		ByteBufferInputStream s = new ByteBufferInputStream(buf);
		return new ObjectInputStream(s).readObject();
	}

	@Override
	public int 	getObjectLength(Object o) throws Exception {
		byte[] bytes = serializeToArray(o);
		return bytes.length;
	}

	@Override
	public void serializeObjectToBuffer(Object obj, ByteBuffer buf) throws Exception{
		buf.position(0);
		ByteBufferOutputStream os = new ByteBufferOutputStream(buf);
		ObjectOutput out = new ObjectOutputStream(os);
		out.writeObject(obj);
		out.flush();
		out.close();
	}

	@Override
	public ByteBuffer serializeObjectToBuffer(Object obj) throws Exception{
		byte[] bytes = serializeToArray(obj);
		return ByteBuffer.wrap(bytes,0,bytes.length);
	}

}
