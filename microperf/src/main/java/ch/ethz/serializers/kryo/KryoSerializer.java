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
package ch.ethz.serializers.kryo;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import ch.ethz.benchmarks.pagerank.RankUpdate;
import ch.ethz.benchmarks.pagerank.RankUpdateFast;
import ch.ethz.microperf.Utils;
import ch.ethz.microperf.datastructures.PointFloat;
import ch.ethz.microperf.datastructures.Pair;
import ch.ethz.serializers.Serializer;

public class KryoSerializer implements Serializer {

	private Kryo kryo;
	
	public KryoSerializer() {
		this.kryo = new Kryo();
		kryo.setRegistrationRequired(true);
		kryo.register(HashMap.class);
		kryo.register(ArrayList.class);
		kryo.register(int[].class);
		kryo.register(char[].class);
		kryo.register(float[].class);
		kryo.register(double[].class);
		kryo.register(double[][].class);
		kryo.register(PointFloat.class);
		kryo.register(PointFloat[].class);
		kryo.register(Pair.class);
		kryo.register(Pair[].class);
		kryo.register(RankUpdate.class);
		kryo.register(RankUpdateFast.class);
		kryo.register(RankUpdate[].class);
		kryo.setReferences(true);
	}
	
	private byte[] serializeToArray(Object o) throws Exception {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		Output out = new Output(bos);		
		kryo.writeClassAndObject(out, o);
		out.close();
		return bos.toByteArray();
	}
	
	private Object deserializeFromArray(byte[] bytes) throws Exception {
		Input input = new Input(new ByteArrayInputStream(bytes));
		return kryo.readClassAndObject(input);
	}


	@Override
	public Object deserializeObjectFromBuffer(ByteBuffer buf) throws Exception {
		Input input = new Input(new ByteBufferInputStream(buf));
		return kryo.readClassAndObject(input);
	}
	
	@Override
	public void serializeObjectToStream(OutputStream stream, Object o) throws Exception {		
		long time = Utils.getTimeMicroseconds();
		byte[] bytes = serializeToArray(o);
		//System.out.println(String.format("\t serialization took %d us", Utils.getTimeMicroseconds() - time));
		
		time = Utils.getTimeMicroseconds();
		sendBytes(stream, bytes);
		//System.out.println(String.format("\t network sending %d bytes took %d us", bytes.length, Utils.getTimeMicroseconds() - time));
	}

	@Override
	public Object deserializeObjectFromStream(InputStream stream) throws Exception {
		long time = Utils.getTimeMicroseconds();
		byte[] bytes = recvBytes(stream);
		//System.out.println(String.format("\t network receiving %d bytes took %d us", bytes.length, Utils.getTimeMicroseconds() - time));
		
		time = Utils.getTimeMicroseconds();
		Object o = deserializeFromArray(bytes);
		//System.out.println(String.format("\t deserializaion took %d us", Utils.getTimeMicroseconds() - time));
		return o;
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
		Output out = new Output(os);
		kryo.writeClassAndObject(out, obj);
		out.close();
	}

	@Override
	public ByteBuffer serializeObjectToBuffer(Object obj) throws Exception{
		byte[] bytes = serializeToArray(obj);
		return ByteBuffer.wrap(bytes,0,bytes.length);
	}
	
}
