package ch.ethz.serializers.skyway;


import java.io.InputStream;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

import ch.ethz.benchmarks.pagerank.RankUpdate;
import ch.ethz.benchmarks.pagerank.RankUpdateFast;

import ch.ethz.microperf.datastructures.Pair;
import ch.ethz.microperf.datastructures.PointFloat;
import ch.ethz.naos.skyway.Skyway;
import ch.ethz.serializers.Serializer;
 
public class SkywaySerializer implements Serializer {

	private final Skyway sk;
	private final int buffer_size;
	private final byte[] cache_bytes;



	public SkywaySerializer(int buffer_size)  throws Exception{
		this.buffer_size = buffer_size;
		if(buffer_size != 0){
			this.cache_bytes = new byte[buffer_size];
		} else {
			this.cache_bytes = null;
		}

		this.sk = new Skyway();
		sk.registerClass(HashMap.class,0);
		sk.registerClass(ArrayList.class,1);
		sk.registerClass(int[].class,2);
		sk.registerClass(char[].class,3);
		sk.registerClass(float[].class,4);
		sk.registerClass(double[].class,5);
		sk.registerClass(double[][].class,6);
		sk.registerClass(PointFloat.class,7);
		sk.registerClass(PointFloat[].class,8);
		sk.registerClass(Pair.class,9);
		sk.registerClass(Pair[].class,10);
		sk.registerClass(RankUpdate.class,13);
		sk.registerClass(RankUpdate[].class,14);
		sk.registerClass(Integer.class,16);
	 	try {
            Class cl = Class.forName("java.util.HashMap$Node");
            sk.registerClass(cl,17);
            Class cl2 = Class.forName("[Ljava.util.HashMap$Node;");
            sk.registerClass(cl2,18);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        sk.registerClass(RankUpdateFast.class,21);
	}

	public SkywaySerializer() throws Exception{
		this(0);
	}

	@Override
	public void serializeObjectToStream(OutputStream stream, Object o) throws Exception {
		if(buffer_size == 0){ // no buffer. create a new one.
			int size = (int)sk.sizeOfObject(o);
			byte[] bytes = new byte[size];
			sk.writeObject(o,bytes);
			sendBytes(stream, bytes);
		} else { // write obejcts to existing buffer
			int len = sk.writeObject(o,cache_bytes); 
			sendBytes(stream, cache_bytes, 0, len);
		}
	}

	@Override
	public Object deserializeObjectFromStream(InputStream stream) throws Exception {
		byte[] bytes = recvBytes(stream);
		return sk.readObject(bytes);
	}

	@Override
	public Object deserializeObjectFromBuffer(ByteBuffer buf) throws Exception {
		return sk.readObject(buf.array());
	}

	@Override
	public int 	getObjectLength(Object o) throws Exception {
		return (int)sk.sizeOfObject(o);
	}

	@Override
	public void serializeObjectToBuffer(Object obj, ByteBuffer buf) throws Exception{
		int len = sk.writeObject(obj, buf.array());
		buf.position(len);
	}

	@Override
	public ByteBuffer serializeObjectToBuffer(Object obj) throws Exception{
		return ByteBuffer.wrap(sk.writeObject(obj));
	}

}
