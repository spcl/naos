/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * RDMA Naos Sender/Receiver. It combines active and passive connections.
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.naos.rdma;

import java.io.IOException;
import java.util.Iterator;
import ch.ethz.naos.jni.NativeDispatcher;

public class EndpointRDMA {
	
 	final private ActiveEndpointRDMA aep;
	final private PassiveEndpointRDMA pep;

	public EndpointRDMA(ActiveEndpointRDMA aep, PassiveEndpointRDMA pep) { // todo make protected
		this.aep = aep;
		this.pep = pep;
	}

	public int getRemoteBufferSize(){
		return aep.getRemoteBufferSize();
	}

	public int getRemoteBufferNum(){
		return aep.getRemoteBufferNum();
	}

	public int getLocalBufferSize(){
		return pep.getLocalBufferSize();
	}

	public int getLocalBufferNum(){
		return pep.getLocalBufferNum();
	}

	public Object readObject() throws IOException{
		return pep.readObject();
	}

	public boolean writeObject(Object objToWrite) throws IOException{
		return aep.writeObject(objToWrite);
	}

	public long writeObjectAsync(Object objToWrite) throws IOException{
		return aep.writeObjectAsync(objToWrite);
	}

    public boolean writeArray(Object objToWrite, int elements) throws IOException{
        return aep.writeArray(objToWrite,elements);
    }

    public long writeArrayAsync(Object objToWrite, int elements) throws IOException{
    	return aep.writeArrayAsync(objToWrite,elements);
    }

	public int writeIterable(Iterator it, int batch_size) throws IOException{
		return aep.writeIterable(it,batch_size);
	}

	public long writeIterableAsync(Iterator it, int batch_size) throws IOException{
		return aep.writeIterableAsync(it,batch_size);
	}

    public void waitHandle(long handle) throws IOException{
        aep.waitHandle(handle);
    }

    public boolean testHandle(long handle) throws IOException{
        return aep.testHandle(handle);
    }

	public int readInt() throws IOException{
		return pep.readInt();
	}

	public boolean writeInt(int val) throws IOException{
		return aep.writeInt(val);
	}

	/**
	 * @param timeout in ms. Negative values mean unlimited waiting time. 0 means unblocking.
	 * @return A value of 0 indicates that the call timed out. Negative values on error. 1 - has object to read, 2 - has int to read, 3 - has both
	 */
	public int isReadable(int timeout) throws IOException {
		return pep.isReadable(timeout);
	}

 
	public void close() throws IOException {
		aep.close();
		pep.close();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + aep.hashCode() + pep.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		EndpointRDMA other = (EndpointRDMA) obj;
		if (!aep.equals(other.aep)) {
			return false;
		} else if (!pep.equals(other.pep)) {
			return false;
		}
		return true;
	}

	public long sizeOfObject(Object obj) throws IOException {
		return aep.sizeOfObject(obj);
	}
}
