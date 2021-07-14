/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * RDMA Naos Receiver. It is a passive endpoint.
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.naos.rdma;

import ch.ethz.naos.jni.NativeDispatcher;

import java.io.IOException;

public class PassiveEndpointRDMA {
    private final long objId;
    private final int buffer_size;
    private final int buffer_num;

    public PassiveEndpointRDMA(long objId,int buffer_size,int buffer_num) { // todo make protected
        this.objId = objId;
        this.buffer_size = buffer_size;
        this.buffer_num = buffer_num;
    }

    public int getLocalBufferSize(){
        return buffer_size;
    }

    public int getLocalBufferNum(){
        return buffer_num;
    }

    public Object readObject() throws IOException {
        return NativeDispatcher._readObj(objId);
    }

    public int readInt() throws IOException{
        return NativeDispatcher._readInt(objId);
    }

    // 0 - nothing, 1 - has object, 2-has int, 3-has both
    // timeout 0 - no blocking. timeout in ms.
    /**
     * @param timeout in ms. Negative values mean unlimited waiting time. 0 means unblocking.
     * @return A value of 0 indicates that the call timed out. Negative values on error. 1 - has object to read, 2 - has int to read, 3 - has both
     */
    public int isReadable(int timeout) throws IOException {
        int val = NativeDispatcher._isReadable(objId,timeout);
        return val;
    }

    public void close() throws IOException {
        NativeDispatcher._closeEP(objId);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Long.hashCode(objId);
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
        PassiveEndpointRDMA other = (PassiveEndpointRDMA) obj;
        return other.objId == objId;
    }

    public long sizeOfObject(Object obj) throws IOException {
        return NativeDispatcher._sizeof(obj,false);
    }
}
