/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * RDMA Naos Sender. It is an active endpoint.
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.naos.rdma;

import ch.ethz.naos.jni.NativeDispatcher;
import java.util.Iterator;
import java.io.IOException;

public class ActiveEndpointRDMA {
    private final long objId;
    private final int buffer_size;
    private final int buffer_num;

    private Object[] objArray; 
    private int arrayLen; 

    public ActiveEndpointRDMA(long objId, int buffer_size, int buffer_num) { // todo make protected
        this.arrayLen = 128;
        this.objArray = new Object[arrayLen];
        this.objId = objId;
        this.buffer_size = buffer_size;
        this.buffer_num = buffer_num;
    }

    public int getRemoteBufferSize(){
        return buffer_size;
    }

    public int getRemoteBufferNum(){
        return buffer_num;
    }

    public boolean writeObject(Object objToWrite) throws IOException{
        NativeDispatcher._writeObj(objId,objToWrite,0);
        return true;
    }

    public long writeObjectAsync(Object objToWrite) throws IOException{
        return NativeDispatcher._writeObjAsync(objId,objToWrite,0);
    }

    public boolean writeArray(Object objToWrite, int elements) throws IOException{
        NativeDispatcher._writeObj(objId,objToWrite,(-elements));
        return true;
    }

    public long writeArrayAsync(Object objToWrite, int elements) throws IOException{
        return NativeDispatcher._writeObjAsync(objId,objToWrite,(-elements));
    }

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
                NativeDispatcher._writeObj(objId,objArray,batch_size);
                c = 0;      
            }
        } 
        if(c>0){
            NativeDispatcher._writeObj(objId,objArray,c);
        }
        return total;
    }

    public long writeIterableAsync(Iterator it, int batch_size) throws IOException {
        int c = 0;
        long token = 0;
        while (it.hasNext()) 
        { 
            if(c == arrayLen){ // then double array
                Object[] doubledObjArray = new Object[2*arrayLen];
                System.arraycopy(objArray, 0, doubledObjArray, 0, arrayLen);
                arrayLen = arrayLen*2;
                objArray = doubledObjArray;
            }
            objArray[c++] = it.next(); 
            if(c==batch_size){
                token = NativeDispatcher._writeObjAsync(objId,objArray,batch_size);
                c = 0;      
            }
        } 
        if(c>0){
            token = NativeDispatcher._writeObjAsync(objId,objArray,c);
        }

        return token; // return last token
    }

    public void waitHandle(long handle) throws IOException{
        NativeDispatcher._waitRdma(objId,handle); 
    }

    public boolean testHandle(long handle) throws IOException{
        return NativeDispatcher._testRdma(objId,handle);
    }

    public boolean writeInt(int val) throws IOException{
        NativeDispatcher._writeInt(objId,val);
        return true;
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
        ActiveEndpointRDMA other = (ActiveEndpointRDMA) obj;
        return other.objId == objId;
    }


    public long sizeOfObject(Object obj) throws IOException {
        return NativeDispatcher._sizeof(obj,false);
    }
    
}
