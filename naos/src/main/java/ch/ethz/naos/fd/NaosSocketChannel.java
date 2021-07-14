/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * Naos over java.nio.channels.SocketChannel
 *
 * Copyright (c) 2019-2021 ETH-Zurich. All rights reserved.
 * 
 * Author(s): Konstantin Taranov <konstantin.taranov@inf.ethz.ch>
 *            Rodrigo Bruno <rodrigo.bruno@tecnico.ulisboa.pt>
 * 
 */
package ch.ethz.naos.fd;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.*;
import java.util.Iterator;

import ch.ethz.naos.jni.NativeDispatcher;

public class NaosSocketChannel {

    private final int fd;
    private final long naostcp;
    private Object[] objArray; 
    private int arrayLen; 

    public NaosSocketChannel(SocketChannel channel) {
        arrayLen = 128;
        objArray = new Object[arrayLen];
        try {

            Field field = Class.forName("sun.nio.ch.SocketChannelImpl").getDeclaredField("fdVal");
            field.setAccessible(true);
            this.fd = field.getInt(channel);
            field.setAccessible(false);

            naostcp = NativeDispatcher._createNaosTcp(fd);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Object readObject() throws IOException {
        return NativeDispatcher._readObjFD(naostcp,0);
    }

    public void writeObject(Object obj) throws IOException {
        NativeDispatcher._writeObjFD(naostcp,obj,0);
    }

    public void writeArray(Object objArray, int elements) throws IOException{
        NativeDispatcher._writeObjFD(naostcp,objArray,(-elements));
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
                NativeDispatcher._writeObjFD(naostcp,objArray,batch_size);
                c = 0;      
            }
        } 
        if(c>0){
            NativeDispatcher._writeObjFD(naostcp,objArray,c);
        }
        return total;
    }

    public long sizeOfObject(Object obj) throws IOException {
        return NativeDispatcher._sizeof(obj,false);
    }
}
