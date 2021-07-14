/**                                                                                                      
 * Naos: Serialization-free RDMA networking in Java
 * 
 * RDMA Naos endpoint that is not connected yet.
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RawEndpointRDMA {

    final long objId;
    final int remote_buffer_size;
    final int remote_number_of_buffers;
    final int connectid;

    public RawEndpointRDMA(long objId, int remote_buffer_size, int remote_number_of_buffers, int connectid){
        this.objId = objId;
        this.remote_buffer_size = remote_buffer_size;
        this.remote_number_of_buffers = remote_number_of_buffers;
        this.connectid = connectid;
    }

    public boolean isActive(){
        return remote_buffer_size >= 0;
    }

    public boolean isPassive(){
        return remote_buffer_size < 0;
    }

    public int getConnectId(){
        return connectid;
    }

    public int getRemoteBufferSize(){
        return remote_buffer_size;
    }

    public int getRemoteBufferNum(){
        return remote_number_of_buffers;
    }

    public ActiveEndpointRDMA acceptActive() throws IOException {
        assert isActive() : "must be active";

        ConnectParamsRDMA params = new ConnectParamsRDMA(remote_buffer_size,remote_number_of_buffers);
        ByteBuffer dstBuf = ByteBuffer.allocateDirect(ConnectParamsRDMA.CSIZE).order(ByteOrder.nativeOrder());
        params.writeBack(dstBuf);
        long ptr_addr = NativeDispatcher.getDirectBufferAddress(dstBuf);

        long epObj = NativeDispatcher._acceptActive(objId,ptr_addr);
        System.out.println(String.format("Accept active 0x%08X", epObj));
        return new ActiveEndpointRDMA(epObj,remote_buffer_size,remote_number_of_buffers);
    }

    public PassiveEndpointRDMA acceptPassive(ConnectParamsRDMA params) throws IOException {
        assert isPassive() : "must be passive";

        ByteBuffer dstBuf = ByteBuffer.allocateDirect(ConnectParamsRDMA.CSIZE).order(ByteOrder.nativeOrder());
        params.writeBack(dstBuf);
        long ptr_addr = NativeDispatcher.getDirectBufferAddress(dstBuf);

        long epObj = NativeDispatcher._acceptPassive(objId,ptr_addr);
        System.out.println(String.format("Accept passive 0x%08X", epObj));
        return new PassiveEndpointRDMA(epObj, params.buffersize, params.to_preallocate );
    }

    public void reject(){
        //todo
    }

}
