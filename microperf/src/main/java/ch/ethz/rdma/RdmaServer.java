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
package ch.ethz.rdma;


import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.ibm.disni.verbs.*;

public class RdmaServer {

    RdmaEventChannel cmChannel;
    RdmaCmId idPriv;

    public RdmaServer(String ipAddress, int port) throws IOException{
        cmChannel = RdmaEventChannel.createEventChannel();
        if (cmChannel == null){
            System.out.println("VerbsServer::CM channel null");
            return;
        }
        //create a RdmaCmId for the server
        idPriv = cmChannel.createId(RdmaCm.RDMA_PS_TCP);
        if (idPriv == null){
            System.out.println("idPriv null");
            return;
        }

        InetAddress _src = InetAddress.getByName(ipAddress);
        InetSocketAddress src = new InetSocketAddress(_src, port);
        idPriv.bindAddr(src);

        //listen on the id
        idPriv.listen(10);
    }


    public RdmaEP getConnectionRequest() throws IOException {
        RdmaCmEvent cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent == null){
            System.out.println("cmEvent null");
            throw new IOException("cmEvent null");
        }
        else if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_REQUEST
                .ordinal()) {
            System.out.println("VerbsServer::wrong event received: " + cmEvent.getEvent());
            throw new IOException("wrong event received");
        }
        //always acknowledge CM events
        cmEvent.ackEvent();

        return new RdmaEP(cmEvent.getConnIdPriv());
    }

    public void accept(RdmaEP ep, RdmaConnParam connParam) throws IOException {
        ep.connId.accept(connParam);
        RdmaCmEvent cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED
                .ordinal()) {
            System.out.println("VerbsServer::wrong event received: " + cmEvent.getEvent());
            throw new IOException("wrong event received");
        }
        //always ack CM events
        cmEvent.ackEvent();
    }

    public RdmaChannel lazy_accept() throws Exception{
        IbvQPInitAttr attr = new IbvQPInitAttr();
        attr.cap().setMax_recv_sge(1);
        attr.cap().setMax_recv_wr(10); // no more than 10 receive requests at a time
        attr.cap().setMax_send_sge(1);
        attr.cap().setMax_send_wr(10); // no more than 10 send requests at a time
        attr.setQp_type(IbvQP.IBV_QPT_RC);

        RdmaConnParam connParam = new RdmaConnParam();
        connParam.setRetry_count((byte) 2);

        RdmaEP ep = this.getConnectionRequest();
        ep.createPD();

        IbvCQ sendcq = ep.createCQ(20, false);
        IbvCQ recvcq = ep.createCQ(20, false);

        attr.setRecv_cq(recvcq);
        attr.setSend_cq(sendcq);

        ep.createQP(attr);

        this.accept(ep, connParam);
        return new RdmaChannel(ep,attr);
    }

    public RdmaChannel lazy_accept(int size) throws Exception{
        IbvQPInitAttr attr = new IbvQPInitAttr();
        attr.cap().setMax_recv_sge(1);
        attr.cap().setMax_recv_wr(size);
        attr.cap().setMax_send_sge(1);
        attr.cap().setMax_send_wr(size);
        attr.setQp_type(IbvQP.IBV_QPT_RC);

        RdmaConnParam connParam = new RdmaConnParam();
        connParam.setRetry_count((byte) 2);

        RdmaEP ep = this.getConnectionRequest();
        ep.createPD();

        IbvCQ sendcq = ep.createCQ(2*size, false);

        attr.setRecv_cq(sendcq);
        attr.setSend_cq(sendcq);

        ep.createQP(attr);

        this.accept(ep, connParam);
        return new RdmaChannel(ep,attr);
    }

    public void  close(){
        // todo implement
    }
}