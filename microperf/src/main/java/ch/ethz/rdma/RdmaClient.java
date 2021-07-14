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

public class RdmaClient {
    RdmaEventChannel cmChannel;
    RdmaCmId idPriv;
    InetSocketAddress dst;

    public RdmaClient(String ipAddress, int port) throws IOException{
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

        InetAddress _dst = InetAddress.getByName(ipAddress);
        dst = new InetSocketAddress(_dst, port);

    }

    public RdmaEP sendConnectionRequest() throws IOException {
        idPriv.resolveAddr(null, dst, 2000);

        //resolve addr returns an event, we have to catch that event
        RdmaCmEvent cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent == null){
            System.out.println("VerbsClient::cmEvent null");
            throw new IOException("wrong event received");
        } else if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ADDR_RESOLVED
                .ordinal()) {
            System.out.println("VerbsClient::wrong event received: " + cmEvent.getEvent());
            throw new IOException("wrong event received");
        }
        cmEvent.ackEvent();

        //we also have to resolve the route
        idPriv.resolveRoute(2000);
        //and catch that event too
        cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent == null){
            System.out.println("VerbsClient::cmEvent null");
            throw new IOException("wrong event received");
        } else if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ROUTE_RESOLVED
                .ordinal()) {
            System.out.println("VerbsClient::wrong event received: " + cmEvent.getEvent());
            throw new IOException("wrong event received");
        }
        cmEvent.ackEvent();

        return new RdmaEP(idPriv);
    }


    public void connect(RdmaEP ep, RdmaConnParam connParam) throws IOException {
        ep.connId.connect(connParam);
        RdmaCmEvent cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED
                .ordinal()) {
            System.out.println("VerbsClient::wrong event received: " + cmEvent.getEvent());
            throw new IOException("wrong event received");
        }
        //always ack CM events
        cmEvent.ackEvent();
    }

    public RdmaChannel lazy_connect() throws Exception{

        IbvQPInitAttr attr = new IbvQPInitAttr();
        attr.cap().setMax_recv_sge(1);
        attr.cap().setMax_recv_wr(10); // no more than 10 receive requests at a time
        attr.cap().setMax_send_sge(1);
        attr.cap().setMax_send_wr(10); // no more than 10 send requests at a time
        attr.setQp_type(IbvQP.IBV_QPT_RC);

        RdmaConnParam connParam = new RdmaConnParam();
        connParam.setRetry_count((byte) 2);
        RdmaEP ep = this.sendConnectionRequest();
        ep.createPD();

        IbvCQ sendcq = ep.createCQ(20, false);
        IbvCQ recvcq = ep.createCQ(20, false);

        attr.setRecv_cq(recvcq);
        attr.setSend_cq(sendcq);

        ep.createQP(attr);
        //Thread.sleep(500);
        this.connect(ep, connParam);

        return new RdmaChannel(ep,attr);
    }

    // it is the second connect
    public RdmaChannel lazy_connect(int size) throws Exception{

        IbvQPInitAttr attr = new IbvQPInitAttr();
        attr.cap().setMax_recv_sge(1);
        attr.cap().setMax_recv_wr(size);
        attr.cap().setMax_send_sge(1);
        attr.cap().setMax_send_wr(size);
        attr.setQp_type(IbvQP.IBV_QPT_RC);

        RdmaConnParam connParam = new RdmaConnParam();
        connParam.setRetry_count((byte) 2);
        RdmaEP ep = this.sendConnectionRequest();
        ep.createPD();

        IbvCQ sendcq = ep.createCQ(2*size, false);

        attr.setRecv_cq(sendcq);
        attr.setSend_cq(sendcq);

        ep.createQP(attr);
        //Thread.sleep(500);
        this.connect(ep, connParam);

        return new RdmaChannel(ep,attr);
    }
}