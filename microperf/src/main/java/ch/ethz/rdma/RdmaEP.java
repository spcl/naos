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

import com.ibm.disni.verbs.IbvCQ;
import com.ibm.disni.verbs.IbvCompChannel;
import com.ibm.disni.verbs.IbvContext;
import com.ibm.disni.verbs.IbvPd;
import com.ibm.disni.verbs.IbvQP;
import com.ibm.disni.verbs.IbvQPInitAttr;
import com.ibm.disni.verbs.RdmaCmId;

// Class is an abstraction of RDMA endpoint
public class RdmaEP {
    //get the id of the newly connection
    RdmaCmId connId;
    IbvContext context;
    public IbvPd pd;
    public IbvQP qp;
    IbvCQ sendcq;
    IbvCQ recvcq;

    RdmaEP(RdmaCmId connId) throws IOException{
        this.connId = connId;
        this.pd = null;
        this.qp = null;
        this.sendcq = null;
        this.recvcq = null;
        //get the device context of the new connection, typically the same as with the server id
        context = connId.getVerbs();
        if (context == null){
            System.out.println("VerbsServer::context null");
            throw new IOException("context null");
        }
    }

    IbvPd createPD() throws IOException{
        if(this.pd !=null) {
            throw new IOException("pd already exist null");
        }
        this.pd = context.allocPd();
        return this.pd;
    }

    void setPD(IbvPd pd) throws IOException{
        if(this.pd !=null) {
            throw new IOException("pd already exist null");
        }
        this.pd = pd;
    }

    IbvQP createQP(IbvQPInitAttr attr) throws IOException{
        if(this.qp !=null) {
            throw new IOException("qp already exist null");
        }
        this.sendcq = attr.getSend_cq();
        this.recvcq = attr.getRecv_cq();
        this.qp = connId.createQP(pd, attr);
        return this.qp;
    }

    IbvCQ createCQ(int size, boolean blocking) throws IOException {
        //the comp channel is used for getting CQ events
        IbvCompChannel compChannel = context.createCompChannel();
        if (compChannel == null){
            System.out.println("VerbsClient::compChannel null");
            throw new IOException("VerbsClient::compChannel null");
        }

        IbvCQ cq = context.createCQ(compChannel, size, 0);
        if (cq == null){
            System.out.println("VerbsClient::cq null");
            throw new IOException("VerbsClient::compChannel null");
        }
        //and request to be notified for this queue
        return cq;
    }

    public void close (){
        this.connId.close();
        //this.qp.close();
        // todo implement
    }

}