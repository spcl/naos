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

import com.ibm.disni.verbs.IbvQPInitAttr;

public class RdmaChannel {
    public RdmaEP ep;
    public IbvQPInitAttr attr;

    public RdmaChannel(RdmaEP ep, IbvQPInitAttr attr) {
        this.ep = ep;
        this.attr = attr;
    }

    public void close() {
        //  todo
    }
}
