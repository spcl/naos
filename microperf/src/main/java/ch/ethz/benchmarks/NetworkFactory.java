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
package ch.ethz.benchmarks;

public class NetworkFactory {


    public static Network getNetwork(String networktype, String serializer, int nodeid, Node[] nodes, int batch_size) throws Exception {

        if(networktype.equalsIgnoreCase("tcp")){
            if(serializer.equalsIgnoreCase("naos")){
                return new NaosTCPNetwork(serializer,nodeid,nodes,batch_size);
            } else {
                return new TCPNetwork(serializer,nodeid,nodes);
            }
        }
        else if(networktype.equalsIgnoreCase("rdma")){
            if(serializer.contains("naos")){ // for using async naos
                return new NaosRDMANetwork(serializer,nodeid,nodes,batch_size);
            } else {
                return new DisniRDMANetwork(serializer,nodeid,nodes);
            }
        }
        throw new Exception("unknown networktype or serializer: " + networktype +"  " +serializer);

    }
}
