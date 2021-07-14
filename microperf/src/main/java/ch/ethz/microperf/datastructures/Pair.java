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
package ch.ethz.microperf.datastructures;

import java.io.Serializable;

public class Pair implements Serializable {
    public char[] name;
    public float val;

    // required by Kryo...
    public Pair() {

    }

    public Pair(char[]  name, float val) {
        this.name = name;
        this.val = val;
    }
}
