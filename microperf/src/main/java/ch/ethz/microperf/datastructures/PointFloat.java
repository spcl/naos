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

public class PointFloat implements Serializable {
	public int x;
	public float y;
	
	// required by Kryo...
	public PointFloat() {
		
	}

	public PointFloat(int x, float y) {
	  this.x = x;
	  this.y = y;
	}

    @Override
    public String toString() {
        return String.format("PointFloat x=%s y=%s", x, y);
    }

}
