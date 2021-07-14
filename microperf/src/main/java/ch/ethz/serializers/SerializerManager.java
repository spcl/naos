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
package ch.ethz.serializers;

import ch.ethz.naos.skyway.Skyway;
import ch.ethz.serializers.java.JavaSerializer;
import ch.ethz.serializers.kryo.KryoSerializer;
import ch.ethz.serializers.skyway.SkywaySerializer;

public class SerializerManager {

	public static Serializer getSerializer(String serializer) throws Exception {
		switch(serializer) {
		case "java": return new JavaSerializer();
		case "kryo": return new KryoSerializer();
		case "skyway": return new SkywaySerializer(64*1024*1024); // 64 MiB
		case "naos": return null;
		default: throw new Exception("unknown serializer: " + serializer);
		}
	}
}
