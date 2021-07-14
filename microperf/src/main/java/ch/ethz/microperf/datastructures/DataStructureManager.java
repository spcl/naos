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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map; 

public class DataStructureManager {
	
	private static boolean MAP_WITH_CYCLES = true;
	
    public static long memoryUsageBytes() {                                        
        long bytes = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        return bytes;                                           
    }
	
	private static int[] setupInts(int nodes) {
		int[] array = new int[nodes];
		for (int i = 0; i < nodes; i++) {
			array[i] = i;
		}
		return array;
	}

    private static float[] setupFloats(int nodes) {
        float[] array = new float[nodes];
        for (int i = 0; i < nodes; i++) {
            array[i] = i+1.1f;
        }
        return array;
    }
	private static PointFloat[] setupArray(int nodes) {
		PointFloat[] array = new PointFloat[nodes];
		for (int i = 0; i < nodes; i++) {
			array[i] = new PointFloat(i, nodes - i);
		}
		return array;
	}
	
	private static void validateInts(int nodes, int[] array) {
		if (nodes != array.length) {
			throw new RuntimeException("Array length does not match number of nodes!");
		}
		
		for (int i = 0; i < nodes; i++) {
			if (array[i] != i) {
				throw new RuntimeException(String.format("array[%d] = %d (expected %d)", i, array[i], i));	
			}
		}	
	}
	
	private static void validateArray(int nodes, PointFloat[] array) {
		if (nodes != array.length) {
			throw new RuntimeException("Array length does not match number of nodes!");
		}
		
		for (int i = 0; i < nodes; i++) {
			if (array[i].x != i || array[i].y != (nodes - i)) {
				throw new RuntimeException(String.format("array[%d].x = %d (expected %d) array[%d].y = %f (expected %d)", 
						i, array[i].x, i, i, array[i].y, nodes - i));	
			}
		}	
	}
	private static Pair[] setupMapKvp(int nodes) {
 		Pair[] kvp = new Pair[nodes];
		for (int i = 0; i < nodes; i++) {
			char[] word = new char[5]; // it is the average word length of English
			word[0] = (char)('a'+i); word[1] = (char)('b'+i); word[2]=(char)('c'+i);
			word[3] = (char)('d'+i); word[4] = (char)('e'+i);
			kvp[i] = new Pair(word , i);
		}
		return kvp;
	}

	
	private static HashMap<Integer, PointFloat> setupMap(int nodes) {
		HashMap<Integer, PointFloat> map = new HashMap<>();
				
		for (int i = 0; i < nodes; i++) {
			map.put(i, new PointFloat(MAP_WITH_CYCLES ? i : nodes + i, nodes - i));
		}
		
		return map;
	}

	private static void validateMap(int nodes, HashMap<Integer, PointFloat> map) {
		if (nodes != map.size()) {
			throw new RuntimeException("Map size does not match number of nodes!");
		}
		
		for (int i = 0; i < nodes; i++) {
			PointFloat pf = map.get(i);
			
			if (pf == null) {
				throw new RuntimeException(String.format("Map does not contain expected key %d", i));	
			}
			
			if (pf.x != (MAP_WITH_CYCLES ? i : nodes + i) || pf.y != (nodes - i)) {
				throw new RuntimeException(String.format("array[%d].x = %d (expected %d) array[%d].y = %f (expected %f)",
						i, pf.x, i, i, pf.y, nodes - i));	
			}
		}
	}
	
	public static Object setupObject(String datastructure, int nodes, boolean make_array ) throws Exception {
	 

 		switch(datastructure) {
			case "floats": {
				return setupFloats(nodes); 
			}
			case "array":  {
				return setupArray(nodes); 
			}
			case "kvp": { 
				return setupMapKvp(nodes); 
			}
			case "map": {
				HashMap<Integer, PointFloat> map = setupMap(nodes); 
				if(make_array){
					Object[] array = new Object[nodes];
					int c = 0;
					for (Map.Entry<Integer, PointFloat> entry : map.entrySet()) {
					    array[c++] = entry;
					}
					return array;
				} else {
					return map;
				}
			}
			default: throw new Exception("unknown data structure: " + datastructure);
		}
 
	}

	public static Iterable setupObjectIterable(String datastructure, int nodes) throws Exception {
		long memory = 0;
		System.gc();
		ArrayList<Object> t = new ArrayList<>(nodes);

		switch(datastructure) {
			case "floats": return null;
			case "array": {
				PointFloat[] a = setupArray(nodes);
				for(int i =0; i < nodes; i++){
					t.add(a[i]);
				}
				return t;
			}
			case "map": {
				return setupMap(nodes).entrySet();
			}
			case "kvp":  {
				Pair[] a = setupMapKvp(nodes);
				for(int i =0; i < nodes; i++){
					t.add(a[i]);
				}
				return t;
			}
			default: throw new Exception("unknown data structure: " + datastructure);
		}

	}


	
	@SuppressWarnings({ "unchecked" })
	public static void validateObject(String datastructure, int nodes, Object o) throws Exception {
		switch(datastructure) {
		case "floats": {
			float[] array = (float[]) o;
			//validateInts(nodes, array);
            System.out.printf("[TODO] implement validation later");
			break;
		}
		case "array": {
			PointFloat[] array = (PointFloat[]) o;
			validateArray(nodes, array);
			break;
		}
		case "map": {
			HashMap<Integer, PointFloat> map = (HashMap<Integer, PointFloat>) o;
			validateMap(nodes, map);
			break;
		}
		case "kvp": {
				System.out.printf("[TODO] implement validation later");
				break;
			}
		default: throw new Exception("unknown data structure: " + datastructure);
		}
	}
}
