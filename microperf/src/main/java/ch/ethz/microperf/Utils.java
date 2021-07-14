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
package ch.ethz.microperf;

import java.util.ArrayList;

public class Utils {
    static ArrayList<Long> times = new ArrayList<>(128);


    // mark and print use for microapps
    public static void markTime() {
        markTime(false);
    }

        // mark and print use for microapps
    public static void markTime(boolean print) {
        times.add(System.nanoTime());
        int i = times.size() - 2;
        if(print && i >= 0){
            System.out.println(String.format("[progress] Stage %d (took %d nanos)!",i, (times.get(i+1)-times.get(i))));
        }
    }

    public static void print(String str) {
        for(int i=0; i < times.size() -1 ; i++){
            System.out.println(String.format("[%s] Stage %d (took %d nanos)!", str,i, (times.get(i+1)-times.get(i))));
        }
        if(times.size()>1){
            System.out.println(String.format("[%s] Total (took %d nanos)!", str,times.get(times.size()-1)-times.get(0)));
        }

    }

    public static long getTimeMicroseconds() {
    	return System.nanoTime() / 1000;
    }
}
