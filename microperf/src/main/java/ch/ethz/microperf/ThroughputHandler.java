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

public class ThroughputHandler {

    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;

    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS; // 2 ms accuracy
    private static final long COUNT_ITER = 64; // 64 iterations accuracy for unlimited speed tests

    final private long targetThroughput;
    final private long warmupMs;
    final private long testMs;
    final private long sleepTimeNs;

    private long lastTime;

    public ThroughputHandler(long targetThroughput, long warmupMs, long testMs) {
        this.targetThroughput = targetThroughput;
        this.warmupMs = warmupMs;
        this.testMs = testMs;
        this.sleepTimeNs = targetThroughput == 0 ? 0 : NS_PER_SEC / targetThroughput;
        this.lastTime = System.nanoTime();
    }

    public long getWarmupNs(){
        return warmupMs*NS_PER_MS;
    }

    public long getWarmupMs(){
        return warmupMs;
    }

    public long getTestNs(){
        return testMs*NS_PER_MS;
    }

    public long getTestMs(){
        return testMs;
    }

    public long sleepAndGetTime(long i, long startNs) {

        if(this.targetThroughput == 0){ // unlimited throughput
            if(i % COUNT_ITER == 0){ // take time each COUNT_ITER iterations
                lastTime = System.nanoTime();
            }
            return lastTime;
        }

        // limited throughput case
        long expectedTime = startNs  + i * sleepTimeNs;
        long timediff = expectedTime - lastTime;

        if(timediff > MIN_SLEEP_NS){ // the case the sender is fast. only sync each 2 ms
            while(lastTime < expectedTime){
                lastTime = System.nanoTime();
            }
        } else if(timediff < 0){ // the sender is slower then expected
            if(i % COUNT_ITER == 0){ // take time each COUNT_ITER iterations
                lastTime = System.nanoTime();
            }
        }

        return lastTime;
    }


}
