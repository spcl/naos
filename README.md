# Naos: Serialization-free RDMA networking in Java
Naos  is  the  first serialization-free communication library for JVM that allows applications to send objects directly through RDMA or TCP connections.  

This is the source code for our [Usenix ATC 2021 paper](https://www.usenix.org/conference/atc21/presentation/taranov).

## Requirements
 * All requirements of openJDK 11. See [Building the JDK](doc/building.md).
 * rdma-core library, or equivalent RDMA verbs library.
 * Disni library for microbenchmarks. See [Disni](https://github.com/zrlio/disni).
 * RDMA-capable network devices must have assigned IP addresses.

## Compilation of openJDK 11 with Naos patch

Naos can be shipped as a patch to openJDK 11. We implemented Naos for [shenandoah-jdk-11.0.6+8](http://hg.openjdk.java.net/shenandoah/jdk11/rev/3cf5b251dc0b).

To ease deployment of Naos we provide openJDK 11 with Naos installed.
Thus, you need first to compile openJDK 11, provided in this repository. For that you can use `./configure.sh` and `./build.sh` scripts. 

Please, read the content of `./configure.sh` and choose the configure option you need. Otherwise it will configure the build in `release` mode. 

`./build.sh`, that builds the openJDK, will ask which build to make and whether to clean the build. We recommend to compile in `release` mode and with a `clean` build.

To compile the openJDK 11 you may need the following packages:
```
sudo apt-get install libx11-dev libxext-dev libxrender-dev libxrandr-dev libxtst-dev libxt-dev
sudo apt-get install libcups2-dev libfontconfig1-dev libasound2-dev rdma-core
```

## Compilation of Naos library
To build Naos library, you should `cd naos` and then build it with  `./build.sh`. Choose the build option according to your needs.
It will produce `target/naos-0.0.1-SNAPSHOT-jar-with-dependencies.jar` that you can use for serialization-free networking. Note that Naos will work only with the patched JDK compiled in the previous step. 

## Basic usage
You can find basic usages of Naos in `microperf` folder. To run a simple test of Naos and other serializers that we implemented you can do the following:

```
cd microperf
./build.sh
./run_standalone_test.sh 192.168.1.10
```
where 192.168.1.10 is the IP address of your RDMA-capable device. It will start all basics tests and print latency of sending an array of `floats`. 

### Advanced Examples

Our microperf tool implements many others benchmarks that can be launched using scripts in `microperf/scripts`.
Most of java binaries use the following arguments:

```
 -a,--special <arg>    specialized_reg_size in bytes (it is for ODP experiments). Default: 0 - off
 -b,--batch <arg>      naos batch size in bytes. It is used for the iterative send. Default: 0 - off
 -d,--data <arg>       datastructure (floats, array, map, kvp). Default: array
 -e,--elements <arg>   datastructure number of elements. Default: 128000
 -h,--host <arg>       receiver address. Default: 127.0.0.1
 -i,--iter <arg>       number of iterations. Default: 5
 -m,--time <arg>       duration of the experiment in ms. Default: 1000
 -n,--net <arg>        network (tcp, rdma). Default: tcp
 -o,--offheap          use offheap memory. Default: false
 -p,--port <arg>       receiver port. Default: 9999
 -r,--array            enable naos array send approach that sends only elements of a container, Default: false
 -s,--ser <arg>        serialization type (java, kryo, naos, skyway). Default: java
 -t,--tput <arg>       target throughput in req/sec. Default: 100

```

Note that some implemented ideas (e.g., iterative send (that sends containers) and ODP usage (an RDMA capability that allows to register memory on demand)) are not described in the published paper. 


### Error handling
If you experience the following error for RDMA experiments, then you have not added [libdisni](https://github.com/zrlio/disni) to your library path. You can use `-Djava.library.path` for java or `LB_LIBRARY_PATH` in linux to include it. 

```
Exception in thread "main" java.lang.UnsatisfiedLinkError: no disni in java.library.path: [/usr/java/packages/lib, /usr/lib64, /lib64, /lib, /usr/lib]
    at java.base/java.lang.ClassLoader.loadLibrary(ClassLoader.java:2660)
    at java.base/java.lang.Runtime.loadLibrary0(Runtime.java:829)
    at java.base/java.lang.System.loadLibrary(System.java:1870)
    at com.ibm.disni.verbs.impl.NativeDispatcher.<clinit>(NativeDispatcher.java:36)
    at com.ibm.disni.verbs.impl.RdmaProviderNat.<init>(RdmaProviderNat.java:43)
    at com.ibm.disni.verbs.RdmaProvider.provider(RdmaProvider.java:58)
    at com.ibm.disni.verbs.RdmaCm.open(RdmaCm.java:49)
    at com.ibm.disni.verbs.RdmaEventChannel.createEventChannel(RdmaEventChannel.java:66)
    at ch.ethz.rdma.RdmaClient.<init>(RdmaClient.java:16)
    at ch.ethz.microperf.SenderNIO.main(SenderNIO.java:61)
```

## JVM flags related to Naos

### Shenandoah 

RDMA Naos works only with Shenandoah GC. We use the following options in our work:

```
-XX:+UnlockDiagnosticVMOptions -XX:+UnlockExperimentalVMOptions \
-XX:+UseShenandoahGC -XX:ShenandoahGCMode=passive -XX:-UseBiasedLocking  \
-XX:+AlwaysPreTouch -XX:ShenandoahHeapRegionSize=32m
```

For TCP Naos you can use any garbage collector. 

### Debugging

For debugging, you can enable some prints from Naos library by using a combination of the following options:
```
-Xlog:naos=info
-Xlog:naos=debug
-Xlog:naos=trace
```

We debugging, we also recommend to configure JDK in `debug` mode and recompile it with a clean build.

### Experimental options
Users can disable cycle detection in Naos using `-XX:+NaosDisableCycles` option.

Users can experiment with ODP by using `-XX:+NaosUseODP`. Note it was not tested for a while after a considerable code update.

Users can enable pipelining of graph traversal and networking by using `-XX:NaosPipelineSize=1047552`, where `1047552` is the size of pipeline block in bytes.

## API details
Naos provides the following API. Note that some functions are not mentioned in the paper.


### Naos TCP
```java
    // read object from a socket.
    public Object readObject() throws IOException;

    // write object to a socket.
    public void writeObject(Object obj) throws IOException;  

    // write an array to a socket. It trims the container and sends only elements of an array.
    public void writeArray(Object objArray, int elements) throws IOException;
 
    // send all the object of the iterator and also it sends objects in batches for performance purposes.
    public int writeIterable(Iterator it, int batch_size) throws IOException;
 
    /**
     * @param timeout in ms. Negative values mean unlimited waiting time. 0 means non-blocking.
     * @return A value of 0 indicates that the call timed out. Positive values if it is readable. Negative values on error.
     */
    public int isReadable(int timeout) throws IOException;
    
    // the call traverses the object and estimates how much data Naos would send. Used for research purposes only.
    public long sizeOfObject(Object obj) throws IOException;

```


### Naos RDMA
A Naos RDMA connection is uni-directional, where one side is a passive receiver and another is an active sender.
To have bi-directional connection, a user needs to establish two QP connections. 

Each RDMA connection has two datapaths:
one for objects and one for Integers that are internally sent via empty RDMA requests with immediate data. So Objects and Integers can be sent independently without blocking each other. It can be used to signal the remote side about the type of received objects or to have fast low-latency notifications.

A bi-directional connection exposes the following API:

```java
    // read object from an RDMA connection
    public Object readObject() throws IOException;
    // write object to an RDMA connection
    public boolean writeObject(Object objToWrite) throws IOException;
    // write object asynchronously to an RDMA connection
    public long writeObjectAsync(Object objToWrite) throws IOException;
    // write an array to an RDMA connection. It trims the container and sends only elements of an array.
    public boolean writeArray(Object objToWrite, int elements) throws IOException;
    // write an array asynchronously to an RDMA connection. It trims the container and sends only elements of an array.
    public long writeArrayAsync(Object objToWrite, int elements) throws IOException;
    // send all the object of the iterator and also it sends objects in batches for performance purposes
    public int writeIterable(Iterator it, int batch_size) throws IOException;
    // send asynchronously all the object of the iterator and also it sends objects in batches for performance purposes
    public long writeIterableAsync(Iterator it, int batch_size) throws IOException;
    // wait for completion of an asynchronous request
    public void waitHandle(long handle) throws IOException;
    // test for completion of an asynchronous request
    public boolean testHandle(long handle) throws IOException;
    // send one Integer to a remote endpoint. We use another datapath for that.
    public boolean writeInt(int val) throws IOException;
    // read one Integer sent by a remote endpoint. We use another datapath for that.
    public int readInt() throws IOException;

    /**
     * @param timeout in ms. Negative values mean unlimited waiting time. 0 means non-blocking.
     * @return A value of 0 indicates that the call timed out. Negative values on error. 1 - has object to read, 2 - has int to read, 3 - has both
     */
    public int isReadable(int timeout) throws IOException;

    // the call traverses the object and estimates how much data Naos would send. Used for research purposes only.
    public long sizeOfObject(Object obj) throws IOException;

```

### Skyway
We also implemented [Skyway](https://dl.acm.org/doi/10.1145/3173162.3173200), but with manual class registration (see `registerClass(Class type, int id)` in `Skyway.java`). 
Our Skyway implementation can be found in  `src/hotspot/share/prims/jvm_skyway*`.


## Implementation notice
The code is not written by professional software developers, and that is why the code is not of the production-level quality.
Notably, some Naos settings are still hard-coded. 

Please, read the JDK patch to see all settings. The core implementation is in `src/hotspot/share/prims/jvm_naos*`.


## Citing this work

If you use our code, please consider citing our [Usenix ATC 2021 paper](https://www.usenix.org/conference/atc21/presentation/taranov):

```
@inproceedings{taranov-naos,
author = {Konstantin Taranov and Rodrigo Bruno and Gustavo Alonso and Torsten Hoefler},
title = {Naos: Serialization-free {RDMA} networking in Java},
booktitle = {2021 {USENIX} Annual Technical Conference ({USENIX} {ATC} 21)},
year = {2021},
isbn = {978-1-939133-23-6},
pages = {1--14},
url = {https://www.usenix.org/conference/atc21/presentation/taranov},
publisher = {{USENIX} Association},
month = jul,
}
```

## Contact 
If you have questions, please, contact:

Konstantin Taranov (konstantin.taranov "at" inf.ethz.ch)    
Rodrigo Bruno (rodrigo.bruno "at" tecnico.ulisboa.pt)    
