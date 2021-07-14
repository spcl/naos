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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public abstract class ExperimentEntryPoint {
    
    // TODO - add documentation to these options.
    protected static int iters;
    protected static String host;
    protected static int port;
    protected static String serializer;
    protected static String network;
    protected static String datastructure;
    protected static int nodes;
    protected static boolean isdirect;
    protected static long timeinms;
    protected static long target_throughput;
    protected static int specialized_reg_size;
    protected static int batch_size;
    protected static boolean naosarray;
    
    public static void parseArgs(String entryPointName, String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption( "i", "iter", true, "number of iterations. Default: 5");
        options.addOption( "h", "host", true, "receiver address. Default: 127.0.0.1");
        options.addOption( "p", "port", true, "receiver port. Default: 9999");
        options.addOption( "s", "ser", true, "serialization type (java, kryo, naos, skyway). Default: java");
        options.addOption( "n", "net", true, "network (tcp, rdma). Default: tcp");
        options.addOption( "d", "data", true, "datastructure (floats, array, map, kvp). Default: array");
        options.addOption( "e", "elements", true, "datastructure number of elements. Default: 128000");
        options.addOption( "o", "offheap", false, "use offheap memory. Default: false");
        options.addOption( "t", "tput", true, "target throughput in req/sec. Default: 1000");
        options.addOption( "m", "time", true, "duration of the experiment in ms. Default: 1000");
        options.addOption( "b", "batch", true, "naos batch size in bytes. It is used for the iterative send. Default: 0 - off");
        options.addOption( "a", "special", true, "specialized_reg_size in bytes (it is for ODP experiments). Default: 0 - off"); // it is for ODP experiment
        options.addOption( "r", "array", false, "enable naos array send approach that sends only elements of a container, Default: false");

        try {
            CommandLine line = parser.parse(options, args);
            iters = Integer.parseInt(line.getOptionValue("iter", "5"));
            host = line.getOptionValue("host", "127.0.0.1");
            port = Integer.parseInt(line.getOptionValue("port", "9999"));           
            serializer = line.getOptionValue("ser", "java");
            network = line.getOptionValue("net", "tcp");
            datastructure = line.getOptionValue("data", "array");
            nodes = Integer.parseInt(line.getOptionValue("elements", "128000"));
            isdirect = line.hasOption("offheap");
            timeinms = Integer.parseInt(line.getOptionValue("time", "1000"));
            target_throughput = Integer.parseInt(line.getOptionValue("tput", "1000"));
            batch_size = Integer.parseInt(line.getOptionValue("batch", "0"));
            specialized_reg_size = Integer.parseInt(line.getOptionValue("special", "0"));
            naosarray = line.hasOption("array");
            
        } catch (Exception e) {
            new HelpFormatter().printHelp(128, entryPointName, null, options, null);
            throw e;
        }       
    }
}
