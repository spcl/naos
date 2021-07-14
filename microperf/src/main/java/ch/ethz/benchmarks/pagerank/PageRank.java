package ch.ethz.benchmarks.pagerank;

import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

import ch.ethz.benchmarks.NetworkFactory;
import ch.ethz.microperf.Utils;
import ch.ethz.benchmarks.Network;
import ch.ethz.benchmarks.Node;

// Dataset: http://data.law.di.unimi.it/webdata/twitter-2010/
// Input syntax: <node id> <to node id> ... <to node id>

public class PageRank {

	public static Node[] nodes;
	public static Network network;
	
	public static String input;
	
	public static HashMap<Integer, ArrayList<Integer>> graph = new HashMap<>(3000000);
	public static HashMap<Integer, Float> ranks = new HashMap<>(3000000);
	public static int[] graphNodes;
	
	public static int ITERS = 100;
	public static int numThreads = 8;
	
	static class Worker extends Thread {
		
		int tid;
		int start;
		int finish;
		HashMap<Integer, Float> updates;
		
		public Worker(int tid, int start, int finish) {
			this.tid = tid;
			this.start = start;
			this.finish = finish;
			this.updates =  new HashMap<>();
		}
		
		@Override
		public void run() {
			for (int i = start; i < finish; i++) {
				Integer key = graphNodes[i];
				float update = ranks.containsKey(key) ? ranks.get(key) / graph.get(key).size() : 0;
				for (Integer edge : graph.get(key)) {
					Float f = updates.get(edge);
					updates.put(edge, f == null ? update: update + f);
				}
			}
		}
	}
	
	public static void loadGraph() throws IOException {
		InputStream inputStream = null;
		Scanner sc = null;
		try {
		    inputStream = new GZIPInputStream(new FileInputStream(input));
		    sc = new Scanner(inputStream);
		    
		    while (sc.hasNextLine()) {
		        String[] splits = sc.nextLine().split("\\s+");
		        if(splits[0].length() == 1 && splits[0].charAt(0) == '#'){
		        	continue;
		        }
		        int id = Integer.parseInt(splits[0]);
		        ArrayList<Integer> edges = new ArrayList<>(splits.length - 1);
		        for (int i = 1; i < splits.length; i++ ) {
		        	edges.add(Integer.parseInt(splits[i]));
		        }
		        
		        ArrayList<Integer> existing = graph.get(id); 
		        if (existing == null) {
		        	graph.put(id, edges);
		        } else {
		        	existing.addAll(edges);
		        }
		    }
		    
		    if (sc.ioException() != null) {
		        throw sc.ioException();
		    }
		    
		} finally {
		    if (inputStream != null) {
		        inputStream.close();
		    }
		    if (sc != null) {
		        sc.close();
		    }
		}
	}
	
	public static void initializeGraph() {
		graphNodes = new int[graph.size()];
		int counter = 0;
		for (Map.Entry<Integer, ArrayList<Integer>> entry : graph.entrySet()) {
			ranks.put(entry.getKey(), 1.0f);
			graphNodes[counter++] = entry.getKey();
		}
		
	}

	public static HashMap<Integer, Float> iteration() throws InterruptedException {
		HashMap<Integer, Float> updates = new HashMap<>();	
		
		List<Worker> threads = new ArrayList<>(numThreads);
		int numNodesThread  = graphNodes.length / numThreads;
		
		for (int i = 0; i < numThreads; i++) {
			Worker t = new Worker(i, i*numNodesThread, (i + 1) * numNodesThread);
			threads.add(t);
			t.start();
		}
		
		for (int i = 0; i < numThreads; i++) {
			threads.get(i).join();
			// merge updates from threads
			updates.putAll(threads.get(i).updates);
		}
		
		return updates;
	}
	public static int updateRanks(ArrayList<RankUpdate> updates) {
		for(RankUpdate update : updates) {
			Float f = ranks.get(update.id);
			ranks.put(update.id, f == null ? update.rank : update.rank + f);
		}
		return updates.size();
	}

	public static int updateRanks(RankUpdate[] updates) {
		for(RankUpdate update : updates) {
			if(update == null){
				break;
			}
			Float f = ranks.get(update.id);
			ranks.put(update.id, f == null ? update.rank : update.rank + f);
		}
		return updates.length;
	}
	
	public static void printRanks(int id) throws FileNotFoundException {
    	try (PrintWriter out = new PrintWriter("pagerank.out." + id)) {
    		for (Entry<Integer, Float> rank : ranks.entrySet()) {
    			out.println(String.format("%d -> %f", rank.getKey(), rank.getValue()));
    		}
    	}
	}	
	
	// Syntax: <serializer> <id> [<host:port:input> ... <hostN:portN:inputN]
    @SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
    	String serializer = args[0];
		String networktype = args[1];
    	int id = Integer.parseInt(args[2]);
		int batch_size = Integer.parseInt(args[3]);
		String filepath = args[4];

    	int prefix = 5;
    	
    	nodes = new Node[args.length - prefix];
    	
    	for (int i = prefix; i < args.length; i++) {
    		String[] splits = args[i].split(":");
    		Node node = new Node(splits[0], Integer.parseInt(splits[1]));
    		nodes[i - prefix]  = node;
    		System.out.println(String.format("Loaded node %s", node));
    	}

		//N is the number of parts
		//split -d -a 2 -n l/N soc-LiveJournal1.txt part
		input = filepath.concat(String.format("%02d.gz" , id)); // 2 digits


		System.out.println(String.format("Node id = %d input = %s", id, input));
    	System.out.println(String.format("Network = %s Serializer = %s", networktype, serializer));
    	network = NetworkFactory.getNetwork(networktype, serializer, id, nodes, batch_size);
		batch_size = Math.abs(batch_size); // I use negative values for Naos special Batch

		Utils.markTime(true);
    	System.out.println("[PageRank] loading graph ... ");
    	loadGraph();
    	//System.out.println("[PageRank] loading graph ... done!");
    	//System.out.println("[PageRank] initializing graph ... ");
    	initializeGraph();
    	System.out.println("[PageRank] initializing graph ... done!");

    	for (int iter = 0; iter < ITERS; iter++) {
    		//long time = Utils.getTimeMicroseconds();
    		
    		// run one full iteration of pagerank
    		HashMap<Integer, Float> updates = iteration();
    		
    		// clean ranks, preparing for the next iteration

    		// partition (allocation data structures)
    		ArrayList<RankUpdate>[] partitions = new ArrayList[nodes.length];
    		for (int i = 0; i < nodes.length; i++) {
    			partitions[i] = new ArrayList<RankUpdate>();
    		}

    		// partition (partition data)    		
    		for (Entry<Integer, Float> entry : updates.entrySet()) {
    			partitions[Math.abs(entry.getKey()) % partitions.length].add(new RankUpdate(entry.getKey(), entry.getValue()));
    		}

			Utils.markTime(true);
    		// shuffle (send)
    		for (int i = 0 ; i< nodes.length; i++) {
    			if (i != id) {
					RankUpdate[] array = new RankUpdate[batch_size];
					int c=0;
					for(RankUpdate t : partitions[i]){
						array[c++] = t;
						if(c==batch_size){
							network.sendObject(i,array);
							array = new RankUpdate[batch_size];
							c=0;
						}
					}
					if(array[array.length-1] != null){
						System.out.printf("Incorrect. The last element must be null");
					}
					network.sendObject(i,array);
 
    				System.out.println(String.format("[PageRank] Sent %d rank updates to %d!", partitions[i].size(), i));
    			}
    		}

			updateRanks(partitions[id]);

			int expect = nodes.length -1;
			while(expect>0){
				RankUpdate[] update = (RankUpdate[]) network.receiveObject(); // is sorted
				if(update[update.length-1] == null){
					expect--;
				}
				//System.out.println(String.format("[PageRank] Received %d updates!", update.length));
				updateRanks(update);
			}

 			//System.out.println(String.format("[PageRank] iteration %d ... done (took %d us)!", iter, Utils.getTimeMicroseconds() - time));
    	}

		Utils.markTime(true);
    	Utils.print("PageRank");
    	
		// Waiting for other nodes to receive all data
		Thread.sleep(1000);

    	network.shutdown();
    	//printRanks(id);
    	System.out.println(String.format("[PageRank] Finished %d!", id));
    }
}
