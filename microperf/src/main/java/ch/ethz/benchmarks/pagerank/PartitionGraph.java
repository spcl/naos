package ch.ethz.benchmarks.pagerank;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Scanner;

import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class PartitionGraph {
	
	public static ArrayList<PrintStream> partitionStreams = new ArrayList<>();
	
	// Syntax: <# partitions> <input graph>
	public static void main(String[] args) throws IOException {
		int partitions = Integer.parseInt(args[0]);
		String filein = args[1];
		String prefix = args[2];
		
		for (int i = 0; i < partitions; i++) {
			partitionStreams.add(
					new PrintStream(
							new GZIPOutputStream(
									new FileOutputStream(String.format("%s%02d_%02d.gz", prefix, partitions, i)), 
									32*1024*1024)));
		}
		
		InputStream inputStream = null;
		Scanner sc = null;
		try {
		    inputStream = new GZIPInputStream(new FileInputStream(filein), 32*1024*1024);
		    sc = new Scanner(inputStream);
		    
		    while (sc.hasNextLine()) {
		    	String line = sc.nextLine();
		    	if(line.length() > 0 && line.charAt(0) == '#'){
		        	continue;
		        }
		        int node = Integer.parseInt(line.split("\\s+", 2)[0]);
		        partitionStreams.get(node % partitions).println(line);
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

		    for (int i = 0; i < partitions; i++) {
				partitionStreams.get(i).flush();
				partitionStreams.get(i).close();
			}

		}
	
		
		
	}
}
