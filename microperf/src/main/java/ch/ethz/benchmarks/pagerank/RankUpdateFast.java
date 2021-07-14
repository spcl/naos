package ch.ethz.benchmarks.pagerank;

import java.io.Serializable;

public class RankUpdateFast implements Serializable {
 	
 	public int total_size;
 	public int current_size;
	public int[] ids;
	public float[] ranks;
 
	public RankUpdateFast() { }

	public RankUpdateFast(int size) {
		this.current_size = 0;
		this.total_size = size;
		this.ids = new int[size];
		this.ranks = new float[size];
	}

	public boolean add(int x, float y){
		ids[current_size] = x;
		ranks[current_size] = y;
		current_size++;
		return current_size == total_size ;
	}
	
	@Override
	public String toString() {
		return String.format("id = %d rank = %f", ids, ranks);
	}
	
}
