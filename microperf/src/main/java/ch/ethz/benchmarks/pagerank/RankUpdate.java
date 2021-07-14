package ch.ethz.benchmarks.pagerank;

import java.io.Serializable;

public class RankUpdate implements Serializable {

	public int id;
	public float rank;
	
	public RankUpdate() { }
	public RankUpdate(int id, float rank) {
		this.id = id;
		this.rank = rank;
	}
	
	@Override
	public String toString() {
		return String.format("id = %d rank = %f", id, rank);
	}
	
}
