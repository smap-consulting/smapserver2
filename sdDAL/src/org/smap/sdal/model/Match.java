package org.smap.sdal.model;

public class Match {
	int id;
	public LinkageItem linkageItem;
	public double score;
	
	public Match(int id, double score, LinkageItem linkageItem) {
		this.id = id;
		this.score = score;
		this.linkageItem = linkageItem;
	}
}
