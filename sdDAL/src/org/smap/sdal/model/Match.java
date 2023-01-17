package org.smap.sdal.model;

public class Match {
	int id;
	public LinkageItem linkageItem;
	public double score;
	public String url;
	
	public Match(int id, double score, String url, LinkageItem linkageItem) {
		this.id = id;
		this.score = score;
		this.url = url;
		this.linkageItem = linkageItem;
	}
}
