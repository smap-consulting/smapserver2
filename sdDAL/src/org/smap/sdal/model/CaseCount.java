package org.smap.sdal.model;

public class CaseCount {
	public String day;
	public int opened;
	public int closed;
	
	public CaseCount(String day, int opened, int closed) {
		this.day = day;
		this.opened = opened;
		this.closed = closed;
	}
}
