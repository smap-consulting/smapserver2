package org.smap.sdal.model;

public class Queue {
	public String name;
	public int length;
	public int new_rpm;			// New entries in queue per minute
	public int processed_rpm;	// Queue entries processed per minute
}
