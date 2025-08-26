package org.smap.sdal.model;

public class RecordLocator {
	public int key;			// The most recent primary key of the sequence of changes to a record
	public String thread;	// The thread
	
	public RecordLocator(int id, String thread) {
		this.key = id;
		this.thread = thread;
	}
}
