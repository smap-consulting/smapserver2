package org.smap.sdal.model;

public class Case {
	public int id;	// primary key of the record
	public String title;
	public String instanceid;
	
	public Case(int id, String title, String instanceid) {
		this.id = id;
		this .title = title;
		this.instanceid = instanceid;
	}
}
