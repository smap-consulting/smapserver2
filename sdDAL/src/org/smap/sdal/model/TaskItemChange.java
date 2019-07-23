package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Contains details of a change item
 */
public class TaskItemChange {
	int id;
	String name;
	String status;
	String assigned;
	String comment;

	// Normal constructor
	public TaskItemChange(int id, String name, String status, String assigned, String comment) {
		this.id = id;
		this.name = name;
		this.status = status;
		this.assigned = assigned;
		this.comment = comment;
	}
}
