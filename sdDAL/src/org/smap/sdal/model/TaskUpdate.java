package org.smap.sdal.model;

/*
 * Use to pass information to server required to update a task
 */
public class TaskUpdate {
	
	// This first section is aligned to the Assignment class which used to be used for this purpose
	public int assignment_id;
	public String assignment_status;
	public String task_comment;
	public int task_id;
	public long dbId;		// included to match fieldTask definition
	public String uuid;		// The identifier of the data created by this task
	public User user;
	
	// This section extends Assignment with additional attributes
	public String type;
	public String sIdent;
}
