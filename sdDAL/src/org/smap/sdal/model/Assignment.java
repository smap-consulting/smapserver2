package org.smap.sdal.model;

import org.smap.sdal.model.User;

public class Assignment {
	public int assignment_id;
	public String assignment_status;
	public String task_comment;
	public User user;
	public int task_id;
	public String uuid;		// The identifier of the data created by this task
}
