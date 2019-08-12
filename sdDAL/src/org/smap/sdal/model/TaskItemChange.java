package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.Date;

/*
 * Contains details of a change item
 */
public class TaskItemChange {
	public int taskId;
	public int assignmentId;
	public ArrayList<TaskEventChange> taskEvents = new ArrayList<> ();

	// Normal constructor
	public TaskItemChange(int taskId, int assignmentId, String name, String status, String assigned, 
			String comment,
			Date scheduleAt,
			Date scheduleFinish) {
		this.taskId = taskId;
		this.assignmentId = assignmentId;
		taskEvents.add(new TaskEventChange(name, status, assigned, comment, scheduleAt, scheduleFinish));
		
	}
}
