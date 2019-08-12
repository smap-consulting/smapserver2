package org.smap.sdal.model;

import java.util.Date;

/*
 * Contains details of an event within a task's lifecycle
 */
public class TaskEventChange {
	Date when;
	String name;
	String status;
	String assigned;
	String comment;
	Date schedule_at;
	Date schedule_finish;

	// Normal constructor
	public TaskEventChange(String name, String status, String assigned, String comment,
			Date scheduleAt,
			Date scheduleFinish) {
		
		this.when = new Date();
		this.name = name;
		this.status = status;
		this.assigned = assigned;
		this.comment = comment;
		this.schedule_at = scheduleAt;
		this.schedule_finish = scheduleFinish;
	}
}
