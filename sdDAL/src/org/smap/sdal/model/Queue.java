package org.smap.sdal.model;

import java.sql.Timestamp;
import java.util.ArrayList;

public class Queue {
	public String name;
	public int length;
	public int new_rpm;			// New entries in queue per minute
	public int processed_rpm;	// Queue entries processed per minute
	public int error_rpm;		// Error rate per minute
	/*
	 * Set on the message queue when the relay is refusing mail because we have sent too
	 * much of it.  The backlog grows and nothing is processed, which otherwise looks like
	 * a broken subscriber rather than a queue waiting its turn.
	 */
	public Timestamp email_paused_until;
	public String email_paused_reason;
	public ArrayList<WorkerInfo> workers;
}
