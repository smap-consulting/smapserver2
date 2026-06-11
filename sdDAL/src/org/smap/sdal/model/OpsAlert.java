package org.smap.sdal.model;

/*
 * A summary of one open alert for the manager overview.
 */
public class OpsAlert {
	public int id;
	public String message;
	public int priority;		// 1 = high .. 3 = low
	public String bundle;		// group survey / bundle name, may be null
	public long sinceSeconds;	// age of the alert
	public String link;

	public OpsAlert(int id, String message, int priority, String bundle, long sinceSeconds, String link) {
		this.id = id;
		this.message = message;
		this.priority = priority;
		this.bundle = bundle;
		this.sinceSeconds = sinceSeconds;
		this.link = link;
	}
}
