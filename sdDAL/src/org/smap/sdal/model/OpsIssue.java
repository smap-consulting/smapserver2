package org.smap.sdal.model;

/*
 * One operational problem that somebody needs to resolve, for the operational issues report.
 * Problems that happen repeatedly, such as a notification failing on every submission, are one
 * issue with a count.
 */
public class OpsIssue {
	public String type;			// conflict, notification_error, alert_error, submission_error, notification_filter,
								// alert_filter, notification_orphan, task_deleted_user, task_not_sent
	public String typeLabel;	// type in the user's language
	public int sId;				// survey the issue concerns, 0 when it concerns none
	public String survey;		// survey display name
	public String project;		// project name
	public int pId;				// project id
	public String item;			// the record, notification, alert or task group concerned
	public String detail;		// what is wrong, as recorded
	public String first;		// when it first happened in the period, null when it is a current state
	public String last;			// when it last happened
	public int count = 1;		// how many times it happened in the period
	public String fixUrl;		// page that can fix it, built only from ids

	public OpsIssue(String type) {
		this.type = type;
	}
}
