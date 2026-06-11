package org.smap.sdal.model;

/*
 * A single at-risk work item (overdue task or stale case) shown in the unit drill-down.
 */
public class OpsItem {
	public String type;			// task || case
	public String title;
	public String bundle;		// survey / bundle name, may be null
	public String assignee;		// user the item is assigned to
	public long ageDays;		// age of the item in days
	public boolean overdue;		// true = past due date; false = stale by age only
	public String link;

	public OpsItem(String type, String title, String bundle, String assignee, long ageDays, boolean overdue, String link) {
		this.type = type;
		this.title = title;
		this.bundle = bundle;
		this.assignee = assignee;
		this.ageDays = ageDays;
		this.overdue = overdue;
		this.link = link;
	}
}
