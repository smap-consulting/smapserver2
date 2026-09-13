package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * What DHIS2 reported about an import
 *
 * The per value conflicts matter as much as the counts.  "4 rejected" is not actionable,
 * "unknown org unit OU_9999" is, and the difference decides whether someone can fix their own
 * data or has to ask us
 */
public class Dhis2ImportSummary {

	public boolean success;
	public boolean dry_run;

	public int sent;			// How many values Smap built
	public int imported;
	public int updated;
	public int ignored;
	public int deleted;

	public String status;		// As DHIS2 reported it
	public String description;

	public ArrayList<String> conflicts = new ArrayList<>();
}
