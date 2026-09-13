package org.smap.sdal.model;

/*
 * A period and organisation unit whose last send to DHIS2 failed
 *
 * Deliberately holds the slice and not the action.  Remembering "delete this" and replaying it
 * later would remove data that had since been restored, so a retry rebuilds the slice from the
 * records as they stand and sends whatever that says.  That way an undelete, a re-delete or an
 * edit in between all reach the right answer without any of them being tracked
 */
public class Dhis2PendingSlice {

	public int id;
	public int e_id;			// The export the slice belongs to
	public int o_id;
	public String period;		// DHIS2 period identifier, eg 202608
	public String org_unit;		// DHIS2 organisation unit code
	public int attempts;
	public String last_error;
}
