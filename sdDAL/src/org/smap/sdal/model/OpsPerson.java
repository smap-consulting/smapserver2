package org.smap.sdal.model;

/*
 * Somebody who has a case, for the operations case viewer: an owner, who works on it, or a reference,
 * who can read it.
 */
public class OpsPerson {
	public String ident;
	public String name;
	public String access;		// owner || reference
	public String source;		// how an owner was assigned: console, escalation, device, ops, mcp, backfill
	public String viaRole;		// the role they were assigned through, if any
	public String addedBy;		// who added them
	public String added;		// when, in the user's time zone
}
