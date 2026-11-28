package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * The answers two people changed at the same time on one save
 * Stored as the changes of a conflict record event
 */
public class UpdateConflicts {
	public String table;			// Main table of the record
	public String openedInstance;	// The version the person saving had opened
	public String savedInstance;	// The version saved with the conflicting changes
	public String theirsInstance;	// The version saved by somebody else in the meantime
	public String theirsUser;		// Who saved that version
	public String theirsTime;		// When it was saved
	public String resolvedBy;
	public String resolvedTime;
	public ArrayList<UpdateConflict> items = new ArrayList<>();
}
