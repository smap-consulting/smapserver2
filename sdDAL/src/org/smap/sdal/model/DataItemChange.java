package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Contains details of a change item
 */
public class DataItemChange {
	String col;
	String displayName;
	public String type;
	public String newVal;		// Set for all types other than begin repeat
	public String oldVal;		// Set for all types other than begin repeat
	ArrayList<SubFormRowChange> rows = null;					// begin repeat: new format (typed rows)
	ArrayList<ArrayList<DataItemChange>> changes = null;	// begin repeat: legacy format (backward compat)

	// Normal constructor
	public DataItemChange(String col, String displayName, String type, String newVal, String oldVal) {
		this.col = col;
		this.displayName = displayName;
		this.type = type;
		this.newVal = newVal;
		this.oldVal = oldVal;
	}

	// Constructor for subform
	public DataItemChange(String col, String displayName, ArrayList<SubFormRowChange> rows) {
		this.col = col;
		this.displayName = displayName;
		type = "begin repeat";
		this.rows = rows;
	}
}
