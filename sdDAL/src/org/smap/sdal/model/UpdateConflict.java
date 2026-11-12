package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * One answer, or one repeat row, that two people changed on a record at the same time
 * The value of the person who saved last was applied
 */
public class UpdateConflict {

	public static final String VALUE = "value";					// Both changed the answer
	public static final String THEIRS_DELETED = "theirs_deleted";	// The row was changed by the saver and deleted by the other
	public static final String MINE_DELETED = "mine_deleted";		// The row was deleted by the saver and changed by the other

	public String kind;
	public String col;			// Column of the answer, null for a row
	public String name;			// Question name, null for a row
	public String type;			// Question type
	public String repeat;		// Repeat name, null if this is in the main form
	public String repeatTable;	// Table of the repeat
	public String rowId;		// Repeat row id
	public int mineKey;			// Repeat row in the applied version
	public int theirsKey;		// Repeat row in the other person's version

	public String base;			// The value when the record was opened
	public String theirs;		// The value the other person saved
	public String mine;			// The value applied

	public ArrayList<DataItemChange> values;	// The answers in a row that was deleted on one side
	
	public String resolution;	// Set when reviewed: applied (kept) or theirs

	public static UpdateConflict value(String col, String name, String type, String base, String theirs, String mine) {
		UpdateConflict c = new UpdateConflict();
		c.kind = VALUE;
		c.col = col;
		c.name = name;
		c.type = type;
		c.base = base;
		c.theirs = theirs;
		c.mine = mine;
		return c;
	}

	public static UpdateConflict row(String kind, String repeat, String repeatTable, String rowId, int mineKey, int theirsKey, ArrayList<DataItemChange> values) {
		UpdateConflict c = new UpdateConflict();
		c.kind = kind;
		c.repeat = repeat;
		c.repeatTable = repeatTable;
		c.rowId = rowId;
		c.mineKey = mineKey;
		c.theirsKey = theirsKey;
		c.values = values;
		return c;
	}
}
