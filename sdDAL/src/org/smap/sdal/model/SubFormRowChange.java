package org.smap.sdal.model;

import java.util.ArrayList;

public class SubFormRowChange {
	public static final String NEW_RECORD = "new_record";
	public static final String DELETED_RECORD = "deleted_record";
	public static final String CHANGED = "changed";

	public String type;
	public ArrayList<DataItemChange> changes;

	public SubFormRowChange(String type, ArrayList<DataItemChange> changes) {
		this.type = type;
		this.changes = changes;
	}
}
