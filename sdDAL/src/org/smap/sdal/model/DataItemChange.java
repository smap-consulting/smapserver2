package org.smap.sdal.model;

/*
 * Contains details of a change item
 */
public class DataItemChange {
	String col;
	String type;
	String newVal;
	String oldVal;

	public DataItemChange(String col, String type, String newVal, String oldVal) {
		this.col = col;
		this.type = type;
		this.newVal = newVal;
		this.oldVal = oldVal;
	}
}
