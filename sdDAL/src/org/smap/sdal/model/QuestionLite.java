package org.smap.sdal.model;

/*
 * Form Class
 * Used for survey editing
 */
public class QuestionLite {
	public boolean toplevel = false;		// Set true if the form has no parent
	public int id;
	public String type;
	public String q;				// The question for a specified language
	public String name;
	public String column_name;
	public int f_id;
	
	public QuestionLite() {}
	public QuestionLite(String type, String name, String column_name) {
		this.type = type;
		this.name = name;
		this.column_name = column_name;
	}
}
