package org.smap.sdal.model;

/*
 * Stores column name, table name pairs
 */
public class QuestionForm {
	public String columnName;
	public String formName;
	
	public QuestionForm(String cn, String fn) {
		columnName = cn;
		formName = fn;
	}
}
