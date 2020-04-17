package org.smap.sdal.model;

/*
 * Stores column name, table name pairs
 */
public class QuestionForm {
	public String qName;
	public String columnName;
	public String formName;
	public String tableName;
	public String parameters;
	public String qType;
	public int s_id;
	public boolean reference;
	
	public QuestionForm(
			String qName, 
			String columnName, 
			String formName, 
			String tableName, 
			String parameters,
			String qType,
			int s_id,
			boolean reference) {
		
		this.qName = qName;
		this.columnName = columnName;
		this.formName = formName;
		this.tableName = tableName;
		this.parameters = parameters;
		this.qType = qType;
		this.s_id = s_id;
		this.reference = reference;
	}
}
