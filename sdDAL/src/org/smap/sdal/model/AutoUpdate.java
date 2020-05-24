package org.smap.sdal.model;

public class AutoUpdate {
	
	public int oId;
	public String locale;					// For showing translated messages to the user
	public String type;						
	public String tableName;
	public String sourceColName;			// Column that has the source of the data
	public String targetColName;			// Column that will store the labels	
	public String labelColType;				// For image labels set to label || text
											// This will determine how the response is formatted
	
	public String fromLang;
	public String toLang;
	public boolean medical;
	
	public AutoUpdate(String type) {
		this.type = type;
	}
}
