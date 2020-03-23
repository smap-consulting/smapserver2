package org.smap.sdal.model;

public class AutoUpdate {
	
	public String type;						// Types as above
	public String tableName;
	public String sourceColName;			// Column that has the source of the data
	public String targetColName;			// Column that will store the labels
	
	public String labelColType;				// For image labels set to label || text
											// This will determine how the response is formatted
	
	public AutoUpdate(String type) {
		this.type = type;
	}
}
