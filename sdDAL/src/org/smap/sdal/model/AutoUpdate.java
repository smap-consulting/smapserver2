package org.smap.sdal.model;

public class AutoUpdate {
	
	public static String AUTO_UPDATE_IMAGE = "imagelabel";
	public static String AUTO_UPDATE_AUDIO = "audiotranscript";
	
	public String type;						// Types as above
	public String tableName;
	public String sourceColName;			// Column that has the source of the data
	public String targetColName;			// Column that will store the labels
	
	public String labelColType;				// For image labels set to label || text
	
	public AutoUpdate(String type) {
		this.type = type;
	}
}
