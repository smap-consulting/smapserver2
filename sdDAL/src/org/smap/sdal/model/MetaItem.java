package org.smap.sdal.model;

public class MetaItem {
	public String type;
	public String name;		
	public String sourceParam;
	public String columnName;
	public String dataType;
	
	public MetaItem(String type, String name, String sourceParam, String columnName, String dataType) {
		this.type = type;
		this.name = name;
		this.sourceParam = sourceParam;
		this.columnName = columnName;
		this.dataType = dataType;
	}
}
