package org.smap.sdal.model;

public class MetaItem {
	public String type;
	public String name;		
	public String sourceParam;
	public String columnName;
	public String dataType;
	public boolean isPreload;
	public String display_name;
	
	public MetaItem(String type, 	// type
			String name, 			// nodeset
			String sourceParam, 		// jr:preloadParams
			String columnName, 
			String dataType, 		// jr:preload
			boolean isPreload,
			String display_name
			) {
		this.type = type;
		this.name = name;
		this.sourceParam = sourceParam;
		this.columnName = columnName;
		this.dataType = dataType;
		this.isPreload = isPreload;
		this.display_name = display_name;
	}
}
