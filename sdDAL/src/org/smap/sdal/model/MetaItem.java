package org.smap.sdal.model;

//MetaItem ids start from -1000 and go down
public class MetaItem {
	public int id;	// An integer id for preloads so they can be used in lists with questions that are idnetified by their id
	public String type;
	public String name;		
	public String sourceParam;
	public String columnName;
	public String dataType;
	public boolean isPreload;
	public String display_name;
	
	public MetaItem(
			int id,
			String type, 	// type				
			String name, 			// nodeset
			String sourceParam, 		// jr:preloadParams
			String columnName, 
			String dataType, 		// jr:preload
			boolean isPreload,
			String display_name
			) throws Exception {
		if(id > -1000) {
			throw new Exception("Invalid Meta Item ID");
		}
		this.id = id;
		this.type = type;
		this.name = name;
		this.sourceParam = sourceParam;
		this.columnName = columnName;
		this.dataType = dataType;
		this.isPreload = isPreload;
		this.display_name = display_name;
	}
}
