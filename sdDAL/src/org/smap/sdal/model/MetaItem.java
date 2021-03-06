package org.smap.sdal.model;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

// Device MetaItem ids start from -1000 and go down
// Prikey id is set to -1
// Server user is set to -2
public class MetaItem {
	
	public static int INITIAL_ID = -1000;
	public int id;	// An integer id for preloads so they can be used in lists with questions that are identified by their id
	public String type;
	public String name;		
	public String sourceParam;
	public String columnName;
	public String dataType;
	public boolean isPreload;
	public String display_name;
	public boolean published = false;		// Only used for temporary indication that item is published
											// Ie test for availability in database when creating query then use this indicator when reading results 
	public String settings;
	
	public MetaItem(
			int id,
			String type, 			// dateTime, string, geopoint	
			String name, 			// nodeset
			String sourceParam, 		// jr:preloadParams  - deviceid, end, start, 
			String columnName, 
			String dataType, 		// jr:preload - timestamp, property, null
			boolean isPreload,
			String display_name,
			String settings
			) throws Exception {
		if(id > -INITIAL_ID) {
			throw new Exception("Invalid Meta Item ID");
		}
		this.id = id;
		this.type = type;
		this.name = name;
		this.sourceParam = sourceParam;
		this.columnName = GeneralUtilityMethods.cleanName(columnName, true, true, false);
		this.dataType = dataType;
		this.isPreload = isPreload;
		this.display_name = display_name;
		this.settings = settings;
	}
}
