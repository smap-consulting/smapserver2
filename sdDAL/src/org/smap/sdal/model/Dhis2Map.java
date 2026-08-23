package org.smap.sdal.model;

/*
 * One DHIS2 resource cached as an organisation level CSV
 *
 * Referenced in a form as "dhis2_{smap_name}", the same way a SharePoint list is referenced as
 * "sharepointlist_{smap_name}"
 */
public class Dhis2Map {

	public static final String TYPE_ORGUNITS = "orgunits";
	public static final String TYPE_OPTIONSET = "optionset";
	public static final String TYPE_PROGRAMS = "programs";

	public int id;
	public int o_id;
	public int dhis2_server_id;
	public String smap_name;			// Referenced in a form as "dhis2_{smap_name}"
	public String resource_type;		// One of the TYPE_ constants above
	public String dhis2_ref;			// The uid or code of the object, where the type needs one
	public String ou_filter;			// Optional org unit subtree, the uid of the root to sync
	public int refresh_minutes;
	public String last_sync;
	public String last_sync_result;
	public int row_count;
	public int csv_table_id;
	public boolean enabled;
}
