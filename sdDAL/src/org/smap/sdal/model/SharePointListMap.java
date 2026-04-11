package org.smap.sdal.model;

import java.sql.Timestamp;

public class SharePointListMap {
	public int id;
	public int o_id;
	public String smap_name;        // Internal name; resource is referenced as "sharepointlist_{smap_name}"
	public String list_title;       // SharePoint list display name
	public int refresh_minutes;     // Cache refresh interval (default 60)
	public Timestamp last_sync;
	public int csv_table_id;        // FK to csvtable — the local cache
	public boolean enabled;
}
