package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Lightweight DTO for SharePoint list notifications.
 * Used by the SP Add-In API (/sharepoint/notifications).
 */
public class SpNotification {
	public int id;
	public String name;
	public String survey_ident;
	public String survey_name;          // read-only: display name populated on GET
	public boolean enabled;
	public String sp_list_title;
	public String sp_operation;         // "insert" (default) or "update"
	public String sp_match_column;      // SP column used to find existing row (update only)
	public String sp_match_field;       // Smap field whose value is matched (update only)
	public ArrayList<SharePointColumnMap> column_map;
}
