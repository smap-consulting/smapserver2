package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.HashMap;

public class SqlDesc {
	public ArrayList<ColDesc> column_details = new ArrayList<ColDesc>();
	public ArrayList<String> column_names = new ArrayList<String> ();
	public String cols;
	public int numberFields = 0;	// Shapefile is limited to 255 - make sure there is space for the geometry
	public String geometry_type = null;
	public String geometry_column;
	public String geometry_table;
	public String sql;
	public boolean gotPriKey = false;
	public ArrayList<String> availableColumns = new ArrayList<String>();	// The subset of required columns that are available due to RBAC etc
	public ArrayList<SqlParam> params = new ArrayList<SqlParam>();
	public HashMap<String, String>  colNameLookup = new HashMap<> ();
	public ArrayList<SqlFrag> columnSqlFrags = new ArrayList<> ();
}
