package org.smap.sdal.model;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;

/*
 * Contains data used in a survey view
 */
public class SurveySettingsDefn {
	
	public int limit;
	public String filter;
	public String dateName;
	public Date fromDate;
	public Date toDate;
	public String overridenDefaultLimit;
	public String include_bad;
	public String include_completed;
	public int pageLen;				// Datatables
	public String colOrder;			// Datatables
	
	public ArrayList<MapLayer> layers = new ArrayList<MapLayer> ();             // Map
	public ArrayList<ChartDefn> charts = new ArrayList<ChartDefn> ();           // Charts
	public HashMap<String, ConsoleColumn> columnSettings = new HashMap<> ();    // Columns

	
}
