package org.smap.sdal.model;

import java.util.HashMap;

public class SurveySummary {	
	public String ident;
	public String displayName;
	public String projectName;	
	public String organisation;
	public String enterprise;
	public int records;		// Number of records in the main table
	public String firstDate;
	public String lastDate;
	public HashMap<String, Integer> subForms;
}
