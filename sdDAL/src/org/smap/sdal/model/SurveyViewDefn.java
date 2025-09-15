package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.HashMap;

/*
 * Contains data used in a survey view
 */
public class SurveyViewDefn {
	public int sId;
	public HashMap<String, String> parameters = null;
	public HashMap<String, Integer> mainColumnNames = new HashMap<>();
	public HashMap<Integer, Integer> mainColumnsRemoved = new HashMap<>();
	public ArrayList<TableColumn> columns = new ArrayList<TableColumn> ();		// Table
	public ArrayList<MapLayer> layers = new ArrayList<MapLayer> ();             // Map
	public ArrayList<ChartDefn> charts = new ArrayList<ChartDefn> ();           // Charts
	public ArrayList<ChoiceList> choiceLists;           						// Choices
	public String tableName;
	
	public SurveyViewDefn() {
		
	}
	
	public SurveyViewDefn(int sId) {
		this.sId = sId;
	}
}
