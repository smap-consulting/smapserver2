package org.smap.sdal.model;

import java.util.ArrayList;

public class ColDesc {
	public String name;
	public String google_type;
	public String db_type;
	public String qType;
	public String label;
	public boolean needsReplace;
	public String question_name;		// Used with select multiple questions
	public String humanName;
	public ArrayList<OptionDesc> optionLabels = null;		// Used for Stata generation
	public ArrayList<KeyValue>  choices = null;			// Used for compressing select multiples
	public boolean compressed = false;
	
	public ColDesc(String n, String db_type, String qType, String label, 
			ArrayList<OptionDesc> optionLabels,
			boolean needsReplace,
			String question_name,
			ArrayList<KeyValue> choices,
			boolean compressed,
			String humanName) {
		
		name = n;
		this.db_type = db_type;
		this.qType = qType;
		this.label = label;
		this.optionLabels = optionLabels;
		this.needsReplace = needsReplace;
		this.question_name = question_name;
		this.choices = choices;
		this.compressed = compressed;
		this.humanName = humanName;
		
		// Google Maps Engine supports the types "String", "Integer", and "Real"
		if(db_type.startsWith("int") || db_type.equals("serial")) {
			google_type = "Integer";
		} else if(db_type.startsWith("double") || db_type.equals("real")) {
			google_type = "Real";
		} else {
			google_type = "String";
		}
	}
}
