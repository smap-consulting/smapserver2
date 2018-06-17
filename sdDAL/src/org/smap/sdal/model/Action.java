package org.smap.sdal.model;

import java.util.ArrayList;

public class Action {
	public String name;
	public String action;		// respond (managed forms) || report || task
	public String notify_type;
	public String notify_person;
	public String link;
	public ArrayList<Role> roles = null;
	
	// Attributes specific to an action type
	public int sId;
	public String surveyName;
	public String surveyIdent;
	public int pId;
	public int managedId;
	public int prikey;
	
	// Attributes for tasks
	public String datakey;
	public String datakeyvalue;
	public int assignmentId;
	
	// Attributes for reports
	public String reportType;
	public String filename;
	
	// General parameters
	public ArrayList<KeyValueSimp> parameters = null;
	
	public Action(String a) {
		action = a;
	}
}
