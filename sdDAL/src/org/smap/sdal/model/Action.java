package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.managers.EmailManager;

public class Action {
	private static Logger log =
			Logger.getLogger(EmailManager.class.getName());
	
	public String name;
	public String action;		// respond (managed forms) || report || task || mailout
	public String notify_type;
	public String notify_person;
	public String link;
	public boolean single = false;	// Set to true to only allow a single submission
	public String email;
	public ArrayList<Role> roles = null;
	public Instance initialData;
	
	// Attributes specific to an action type
	public int sId;
	public String surveyName;
	public String surveyIdent;
	public int pId;
	public int managedId;		// deprecate
	public String groupSurvey;
	public int prikey;
	public int taskKey;
	
	// Attributes for tasks
	public String datakey;
	public String datakeyvalue;
	public int assignmentId;
	
	// Attributes for reports
	public String reportType;
	public String filename;
	
	// Attibuts for mailouts
	public int mailoutPersonId;
	
	// General parameters
	public ArrayList<KeyValueSimp> parameters = null;
	
	public Action(String a) {
		action = a;
	}
	
	public int getFormId() {
		int fId = 0;
		for(KeyValueSimp kv : parameters) {
			if(kv.k.equals("form")) {
				try {
					fId = Integer.parseInt(kv.v);
				} catch (Exception e) {
					log.log(Level.SEVERE, e.getMessage(), e);
				}
			}
		}
		return fId;
	}
}
