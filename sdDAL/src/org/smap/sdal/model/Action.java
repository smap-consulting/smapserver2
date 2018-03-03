package org.smap.sdal.model;

import java.sql.Date;
import java.util.ArrayList;

public class Action {
	public String action;		// respond (managed forms) || report
	public String notify_type;
	public String notify_person;
	public String link;
	public ArrayList<Role> roles = null;
	
	// Attributes specific to an action type
	public int sId;
	public int pId;
	public int managedId;
	public int prikey;
	
	// Attributes for reports
	public String filename;
	public boolean split_locn;
	public boolean merge_select_multiple;
	public String language;
	public boolean exp_ro;
	public boolean embedImages;
	public boolean excludeParents;
	public boolean hxl;
	public int fId;
	public Date startDate;
	public Date endDate;
	public int dateId;
	public String filter;
	public boolean meta;
	
	public Action(String a) {
		action = a;
	}
}
