package org.smap.sdal.model;

public class GroupSurvey {
	
	public int sId;
	public String groupIdent;
	public String fName;
	
	public GroupSurvey(int id, String ident, String fName) {
		sId = id;
		groupIdent = ident;
		this.fName = fName;
	}
	
	public GroupSurvey(int id) {
		sId = id;
	}
}
