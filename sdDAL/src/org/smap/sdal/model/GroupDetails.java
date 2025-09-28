package org.smap.sdal.model;

public class GroupDetails {
	public int sId;
	public String surveyName;
	public String groupSurveyIdent;
	public String surveyIdent;
	public boolean dataSurvey;
	public boolean oversightSurvey;
	public boolean readOnlySurvey;
	public boolean hideOnDevice;
	public int pId;
	
	public GroupDetails(int id, String name, String ident, boolean ds, 
			boolean os, String gsi, int pId, boolean ro, boolean hod) {
		sId = id;
		surveyName = name;
		surveyIdent = ident;
		dataSurvey = ds;
		oversightSurvey = os;
		groupSurveyIdent = gsi;
		this.pId = pId;
		this.readOnlySurvey = ro;
		this.hideOnDevice = hod;
	}
}
