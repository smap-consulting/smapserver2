package org.smap.sdal.model;

public class GroupDetails {
	public int sId;
	public String surveyName;
	public String groupSurveyIdent;
	public String surveyIdent;
	public boolean dataSurvey;
	public boolean oversightSurvey;
	
	public GroupDetails(int id, String name, String ident, boolean ds, 
			boolean os, String gsi) {
		sId = id;
		surveyName = name;
		surveyIdent = ident;
		dataSurvey = ds;
		oversightSurvey = os;
		groupSurveyIdent = gsi;
	}
}
