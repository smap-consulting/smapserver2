package org.smap.sdal.model;

public class CaseManagementAlert {
	public int id;
	public String group_survey_ident;
	public String name;
	public String period;
	public String filter;
	
	public CaseManagementAlert(int id, String group_survey_ident, String name, String period, String filter) {
		this.id = id;
		this.group_survey_ident = group_survey_ident;
		this.name = name;
		this.period = period;
		this.filter = filter;
	}
}
