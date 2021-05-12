package org.smap.sdal.model;

public class SurveyIdent {
	public int id;
	public String project;
	public String name;
	public String ident;
	
	public SurveyIdent(int id, String p, String n, String i) {
		this.id = id;
		project = p;
		name = n;
		ident = i;
	}
}
