package org.smap.sdal.model;


public class Mailout {
	public int id;
	public String survey_ident;
	public String name;
	
	public Mailout(int id, String surveyIdent, String name) {
		this.id = id;
		this.survey_ident = surveyIdent;
		this.name = name;
	}
}
