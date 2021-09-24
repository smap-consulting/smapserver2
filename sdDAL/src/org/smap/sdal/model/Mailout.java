package org.smap.sdal.model;


public class Mailout {
	public int id;
	public String survey_ident;
	public String name;
	public String subject;
	public String content;
	public boolean multiple_submit;		// The URL can be used for multiple submissions
	
	public MailoutLinks links;
	
	public Mailout(int id, String surveyIdent, String name, String subject, String content, boolean ms) {
		this.id = id;
		this.survey_ident = surveyIdent;
		this.name = name;
		this.subject = subject;
		this.content = content;
		this.multiple_submit = ms;
	}
}
