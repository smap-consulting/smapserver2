package org.smap.sdal.model;

/*
 * Smap extension
 */
public class SMSNumber {
	
	public String identifier;
	public String ourNumber;
	public String surveyIdent;
	public String theirNumberQuestion;
	public String messageQuestion;
	
	public SMSNumber(String identifier, String ourNumber, String surveyIdent, 
			String theirNumberQuestion, String messageQuestion) {
		this.identifier = identifier;
		this.ourNumber = ourNumber;
		this.surveyIdent = surveyIdent;
		this.theirNumberQuestion = theirNumberQuestion;	
		this.messageQuestion = messageQuestion;	
	}
}
