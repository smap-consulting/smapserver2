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
	public String channel;
	
	public int oId;
	public int pId;
	public int sId;
	
	public String orgName;
	public String surveyName;
	
	public SMSNumber(String identifier, String ourNumber, String surveyIdent, 
			String theirNumberQuestion, String messageQuestion, int oId,
			String channel) {
		this.identifier = identifier;
		this.ourNumber = ourNumber;
		this.surveyIdent = surveyIdent;
		this.theirNumberQuestion = theirNumberQuestion;	
		this.messageQuestion = messageQuestion;	
		this.oId = oId;
		this.channel = channel;
	}
}
