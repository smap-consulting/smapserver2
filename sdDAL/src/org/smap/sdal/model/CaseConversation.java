package org.smap.sdal.model;

/*
 * The conversation a case holds with the number that established it
 * Replies go only to that number and from the number linked to the case's survey
 */
public class CaseConversation {
	public String theirNumber;
	public String ourNumber;
	public String channel;			// sms or whatsapp

	public CaseConversation(String theirNumber, String ourNumber, String channel) {
		this.theirNumber = theirNumber;
		this.ourNumber = ourNumber;
		this.channel = channel;
	}
}
