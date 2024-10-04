package org.smap.sdal.model;

import java.sql.Timestamp;

/*
 * Smap extension
 */
public class ConversationItemDetails {
	
	public static String EMAIL_CHANNEL = "email";
	public static String SMS_CHANNEL = "sms";
	public static String WHATSAPP_CHANNEL = "whatsapp";
	
	public String theirNumber;
	public String ourNumber;
	public String msg;
	public boolean inbound;		// Set true if we received the message
	public String channel;				// sms or whatsapp or email
	public Timestamp ts;
	
	public ConversationItemDetails(String theirNumber, String ourNumber, String msg, 
			boolean inbound,
			String channel,
			Timestamp ts) {
		this.theirNumber = theirNumber;
		this.ourNumber = ourNumber;
		this.msg = msg;
		this.inbound = inbound;	
		this.channel = channel;
		this.ts = ts;
	}
}
