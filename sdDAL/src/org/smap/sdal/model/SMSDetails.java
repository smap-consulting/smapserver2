package org.smap.sdal.model;

import java.sql.Timestamp;

/*
 * Smap extension
 */
public class SMSDetails {
	
	public String theirNumber;
	public String ourNumber;
	public String msg;
	public boolean inbound;		// Set true if we received the message
	public Timestamp ts;
	
	public SMSDetails(String theirNumber, String ourNumber, String msg, 
			boolean inbound,
			Timestamp ts) {
		this.theirNumber = theirNumber;
		this.ourNumber = ourNumber;
		this.msg = msg;
		this.inbound = inbound;	
		this.ts = ts;
	}
}
