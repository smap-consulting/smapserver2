package org.smap.sdal.model;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

public class PeriodicMessage {
	public int pId;
	public String from;
	public String subject;
	public String content;
	public String attach;
	public ArrayList<String> emails;
	public String target;
	public String user;
	public int reportId;
	
	public PeriodicMessage(
			int pId, 
			String from, 
			String subject, 
			String content, 
			String attach, 
			ArrayList<String> emails,
			String target,
			String user,
			int reportId) {
		
		this.pId = pId;
		this.from = from;
		this.subject = subject;
		this.content = content;
		this.attach = attach;
		this.emails = emails;
		this.target = target;
		this.user = user;
		this.reportId = reportId;
	}
	
	// copy constructor
	public PeriodicMessage(PeriodicMessage orig) {
		this.pId = orig.pId;
		this.from = orig.from;
		this.subject = orig.subject;
		this.content = orig.content;
		this.attach = orig.attach;
		
		if(emails != null) {
			this.emails = new ArrayList<>(orig.emails);
		}
		
		this.target = orig.target;
		this.user = orig.user;
		this.reportId = orig.reportId;
	}

}
