package org.smap.sdal.model;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

public class MailoutMessage {
	public int mpId;
	public String survey_ident;
	public int pId;
	public String from;
	public String subject;
	public String content;
	public ArrayList<String> emails;
	public String target;
	public String user;
	public String scheme;
	public String server;
	public String basePath;
	
	public MailoutMessage(
			int mpId,
			String survey_ident,
			int pId, 
			String from, 
			String subject, 
			String content, 
			ArrayList<String> emails,
			String target,
			String user,
			String scheme,
			String server,
			String basePath) {
		
		this.mpId = mpId;
		this.survey_ident = survey_ident;
		this.pId = pId;
		this.from = from;
		this.subject = subject;
		this.content = content;
		this.emails = emails;
		this.target = target;
		this.user = user;
		this.scheme = scheme;
		this.server = server;
		this.basePath = basePath;
	}
	
	// copy constructor
	public MailoutMessage(MailoutMessage orig) {
		this.mpId = orig.mpId;
		this.survey_ident = orig.survey_ident;
		this.pId = orig.pId;
		this.from = orig.from;
		this.subject = orig.subject;
		this.content = orig.content;
		
		if(emails != null) {
			this.emails = new ArrayList<>(orig.emails);
		}
		
		this.target = orig.target;
		this.user = orig.user;
		this.scheme = orig.scheme;
		this.server = orig.server;
		this.basePath = orig.basePath;
	}
}
