package org.smap.sdal.model;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

public class SubmissionMessage {
	public int sId;
	public String ident;
	public int pId;
	public String instanceId;
	public String from;
	public String subject;
	public String content;
	public String attach;
	private int emailQuestion;			// Legacy question identifier
	private String emailQuestionName;	// New question identifier
	public String emailMeta;
	public ArrayList<String> emails;
	public String target;
	public String user;
	public String scheme;
	public String server;
	public String basePath;
	public String serverRoot;
	
	public SubmissionMessage(
			int sId, 
			String ident,
			int pId, 
			String instanceId, 
			String from, 
			String subject, 
			String content, 
			String attach, 
			int emailQuestion,
			String emailQuestionName,
			String emailMeta,
			ArrayList<String> emails,
			String target,
			String user,
			String scheme,
			String server,
			String basePath,
			String serverRoot) {
		
		this.sId = sId;
		this.ident = ident;
		this.pId = pId;
		this.instanceId = instanceId;
		this.from = from;
		this.subject = subject;
		this.content = content;
		this.attach = attach;
		this.emailQuestion = emailQuestion;
		this.emailQuestionName = emailQuestionName;
		this.emailMeta = emailMeta;
		this.emails = emails;
		this.target = target;
		this.user = user;
		this.scheme = scheme;
		this.server = server;
		this.basePath = basePath;
		this.serverRoot = serverRoot;
	}
	
	public boolean emailQuestionSet() {
		boolean set = false;
		if(emailQuestionName != null) {
			if(!emailQuestionName.equals("-1")) {
				set = true;
			}
		} else if(emailQuestion > 0) {
			set = true;
		}
		return set;
	}
	
	public String getEmailQuestionName(Connection sd) throws SQLException {
		String name = null;;
		
		if(emailQuestionName != null) {
			name = emailQuestionName;
		} else if(emailQuestion > 0) {
			name = GeneralUtilityMethods.getNameForQuestion(sd, emailQuestion);
		}
		
		return name;
	}
}
