package org.smap.sdal.model;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

public class SubmissionMessage {
	public int sId;					// Legacy reference to survey - now use survey_ident
	public int taskId;
	public String survey_ident;
	public String update_ident;
	public int pId;
	public String instanceId;
	public String from;
	public String subject;
	public String content;
	public String attach;
	public boolean include_references;	// Follow links to referenced surveys
	public boolean launchedOnly;
	private int emailQuestion;			// Legacy question identifier
	private String emailQuestionName;	// New question identifier
	public String emailMeta;
	public ArrayList<String> emails;
	public String target;
	public String user;
	public String scheme;
	public String server;
	public String basePath;
	public String callback_url;
	public String remoteUser;
	public String remotePassword;
	public int pdfTemplateId;
	
	public SubmissionMessage(
			int taskId,
			String survey_ident,
			String update_ident,
			int pId, 
			String instanceId, 
			String from, 
			String subject, 
			String content, 
			String attach, 
			boolean include_references,
			boolean launchedOnly,
			int emailQuestion,
			String emailQuestionName,
			String emailMeta,
			ArrayList<String> emails,
			String target,
			String user,
			String scheme,
			String server,
			String basePath,
			String callout_url,
			String remoteUser,
			String remoTePassword,
			int pdfTemplateId) {
		
		this.taskId = taskId;
		this.survey_ident = survey_ident;
		this.update_ident = update_ident;
		this.pId = pId;
		this.instanceId = instanceId;
		this.from = from;
		this.subject = subject;
		this.content = content;
		this.attach = attach;
		this.include_references = include_references;
		this.launchedOnly = launchedOnly;
		this.emailQuestion = emailQuestion;
		this.emailQuestionName = emailQuestionName;
		this.emailMeta = emailMeta;
		this.emails = emails;
		this.target = target;
		this.user = user;
		this.scheme = scheme;
		this.server = server;
		this.basePath = basePath;
		this.callback_url = callout_url;
		this.remoteUser = remoteUser;
		this.remotePassword = remoTePassword;
		this.pdfTemplateId = pdfTemplateId;
	}
	
	// copy constructor
	public SubmissionMessage(SubmissionMessage orig) {
		this.sId = orig.sId;
		this.taskId = orig.taskId;
		this.survey_ident = orig.survey_ident;
		this.update_ident = orig.update_ident;
		this.pId = orig.pId;
		this.instanceId = orig.instanceId;
		this.from = orig.from;
		this.subject = orig.subject;
		this.content = orig.content;
		this.attach = orig.attach;
		this.include_references = orig.include_references;
		this.launchedOnly = orig.launchedOnly;
		this.emailQuestion = orig.emailQuestion;
		this.emailQuestionName = orig.emailQuestionName;
		this.emailMeta = orig.emailMeta;
		
		if(emails != null) {
			this.emails = new ArrayList<>(orig.emails);
		}
		
		this.target = orig.target;
		this.user = orig.user;
		this.scheme = orig.scheme;
		this.server = orig.server;
		this.basePath = orig.basePath;
		this.callback_url = orig.callback_url;
		this.remoteUser = orig.remoteUser;
		this.remotePassword = orig.remotePassword;
		this.pdfTemplateId = orig.pdfTemplateId;
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
	
	public void clearEmailQuestions() {
		emailQuestionName = null;
		emailQuestion = 0;
		emailMeta = null;
	}
}
