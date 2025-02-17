package org.smap.sdal.model;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

public class SubmissionMessage {
	public String notificationName;
	public String title;
	public int taskId;
	public String survey_ident;
	public String survey_case;
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
	public String assignQuestion;
	public String emailMeta;
	public boolean emailAssigned;
	public String ourNumber;			// SMS / WhatsApp messaging
	public String msgChannel;			// sms or whatsapp
	public ArrayList<String> emails;
	public String target;
	public String user;
	public String scheme;
	public String callback_url;
	public String remoteUser;
	public String remotePassword;
	public int pdfTemplateId;
	public String period;
	public int reportId;
	public Timestamp ts;
	
	public SubmissionMessage(
			String notificationName,
			String title,
			int taskId,
			int pId,
			String survey_ident,
			String update_ident,
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
			boolean emailAssigned,
			ArrayList<String> emails,
			String target,
			String user,
			String scheme,
			String callout_url,
			String remoteUser,
			String remotePassword,
			int pdfTemplateId,
			String survey_case,
			String assignQuestion,
			String period,
			int reportId,
			String ourNumber,
			String msgChannel,
			Timestamp ts) {
		
		this.notificationName = notificationName;
		this.title = title;
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
		this.emailAssigned = emailAssigned;
		this.emails = emails;
		this.target = target;
		this.user = user;
		this.scheme = scheme;
		this.callback_url = callout_url;
		this.remoteUser = remoteUser;
		this.remotePassword = remotePassword;
		this.pdfTemplateId = pdfTemplateId;
		this.survey_case = survey_case;
		this.assignQuestion = assignQuestion;
		this.period = period;
		this.reportId = reportId;
		this.ourNumber = ourNumber;
		this.msgChannel = msgChannel;
		this.ts = ts;
	}
	
	// copy constructor
	public SubmissionMessage(SubmissionMessage orig) {
		this.notificationName = orig.notificationName;
		this.title = orig.title;
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
		this.emailAssigned = orig.emailAssigned;
		
		if(emails != null) {
			this.emails = new ArrayList<>(orig.emails);
		}
		
		this.target = orig.target;
		this.user = orig.user;
		this.scheme = orig.scheme;
		this.callback_url = orig.callback_url;
		this.remoteUser = orig.remoteUser;
		this.remotePassword = orig.remotePassword;
		this.pdfTemplateId = orig.pdfTemplateId;
		this.survey_case = orig.survey_case;
		this.assignQuestion = orig.assignQuestion;
		this.period = orig.period;
		this.reportId = orig.reportId;
		this.ourNumber = orig.ourNumber;
		this.msgChannel = orig.msgChannel;
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
