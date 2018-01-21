package org.smap.sdal.model;

import java.util.ArrayList;

public class SubmissionMessage {
	public int sId;
	public int pId;
	public String instanceId;
	public String from;
	public String subject;
	public String content;
	public String attach;
	public int emailQuestion;
	public ArrayList<String> emails;
	public String target;
	
	public SubmissionMessage(int sId, int pId, String instanceId, String from, String subject, String content, String attach, 
			int emailQuestion,
			ArrayList<String> emails,
			String target) {
		
		this.sId = sId;
		this.pId = pId;
		this.instanceId = instanceId;
		this.from = from;
		this.subject = subject;
		this.content = content;
		this.attach = attach;
		this.emailQuestion = emailQuestion;
		this.emails = emails;
		this.target = target;
	}
}
