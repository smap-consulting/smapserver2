package org.smap.sdal.model;

import java.util.ArrayList;

public class SubmissionMessage {
	public int sId;
	public String ident;
	public int pId;
	public String instanceId;
	public String from;
	public String subject;
	public String content;
	public String attach;
	public int emailQuestion;
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
		this.emails = emails;
		this.target = target;
		this.user = user;
		this.scheme = scheme;
		this.server = server;
		this.basePath = basePath;
		this.serverRoot = serverRoot;
	}
}
