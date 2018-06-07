package org.smap.sdal.model;

public class EmailTaskMessage {
	public int sId;
	public int pId;
	public String instanceId;
	public String from;
	public String subject;
	public String content;
	public String attach;
	public String email;
	public String target;
	public String user;
	public String scheme;
	public String server;
	public String basePath;
	public String serverRoot;
	public String tempUserId;

	
	public EmailTaskMessage(
			int sId, 
			int pId, 
			String instanceId, 
			String from, 
			String subject, 
			String content, 
			String attach, 
			String email,
			String target,
			String user,
			String scheme,
			String server,
			String basePath,
			String serverRoot,
			String tempUserId) {
		
		this.sId = sId;
		this.pId = pId;
		this.instanceId = instanceId;
		this.from = from;
		this.subject = subject;
		this.content = content;
		this.attach = attach;	
		this.email = email;
		this.target = target;
		this.user = user;
		this.scheme = scheme;
		this.server = server;
		this.basePath = basePath;
		this.serverRoot = serverRoot;
		this.tempUserId = tempUserId;
	}
}
