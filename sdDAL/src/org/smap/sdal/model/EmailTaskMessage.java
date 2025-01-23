package org.smap.sdal.model;

import java.util.Date;

public class EmailTaskMessage {
	public int sId;
	public int pId;
	public int aId;
	public String instanceId;
	public String from;
	public String subject;
	public String content;
	public String attach;
	public String email;
	public String target;
	public String user;
	public boolean temporaryUser;
	public String actionLink;
	public Date scheduledAt;
	public String tgName;

	
	public EmailTaskMessage(
			int sId, 
			int pId,
			int aId,
			String instanceId, 
			String from, 
			String subject, 
			String content, 
			String attach, 
			String email,
			String target,
			String user,
			boolean temporaryUser,
			String actionLink,
			Date scheduledAt,
			String tgName) {
		
		this.sId = sId;
		this.pId = pId;
		this.aId = aId;
		this.instanceId = instanceId;
		this.from = from;
		this.subject = subject;
		this.content = content;
		this.attach = attach;	
		this.email = email;
		this.target = target;
		this.user = user;
		this.temporaryUser = temporaryUser;
		this.actionLink = actionLink;
		this.scheduledAt = scheduledAt;
		this.tgName = tgName;
	}
	
}
