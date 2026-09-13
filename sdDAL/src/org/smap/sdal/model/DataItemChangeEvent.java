package org.smap.sdal.model;

import java.util.ArrayList;

/*
 * Contains details of a change item
 */
public class DataItemChangeEvent {
	
	public String event;
	public String status;
	public String userName;
	public String surveyName;
	public int surveyVersion;
	public String eventTime;
	public String tz;
	public ArrayList<DataItemChange> changes;
	public TaskItemChange task;
	public ConversationItemDetails message;
	public SubmissionMessage notification;
	public EmailReplyData emailReply;
	public String description;

	/*
	 * The application that made the change, when it was not a person working directly. userName is
	 * the person either way, and for an agent that is who approved it.
	 */
	public String agent;

	/*
	 * The application's registered identifier, which agent is the readable name of. Kept beside it
	 * because a client can be renamed or removed and the trail still has to say what acted.
	 */
	public String agentId;

	/*
	 * Set when this change was one of many made together, and the same on all of them, so the whole
	 * can be undone as the single action it was.
	 */
	public String changeSet;
}
