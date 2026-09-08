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
}
