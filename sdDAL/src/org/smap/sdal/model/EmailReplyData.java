package org.smap.sdal.model;

import java.util.ArrayList;

public class EmailReplyData {
	public String fromAddress;
	public String subject;
	public String body;
	public String sentAt;          // ISO 8601
	public String replyMessageId;  // Message-ID of this reply (idempotency key)
	public String sesInReplyTo;    // In-Reply-To header (original email's Message-ID, for reference)
	public int    messageId;       // notification_log message_id reference
	public ArrayList<String> attachments; // URL fragments: attachments/{surveyIdent}/{uuid}.{ext}
}
