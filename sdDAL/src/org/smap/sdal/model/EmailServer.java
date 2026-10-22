package org.smap.sdal.model;

import java.util.ResourceBundle;
import java.util.logging.Logger;

/*
 * Form Class
 * Used for storing details of an email server
 */
abstract public class EmailServer {
	protected static Logger log = Logger.getLogger(EmailServer.class.getName());
	
	protected ResourceBundle localisation;
	
	public String smtpHost;
	public String emailDomain;
	public String emailUser;
	public String emailPassword;
	public int emailPort;
	
	EmailServer(ResourceBundle localisation) {
		this.localisation = localisation;
	}
	
	/*
	 * True when this server's account is being rate limited by the relay and there is no
	 * point building a message for it yet.  Only smtp tracks this.
	 */
	public boolean isSendingPaused() {
		return false;
	}

	/*
	 * Why this server's account is paused, for a message that has to be told apart from one
	 * that failed.  Null when it is not paused, and always null where pausing is not tracked.
	 */
	public String getPauseReason() {
		return null;
	}

	/*
	 * When this server's account will take mail again, as epoch milliseconds, or 0 when it is
	 * not paused or pausing is not tracked
	 */
	public long getPauseUntil() {
		return 0;
	}

	// Returns SES MessageId for AWS sends; null for SMTP
	// unsubscribeUrl: one click unsubscribe for the List-Unsubscribe header, null if none
	public abstract String send(String email, String ccType, String subject,
			String emailId,
			String contentString,
			java.util.ArrayList<String> filePaths,
			java.util.ArrayList<String> filenames,
			String replyTo,
			String unsubscribeUrl) throws Exception;

	/*
	 * Headers that let a mail client offer its own unsubscribe button, which unsubscribes with
	 * a single POST to the url (RFC 2369 and RFC 8058).  Gmail and Yahoo require them of bulk
	 * senders, and without one a recipient who wants no more email tends to report it as spam.
	 */
	public static void addUnsubscribeHeaders(jakarta.mail.Message msg, String unsubscribeUrl) throws jakarta.mail.MessagingException {
		if(unsubscribeUrl != null) {
			msg.setHeader("List-Unsubscribe", "<" + unsubscribeUrl + ">");
			msg.setHeader("List-Unsubscribe-Post", "List-Unsubscribe=One-Click");
		}
	}
}
