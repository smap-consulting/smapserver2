package org.smap.sdal.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;

import jakarta.activation.DataHandler;
import jakarta.activation.DataSource;
import jakarta.activation.FileDataSource;
import jakarta.mail.AuthenticationFailedException;
import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Multipart;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.Message.RecipientType;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

public class SmtpEmailServer extends EmailServer {

	/*
	 * Connection reuse.
	 *
	 * Transport.send() opens a socket, does the TLS handshake, authenticates, sends one
	 * message and disconnects.  Against a remote relay that is a second or more of round
	 * trips per email, which is most of the cost of a notification.  The batch subscribers
	 * send a continuous stream of them, so they keep one connection per thread and reuse it.
	 *
	 * This is opt-in.  A web request thread sends the odd email and then sits in the Tomcat
	 * pool, so caching a socket on it would just leak connections; the services leave reuse
	 * off and get a connection per send exactly as before.
	 */
	private static volatile boolean reuseConnections = false;

	// Relays drop idle connections and often cap the messages accepted on one of them
	private static final long MAX_IDLE_MS = 120000;
	private static final int MAX_SENDS_PER_CONNECTION = 100;

	public static void enableConnectionReuse() {
		reuseConnections = true;
	}

	private static class SmtpConnection {
		String key;
		Session session;
		Transport transport;
		long lastUsed;
		int sends;
	}

	private static final ThreadLocal<SmtpConnection> threadConnection = new ThreadLocal<>();

	/*
	 * Add an authenticator class
	 */
	private class Authenticator extends jakarta.mail.Authenticator {
		private PasswordAuthentication authentication;

		public Authenticator(String username, String password) {
			authentication = new PasswordAuthentication(username, password);
		}

		protected PasswordAuthentication getPasswordAuthentication() {
			return authentication;
		}
	}

	public SmtpEmailServer(ResourceBundle localisation) {
		super(localisation);
	}

	@Override
	public String send(String email, String ccType, String subject,
			String emailId,
			String contentString,
			ArrayList<String> filePaths,
			ArrayList<String> filenames,
			String replyTo) throws Exception {

		if(smtpHost == null) {
			throw new Exception("Cannot send email, smtp_host not available");
		}

		boolean retried = false;

		while(true) {
			SmtpConnection conn = null;
			boolean wasReused = false;
			try {
				conn = getConnection();
				wasReused = conn.sends > 0;

				Message msg = buildMessage(conn.session, email, ccType, subject, emailId,
						contentString, filePaths, filenames);

				// sendMessage does not do this for us, unlike the static Transport.send
				msg.saveChanges();
				conn.transport.sendMessage(msg, msg.getAllRecipients());

				conn.lastUsed = System.currentTimeMillis();
				conn.sends++;
				releaseConnection(conn);
				return null;

			} catch(AuthenticationFailedException ae) {
				discardConnection(conn);
				log.log(Level.SEVERE, "Messaging Exception", ae);
				throw new Exception(localisation.getString("email_cs") + ":  " + localisation.getString("ae"));
			} catch(MessagingException me) {
				int sends = (conn == null) ? 0 : conn.sends;
				discardConnection(conn);
				/*
				 * A socket error on a connection we had already used is almost always the relay
				 * having dropped it while it was idle.  Nothing was delivered, so open a fresh
				 * connection and try once more before reporting a failure.
				 */
				if(!retried && wasReused && isConnectionLost(me)) {
					retried = true;
					log.log(Level.INFO, "Reopening smtp connection after: " + me.getMessage());
					continue;
				}
				/*
				 * A broken pipe reads the same whether the relay dropped an idle connection,
				 * refused an oversized message, or the write timeout closed the socket on us,
				 * so say what was being sent and how the connection was being used
				 */
				log.log(Level.SEVERE, "Messaging Exception sending to " + smtpHost
						+ " (" + describeAttachments(filePaths)
						+ ", connection reused=" + wasReused + " sends=" + sends + ")", me);
				String msg = me.getMessage();
				throw new Exception(localisation.getString("email_cs") + ":  " + msg);
			} catch(Exception e) {
				discardConnection(conn);
				log.log(Level.SEVERE, "Exception", e);
				String msg = e.getMessage();
				throw new Exception(localisation.getString("email_cs") + ":  " + msg);
			}
		}
	}

	/*
	 * Build the mime message
	 */
	private Message buildMessage(Session session, String email, String ccType, String subject,
			String emailId,
			String contentString,
			ArrayList<String> filePaths,
			ArrayList<String> filenames) throws Exception {

		RecipientType rt = null;
		String sender = emailUser;

		Message msg = new MimeMessage(session);
		if(ccType.equals("bcc")) {
			rt = Message.RecipientType.BCC;
		} else {
			rt = Message.RecipientType.TO;
		}

		log.fine("Sending to email addresses: " + email);
		InternetAddress[] emailArray = InternetAddress.parse(email);
		log.fine("Number of email addresses: " + emailArray.length);
		msg.setRecipients(rt,	emailArray);
		msg.setSubject(subject + " " + emailId);	// Include email ID with subject to make it unique

		// Add the email server domain if not already set for sender
		if(sender.indexOf('@') < 0) {
			sender = sender + "@" + emailDomain;
		}

		log.fine("Sending email from: (sendEmailHtml1) " + sender + " with subject " + subject);
		msg.setFrom(InternetAddress.parse(sender, false)[0]);

		Multipart multipart = new MimeMultipart();

		// Add body part
		MimeBodyPart messageBodyPart = new MimeBodyPart();
		messageBodyPart.setText(contentString, "utf-8", "html");
		multipart.addBodyPart(messageBodyPart);

		// Add file attachments if they exist
		if(filePaths != null) {
			for(int i = 0; i < filePaths.size(); i++) {
				messageBodyPart = new MimeBodyPart();
				DataSource source = new FileDataSource(filePaths.get(i));
				messageBodyPart.setDataHandler(new DataHandler(source));
				messageBodyPart.setFileName(filenames.get(i));
				multipart.addBodyPart(messageBodyPart);
			}
		}

		msg.setContent(multipart);

		msg.setHeader("X-Mailer", "msgsend");
		log.fine("Sending email from: (sendEmailHtml2) " + sender);

		return msg;
	}

	/*
	 * Get a connected transport, reusing the one held by this thread if it is still good
	 */
	private SmtpConnection getConnection() throws MessagingException {

		String key = smtpHost + ":" + getPort() + ":" + (emailUser == null ? "" : emailUser);

		if(reuseConnections) {
			SmtpConnection existing = threadConnection.get();
			if(existing != null) {
				boolean usable = existing.key.equals(key)
						&& existing.transport.isConnected()
						&& existing.sends < MAX_SENDS_PER_CONNECTION
						&& (System.currentTimeMillis() - existing.lastUsed) < MAX_IDLE_MS;
				if(usable) {
					return existing;
				}
				threadConnection.remove();
				closeQuietly(existing.transport);
			}
		}

		SmtpConnection conn = new SmtpConnection();
		conn.key = key;
		conn.session = getEmailSession();
		conn.transport = conn.session.getTransport("smtp");
		if(hasAuthentication()) {
			conn.transport.connect(smtpHost, getPort(), emailUser + "@" + emailDomain, emailPassword);
		} else {
			conn.transport.connect(smtpHost, getPort(), null, null);
		}
		conn.lastUsed = System.currentTimeMillis();
		return conn;
	}

	/*
	 * Hold the connection open for the next message, or close it if reuse is off
	 */
	private void releaseConnection(SmtpConnection conn) {
		if(reuseConnections) {
			threadConnection.set(conn);
		} else {
			closeQuietly(conn.transport);
		}
	}

	private void discardConnection(SmtpConnection conn) {
		if(conn != null) {
			if(threadConnection.get() == conn) {
				threadConnection.remove();
			}
			closeQuietly(conn.transport);
		}
	}

	/*
	 * Release the connection held by the calling thread.  Called by the batch processors
	 * when their loop ends.
	 */
	public static void closeThreadConnection() {
		SmtpConnection conn = threadConnection.get();
		if(conn != null) {
			threadConnection.remove();
			closeQuietly(conn.transport);
		}
	}

	private static void closeQuietly(Transport transport) {
		if(transport != null) {
			try {
				transport.close();
			} catch (Exception e) {
				// Nothing useful to do, the connection is being thrown away
			}
		}
	}

	/*
	 * Attachment count and size, for the failure log.  A relay that closes the connection
	 * part way through a large attachment looks identical to one that dropped an idle
	 * connection until you know how much was being pushed at it.
	 */
	private String describeAttachments(ArrayList<String> filePaths) {
		if(filePaths == null || filePaths.size() == 0) {
			return "no attachments";
		}
		long bytes = 0;
		for(String path : filePaths) {
			try {
				bytes += new java.io.File(path).length();
			} catch (Exception e) {
				// The size is only for the log, an unreadable path is not worth reporting here
			}
		}
		return filePaths.size() + " attachment(s), " + (bytes / 1024) + "kb";
	}

	/*
	 * True if the failure looks like the socket having gone away rather than the relay
	 * rejecting the message
	 */
	private boolean isConnectionLost(MessagingException me) {
		Throwable t = me;
		while(t != null) {
			if(t instanceof IOException) {
				return true;
			}
			if(t instanceof MessagingException) {
				Exception next = ((MessagingException) t).getNextException();
				if(next != null && next != t) {
					t = next;
					continue;
				}
			}
			t = t.getCause();
		}
		return false;
	}

	private boolean hasAuthentication() {
		return emailUser != null && emailPassword != null
				&& emailUser.trim().length() > 0
				&& emailPassword.trim().length() > 0;
	}

	private int getPort() {
		if(emailPort > 0) {
			return emailPort;
		}
		return hasAuthentication() ? 587 : 25;
	}

	/*
	 * Get an email session for SMTP
	 */
	private Session getEmailSession() {

		/*
		 * Use a fresh Properties.  This used to write into System.getProperties(), which
		 * meant every send mutated jvm wide state that the other sending threads were
		 * reading, and settings from one organisation's server leaked into the next one's.
		 */
		Properties props = new Properties();
		props.setProperty("mail.transport.protocol", "smtp");
		props.setProperty("mail.smtp.host", smtpHost);
		props.setProperty("mail.smtp.port", String.valueOf(getPort()));

		Authenticator authenticator = null;

		// Create an authenticator if the user name and password is available
		if(hasAuthentication()) {
			String authUser = emailUser + "@" + emailDomain;
			authenticator = new Authenticator(authUser, emailPassword);
			props.setProperty("mail.smtp.submitter", authenticator.getPasswordAuthentication().getUserName());
			props.setProperty("mail.smtp.auth", "true");
			props.setProperty("mail.smtp.ssl.trust", smtpHost);
			props.setProperty("mail.smtp.starttls.enable", "true");
			props.setProperty("mail.smtp.ssl.protocols", "TLSv1.2");

			log.fine("Trying to send email as html with authentication");
		} else {
			log.fine("No authentication");
		}

		/*
		 * Timeouts.  Only the connect timeout is short.  That is the one that parked a worker
		 * for a minute on a relay that had stopped answering, and connecting either happens
		 * quickly or is not going to happen at all.
		 *
		 * Read and write stay at 60 seconds.  They bound a transfer that is already in
		 * progress, which is the slow part when a large pdf is attached, and angus implements
		 * the write timeout by closing the socket underneath us: trip it and the send fails
		 * with a broken pipe part way through the attachment.  Cutting these was a bad trade,
		 * a relay slow to drain a big attachment is not the failure being guarded against.
		 */
		props.setProperty("mail.smtp.connectiontimeout", "10000");
		props.setProperty("mail.smtp.timeout", "60000");
		props.setProperty("mail.smtp.writetimeout", "60000");

		//log.fine("Email properties: " + props.toString());

		return Session.getInstance(props, authenticator);
	}
}
