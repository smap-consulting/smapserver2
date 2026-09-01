package org.smap.sdal.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
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
import org.eclipse.angus.mail.smtp.SMTPSendFailedException;
import org.smap.sdal.Utilities.EmailRateLimitException;

public class SmtpEmailServer extends EmailServer {

	/*
	 * Connection reuse.
	 *
	 * Transport.send() opens a socket, does the TLS handshake, authenticates, sends one
	 * message and disconnects.  Against a remote relay that is a second or more of round
	 * trips per email, which is most of the cost of a notification.  The batch subscribers
	 * send a continuous stream of them, so they hold connections open and reuse them.
	 *
	 * The connections live in a small fixed pool rather than one per sending thread.
	 * Relays cap how many connections a mailbox may hold at once - office 365 allows three -
	 * and a per thread connection quietly opens one for every worker, whether it is sending
	 * or not.  A pool of n slots holds at most n sockets no matter how many workers draw on
	 * it, and a worker waiting for a slot is not the bottleneck: the survey load and the pdf
	 * render are the expensive part of a notification and they still run in parallel.
	 *
	 * This is opt-in.  A web request thread sends the odd email and then sits in the Tomcat
	 * pool, so holding a socket for it would just leak connections; the services leave reuse
	 * off and get a connection per send exactly as before.
	 */
	private static volatile boolean reuseConnections = false;

	// Relays drop idle connections and often cap the messages accepted on one of them
	private static final long MAX_IDLE_MS = 120000;
	private static final int MAX_SENDS_PER_CONNECTION = 100;

	// Long enough that only a leaked slot trips it, short enough to be visible in the log
	private static final long SLOT_WAIT_MS = 120000;

	/*
	 * Rate limiting.
	 *
	 * A relay that has had enough says so and stops accepting mail - gmail answers
	 * "550 5.4.5 Daily user sending limit exceeded" and holds that until its rolling
	 * twenty four hour window moves on.  Carrying on regardless costs a survey load and a
	 * pdf render per message for a send that cannot succeed, and the message is marked
	 * processed and lost.  So note the refusal, stop sending for a while, and let the
	 * caller put the message back on the queue for when sending resumes.
	 *
	 * The pause is a retry interval rather than a wait until midnight: nothing here knows
	 * when the relay's window resets, so it tries again periodically until one gets
	 * through.  Left alone overnight that is what "stopped and restarted the next day"
	 * looks like, without having to guess the reset time.
	 */
	private static volatile long sendingPausedUntil = 0;
	private static volatile String sendingPausedReason = null;
	private static volatile int pauseMinutes = 60;

	public static void setRateLimitPauseMinutes(int minutes) {
		if(minutes > 0) {
			pauseMinutes = minutes;
		}
	}

	public static boolean isSendingPaused() {
		return System.currentTimeMillis() < sendingPausedUntil;
	}

	public static String getSendingPausedReason() {
		return isSendingPaused() ? sendingPausedReason : null;
	}

	/*
	 * When sending resumes, as epoch millis, or 0 if it is not paused.  The batch workers
	 * publish this on their heartbeat so the monitor page can say why a queue that is
	 * filling up is not being drained.
	 */
	public static long getSendingPausedUntil() {
		return isSendingPaused() ? sendingPausedUntil : 0;
	}

	private static synchronized void pauseSending(String reason) {
		long until = System.currentTimeMillis() + (pauseMinutes * 60000L);
		if(until > sendingPausedUntil) {
			sendingPausedUntil = until;
			sendingPausedReason = tidyReason(reason);
			log.log(Level.WARNING, "Relay is rate limiting, pausing email for " + pauseMinutes
					+ " minutes: " + sendingPausedReason);
		}
	}

	/*
	 * An smtp refusal arrives as several wrapped lines.  Flatten it so it reads as one
	 * sentence in the monitor, and cap it so a talkative relay cannot bloat the worker row.
	 */
	private static String tidyReason(String reason) {
		if(reason == null) {
			return null;
		}
		String tidy = reason.trim().replaceAll("\\s+", " ");
		if(tidy.length() > 200) {
			tidy = tidy.substring(0, 200) + "...";
		}
		return tidy;
	}

	/*
	 * True if the relay is refusing because we have sent too much, rather than because
	 * there is something wrong with this particular message.  The codes are a mixture:
	 * gmail's daily limit arrives as a permanent 550 even though it clears by itself, so
	 * the text matters as much as the number.  Deliberately not treating a concurrent
	 * connection refusal as rate limiting - that one clears in seconds and the connection
	 * pool already keeps us under the limit.
	 */
	private boolean isRateLimited(MessagingException me) {
		String text = me.getMessage();
		if(text != null) {
			String t = text.toLowerCase();
			if(t.contains("concurrent connections")) {
				return false;
			}
			if(t.contains("sending limit") || t.contains("daily limit") || t.contains("rate limit")
					|| t.contains("quota exceeded") || t.contains("too many messages")
					|| t.contains("try again later") || t.contains("message rate")) {
				return true;
			}
		}
		if(me instanceof SMTPSendFailedException) {
			int rc = ((SMTPSendFailedException) me).getReturnCode();
			// Service unavailable, mailbox busy, local error, insufficient storage
			return rc == 421 || rc == 450 || rc == 451 || rc == 452;
		}
		return false;
	}

	private static class SmtpConnection {
		String key;
		Session session;
		Transport transport;		// null when the slot holds no open connection
		long lastUsed;
		int sends;
		boolean pooled;				// must be handed back to the pool when done with
	}

	private static volatile BlockingQueue<SmtpConnection> pool = null;
	private static int poolSize = 0;

	/*
	 * Hold up to maxConnections open at once, shared by every sending thread in this jvm.
	 * Size it below whatever the relay allows, remembering that the forward and the upload
	 * subscriber are separate processes and so get a pool each.
	 */
	public static synchronized void enableConnectionReuse(int maxConnections) {
		if(pool != null) {
			return;				// Already set up
		}
		poolSize = maxConnections > 0 ? maxConnections : 1;
		pool = new ArrayBlockingQueue<>(poolSize);
		for(int i = 0; i < poolSize; i++) {
			pool.add(new SmtpConnection());
		}
		reuseConnections = true;
		log.info("Smtp connection reuse enabled, at most " + poolSize + " connection(s) in this process");
	}

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

				if(isRateLimited(me)) {
					pauseSending(me.getMessage());
					throw new EmailRateLimitException(me.getMessage());
				}
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
	 * Take a slot from the pool and make sure it holds a connection we can send on.  Waiting
	 * here is what keeps the number of open connections within what the relay allows.
	 */
	private SmtpConnection getConnection() throws Exception {

		String key = smtpHost + ":" + getPort() + ":" + (emailUser == null ? "" : emailUser);

		SmtpConnection conn;
		if(reuseConnections) {
			conn = pool.poll(SLOT_WAIT_MS, TimeUnit.MILLISECONDS);
			if(conn == null) {
				throw new Exception("Timed out waiting for one of the " + poolSize
						+ " smtp connection slot(s)");
			}
			conn.pooled = true;
		} else {
			conn = new SmtpConnection();
			conn.pooled = false;
		}

		try {
			if(conn.transport != null) {
				boolean usable = key.equals(conn.key)
						&& conn.transport.isConnected()
						&& conn.sends < MAX_SENDS_PER_CONNECTION
						&& (System.currentTimeMillis() - conn.lastUsed) < MAX_IDLE_MS;
				if(!usable) {
					closeQuietly(conn.transport);
					clear(conn);
				}
			}

			if(conn.transport == null) {
				conn.key = key;
				conn.session = getEmailSession();
				conn.transport = conn.session.getTransport("smtp");
				if(hasAuthentication()) {
					conn.transport.connect(smtpHost, getPort(), emailUser + "@" + emailDomain, emailPassword);
				} else {
					conn.transport.connect(smtpHost, getPort(), null, null);
				}
				conn.sends = 0;
			}
			conn.lastUsed = System.currentTimeMillis();
			return conn;

		} catch (Exception e) {
			discardConnection(conn);		// Never hold on to the slot on the way out
			throw e;
		}
	}

	/*
	 * Hand the slot back with its connection still open, or close it if reuse is off
	 */
	private void releaseConnection(SmtpConnection conn) {
		if(conn == null) {
			return;
		}
		if(conn.pooled) {
			pool.offer(conn);
		} else {
			closeQuietly(conn.transport);
			clear(conn);
		}
	}

	/*
	 * Close the connection and hand the empty slot back, so a failed send does not cost the
	 * pool a slot for the life of the process
	 */
	private void discardConnection(SmtpConnection conn) {
		if(conn == null) {
			return;
		}
		closeQuietly(conn.transport);
		clear(conn);
		if(conn.pooled) {
			pool.offer(conn);
		}
	}

	private static void clear(SmtpConnection conn) {
		conn.transport = null;
		conn.session = null;
		conn.key = null;
		conn.sends = 0;
	}

	/*
	 * Close whatever connections are sitting idle in the pool, leaving the slots in place.
	 * Called by the batch processors when their loop ends.
	 */
	public static void closeIdleConnections() {
		if(pool == null) {
			return;
		}
		for(int i = 0; i < poolSize; i++) {
			SmtpConnection conn = pool.poll();
			if(conn == null) {
				break;			// The rest are in use by another thread
			}
			closeQuietly(conn.transport);
			clear(conn);
			pool.offer(conn);
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
