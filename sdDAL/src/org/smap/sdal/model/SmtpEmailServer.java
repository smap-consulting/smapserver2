package org.smap.sdal.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
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
	 * Everything here is per relay account, not per process.  Organisations configure their
	 * own smtp server, so one subscriber talks to gmail for several of them, to office 365
	 * for others and to turbo-smtp for the rest, and both the connection limit and the
	 * sending allowance belong to the account rather than to us: office 365 allows three
	 * connections per mailbox, gmail counts a daily quota per account.  A pool per account
	 * keeps each within its own limit while letting organisations on different relays send
	 * at the same time, and one account being throttled leaves the others alone.
	 *
	 * The connections live in a small fixed pool per account rather than one per sending
	 * thread.  A per thread connection quietly opens one for every worker whether it is
	 * sending or not; a pool of n slots holds at most n sockets no matter how many workers
	 * draw on it, and a worker waiting for a slot is not the bottleneck since the survey
	 * load and the pdf render still run in parallel.
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
	 * processed and lost.  So note the refusal, stop sending on that account for a while,
	 * and let the caller put the message back on the queue for when sending resumes.
	 *
	 * Held per account: one organisation exhausting its gmail quota must not stop the
	 * organisations sending through office 365, turbo-smtp, or a different gmail account.
	 *
	 * The pause is a retry interval rather than a wait until midnight: nothing here knows
	 * when the relay's window resets, so it tries again periodically until one gets
	 * through.  Left alone overnight that is what "stopped and restarted the next day"
	 * looks like, without having to guess the reset time.
	 */
	private static class Pause {
		volatile long until;
		volatile String reason;
	}

	/*
	 * One slot in a relay account's pool.  It holds an open connection between sends, or
	 * nothing when it has yet to be used or its connection has been closed.
	 */
	private static class SmtpConnection {
		String key;
		Session session;
		Transport transport;		// null when the slot holds no open connection
		long lastUsed;
		int sends;
		BlockingQueue<SmtpConnection> pool;		// The pool to hand the slot back to, null if unpooled
	}

	private static final ConcurrentHashMap<String, Pause> pauses = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, BlockingQueue<SmtpConnection>> pools =
			new ConcurrentHashMap<>();

	private static volatile int poolSize = 1;
	private static volatile int pauseMinutes = 60;

	public static void setRateLimitPauseMinutes(int minutes) {
		if(minutes > 0) {
			pauseMinutes = minutes;
		}
	}

	/*
	 * True if this server's account is being rate limited.  Overrides EmailServer so a
	 * caller can ask before doing the work of building a message it cannot send.
	 */
	@Override
	public boolean isSendingPaused() {
		return isAccountPaused(relayKey());
	}

	private static boolean isAccountPaused(String key) {
		Pause p = pauses.get(key);
		return p != null && System.currentTimeMillis() < p.until;
	}

	/*
	 * True if any account at all is paused.  Cheap, so a caller can check this before
	 * looking up which relay an organisation actually uses.
	 */
	public static boolean anySendingPaused() {
		long now = System.currentTimeMillis();
		for(Pause p : pauses.values()) {
			if(now < p.until) {
				return true;
			}
		}
		return false;
	}

	/*
	 * The account with furthest to wait, for the monitor, which shows one state for the
	 * whole message queue.  0 and null when nothing is paused.
	 */
	public static long getSendingPausedUntil() {
		long now = System.currentTimeMillis();
		long furthest = 0;
		for(Pause p : pauses.values()) {
			if(p.until > now && p.until > furthest) {
				furthest = p.until;
			}
		}
		return furthest;
	}

	public static String getSendingPausedReason() {
		long now = System.currentTimeMillis();
		long furthest = 0;
		String reason = null;
		for(Pause p : pauses.values()) {
			if(p.until > now && p.until > furthest) {
				furthest = p.until;
				reason = p.reason;
			}
		}
		return reason;
	}

	private void pauseSending(String reason) {
		String key = relayKey();
		Pause p = pauses.computeIfAbsent(key, k -> new Pause());
		long until = System.currentTimeMillis() + (pauseMinutes * 60000L);
		synchronized(p) {
			if(until > p.until) {
				p.until = until;
				p.reason = tidyReason(smtpHost + ": " + reason);
				log.log(Level.WARNING, "Relay " + key + " is rate limiting, pausing email to it for "
						+ pauseMinutes + " minutes: " + p.reason);
			}
		}
	}

	public static synchronized void enableConnectionReuse(int maxConnections) {
		poolSize = maxConnections > 0 ? maxConnections : 1;
		reuseConnections = true;
		log.info("Smtp connection reuse enabled, at most " + poolSize
				+ " connection(s) per relay account in this process");
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

	private String relayKey() {
		return smtpHost + ":" + getPort() + ":" + (emailUser == null ? "" : emailUser);
	}

	/*
	 * The pool for one relay account, created the first time we send through it
	 */
	private static BlockingQueue<SmtpConnection> poolFor(String key) {
		return pools.computeIfAbsent(key, k -> {
			BlockingQueue<SmtpConnection> q = new ArrayBlockingQueue<>(poolSize);
			for(int i = 0; i < poolSize; i++) {
				q.add(new SmtpConnection());
			}
			return q;
		});
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

		String key = relayKey();

		SmtpConnection conn;
		if(reuseConnections) {
			BlockingQueue<SmtpConnection> accountPool = poolFor(key);
			conn = accountPool.poll(SLOT_WAIT_MS, TimeUnit.MILLISECONDS);
			if(conn == null) {
				throw new Exception("Timed out waiting for one of the " + poolSize
						+ " smtp connection slot(s) for " + key);
			}
			conn.pool = accountPool;
		} else {
			conn = new SmtpConnection();
			conn.pool = null;
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
		if(conn.pool != null) {
			conn.pool.offer(conn);
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
		BlockingQueue<SmtpConnection> owner = conn.pool;
		closeQuietly(conn.transport);
		clear(conn);
		if(owner != null) {
			owner.offer(conn);
		}
	}

	private static void clear(SmtpConnection conn) {
		conn.transport = null;
		conn.session = null;
		conn.key = null;
		conn.sends = 0;
		conn.pool = null;
	}

	/*
	 * Close connections that have been sitting unused, leaving the slots in place.  Called
	 * from the batch loop: an organisation that sends one notification and then nothing for
	 * the rest of the day would otherwise hold its relay connection open indefinitely.
	 */
	public static void closeIdleConnections() {
		sweep(MAX_IDLE_MS);
	}

	/*
	 * Close every connection not currently in use.  Called when a processor loop ends.
	 */
	public static void closeAllConnections() {
		sweep(0);
	}

	private static void sweep(long idleMs) {
		long now = System.currentTimeMillis();
		for(BlockingQueue<SmtpConnection> accountPool : pools.values()) {
			for(int i = 0; i < poolSize; i++) {
				SmtpConnection conn = accountPool.poll();
				if(conn == null) {
					break;			// The rest are in use by another thread
				}
				if(conn.transport != null && (now - conn.lastUsed) >= idleMs) {
					closeQuietly(conn.transport);
					clear(conn);
				}
				accountPool.offer(conn);
			}
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
