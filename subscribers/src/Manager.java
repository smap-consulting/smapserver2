import java.io.PrintStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ServerSettings;
import org.smap.sdal.model.DatabaseConnections;
import org.smap.sdal.managers.EmailManager;
import org.smap.sdal.model.SmtpEmailServer;

/*****************************************************************************

This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.

 ******************************************************************************/

/*
 * Usage java -jar subscribers.jar {smap-id path to subscriber configurations} {file base path} {subscriber type upload or forward}
 */

public class Manager {

	private static Logger log =
			 Logger.getLogger(Manager.class.getName());

	/*
	 * Message workers started in each of the forward and upload subscribers, so twice this
	 * many in total.  There was one in each, and a message spends nearly all of its time
	 * waiting on the smtp relay and on database round trips rather than on cpu, so extra
	 * workers add throughput.  They all draw from the one message_queue with skip locked.
	 */
	private static final int MESSAGE_WORKERS = 3;

	/*
	 * How many smtp connections this process may hold open at once.  One by default: office
	 * 365 allows a mailbox three concurrent connections, and the forward subscriber, the
	 * upload subscriber and the web app all draw on the same mailbox.
	 */
	private static int getSmtpMaxConnections(String basePath) {
		return getIntSetting(basePath, "smtp_max_connections", 1);
	}

	/*
	 * A positive integer from a settings file, or the default if it is absent or unreadable
	 */
	private static int getIntSetting(String basePath, String name, int defaultValue) {
		String setting = GeneralUtilityMethods.getSettingFromFile(basePath + "/settings/" + name);
		if(setting != null) {
			try {
				int value = Integer.parseInt(setting.trim());
				if(value > 0) {
					return value;
				}
				log.warning("Ignoring " + name + " of " + value + ", it must be greater than zero");
			} catch (NumberFormatException e) {
				log.warning("Ignoring unreadable " + name + ": " + setting);
			}
		}
		return defaultValue;
	}

	/*
	 * Find out what writes the iText AGPL notice.
	 *
	 * The notice turns up in the subscriber log without the timestamp and level that
	 * SmapLogFormatter puts on everything else, so it is not going through the logging
	 * framework at all and no logging configuration will silence it.  It is written straight
	 * to stdout or stderr, and the text is not in any jar on the class path that a search for
	 * it can find, so the only way left to identify the culprit is to catch it being written.
	 *
	 * Off unless /smap/settings/trace_agpl contains "on", so the streams are only wrapped
	 * while somebody is looking.  Fires once and then stops checking.
	 */
	private static final AtomicBoolean agplNoticeFound = new AtomicBoolean(false);

	private static void traceAgplNotice(String basePath) {
		String setting = GeneralUtilityMethods.getSettingFromFile(basePath + "/settings/trace_agpl");
		if(setting == null || !setting.trim().equalsIgnoreCase("on")) {
			return;
		}
		/*
		 * After LogConfig, deliberately.  Its ConsoleHandler already holds the real stderr, so
		 * ordinary logging does not come back through here and only direct writes are examined.
		 */
		System.setOut(watchForAgpl(System.out));
		System.setErr(watchForAgpl(System.err));
		log.info("Watching stdout and stderr for the iText AGPL notice, "
				+ "a stack trace will be written when it appears");
	}

	private static PrintStream watchForAgpl(final PrintStream target) {
		return new PrintStream(target, true) {
			@Override public void println(String s) { check(s); super.println(s); }
			@Override public void print(String s) { check(s); super.print(s); }
			@Override public void write(byte[] b, int off, int len) {
				if(!agplNoticeFound.get()) {
					check(new String(b, off, len));
				}
				super.write(b, off, len);
			}
			private void check(String s) {
				if(s != null && s.contains("AGPL") && agplNoticeFound.compareAndSet(false, true)) {
					// Straight to the wrapped stream, so reporting it does not come back here
					new Throwable("The iText AGPL notice was written from here").printStackTrace(target);
				}
			}
		};
	}

	/*
	 * Resolve the hostname for this server.
	 * Tries basePath/settings/hostname first, then several system-level fallbacks.
	 */
	private static String getHostname(String basePath) {
		// 1. Admin-configured name
		String hostname = GeneralUtilityMethods.getSettingFromFile(basePath + "/settings/hostname");
		if(hostname != null && !hostname.trim().isEmpty()) return hostname.trim();

		// 2. JVM hostname resolution
		try {
			hostname = InetAddress.getLocalHost().getHostName();
			if(hostname != null && !hostname.trim().isEmpty() && !hostname.equals("localhost")) return hostname.trim();
		} catch(Exception e) {}

		// 3. HOSTNAME environment variable
		hostname = System.getenv("HOSTNAME");
		if(hostname != null && !hostname.trim().isEmpty()) return hostname.trim();

		// 4. /etc/hostname (works without DNS)
		try {
			hostname = new String(Files.readAllBytes(Paths.get("/etc/hostname"))).trim();
			if(!hostname.isEmpty()) return hostname;
		} catch(Exception e) {}

		// 5. Random UUID as last resort
		return UUID.randomUUID().toString().substring(0, 8);
	}

	/*
	 * Remove subscriber_worker entries that have not sent a heartbeat in the last
	 * 5 minutes. Called on startup so dead workers from any previous run are cleaned
	 * up without disturbing other subscribers that are still running.
	 */
	private static void clearWorkers(String smapId) {
		Connection sd = null;
		try {
			DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document xmlConf = db.parse(new java.io.File("./" + smapId + "/metaDataModel.xml"));
			String dbClass  = xmlConf.getElementsByTagName("dbclass").item(0).getTextContent();
			String database = xmlConf.getElementsByTagName("database").item(0).getTextContent();
			String user     = xmlConf.getElementsByTagName("user").item(0).getTextContent();
			String password = xmlConf.getElementsByTagName("password").item(0).getTextContent();
			Class.forName(dbClass);
			sd = DriverManager.getConnection(database, user, password);
			PreparedStatement pstmt = sd.prepareStatement(
					"delete from subscriber_worker where heartbeat < now() - interval '5 minutes'");
			pstmt.executeUpdate();
			pstmt.close();
		} catch(Exception e) {
			log.log(Level.WARNING, "Could not clear subscriber_worker: " + e.getMessage(), e);
		} finally {
			try {if(sd != null) {sd.close();}} catch(Exception e) {}
		}
	}

	/*
	 * One off backfill of the record_user owner index from existing _assigned values.
	 * Runs in a background thread on forward subscriber startup so it does not block the
	 * processors, and is skipped once the server flag is set.
	 */
	private static void runRecordUserBackfill(String smapId) {
		Thread t = new Thread(() -> {
			DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
			DatabaseConnections dbc = new DatabaseConnections();
			try {
				GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, "./" + smapId);
				RecordUserBackfill.runIfNeeded(dbc.sd, dbc.results);
			} catch(Exception e) {
				log.log(Level.WARNING, "Could not run record_user backfill: " + e.getMessage(), e);
			} finally {
				try {if(dbc.sd != null) {dbc.sd.close();}} catch(Exception e) {}
				try {if(dbc.results != null) {dbc.results.close();}} catch(Exception e) {}
			}
		}, "record-user-backfill");
		t.setDaemon(true);
		t.start();
	}

	public static void main(String[] args) {
		
		String fileLocn = "/smap";			// Default for legacy servers that do not set file path
		String subscriberType = "upload";               // Default subscriberType
                String smapId = "default";                      // Default smapId
		
                if(args.length > 0) {
                    smapId = args[0];
                    if(args.length > 1) {
                            if(args[1] != null && !args[1].equals("null")) {
                                    fileLocn = args[1];	
                            }
                    }
                    if(args.length > 2) {
                            subscriberType = args[2];	
                    }
                }		
                ServerSettings.setBasePath(fileLocn);

		// Set mode property before LogConfig so the formatter can read it
		System.setProperty("smap.subscriber.mode", subscriberType);
		LogConfig.init(fileLocn);
		traceAgplNotice(fileLocn);

		String hostname = getHostname(fileLocn);
		long pid = ProcessHandle.current().pid();
		log.info("Subscriber starting: hostname=" + hostname + " pid=" + pid + " type=" + subscriberType);
		clearWorkers(smapId);

		/*
		 * The message workers send a continuous stream of notifications, so let them hold an
		 * smtp connection open between messages rather than doing the tls handshake and the
		 * authentication again for every email.
		 *
		 * The pool is deliberately smaller than the worker count.  Relays limit how many
		 * connections a mailbox may hold at once and office 365 allows only three, counting
		 * both subscribers and anything the web app sends, so one apiece leaves headroom.
		 * Override in /smap/settings/smtp_max_connections if the relay is more generous.
		 */
		SmtpEmailServer.enableConnectionReuse(getSmtpMaxConnections(fileLocn));

		/*
		 * How long to leave email alone after the relay says we have sent too much.  Gmail's
		 * daily limit clears on a rolling twenty four hour window and nothing here knows when
		 * that is, so retry periodically until one gets through rather than guessing.
		 */
		SmtpEmailServer.setRateLimitPauseMinutes(
				getIntSetting(fileLocn, "smtp_retry_minutes", 60));

		/*
		 * Largest attachment to put on an email.  Bigger than the relay accepts and it is
		 * refused only after the whole file has been pushed at it, so the notification goes
		 * out with a link to the record instead.
		 */
		EmailManager.setMaxAttachmentMb(
				getIntSetting(fileLocn, "max_email_attachment_mb", 20));

		/*
		 * Start asynchronous worker threads
		 */
		if(subscriberType.equals("forward")) {

			// Run one off data migrations that only need to happen once per server
			runRecordUserBackfill(smapId);

			// Start the AWS service processor
			String mediaBucket = GeneralUtilityMethods.getSettingFromFile(fileLocn + "/settings/bucket");
			String region = GeneralUtilityMethods.getSettingFromFile(fileLocn + "/settings/region");
			log.info("Auto Update:  S3 Bucket is: " + region + " : " + mediaBucket);
			
			AutoUpdateProcessor au = new AutoUpdateProcessor();
			au.go(smapId, fileLocn, mediaBucket, region);
			
			/*
			 * Start the storage processor - required if images are stored on s3
			 */
			StorageProcessor sp = new StorageProcessor();
			sp.go(smapId, fileLocn, hostname, pid);

			/*
			 * Start the report processors
			 * A separate processor is started for restores as these can block for a long time
			 */
			ReportProcessor rp = new ReportProcessor();
			rp.go(smapId, fileLocn, false);	// No restore
			rp.go(smapId, fileLocn, true);	// With restore    -- TODO stop restores for performance

			/*
			 * Start the submission event processor
			 */
			SubEventProcessor sep = new SubEventProcessor();
			sep.go(smapId, fileLocn);

			/*
			 * Start the queue monitor process
			 * This is disabled as its output is not used,  however it could be re-enabled if there are issues with queues
			 */
			//MonitorProcessor mp = new MonitorProcessor();
			//mp.go(smapId, fileLocn);

			// Start the default submission queue processor in the forward
			SubmissionProcessor subProcessor = new SubmissionProcessor();
			subProcessor.go(smapId, fileLocn, "qf1", false, hostname, subscriberType, pid);

			// Start the default submission queue processor that processes restore requests
			SubmissionProcessor subProcessor2 = new SubmissionProcessor();
			subProcessor2.go(smapId, fileLocn, "qf2_restore", true, hostname, subscriberType, pid);

			/*
			 * Start the message processors in the forward processor
			 */
			for(int i = 1; i <= MESSAGE_WORKERS; i++) {
				MessageProcessor messageProcessor = new MessageProcessor();
				messageProcessor.go(smapId, fileLocn, "qmf" + i, hostname, subscriberType, pid);
			}

			/*
			 * Start the email response processor (polls S3 for inbound reply emails)
			 */
			EmailResponseProcessor emailResponseProcessor = new EmailResponseProcessor();
			emailResponseProcessor.go(smapId, fileLocn, hostname, pid);

		} else {
			/*
			 * Start the submission queue processors for live uploads. Each one is a
			 * thread holding its own sd and results connection. They spend most of
			 * their time blocked on attachment processing and database round trips
			 * rather than on cpu, so extra workers add throughput.
			 *
			 * Restores are deliberately excluded here - they can block for a long time
			 * and are handled by qf2_restore in the forward subscriber.
			 */
			int UPLOAD_WORKERS = 4;
			for(int i = 1; i <= UPLOAD_WORKERS; i++) {
				SubmissionProcessor subProcessor = new SubmissionProcessor();
				subProcessor.go(smapId, fileLocn, "qu" + i, false, hostname, subscriberType, pid);
			}

			/*
			 * Start the message processors in the upload processor
			 */
			for(int i = 1; i <= MESSAGE_WORKERS; i++) {
				MessageProcessor messageProcessor = new MessageProcessor();
				messageProcessor.go(smapId, fileLocn, "qmu" + i, hostname, subscriberType, pid);
			}
		}
		
		
		log.info("Starting prop subscriber: " + smapId + " : " + fileLocn + " : " + subscriberType);
		int delaySecs = 4;
		
		// The forward batch job processes events less important. In order to reduce the server load set a longer delay between runs.
		if(subscriberType.equals("forward")) {
			delaySecs = 60;					
		}
		
		SubscriberBatch batchJob = new SubscriberBatch();
		boolean loop = true;
		while(loop) {
			String subscriberControl = GeneralUtilityMethods.getSettingFromFile(fileLocn + "/settings/subscriber");
			if(subscriberControl != null && subscriberControl.equals("stop")) {
				log.info("######## Stopped");		
				loop = false;
			} else {
				batchJob.go(smapId, fileLocn, subscriberType);	// Run the batch job for the specified server

				try {
					Thread.sleep(delaySecs * 1000);
				} catch (Exception e) {
					// ignore
				}
			}

		}
				
	}
	
}
