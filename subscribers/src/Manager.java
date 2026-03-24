import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ServerSettings;
import org.smap.sdal.model.DatabaseConnections;

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

		String hostname = getHostname(fileLocn);
		long pid = ProcessHandle.current().pid();
		log.info("Subscriber starting: hostname=" + hostname + " pid=" + pid + " type=" + subscriberType);
		clearWorkers(smapId);

		/*
		 * Start asynchronous worker threads
		 */
		if(subscriberType.equals("forward")) {
			
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
			 * Start the message processor in the forward processor
			 */
			MessageProcessor messageProcessor1 = new MessageProcessor();
			messageProcessor1.go(smapId, fileLocn, "qm1");

		} else {
			// Start the default submission queue processor in the upload subscriber
			SubmissionProcessor subProcessor = new SubmissionProcessor();
			subProcessor.go(smapId, fileLocn, "qu1", false, hostname, subscriberType, pid);

			// Start a restore submission queue processor
			SubmissionProcessor subProcessor3 = new SubmissionProcessor();
			subProcessor3.go(smapId, fileLocn, "qu2", false, hostname, subscriberType, pid);

			/*
			 * Start the message processor in the upload processor
			 */
			MessageProcessor messageProcessor2 = new MessageProcessor();
			messageProcessor2.go(smapId, fileLocn, "qm2");
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
				System.out.print("-");	// Log running of batch job
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
