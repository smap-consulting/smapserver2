import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.MessagingManagerApply;
import org.smap.sdal.model.DatabaseConnections;
import org.smap.subscribers.Subscriber;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * 
 * SMAP is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * SMAP is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * SMAP. If not, see <http://www.gnu.org/licenses/>.
 * 
 ******************************************************************************/

public class MessageProcessor {

	String confFilePath;

	DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
	DocumentBuilder db = null;

	boolean forDevice = false;	// URL prefixes should be in the client format
	
	private static Logger log = Logger.getLogger(Subscriber.class.getName());

	private class MessageLoop implements Runnable {
		DatabaseConnections dbc = new DatabaseConnections();
		String serverName;
		String urlprefix;
		String attachmentPrefix;
		String basePath;
		String awsPropertiesFile;

		public MessageLoop(String basePath, String awsPropertiesFile) {
			this.basePath = basePath;
			this.awsPropertiesFile = awsPropertiesFile;
		}

		public void run() {

			int delaySecs = 2;
			int count = 0;
		
			boolean loop = true;
			while(loop) {
				
				String subscriberControl = GeneralUtilityMethods.getSettingFromFile("/smap/settings/subscriber");
				if(subscriberControl != null && subscriberControl.equals("stop")) {
					log.info("---------- Message Processor Stopped");
					loop = false;
				} else {
					
					System.out.print("(m)");		// Record the running of the message processor
					
					try {
						// Make sure we have a connection to the database
						GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
						serverName = GeneralUtilityMethods.getSubmissionServer(dbc.sd);
						urlprefix = GeneralUtilityMethods.getUrlPrefixBatch(serverName);
						attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefixBatch(serverName, forDevice);
						
						// Apply messages
						MessagingManagerApply mma = new MessagingManagerApply();
						try { 
							mma.applyOutbound(dbc.sd, dbc.results, serverName, 
									basePath, count++, awsPropertiesFile, 
									urlprefix,
									attachmentPrefix);
						} catch (Exception e) {
							log.log(Level.SEVERE, e.getMessage(), e);
						}
						
						try {
							mma.applyPendingEmailMessages(dbc.sd, dbc.results, 
									serverName, basePath, 
									urlprefix, 
									attachmentPrefix);
						} catch (Exception e) {
							log.log(Level.SEVERE, e.getMessage(), e);
						}
						
					} catch (Exception e) {
						log.log(Level.SEVERE, e.getMessage(), e);
					}
					
					// Sleep and then go again
					try {
						Thread.sleep(delaySecs * 1000);
					} catch (Exception e) {
						// ignore
					}
				}

			}
		}
		
	}

	/**
	 * @param args
	 */
	public void go(String smapId, String basePath) {

		confFilePath = "./" + smapId;

		try {
			
			// Send any pending messages
			String awsPropertiesFile = null;
			File pFile = new File(basePath + "_bin/resources/properties/aws.properties");
			if (pFile.exists()) {
				awsPropertiesFile = pFile.getAbsolutePath();
				Thread t = new Thread(new MessageLoop(basePath, awsPropertiesFile));
				t.start();
			} else {
				log.info("Skipping Message Processing. No aws properties file at: " + pFile.getAbsolutePath());
			}	

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			/*
			 * Do not close connections!  This processor is supposed to run forever
			 */

		}

	}
	
}
