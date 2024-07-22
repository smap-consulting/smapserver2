import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.MessagingManagerApply;
import org.smap.sdal.managers.StorageManager;
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

public class StorageProcessor {

	String confFilePath;

	DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
	DocumentBuilder db = null;

	private static Logger log = Logger.getLogger(Subscriber.class.getName());

	private class MessageLoop implements Runnable {
		DatabaseConnections dbc = new DatabaseConnections();
		String basePath;
		public MessageLoop(String basePath, String awsPropertiesFile) {
			this.basePath = basePath;
		}

		public void run() {

			int delaySecs = 2;
			int s3count = 0;
		
			boolean loop = true;
			while(loop) {
				
				String subscriberControl = GeneralUtilityMethods.getSettingFromFile("/smap/settings/subscriber");
				if(subscriberControl != null && subscriberControl.equals("stop")) {
					log.info("---------- Message Processor Stopped");
					loop = false;
				} else {
					
					System.out.print("(a)");		// Record the running of the storage processor
					
					try {
						// Make sure we have a connection to the database
						GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
						GeneralUtilityMethods.getSubmissionServer(dbc.sd);
						
						StorageManager sm = new StorageManager();
					
						try {
							sm.uploadToS3(dbc.sd, basePath, s3count++);
						} catch (Exception e) {
							log.log(Level.SEVERE, e.getMessage(), e);
						}
						
						if(s3count > 100) {
							s3count = 0;		// Only check to truncate s3 table after every 100 uploads
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
			
			// Upload any images or other files waiting to be sent to permanent storage
			String awsPropertiesFile = null;
			File pFile = new File(basePath + "_bin/resources/properties/aws.properties");
			if (pFile.exists()) {
				awsPropertiesFile = pFile.getAbsolutePath();
				Thread t = new Thread(new MessageLoop(basePath, awsPropertiesFile));
				t.start();
			} else {
				log.info("Skipping Storage Processor. No aws properties file at: " + pFile.getAbsolutePath());
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
