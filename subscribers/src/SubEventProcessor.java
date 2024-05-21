import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.SubmissionEventManager;
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

public class SubEventProcessor {

	String confFilePath;

	DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
	DocumentBuilder db = null;
	
	boolean forDevice = false;	// URL prefixes should be in the client format

	private static Logger log = Logger.getLogger(Subscriber.class.getName());

	private class SubEventLoop implements Runnable {
		DatabaseConnections dbc = new DatabaseConnections();
		String basePath;

		public SubEventLoop(String basePath) {
			this.basePath = basePath;
		}

		public void run() {

			int delaySecs = 2;
		
			String serverName = null;
			try {
				GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
				serverName = GeneralUtilityMethods.getSubmissionServer(dbc.sd);
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
			String urlprefix = GeneralUtilityMethods.getUrlPrefixBatch(serverName);
			String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefixBatch(serverName, forDevice);
			
			boolean loop = true;
			while(loop) {
				
				String subscriberControl = GeneralUtilityMethods.getSettingFromFile("/smap/settings/subscriber");
				if(subscriberControl != null && subscriberControl.equals("stop")) {
					log.info("---------- Subevent Processor Stopped");
					loop = false;
				} else {
					
					System.out.print("(e)");		// Record the running of the subevent processor
					
					try {
						// Make sure we have a valid connection to the database
						GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
						
						// Apply events
						SubmissionEventManager sem = new SubmissionEventManager();
						try { 
							sem.applyEvents(dbc.sd, dbc.results, 
									basePath, urlprefix, attachmentPrefix);
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
			
			// Process any pending submission events
			Thread t = new Thread(new SubEventLoop(basePath));
			t.start();	

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			/*
			 * Do not close connections!  This processor is supposed to run forever
			 */

		}

	}
	
}
