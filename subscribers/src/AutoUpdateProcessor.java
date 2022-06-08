import java.io.File;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.AutoUpdateManager;
import org.smap.sdal.model.AutoUpdate;
import org.smap.sdal.model.DatabaseConnections;
import org.smap.subscribers.Subscriber;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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

public class AutoUpdateProcessor {

	String confFilePath;

	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

	private static Logger log = Logger.getLogger(Subscriber.class.getName());

	private class UpdateLoop implements Runnable {
		DatabaseConnections dbc = new DatabaseConnections();
		String serverName;
		String basePath;
		String mediaBucket;
		String region;

		public UpdateLoop(String basePath, String mediaBucket, String region) {
			this.basePath = basePath;
			this.mediaBucket = mediaBucket;
			this.region = region;
		}

		public void run() {

			int delaySecs = 60;
			boolean loop = true;
			while(loop) {
				
				String subscriberControl = GeneralUtilityMethods.getSettingFromFile("/smap/settings/subscriber");
				if(subscriberControl != null && subscriberControl.equals("stop")) {
					log.info("========== Auto update Processor Stopped");
					loop = false;
				} else {
					System.out.print("(u)");	// Log running of update processor
					
					try {
						// Make sure we have a connection to the database
						GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
						serverName = GeneralUtilityMethods.getSubmissionServer(dbc.sd);
						
						AutoUpdateManager aum = new AutoUpdateManager();
						Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
						
						/*
						 * Check for pending jobs
						 */	
						aum.checkPendingJobs(dbc.sd, dbc.results, gson, region, basePath);
									
						/*
						 * Apply auto updates
						 */	
						ArrayList<AutoUpdate> autoUpdates = aum.identifyAutoUpdates(dbc.sd, dbc.results, gson);
						if(autoUpdates != null && autoUpdates.size() > 0) {
							//log.info("-------------- AutoUpdate applying " + autoUpdates.size() + " updates");
							aum.applyAutoUpdates(dbc.sd, dbc.results, gson, serverName, autoUpdates, mediaBucket, region, basePath);
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
	public void go(String smapId, String basePath, String mediaBucket, String region) {

		confFilePath = "./" + smapId;

		try {
			
			// Send any pending messages
			File pFile = new File(basePath + "_bin/resources/properties/aws.properties");
			if (pFile.exists()) {
				Thread t = new Thread(new UpdateLoop(basePath, mediaBucket, region));
				t.start();
			} else {
				// No message!
				log.info("Auto update processing not enabled. No aws.properties file found at " + pFile.getAbsolutePath());
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			/*
			 * Do not close connections!  This processor is suppossed to run forever
			 */

		}

	}
	
}
