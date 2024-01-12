import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.BackgroundReportsManager;
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

public class ReportProcessor {

	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

	private static Logger log = Logger.getLogger(Subscriber.class.getName());

	private class ReportsLoop implements Runnable {
		DatabaseConnections dbc = new DatabaseConnections();

		String basePath;
		String confFilePath;

		public ReportsLoop(String basePath, String confFilePath) {
			this.basePath = basePath;
			this.confFilePath = confFilePath;
		}

		public void run() {

			int delaySecs = 30;
		
			boolean loop = true;
			while(loop) {
				
				String subscriberControl = GeneralUtilityMethods.getSettingFromFile("/smap/settings/subscriber");
				if(subscriberControl != null && subscriberControl.equals("stop")) {
					log.info("---------- Report Processor Stopped");
					loop = false;
				} else {
					
					System.out.print("(r)");	// log the running of the report processor
					
					try {
						// Make sure we have a connection to the database
						GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
					} catch (Exception e) {
						log.log(Level.SEVERE, e.getMessage(), e);
					}
					
					/*
					 * Loop trough available reports
					 */
					BackgroundReportsManager brm = new BackgroundReportsManager(null, null);
					try {
						while(brm.processNextReport(dbc.sd, dbc.results, basePath)) {
							log.info("..............................report processed");
						}
						
						/*
						 * Delete reports older than 2 weeks
						 */
						brm.deleteOldReports(dbc.sd);
						
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
					
					/*
					 * Sleep and then go again
					 */
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

		String confFilePath = "./" + smapId;

		try {
			Thread t = new Thread(new ReportsLoop(basePath, confFilePath));
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
