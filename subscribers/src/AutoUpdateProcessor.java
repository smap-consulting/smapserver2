import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.AutoUpdateManager;
import org.smap.sdal.model.AutoUpdate;
import org.smap.subscribers.Subscriber;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

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
	DocumentBuilder db = null;
	Document xmlConf = null;
	Connection sd = null;
	Connection cResults;

	private static Logger log = Logger.getLogger(Subscriber.class.getName());

	private class UpdateLoop implements Runnable {
		Connection sd;
		Connection cResults;
		String serverName;
		String basePath;
		String mediaBucket;
		String region;

		public UpdateLoop(Connection sd, Connection cResults, String basePath, String mediaBucket, String region) {
			this.sd = sd;
			this.cResults = cResults;
			this.basePath = basePath;
			this.mediaBucket = mediaBucket;
			this.region = region;
		}

		public void run() {

			int delaySecs = 5;
			while (true) {
				System.out.print("u");
				
				try {
					// Make sure we have a connection to the database
					getDatabaseConnection();
					AutoUpdateManager aum = new AutoUpdateManager();
					Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
					
					/*
					 * Check for pending jobs
					 */	
					aum.checkPendingJobs(sd, cResults, gson, region);
								
					/*
					 * Apply auto updates
					 */	
					ArrayList<AutoUpdate> autoUpdates = aum.identifyAutoUpdates(sd, cResults, gson);
					if(autoUpdates != null && autoUpdates.size() > 0) {
						//log.info("-------------- AutoUpdate applying " + autoUpdates.size() + " updates");
						aum.applyAutoUpdates(sd, cResults, gson, serverName, autoUpdates, mediaBucket, region, basePath);
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
		
		private void getDatabaseConnection() throws ParserConfigurationException, SAXException, IOException, ClassNotFoundException, SQLException {
			// Get the connection details for the meta data database
			String dbClassMeta = null;
			String databaseMeta = null;
			String userMeta = null;
			String passwordMeta = null;

			String database = null;
			String user = null;
			String password = null;
			
			// Return if we already have a connection
			if(sd != null && cResults != null && sd.isValid(1) && cResults.isValid(1)) {
				return;
			}
			
			// Make sure any existing connections are closed
			if(sd != null) {
				try {
					sd.close();
				} catch (Exception e) {
					
				}
			}
			
			if(cResults != null) {
				try {
					cResults.close();
				} catch (Exception e) {
					
				}
			}
			
			// Get the database connection
			
			db = dbf.newDocumentBuilder();
			xmlConf = db.parse(new File(confFilePath + "/metaDataModel.xml"));
			dbClassMeta = xmlConf.getElementsByTagName("dbclass").item(0).getTextContent();
			databaseMeta = xmlConf.getElementsByTagName("database").item(0).getTextContent();
			userMeta = xmlConf.getElementsByTagName("user").item(0).getTextContent();
			passwordMeta = xmlConf.getElementsByTagName("password").item(0).getTextContent();

			// Get the connection details for the target results database
			xmlConf = db.parse(new File(confFilePath + "/results_db.xml"));
			database = xmlConf.getElementsByTagName("database").item(0).getTextContent();
			user = xmlConf.getElementsByTagName("user").item(0).getTextContent();
			password = xmlConf.getElementsByTagName("password").item(0).getTextContent();

			Class.forName(dbClassMeta);
			sd = DriverManager.getConnection(databaseMeta, userMeta, passwordMeta);
			cResults = DriverManager.getConnection(database, user, password);
			
			serverName = GeneralUtilityMethods.getSubmissionServer(sd);
			
		}

	}

	/**
	 * @param args
	 */
	public void go(String smapId, String basePath, String mediaBucket, String region) {

		confFilePath = "./" + smapId;

		try {
			
			// Send any pending messages
			File pFile = new File("/smap_bin/resources/properties/aws.properties");
			if (pFile.exists()) {
				Thread t = new Thread(new UpdateLoop(sd, cResults, basePath, mediaBucket, region));
				t.start();
			} else {
				// No message!
				// log.info("Skipping device messages. No aws properties file at:
				// /smap_bin/resources/properties/aws.properties");
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
