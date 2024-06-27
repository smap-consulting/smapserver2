import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.MessagingManagerApply;
import org.smap.sdal.managers.QueueManager;
import org.smap.sdal.model.DatabaseConnections;
import org.smap.sdal.model.Queue;
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

public class MonitorProcessor {

	String confFilePath;

	DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
	DocumentBuilder db = null;

	boolean forDevice = false;	// URL prefixes should be in the client format
	
	private static Logger log = Logger.getLogger(Subscriber.class.getName());

	private class MonitorLoop implements Runnable {
		DatabaseConnections dbc = new DatabaseConnections();
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

		public void run() {

			int delaySecs = 60;	// Record roughly every minute
			String sql = "insert into monitor_data(recorded_at, payload) values(now(), ?::jsonb)";
			PreparedStatement pstmt = null;
		
			boolean loop = true;
			while(loop) {
				
				log.info("########## Monitor Process");
				String subscriberControl = GeneralUtilityMethods.getSettingFromFile("/smap/settings/subscriber");
				if(subscriberControl != null && subscriberControl.equals("stop")) {
					log.info("---------- Monitor Processor Stopped");
					loop = false;
				} else {
					
					try {
						// Make sure we have a connection to the database
						GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
						
						pstmt = dbc.sd.prepareStatement(sql);
						
						// Get monitoring data
						QueueManager qm = new QueueManager();
						HashMap<String, Queue> qd = new HashMap<>();
						
						qd.put(qm.SUBMISSIONS, qm.getSubmissionQueueData(dbc.sd));
						qd.put(qm.SUBEVENT, qm.getSubEventQueueData(dbc.sd));
						qd.put(qm.RESTORE, qm.getRestoreQueueData(dbc.sd));
						qd.put(qm.S3UPLOAD, qm.getS3UploadQueueData(dbc.sd));
							
						pstmt.setString(1, gson.toJson(qd));
						pstmt.executeUpdate();
						
					} catch (Exception e) {
						log.log(Level.SEVERE, e.getMessage(), e);
					} finally{
						if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
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
			
			// Record queue states
			Thread t = new Thread(new MonitorLoop());
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
