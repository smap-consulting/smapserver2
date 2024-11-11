import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.ConversationManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MessagingManagerApply;
import org.smap.sdal.model.DatabaseConnections;
import com.vonage.client.VonageClient;

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

	private static Logger log = Logger.getLogger(MessageProcessor.class.getName());
	private static LogManager lm = new LogManager();		// Application log
	
	boolean forDevice = false;	// URL prefixes should be in the client format

	private class MessageLoop implements Runnable {
		DatabaseConnections dbc = new DatabaseConnections();
		String serverName;
		String urlprefix;
		String attachmentPrefix;
		String hyperlinkPrefix;
		String basePath;
		String queueName;	
		boolean gotVonageClient = false;
		
		public MessageLoop(String basePath, String queueName) {
			this.basePath = basePath;
			this.queueName = queueName;	
		}

		public void run() {

			int delaySecs = 2;
		
			MessagingManagerApply mma = new MessagingManagerApply();
			
			boolean loop = true;
			while(loop) {
				
				String subscriberControl = GeneralUtilityMethods.getSettingFromFile("/smap/settings/subscriber");
				if(subscriberControl != null && subscriberControl.equals("stop")) {
					GeneralUtilityMethods.log(log, "---------- Message Processor Stopped", queueName, null);
					loop = false;
				} else {
					
					System.out.print("(m)");		// Record the running of the message processor
					
					try {
						// Make sure we have a connection to the database
						GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);
						serverName = GeneralUtilityMethods.getSubmissionServer(dbc.sd);
						urlprefix = GeneralUtilityMethods.getUrlPrefixBatch(serverName);
						attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefixBatch(serverName, forDevice);
						// hyperlink prefix assumes that the hyperlink will be used by a human, hence always use client authentication
						hyperlinkPrefix = GeneralUtilityMethods.getAttachmentPrefixBatch(serverName, false);
						
						/*
						 * Get a vonage client
						 */
						VonageClient vonageClient = null;
						if(!gotVonageClient) {
							ConversationManager convMgr = new ConversationManager(null, null);
							vonageClient = convMgr.getVonageClient(dbc.sd);
						}
						
						try { 
							
							/*
							 * Send messages
							 */
							mma.applyOutbound(dbc.sd, dbc.results, queueName, serverName, 
									basePath,
									urlprefix,
									attachmentPrefix,
									hyperlinkPrefix,
									vonageClient);
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
	public void go(String smapId, String basePath, String queueName) {

		confFilePath = "./" + smapId;

		try {
			
			// Send any pending messages
			Thread t = new Thread(new MessageLoop(basePath, queueName));
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
