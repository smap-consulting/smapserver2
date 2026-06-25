package org.smap.notifications.interfaces;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;

import model.DeviceTable;
import tools.AmazonSNSClientWrapper;
import tools.SampleMessageGenerator.Platform;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * copyright Smap Pty Ltd
 * 
 ******************************************************************************/

/*
 * Manage access to the Dynamo table that holds the connection between user and
 * device
 */
public class EmitDeviceNotification {

	private static Logger log = Logger.getLogger(EmitDeviceNotification.class.getName());

	private AmazonSNSClientWrapper snsClientWrapper;
	Properties properties = new Properties();
	String tableName = null;
	String region = null;
	String platformApplicationArn = null;
	AmazonSNS sns = null;

	public EmitDeviceNotification(String awsPropertiesFile) {

		// get properties file
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(awsPropertiesFile);
			properties.load(fis);
			tableName = properties.getProperty("userDevices_table");
			region = properties.getProperty("userDevices_region");
			platformApplicationArn = properties.getProperty("fieldTask_platform");
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error reading properties", e);
		} finally {
			try {fis.close();} catch(Exception e) {}
		}

		//create a new SNS client
		sns = AmazonSNSClient.builder()
				.withRegion(region)
				.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
				.build();
	}

	/*
	 * Send a message to users registered with a server, name combo
	 */
	public void notify(String server, String user) {

		// For testing on local host - can leave in final code
		if(server.equals("localhost")) {
			server = "dev.smap.com.au";
		}

		// Get the device registration ids associated with this user on this server
		DeviceTable deviceTable = new DeviceTable(region, tableName);
		ItemCollection<QueryOutcome> items = deviceTable.getUserDevices(server, user);

		// Process the results
		snsClientWrapper = new AmazonSNSClientWrapper(sns, deviceTable);
		Iterator<Item> iter = items.iterator(); 
		//ArrayList<String> obsoleteTokens = new ArrayList<String> ();
		int count = 0;
		while (iter.hasNext()) {
			count++;
			Item item = iter.next();
			String token = item.getString("registrationId");
			log.info("Token: " + token + " for " + server + ":" + user);

			// Send the notification
			Map<Platform, Map<String, MessageAttributeValue>> attrsMap = new HashMap<Platform, Map<String, MessageAttributeValue>> ();
			snsClientWrapper.sendNotification(Platform.GCM, token, attrsMap, platformApplicationArn);
			
			//obsoleteTokens.add(token);
		}
		
		if(count == 0) {
			log.info("Token not found for " + server + ":" + user);
		} else if(count > 1) {
			// Delete old tokens
			// This relies on tokens being returned in the reverse order to which they were added
			/*
			 * This seems to delete the firebase endpoint
			 *
			for(int i = obsoleteTokens.size() - 1; i> 0; i--) {
				String token = obsoleteTokens.get(i);
				log.info("+++ Deleting obsolete token: " + token);
				deviceTable.deleteToken(obsoleteTokens.get(i));
			}
			*/
		}

	}

}
