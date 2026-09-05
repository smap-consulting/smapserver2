package org.smap.notifications.interfaces;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;

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

	/*
	 * A shared account can have thousands of devices, and one round trip each, one after
	 * the other, on the single batch thread held up everything else the forward subscriber
	 * does.  Send them at the same time instead, but only a few at once so that a large
	 * account cannot flood the SNS api either.
	 */
	private static final int DEVICE_THREADS = 16;
	private static final ExecutorService devicePool = Executors.newFixedThreadPool(DEVICE_THREADS, r -> {
		Thread t = new Thread(r, "device-notify");
		t.setDaemon(true);
		return t;
	});

	/*
	 * SnsClient is thread safe and expensive to build, and this class is constructed once
	 * per run, so share one per region rather than leaving a client behind every time
	 */
	private static final ConcurrentHashMap<String, SnsClient> snsClients = new ConcurrentHashMap<>();

	Properties properties = new Properties();
	String tableName = null;
	String region = null;
	String platformApplicationArn = null;
	SnsClient sns = null;

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

		sns = snsClients.computeIfAbsent(region, r -> SnsClient.builder()
				.region(Region.of(r))
				.credentialsProvider(DefaultCredentialsProvider.create())
				.build());
	}

	/*
	 * Send a message to users registered with a server, name combo
	 *
	 * Returns the number of devices notified, so the caller can report how much of the
	 * fleet it is actually reaching.  Zero means the user has no device registered under
	 * this host name.
	 */
	public int notify(String server, String user) {

		// For testing on local host - can leave in final code
		if(server.equals("localhost")) {
			server = "dev.smap.com.au";
		}

		// Get the device registration ids associated with this user on this server
		DeviceTable deviceTable = new DeviceTable(region, tableName);
		List<Map<String, AttributeValue>> items = deviceTable.getUserDevices(server, user);

		if(items.isEmpty()) {
			log.fine("Token not found for " + server + ":" + user);
			return 0;
		}

		/*
		 * Local, not a field.  Devices are now sent to at the same time, and a field would be
		 * shared between those threads and between users.
		 */
		AmazonSNSClientWrapper wrapper = new AmazonSNSClientWrapper(sns, deviceTable);
		final String stampedServer = server;

		List<Future<?>> sends = new ArrayList<Future<?>> ();
		int count = 0;
		for (Map<String, AttributeValue> item : items) {
			AttributeValue tokenAttr = item.get("registrationId");
			final String token = tokenAttr == null ? null : tokenAttr.s();
			if(token == null) {
				// Nothing can be sent to it, and the row can never be matched again either
				log.warning("Device registration with no token for " + stampedServer + ":" + user);
				continue;
			}
			count++;
			if(log.isLoggable(Level.FINE)) {
				String shortToken = token.length() < 10 ? token : token.substring(0, 10) + "...";
				log.fine("Token: " + shortToken + " for " + stampedServer + ":" + user);
			}
			sends.add(devicePool.submit(() -> {
				Map<Platform, Map<String, MessageAttributeValue>> attrsMap =
						new HashMap<Platform, Map<String, MessageAttributeValue>> ();
				wrapper.sendNotification(Platform.GCM, token, attrsMap, platformApplicationArn, stampedServer);
			}));
		}

		/*
		 * There used to be commented out code here that deleted all but the newest token for
		 * a user, disabled with a note that it seemed to delete the firebase endpoint.  Worth
		 * knowing before anyone tries again: the rows carry no timestamp, so newest cannot be
		 * told, and a shared account can legitimately have thousands of live devices.
		 *
		 * Wait, so that the caller's count is of devices actually attempted and so that one
		 * user with thousands of devices cannot leave work queued up behind the next
		 */
		for(Future<?> send : sends) {
			try {
				send.get();
			} catch (Exception e) {
				log.log(Level.SEVERE, "Sending a device notification for " + stampedServer + ":" + user, e);
			}
		}

		return count;
	}

}
