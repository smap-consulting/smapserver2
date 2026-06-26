package org.smap.notifications.interfaces;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;



/*****************************************************************************

This file is part of SMAP.

Copyright Smap Pty Ltd

 ******************************************************************************/

/*
 * Manage the table that stores details on the forwarding of data onto other systems
 */
public class EmitNotifications {
	
	/*
	 * Events
	 */
	public static int AWS_REGISTER_ORGANISATION = 0;
	
	private static Logger log =
			 Logger.getLogger(EmitNotifications.class.getName());
	
	// SNS client is thread-safe and expensive to create, so share a single instance
	private static volatile SnsClient sns;

	Properties properties = new Properties();

	private static SnsClient getSnsClient() {
		if(sns == null) {
			synchronized (EmitNotifications.class) {
				if(sns == null) {
					sns = SnsClient.builder()
							.region(Region.of("ap-southeast-1"))
							.credentialsProvider(DefaultCredentialsProvider.create())
							.build();
				}
			}
		}
		return sns;
	}

	public EmitNotifications() {
		FileInputStream fis = null;
		try {
			fis = new FileInputStream("/smap_bin/resources/properties/aws.properties");
			properties.load(fis);
		}
		catch (Exception e) { 
			log.log(Level.SEVERE, "Error reading properties", e);
		} finally {
			try {fis.close();} catch (Exception e) {}
		}
	}
	
	/*
	 * Publish to an SNS topic
	 */
	public void publish(int event, String msg, String subject) {
		
		// Reuse the shared SNS client
		SnsClient sns = getSnsClient();

		String topic = getTopic(event);

		if(topic != null) {
			PublishRequest publishRequest = PublishRequest.builder()
					.topicArn(topic)
					.message(msg)
					.subject(subject)
					.build();
			PublishResponse publishResult = sns.publish(publishRequest);
			log.info("Publish: " + subject + " MessageId - " + publishResult.messageId());
		}
		
	}
	
	private String getTopic(int event) {
		
		String topic = null;
		
		if (event == AWS_REGISTER_ORGANISATION) {
			topic = properties.getProperty("register_organisation_topic");
			log.info("Publish: register");
		} else {
			log.info("Error: Publish: invalid event: " + event);
		}
		log.info("Topic: " + topic);
		
		return topic;
	}

}


