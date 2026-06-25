package org.smap.notifications.interfaces;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;



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
	
	Properties properties = new Properties();
	
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
		
		//create a new SNS client
		AmazonSNS sns = AmazonSNSClient.builder()
				.withRegion("ap-southeast-1")
				.withCredentials(new DefaultAWSCredentialsProviderChain())
				.build();
		
		String topic = getTopic(event);
		
		if(topic != null) {
			PublishRequest publishRequest = new PublishRequest(topic, msg, subject);
			PublishResult publishResult = sns.publish(publishRequest);
			log.info("Publish: " + subject + " MessageId - " + publishResult.getMessageId());
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


