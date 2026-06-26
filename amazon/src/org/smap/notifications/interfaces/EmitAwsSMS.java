package org.smap.notifications.interfaces;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/*****************************************************************************

This file is part of SMAP.

Copyright Smap Consulting Pty Ltd

 ******************************************************************************/

/*
 * Manage the table that stores details on the forwarding of data onto other systems
 */
public class EmitAwsSMS extends EmitSMS {
	
	/*
	 * Events
	 */
	public static int AWS_REGISTER_ORGANISATION = 0;
	
	private static Logger log =
			 Logger.getLogger(EmitAwsSMS.class.getName());

	// SNS client is thread-safe and expensive to create, so share a single instance
	private static volatile SnsClient sns;

	Properties properties = new Properties();
	String senderId = "smap";
	ResourceBundle localisation;

	private static SnsClient getSnsClient() {
		if(sns == null) {
			synchronized (EmitAwsSMS.class) {
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
	
	public EmitAwsSMS(String senderId, ResourceBundle l) {
		localisation = l;
		
		if(senderId != null) {
			this.senderId = senderId;
		}
		
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
	
	// Send an sms
	@Override
	public String sendSMS( 
			String number, 
			String content) throws Exception  {
		
		String responseBody = null;
		
		if(!isValidPhoneNumber(number, true)) {
			throw new Exception(localisation.getString("msg_sms") + ": " + number);
		}
		
		// Reuse the shared SNS client
		SnsClient sns = getSnsClient();

		Map<String, MessageAttributeValue> smsAttributes =
                new HashMap<String, MessageAttributeValue>();
		smsAttributes.put("AWS.SNS.SMS.SenderID", MessageAttributeValue.builder()
		        .stringValue(senderId) //The sender ID shown on the device.
		        .dataType("String")
		        .build());

        sendSMSMessage(sns, content, number, smsAttributes);

		return responseBody;
	}

	private void sendSMSMessage(SnsClient snsClient, String message,
			String phoneNumber, Map<String, MessageAttributeValue> smsAttributes) {

	    PublishResponse result = snsClient.publish(PublishRequest.builder()
	    		.message(message)
	        .phoneNumber(phoneNumber)
	        .messageAttributes(smsAttributes)
	        .build());
	    log.info("Message Id:" + result); // Prints the message ID.
	}

}


