package org.smap.notifications.interfaces;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

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
	
	Properties properties = new Properties();
	String senderId = "smap";
	ResourceBundle localisation;
	
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
		
		//create a new SNS client
		AmazonSNS sns = AmazonSNSClient.builder()
				.withRegion("ap-southeast-1")
				.withCredentials(new DefaultAWSCredentialsProviderChain())
				.build();	
	
		Map<String, MessageAttributeValue> smsAttributes = 
                new HashMap<String, MessageAttributeValue>();
		smsAttributes.put("AWS.SNS.SMS.SenderID", new MessageAttributeValue()
		        .withStringValue(senderId) //The sender ID shown on the device.
		        .withDataType("String"));
		
        sendSMSMessage(sns, content, number, smsAttributes);
		
		return responseBody;
	}
	
	private void sendSMSMessage(AmazonSNS snsClient, String message, 
			String phoneNumber, Map<String, MessageAttributeValue> smsAttributes) {
		
	    PublishResult result = snsClient.publish(new PublishRequest()
	    		.withMessage(message)
	        .withPhoneNumber(phoneNumber)
	        .withMessageAttributes(smsAttributes));
	    log.info("Message Id:" + result); // Prints the message ID.
	}

}


