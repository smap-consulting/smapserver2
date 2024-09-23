package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.ResourceBundle;

import javax.mail.internet.InternetAddress;

import org.smap.notifications.interfaces.EmitAwsSES;

public class AwsSdkEmailServer extends EmailServer {

	public EmitAwsSES ses = null;
	
	public AwsSdkEmailServer(ResourceBundle localisation, String region, String basePath) {
		super(localisation);
		ses = new EmitAwsSES(region, basePath);
	}
	
	@Override
	public void send(String email, String ccType, String subject, int emailId, String contentString, String filePath,
			String filename) throws Exception {

		/*
		 * Get an array list of internet addresses
		 */
		log.info("Sending to email addresses via aws: " + email);
		
		InternetAddress[] emailArray = InternetAddress.parse(email);
		log.info("Number of email addresses: " + emailArray.length);
		for(InternetAddress a : emailArray) {
			ArrayList<String> recipients = new ArrayList<>();
			recipients.add(a.getAddress());
			ses.sendSES(recipients, subject, emailId, contentString);	// Send each email separately
		}	
		
	}

}
