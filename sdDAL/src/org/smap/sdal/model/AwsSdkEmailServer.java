package org.smap.sdal.model;

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
	public void send(String email, String ccType, String subject, String emailId, String contentString, String filePath,
			String filename) throws Exception {

		/*
		 * Get an array list of internet addresses
		 */
		log.info("Sending to email addresses via aws: " + email);
		
		InternetAddress[] emailArray = InternetAddress.parse(email);
		log.info("Number of email addresses: " + emailArray.length);
		for(InternetAddress recipient : emailArray) {
			InternetAddress[] recipientArray = new InternetAddress[] {recipient};
			ses.sendSES(recipientArray, subject, emailId, contentString, filePath, filename);	// Send each email separately
		}	
		
	}

}
