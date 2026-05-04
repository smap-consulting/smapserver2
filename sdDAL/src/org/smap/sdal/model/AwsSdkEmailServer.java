package org.smap.sdal.model;

import java.util.ResourceBundle;

import jakarta.mail.internet.InternetAddress;

import org.smap.notifications.interfaces.EmitAwsSES;

public class AwsSdkEmailServer extends EmailServer {

	public EmitAwsSES ses = null;
	
	public AwsSdkEmailServer(ResourceBundle localisation, String region, String basePath) {
		super(localisation);
		ses = new EmitAwsSES(region, basePath);
	}
	
	@Override
	public String send(String email, String ccType, String subject, String emailId, String contentString, String filePath,
			String filename, String replyTo) throws Exception {

		log.fine("Sending to email addresses via aws: " + email);

		InternetAddress[] emailArray = InternetAddress.parse(email);
		log.fine("Number of email addresses: " + emailArray.length);
		String firstMessageId = null;
		for(InternetAddress recipient : emailArray) {
			InternetAddress[] recipientArray = new InternetAddress[] {recipient};
			String mid = ses.sendSES(recipientArray, subject, emailId, contentString, filePath, filename, replyTo);
			if(firstMessageId == null) firstMessageId = mid;
		}
		return firstMessageId;
	}

}
