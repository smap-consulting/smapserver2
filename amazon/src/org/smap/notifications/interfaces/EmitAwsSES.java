package org.smap.notifications.interfaces;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.activation.DataHandler;
import jakarta.activation.DataSource;
import jakarta.activation.FileDataSource;
import jakarta.mail.Message;
import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ses.SesClient;
import software.amazon.awssdk.services.ses.model.RawMessage;
import software.amazon.awssdk.services.ses.model.SendRawEmailRequest;
import software.amazon.awssdk.services.ses.model.SendRawEmailResponse;

/*****************************************************************************

This file is part of SMAP.

Copyright Smap Consulting Pty Ltd

 ******************************************************************************/

/*
 * Manage the table that stores details on the forwarding of data onto other systems
 */
public class EmitAwsSES {
	
	private static Logger log =
			 Logger.getLogger(EmitAwsSES.class.getName());
	
	// SES clients are thread-safe and expensive to create, so share one per region
	private static final ConcurrentHashMap<String, SesClient> sesClients = new ConcurrentHashMap<>();

	Properties properties = new Properties();
	SesClient client;

	public EmitAwsSES(String region, String basePath) {

		FileInputStream fis = null;
		try {
			fis = new FileInputStream(basePath + "_bin/resources/properties/aws.properties");
			properties.load(fis);
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Error reading properties", e);
		} finally {
			try {fis.close();} catch (Exception e) {}
		}

		//get a shared SES client for this region
		client = sesClients.computeIfAbsent(region, r -> SesClient.builder()
				.region(Region.of(r))
				.credentialsProvider(DefaultCredentialsProvider.create())
				.build());
	}
	
	// Send an email; returns the SES MessageId
	public String sendSES(InternetAddress[] recipients, String subject,
			String emailId,
			String content,
			ArrayList<String> filePaths,
			ArrayList<String> filenames,
			String replyTo) throws Exception  {

		// Add email ID to make subject unique and to allow replies
		String subject2 = subject + " " + emailId;

        log.info("Send");
        String messageId = send(client, "Cases Smap Server <smap@server.smap.com.au>", recipients, subject2, content,
        		filePaths,
        		filenames,
        		replyTo);
        log.info("Done Email Sent");
        return messageId;
	}

	public static String send(SesClient client,
            String sender,
            InternetAddress[] recipients,
            String subject,
            String bodyHTML,
            ArrayList<String> filePaths,
			ArrayList<String> filenames,
			String replyTo) throws Exception {

        Session session = Session.getDefaultInstance(new Properties());
        MimeMessage msg = new MimeMessage(session);
        msg.setSubject(subject, "UTF-8");
        msg.setFrom(new InternetAddress(sender));
        msg.setRecipients(Message.RecipientType.TO, recipients);
        if(replyTo != null) {
        	msg.setReplyTo(InternetAddress.parse(replyTo));
        }

        MimeMultipart mmp = new MimeMultipart("mixed");
        msg.setContent(mmp);

        MimeBodyPart htmlPart = new MimeBodyPart();
        htmlPart.setContent(bodyHTML,"text/html; charset=UTF-8");
        mmp.addBodyPart(htmlPart);

        // Add file attachments if they exist
     	if(filePaths != null) {
     		for(int i = 0; i < filePaths.size(); i++) {
     			log.info("Adding file: " + filePaths.get(i));
     			MimeBodyPart attBodyPart = new MimeBodyPart();
     			DataSource source = new FileDataSource(filePaths.get(i));
     			attBodyPart.setDataHandler(new DataHandler(source));
     			attBodyPart.setFileName(filenames.get(i));
     			mmp.addBodyPart(attBodyPart);
     		}
     	}

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        msg.writeTo(outputStream);
        byte[] rawBytes = outputStream.toByteArray();
        outputStream = null;	// Allow GC before allocating SdkBytes
        msg = null;
        RawMessage rawMessage = RawMessage.builder().data(SdkBytes.fromByteArray(rawBytes)).build();
        rawBytes = null;
        SendRawEmailRequest rawEmailRequest = SendRawEmailRequest.builder().rawMessage(rawMessage).build();

        log.info("Attempting to send an email through Amazon SES "
            		+ "using the AWS SDK for Java...");
        log.info("Sending AWS email from: " + sender + " with subject " + subject);
        SendRawEmailResponse result = client.sendRawEmail(rawEmailRequest);
        return result.messageId();
    }

}


