package org.smap.sdal.model;

import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.AuthenticationFailedException;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.Message.RecipientType;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

public class SmtpEmailServer extends EmailServer {

	/*
	 * Add an authenticator class
	 */
	private class Authenticator extends javax.mail.Authenticator {
		private PasswordAuthentication authentication;

		public Authenticator(String username, String password) {
			authentication = new PasswordAuthentication(username, password);
		}

		protected PasswordAuthentication getPasswordAuthentication() {
			return authentication;
		}
	}
	
	public SmtpEmailServer(ResourceBundle localisation) {
		super(localisation);
	}
	
	@Override
	public void send(String email, String ccType, String subject, 
			String emailId, 
			String contentString,
			String filePath,
			String filename) throws Exception {
		
		if(smtpHost == null) {
			throw new Exception("Cannot send email, smtp_host not available");
		}

		try {
			RecipientType rt = null;
			String sender = emailUser;
	
			Session session = getEmailSession();
	
			Message msg = new MimeMessage(session);
			if(ccType.equals("bcc")) {
				rt = Message.RecipientType.BCC;
			} else {
				rt = Message.RecipientType.TO;
			}
	
			log.info("Sending to email addresses: " + email);
			InternetAddress[] emailArray = InternetAddress.parse(email);
			log.info("Number of email addresses: " + emailArray.length);
			msg.setRecipients(rt,	emailArray);
			msg.setSubject(subject + " " + emailId);	// Include email ID with subject to make it unique
	
			// Add the email server domain if not already set for sender
			if(sender.indexOf('@') < 0) {
				sender = sender + "@" + emailDomain;
			}
	
			log.info("Sending email from: (sendEmailHtml1) " + sender + " with subject " + subject);
			msg.setFrom(InternetAddress.parse(sender, false)[0]);
	
			Multipart multipart = new MimeMultipart();
	
			// Add body part
			MimeBodyPart messageBodyPart = new MimeBodyPart();
			messageBodyPart.setText(contentString, "utf-8", "html");
			multipart.addBodyPart(messageBodyPart);
	
			// Add file attachments if they exist
			if(filePath != null) {			 
				messageBodyPart = new MimeBodyPart();
				DataSource source = new FileDataSource(filePath);
				messageBodyPart.setDataHandler(new DataHandler(source));
				messageBodyPart.setFileName(filename);
				multipart.addBodyPart(messageBodyPart);
			}
	
			msg.setContent(multipart);
	
			msg.setHeader("X-Mailer", "msgsend");
			log.info("Sending email from: (sendEmailHtml2) " + sender);
	
			Transport.send(msg);
			
		} catch(AuthenticationFailedException ae) { 
			log.log(Level.SEVERE, "Messaging Exception", ae);
			throw new Exception(localisation.getString("email_cs") + ":  " + localisation.getString("ae"));
		} catch(MessagingException me) {
			log.log(Level.SEVERE, "Messaging Exception", me);
			String msg = me.getMessage();
			throw new Exception(localisation.getString("email_cs") + ":  " + msg);
		} catch(Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			String msg = e.getMessage();
			throw new Exception(localisation.getString("email_cs") + ":  " + msg);
		}
	}
	
	/*
	 * Get an email session for SMTP
	 */
	private Session getEmailSession() {
		Properties props = System.getProperties();
		props.put("mail.smtp.host", smtpHost);	

		Authenticator authenticator = null;

		// Create an authenticator if the user name and password is available
		if(emailUser != null && emailPassword != null 
				&& emailUser.trim().length() > 0 
				&& emailPassword.trim().length() > 0) {
			String authUser = emailUser + "@" + emailDomain;
			authenticator = new Authenticator(authUser, emailPassword);
			props.setProperty("mail.smtp.submitter", authenticator.getPasswordAuthentication().getUserName());
			props.setProperty("mail.smtp.auth", "true");
			props.put("mail.smtp.ssl.trust", smtpHost);
			props.setProperty("mail.smtp.starttls.enable", "true");
			props.setProperty("mail.smtp.ssl.protocols", "TLSv1.2");
			if(emailPort > 0) {
				props.setProperty("mail.smtp.port", String.valueOf(emailPort));
			} else {
				props.setProperty("mail.smtp.port", "587");	
			}

			log.info("Trying to send email as html with authentication");
		} else {
			if(emailPort > 0) {
				props.setProperty("mail.smtp.port", String.valueOf(emailPort));
			} else {
				// Use default port (25?)
			}
			log.info("No authentication");
		}

		props.setProperty("mail.smtp.connectiontimeout", "60000");
		props.setProperty("mail.smtp.timeout", "60000");
		props.setProperty("mail.smtp.writetimeout", "60000");
		
		//log.info("Email properties: " + props.toString());
		
		return Session.getInstance(props, authenticator);
	}
}
