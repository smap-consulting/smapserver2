package org.smap.sdal.model;

import java.util.ResourceBundle;
import java.util.logging.Logger;

/*
 * Form Class
 * Used for storing details of an email server
 */
abstract public class EmailServer {
	protected static Logger log = Logger.getLogger(EmailServer.class.getName());
	
	protected ResourceBundle localisation;
	
	public String smtpHost;
	public String emailDomain;
	public String emailUser;
	public String emailPassword;
	public int emailPort;
	
	EmailServer(ResourceBundle localisation) {
		this.localisation = localisation;
	}
	
	public abstract void send(String email, String ccType, String subject, 
			String emailId, 
			String contentString,
			String filePath,
			String filename) throws Exception;
}
