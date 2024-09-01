package surveyMobileAPI;

import javax.servlet.http.*;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.JWTManager;
import org.smap.sdal.managers.SMSManager;
import org.smap.sdal.model.SMSDetails;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import model.MessageVonage;

import java.sql.Connection;
import java.util.Enumeration;
import java.util.logging.Logger;

@Path("/sms/vonage")
public class Vonage extends Application {

	private static Logger log =
			 Logger.getLogger(Vonage.class.getName());
	
	private Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create(); 
    
	private JWTManager jwt = new JWTManager();
	
	@POST
	@Path("/inbound")
	@Consumes("application/json")
	public Response inbound(@Context HttpServletRequest request, String body) {
        
		Response response = null;
		String connectionString = "sms";
    	Connection sd = null;
    	
		/*
		 * Authenticate request
		 */
    	if(jwt.validate(request.getHeader("authorization"))) {
    		
	    	/*
	    	 * Get message details
	    	 */
	    	log.info("Sms: " + body);
	        MessageVonage inbound = gson.fromJson(body, MessageVonage.class);
	        SMSDetails sms = new SMSDetails(inbound.from, inbound.to, inbound.text, true, inbound.timestamp);
	        
	        /*
	         * Save SMS message for further processing
	         */
	        if(sms.ourNumber != null && sms.msg != null) {	// TODO allow null from number?
	        	
	        	try {
	        		sd = SDDataSource.getConnection(connectionString);
	        		
	        		SMSManager sim = new SMSManager(null, null);
	        		sim.saveMessage(sd, sms, request.getServerName(), inbound.message_uuid);
	        	} finally {
	        		SDDataSource.closeConnection(connectionString, sd);
	        	}
	        	response = Response.ok().build();
	        } else {
	        	log.info("Error: Invalid SMS message");
	        }
    	} else {
    		log.info("Error: Message Authorisation Failed ");
    		throw new AuthorisationException();
    	}
    	
        return response;
	}

	@POST
	@Path("/status")
	@Consumes("application/json")
	public Response status(@Context HttpServletRequest request, String body) {
        
		Response response = null;
		String connectionString = "sms";
    	Connection sd = null;
    	
		/*
		 * TODO Authenticate request
		 *  - Definitely from Vonage account
		 */
		
    	/*
    	 * Get message details
    	 */
        MessageVonage inbound = gson.fromJson(body, MessageVonage.class);
        SMSDetails sms = new SMSDetails(inbound.from, inbound.to, inbound.text, true, inbound.timestamp);
        
        /*
         * Save SMS message for further processing
         */
        if(sms.ourNumber != null && sms.msg != null) {	// TODO allow null from number?
        	
        	try {
        		sd = SDDataSource.getConnection(connectionString);
        		
        		//SMSManager sim = new SMSManager(null, null);
        		//sim.saveMessage(sd, sms, request.getServerName());
        	} finally {
        		SDDataSource.closeConnection(connectionString, sd);
        	}
        	response = Response.ok().build();
        } else {
        	log.info("Error: Invalid SMS message");
        }
        return response;
	}
}
