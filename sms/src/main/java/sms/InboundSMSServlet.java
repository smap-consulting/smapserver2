package sms;

import javax.servlet.http.*;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.SMSManager;
import org.smap.sdal.model.SMSDetails;

import java.sql.Connection;
import java.util.Collections;
import java.util.logging.Logger;

@Path("/inbound")
public class InboundSMSServlet extends Application {

	private static Logger log =
			 Logger.getLogger(InboundSMSServlet.class.getName());
	
    private String FROM_PARAM = "msisdn";
    private String TO_PARAM = "to";
    private String MSG_PARAM = "text";
    
	@GET
	public Response inbound(@Context HttpServletRequest request) {
        
		Response response = null;
		String connectionString = "sms";
    	Connection sd = null;
    	
		/*
		 * TODO Authenticate request
		 *  - Definitely from Vonage account
		 */
		
		/*
		 * Get the parameters
		 */	
		System.out.println("Received SMS: " + request.getMethod());
        String theirNumber = null;
        String ourNumber = null;
        String msg = null;
        for (String param : Collections.list(request.getParameterNames())) {
            
        	String value = request.getParameter(param);
            System.out.println(param + ": " + value);
            
            if(FROM_PARAM.equals(param)) {
            	theirNumber = value;
            } else if(TO_PARAM.equals(param)) {
            	ourNumber = value;
            } else if(MSG_PARAM.equals(param)) {
            	msg = value;
            }         
        }
        SMSDetails sms = new SMSDetails(theirNumber, ourNumber, msg, true);
        
        /*
         * Save SMS message for further processing
         */
        if(sms.ourNumber != null && sms.msg != null) {	// TODO allow null from number?
        	
        	try {
        		sd = SDDataSource.getConnection(connectionString);
        		
        		SMSManager sim = new SMSManager();
        		sim.saveMessage(sd, sms, request.getServerName());
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
