package sms;

import javax.servlet.*;
import javax.servlet.http.*;

import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.SMSInboundManager;
import org.smap.sdal.model.SMSDetails;

import java.sql.Connection;
import java.util.Collections;
import java.util.logging.Logger;

public class InboundSMSServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

	private static Logger log =
			 Logger.getLogger(InboundSMSServlet.class.getName());
	
    private String FROM_PARAM = "msisdn";
    private String TO_PARAM = "to";
    private String MSG_PARAM = "text";
    
	@Override
    protected void service(HttpServletRequest req,
                         HttpServletResponse resp)
            throws ServletException,
                   java.io.IOException {
        
		String connectionString = "sms";
    	Connection sd = null;
    	
		/*
		 * TODO Authenticate request
		 *  - Definitely from Vonage account
		 */
		
		/*
		 * Get the parameters
		 */
		
		
		System.out.println("Received SMS: " + req.getMethod());
        String theirNumber = null;
        String ourNumber = null;
        String msg = null;
        for (String param : Collections.list(req.getParameterNames())) {
            
        	String value = req.getParameter(param);
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
        		
        		SMSInboundManager sim = new SMSInboundManager();
        		sim.saveMessage(sd, sms, req.getServerName());
        	} finally {
        		SDDataSource.closeConnection(connectionString, sd);
        	}
        } else {
        	log.info("Error: Invalid SMS message");
        }
	}
}
