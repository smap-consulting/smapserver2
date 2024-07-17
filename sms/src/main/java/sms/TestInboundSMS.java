package sms;

import java.sql.Connection;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SMSInboundManager;
import org.smap.sdal.model.SMSDetails;

/*
 * Test
 */
@Path("/test")
public class TestInboundSMS extends Application {

	Authorise a = null;

	LogManager lm = new LogManager();		// Application log
	
	/*
	 * Get surveys for a project
	 */
	@GET
	@Produces("application/json")
	@Path("/get")
	public Response getTemplateNames (@Context HttpServletRequest request) {

		Connection sd = null;
		Response response = null;
		String connectionString = "sms test";
		
		try {
			SMSDetails sms = new SMSDetails();
			sms.fromNumber = "9876543210";
			sms.toNumber = "0123456789";
			sms.msg = "Test number 4";
    		sd = SDDataSource.getConnection(connectionString);
    		
    		SMSInboundManager sim = new SMSInboundManager();
    		sim.saveMessage(sd, sms, request.getServerName());
    		
    		response = Response.ok().build();
    	} finally {
    		SDDataSource.closeConnection(connectionString, sd);
    	}
		
		return response;

	}
	

}
