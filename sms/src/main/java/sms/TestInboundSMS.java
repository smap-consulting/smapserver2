package sms;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.UUID;

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
import org.smap.sdal.managers.SMSManager;
import org.smap.sdal.model.ConversationItemDetails;

/*
 * Test
 */
@Path("/testSMS")
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
			Timestamp ts = new Timestamp(System.currentTimeMillis());
			ConversationItemDetails sms = new ConversationItemDetails("9876543212", "0123456789", "Test number 5", true, 
					ConversationItemDetails.SMS_CHANNEL, ts);
    		sd = SDDataSource.getConnection(connectionString);
    		
    		SMSManager sim = new SMSManager(null,null);
    		sim.saveMessage(sd, sms, request.getServerName(), UUID.randomUUID().toString());
    		
    		response = Response.ok().build();
    	} finally {
    		SDDataSource.closeConnection(connectionString, sd);
    	}
		
		return response;

	}
	

}
