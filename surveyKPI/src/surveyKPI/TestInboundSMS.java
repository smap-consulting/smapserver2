package surveyKPI;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.UUID;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
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

	Authorise a = new Authorise(null, Authorise.OWNER);

	LogManager lm = new LogManager();		// Application log
	
	/*
	 * Get surveys for a project
	 */
	@GET
	@Produces("application/json")
	@Path("/get")
	public Response getTemplateNames (@Context HttpServletRequest request) {

		Response response = null;
		String connectionString = "sms test";
		
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		try {
			Timestamp ts = new Timestamp(System.currentTimeMillis());
			ConversationItemDetails sms = new ConversationItemDetails("9876543212", "0123456789", 
					"Test number 5", true, ConversationItemDetails.SMS_CHANNEL, ts);
    		
    		SMSManager sim = new SMSManager(null,null);
    		sim.saveMessage(sd, sms, request.getServerName(), UUID.randomUUID().toString(), SMSManager.SMS_TYPE, "Test");
    		
    		response = Response.ok().build();
    	} finally {
    		SDDataSource.closeConnection(connectionString, sd);
    	}
		
		return response;

	}
	

}
