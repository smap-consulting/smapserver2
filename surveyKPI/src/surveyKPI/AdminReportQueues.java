package surveyKPI;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.UsageManager;


/*
 * Export details on the state of each system queue
 */
@Path("/adminreport/queues")
public class AdminReportQueues extends Application {

	Authorise a = null;
	Authorise aOrg = null;

	LogManager lm = new LogManager();		// Application log
	
	public AdminReportQueues() {
		
	}
	
	/*
	 * Get usage for a specific month and user
	 */
	@GET

	public Response exportSurveyXlsx (@Context HttpServletRequest request, 
			@Context HttpServletResponse response) {

		Response resp = null;
		return resp;

	}

}
