package surveyKPI;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.QueueManager;
import org.smap.sdal.managers.UsageManager;
import org.smap.sdal.model.QueueTime;

import utilities.XLSXAdminReportsQueues;
import utilities.XLSXAdminReportsResourceUsage;

/*
 * Queue reports
 */
@Path("/adminreport/queues")
public class AdminReportQueues extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(AdminReportQueues.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	public AdminReportQueues() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.OWNER);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get history of queue states
	 */
	@GET
	@Path("/{interval}")
	public Response exportQueueHistoryXlsx (@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("interval") int interval,
			@QueryParam("tz") String tz) {

		Response responseVal;
		String connectionString = "API - report queue history";

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
		
			QueueManager qm = new QueueManager();
			ArrayList<QueueTime> data = qm.getHistory(sd, interval, tz, request.getRemoteUser());
		
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());			
		
			XLSXAdminReportsQueues ru = new XLSXAdminReportsQueues(localisation);
			responseVal = ru.getNewReport(sd, request, response, data, "queues", oId);
		} catch(Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return responseVal;

	}

}
