package surveyKPI;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import utilities.XLSXAdminReportsNotifications;

/*
 * Export a survey in XLSX format
 * This export follows the approach of CSV exports where a single sub form can be selected
 * Get usage per user
 */
@Path("/adminreport/notifications")
public class AdminReportNotifications extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(AdminReportNotifications.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	public AdminReportNotifications() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get report on notifications
	 */
	@GET
	public Response exportNotifications (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@QueryParam("tz") String tz) {

		Response responseVal;
		
		// Authorisation - Access
		String connectionString = "surveyKPI - AdminReports - Notifications";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		if(tz == null) {
			tz = "UTC";
		}
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			String filename = localisation.getString("ar_report_name");
			
			XLSXAdminReportsNotifications rn = new XLSXAdminReportsNotifications(localisation);
			responseVal = rn.getNewReport(sd, request, response, filename, oId, tz);
			
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
