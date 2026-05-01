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
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import utilities.XLSXAdminReportsResourceUsage;

/*
 * Export a survey in XLSX format
 * This export follows the approach of CSV exports where a single sub form can be selected
 * Get access to a form for each user
 */
@Path("/adminreport/resourceusage")
public class AdminReportResourceUsage extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(AdminReportResourceUsage.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	public AdminReportResourceUsage() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get usage for a specific month
	 */
	@GET
	public Response exportSurveyXlsx (@Context HttpServletRequest request,	
			@Context HttpServletResponse response) {

		Response responseVal;
		
		// Authorisation - Access
		String connectionString = "surveyKPI - AdminReports - Usage";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());			
			String filename = localisation.getString("ar_report_name");
			
			XLSXAdminReportsResourceUsage ru = new XLSXAdminReportsResourceUsage(localisation);
			responseVal = ru.getNewReport(sd, request, response, filename, oId);
			
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
