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
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import utilities.XLSXAdminReportsManagerFormAccess;

/*
 * Export details on how each user in the organisation can access a survey or are blocked from accessing a survey.
 */
@Path("/adminreport/formaccess")
public class AdminReportFormAccess extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(AdminReportFormAccess.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	public AdminReportFormAccess() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get report on ability of users to access a form
	 */
	@GET
	@Path("/{formIdent}")
	public Response exportSurveyXlsx (@Context HttpServletRequest request, 
			@PathParam("formIdent") String formIdent,
			
			@Context HttpServletResponse response) {

		Response responseVal;
		
		// Authorisation - Access
		String connectionString = "surveyKPI - AdminReports - Form Access";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			String filename = localisation.getString("ar_report_name") + "_" + formIdent ;
			
			XLSXAdminReportsManagerFormAccess rm = new XLSXAdminReportsManagerFormAccess(localisation);
			responseVal = rm.getNewReport(sd, request, response, filename, oId, formIdent);
			
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
