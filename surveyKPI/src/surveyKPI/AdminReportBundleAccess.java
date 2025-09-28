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
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;

import utilities.XLSXAdminReportsManagerBundleAccess;

/*
 * Export details on how each user in the organisation can access surveys in a bundle
 */
@Path("/adminreport/bundleaccess")
public class AdminReportBundleAccess extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(AdminReportBundleAccess.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	public AdminReportBundleAccess() {
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
			
			XLSXAdminReportsManagerBundleAccess rm = new XLSXAdminReportsManagerBundleAccess(localisation);
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
