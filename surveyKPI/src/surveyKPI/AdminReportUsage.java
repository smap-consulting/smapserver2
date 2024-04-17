package surveyKPI;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
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

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.FileManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.XLSXAdminReportsManager;
import org.smap.sdal.model.AR;


/*
 * Export a survey in XLSX format
 * This export follows the approach of CSV exports where a single sub form can be selected
 * Get access to a form for each user
 */
@Path("/adminreport/usage")
public class AdminReportUsage extends Application {

	Authorise a = null;
	Authorise aOrg = null;

	private static Logger log =
			Logger.getLogger(AdminReportUsage.class.getName());

	LogManager lm = new LogManager();		// Application log
	boolean includeTemporaryUsers;
	
	public AdminReportUsage() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
		
		ArrayList<String> authorisationsOrg = new ArrayList<String> ();	
		authorisationsOrg.add(Authorise.ORG);
		aOrg = new Authorise(authorisationsOrg, null);
	}
	
	/*
	 * Get usage for a specific month and user
	 */
	@GET
	@Path("/{year}/{month}/{user}")
	public Response exportSurveyXlsx (@Context HttpServletRequest request, 
			@PathParam("year") int year,
			@PathParam("month") int month,
			@PathParam("user") String userIdent,
			@QueryParam("project") boolean byProject,
			@QueryParam("survey") boolean bySurvey,
			@QueryParam("device") boolean byDevice,
			@QueryParam("inc_temp") boolean includeTemporaryUsers,
			@QueryParam("inc_alltime") boolean includeAllTimeUsers,
			@QueryParam("o_id") int oId,
			@QueryParam("tz") String tz,
			@Context HttpServletResponse response) {

		Response responseVal = null;
		
		// Authorisation - Access
		String connectionString = "surveyKPI - AdminReports - Usage";
		Connection sd = SDDataSource.getConnection(connectionString);		
		// End Authorisation		
		
		if(tz == null) {
			tz = "UTC";
		}
		
		try {
		
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			// start validation			
			if(oId > 0) {
				aOrg.isAuthorised(sd, request.getRemoteUser());
			} else {
				a.isAuthorised(sd, request.getRemoteUser());
			}
			String orgName = "";
			if(oId <= 0) {
				oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			} else {
				orgName = GeneralUtilityMethods.getOrganisationName(sd, oId);
			}
			int uId = GeneralUtilityMethods.getUserId(sd, userIdent);
			if(uId <= 0) {
				String msg = localisation.getString("unf");
				msg = msg.replace("%s1", userIdent);
				throw new Exception(msg);
			}
			a.isValidUser(sd, request.getRemoteUser(), uId);
						
			if(month < 1) {
				throw new ApplicationException(localisation.getString("ar_month_gt_0"));
			}
			// End Validation

			
			String filename = localisation.getString("ar_report_name") + (oId > 0 ? "_" + orgName : "") + year + "_" + month + "_" + userIdent + ".xlsx";
			
			ArrayList<AR> report = null;
			XLSXAdminReportsManager rm = new XLSXAdminReportsManager(localisation, tz);
			if(bySurvey) {
				report = rm.getAdminReportSurvey(sd, oId, month, year, userIdent);
			} else if(byProject) {
				report = rm.getAdminReportProject(sd, oId, month, year, userIdent);
			} else if(byDevice) {
				report = rm.getAdminReportDevice(sd, oId, month, year, userIdent);
			} else {
				report = rm.getAdminReport(sd, oId, month, year, userIdent);
			}
			
			ArrayList<String> header = new ArrayList<String> ();
			header.add(localisation.getString("ar_ident"));
			header.add(localisation.getString("ar_user_name"));
			header.add(localisation.getString("ar_user_created"));
			if(byProject || bySurvey) {
				header.add(localisation.getString("ar_project_id"));
				header.add(localisation.getString("ar_project"));
			}
			if(bySurvey) {
				header.add(localisation.getString("ar_survey_id"));
				header.add(localisation.getString("ar_survey"));
			}
			if(byDevice) {
				header.add(localisation.getString("a_device"));
			}
			header.add(localisation.getString("ar_usage_month"));
			header.add(localisation.getString("ar_usage_at"));
			
			// Get temp file
			String basePath = GeneralUtilityMethods.getBasePath(request);
			String filepath = basePath + "/temp/" + UUID.randomUUID();	// Use a random sequence to keep survey name unique
			File tempFile = new File(filepath);
			
			rm.getNewReport(sd, tempFile, header, report, byProject, bySurvey, byDevice, year, month,
					GeneralUtilityMethods.getOrganisationName(sd, oId));
			
			response.setHeader("Content-type",  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; charset=UTF-8");
			
			FileManager fm = new FileManager();
			fm.getFile(response, filepath, filename);
			
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
