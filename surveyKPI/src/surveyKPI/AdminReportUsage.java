package surveyKPI;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
import org.smap.sdal.managers.BackgroundReportsManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.XLSXAdminReportsManager;
import org.smap.sdal.model.AR;
import org.smap.sdal.model.BackgroundReport;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


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
	 * Get usage for a specific month
	 */
	@POST
	public Response exportSurveyXlsx (@Context HttpServletRequest request, 
			@FormParam("report") String sReport,
			@Context HttpServletResponse response) {

		Response responseVal = null;
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		System.out.println(sReport);
		// Authorisation - Access
		String connectionString = "surveyKPI - AdminReports - Usage";
		Connection sd = SDDataSource.getConnection(connectionString);		
		// End Authorisation		
		
		try {
		
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			BackgroundReport br = gson.fromJson(sReport, BackgroundReport.class);
			
			// Get params
			int oId = GeneralUtilityMethods.getKeyValueInt(BackgroundReportsManager.PARAM_O_ID, br.params);
			int month = GeneralUtilityMethods.getKeyValueInt(BackgroundReportsManager.PARAM_MONTH, br.params);	
			int year = GeneralUtilityMethods.getKeyValueInt(BackgroundReportsManager.PARAM_YEAR, br.params);	
			boolean bySurvey = GeneralUtilityMethods.getKeyValueBoolean(BackgroundReportsManager.PARAM_BY_SURVEY, br.params);	
			boolean byProject = GeneralUtilityMethods.getKeyValueBoolean(BackgroundReportsManager.PARAM_BY_PROJECT, br.params);
			boolean byDevice = GeneralUtilityMethods.getKeyValueBoolean(BackgroundReportsManager.PARAM_BY_DEVICE, br.params);
			includeTemporaryUsers = GeneralUtilityMethods.getKeyValueBoolean(BackgroundReportsManager.PARAM_INC_TEMP, br.params);
			
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
						
			if(month < 1) {
				throw new ApplicationException(localisation.getString("ar_month_gt_0"));
			}
			// End Validation

			
			String filename = localisation.getString("ar_report_name") + "_" + (oId > 0 ? orgName + "_" : "") + year + "_" + month;
			
			ArrayList<AR> report = null;
			//if(bySurvey) {
			//	report = getAdminReportSurvey(sd, oId, month, year);
			//} else if(byProject) {
			//	report = getAdminReportProject(sd, oId, month, year);
			//} else if(byDevice) {
			//	report = getAdminReportDevice(sd, oId, month, year);
			//} else {
			//	report = getAdminReport(sd, oId, month, year);
			//}
			
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
			GeneralUtilityMethods.createDirectory(basePath + "/reports");
			String filepath = basePath + "/reports/" + filename;	// Use a random sequence to keep survey name unique
			File tempFile = new File(filepath);
			
			XLSXAdminReportsManager rm = new XLSXAdminReportsManager(localisation);
			rm.getNewReport(sd, tempFile, header, report, byProject, bySurvey, byDevice, year, month,
					GeneralUtilityMethods.getOrganisationName(sd, oId));
			
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
