package surveyKPI;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.AR;

import utilities.XLSXAdminReportsManager;
import utilities.XLSXReportsManager;

/*
 * Export a survey in XLSX format
 * This export follows the approach of CSV exports where a single sub form can be selected
 *    
 */
@Path("/adminreport/usage/{year}/{month}")
public class AdminReports extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(AdminReports.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	public AdminReports() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get an export with a user authenticated by the web server
	 */
	@GET
	public Response exportSurveyXlsx (@Context HttpServletRequest request, 
			@PathParam("year") int year,
			@PathParam("month") int month,
			@QueryParam("project") boolean byProject,
			@QueryParam("survey") boolean bySurvey,
			
			@Context HttpServletResponse response) {

		Response responseVal;
		
		// Authorisation - Access
		String connectionString = "surveyKPI - AdminReports";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			if(month < 1) {
				throw new ApplicationException(localisation.getString("ar_month_gt_0"));
			}
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser(), 0);
			
			String filename = "report";
			ArrayList<AR> report = null;
			if(bySurvey) {
				report = getAdminReportSurvey(sd, oId, month, year);
			} else if(byProject) {
				report = getAdminReportProject(sd, oId, month, year);
			} else {
				report = getAdminReport(sd, oId, month, year);
			}
			
			ArrayList<String> header = new ArrayList<String> ();
			header.add(localisation.getString("ar_ident"));
			header.add(localisation.getString("ar_user_name"));
			if(byProject || bySurvey) {
				header.add(localisation.getString("ar_project"));
			}
			if(bySurvey) {
				header.add(localisation.getString("ar_survey"));
			}
			header.add(localisation.getString("ar_usage_month"));
			header.add(localisation.getString("ar_usage_at"));
			
			XLSXAdminReportsManager rm = new XLSXAdminReportsManager(localisation);
			responseVal = rm.getNewReport(sd, request, response, header, report, filename, byProject, bySurvey, year, month);
			
		} catch(Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return responseVal;

	}

	private ArrayList<AR> getAdminReport(Connection sd, int oId, int month, int year) throws SQLException {
		ArrayList<AR> rows = new ArrayList<AR> ();
		String sql = "select users.id as id,users.ident as ident, users.name as name, "
				+ "(select count (*) from upload_event ue, subscriber_event se "
					+ "where ue.ue_id = se.ue_id "
					+ "and se.status = 'success' "
					+ "and se.subscriber = 'results_db' "
					+ "and extract(month from upload_time) = ? "
					+ "and extract(year from upload_time) = ? "
					+ "and ue.user_name = users.ident) as month,"
				+ "(select count (*) from upload_event ue, subscriber_event se "
					+ "where ue.ue_id = se.ue_id and se.status = 'success' "
					+ "and se.subscriber = 'results_db' "
					+ "and ue.user_name = users.ident) as all_time "
				+ "from users "
				+ "where users.o_id = ? "
				+ "and not users.temporary "
				+ "order by users.ident";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, month);
			pstmt.setInt(2, year);
			pstmt.setInt(3, oId);
			log.info("Admin report: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				AR ar = new AR();
				ar.userIdent = rs.getString("ident");
				ar.userName = rs.getString("name");
				ar.usageInPeriod = rs.getInt("month");
				ar.allTimeUsage = rs.getInt("all_time");
				rows.add(ar);
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return rows;
	}

	private ArrayList<AR> getAdminReportProject(Connection sd, int oId, int month, int year) throws SQLException {
		ArrayList<AR> rows = new ArrayList<AR> ();
		String sql = "SELECT users.id as id,users.ident as ident, users.name as name, project.name as project_name, "
				+ "(select count (*) from upload_event ue, subscriber_event se "
					+ "where ue.ue_id = se.ue_id "
					+ "and se.status = 'success' "
					+ "and se.subscriber = 'results_db' "
					+ "and extract(month from upload_time) = ? "
					+ "and extract(year from upload_time) = ? "
					+ "and ue.user_name = users.ident "
					+ "and ue.p_id = project.id) as month, "
				+ "(select count (*) from upload_event ue, subscriber_event se "
					+ "where ue.ue_id = se.ue_id "
					+ "and se.status = 'success' "
					+ "and se.subscriber = 'results_db' "
					+ "and ue.user_name = users.ident "
					+ "and ue.p_id = project.id) as all_time "
				+ "from users, project "
				+ "where users.o_id = ? "
				+ "and not users.temporary "
				+ "and project.o_id = ? "
				+ "order by users.ident, project.name";
			
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, month);
			pstmt.setInt(2, year);
			pstmt.setInt(3, oId);
			pstmt.setInt(4, oId);
			log.info("Admin report: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				AR ar = new AR();
				ar.userIdent = rs.getString("ident");
				ar.userName = rs.getString("name");
				ar.project = rs.getString("project_name");
				ar.usageInPeriod = rs.getInt("month");
				ar.allTimeUsage = rs.getInt("all_time");
				rows.add(ar);
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return rows;
	}

	private ArrayList<AR> getAdminReportSurvey(Connection sd, int oId, int month, int year) throws SQLException {
		ArrayList<AR> rows = new ArrayList<AR> ();
		String sql = "SELECT users.id as id,users.ident as ident, users.name as name, project.name as project_name, survey.display_name as survey_name, "
				+ "(select count (*) from upload_event ue, subscriber_event se "
					+ "where ue.ue_id = se.ue_id "
					+ "and se.status = 'success' "
					+ "and se.subscriber = 'results_db' "
					+ "and extract(month from upload_time) = ? "
					+ "and extract(year from upload_time) = ? "
					+ "and ue.user_name = users.ident "
					+ "and ue.p_id = project.id "
					+ "and ue.s_id = survey.s_id) as month, "
				+ "(select count (*) from upload_event ue, subscriber_event se "
					+ "where ue.ue_id = se.ue_id "
					+ "and se.status = 'success' "
					+ "and se.subscriber = 'results_db' "
					+ "and ue.user_name = users.ident "
					+ "and ue.p_id = project.id "
					+ "and ue.s_id = survey.s_id) as all_time "
				+ "from users, project, survey "
				+ "where users.o_id = ? "
				+ "and not users.temporary "
				+ "and project.o_id = ? "
				+ "and project.id = survey.p_id "
				+ "order by users.ident, project.name, survey.display_name";
			
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, month);
			pstmt.setInt(2, year);
			pstmt.setInt(3, oId);
			pstmt.setInt(4, oId);
			log.info("Admin report: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				AR ar = new AR();
				ar.userIdent = rs.getString("ident");
				ar.userName = rs.getString("name");
				ar.project = rs.getString("project_name");
				ar.survey = rs.getString("survey_name");
				ar.usageInPeriod = rs.getInt("month");
				ar.allTimeUsage = rs.getInt("all_time");
				rows.add(ar);
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return rows;
	}
}
