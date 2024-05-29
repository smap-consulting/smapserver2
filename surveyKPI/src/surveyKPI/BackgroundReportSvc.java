package surveyKPI;

/*
This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.

*/

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.BackgroundReportsManager;
import org.smap.sdal.model.BackgroundReport;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/background_report")
public class BackgroundReportSvc extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(BackgroundReportSvc.class.getName());


	public BackgroundReportSvc() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.VIEW_OWN_DATA);
		a = new Authorise(authorisations, null);		
	}

	@GET
	@Path("/{project_id}")
	@Produces("application/json")
	public Response getBackgroundReports(@Context HttpServletRequest request,
			@PathParam("project_id") int pId,
			@QueryParam("tz") String tz) { 
		
		ArrayList<BackgroundReport> reports = new ArrayList<>();
		String requestName = "surveyKPI - Get Background Reports";
		Response response = null;
		ResourceBundle localisation = null;
		
		if(tz == null) {
			tz = "UTC";
		}
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		Connection sd = SDDataSource.getConnection(requestName);
		a.isAuthorised(sd, request.getRemoteUser());
		
		PreparedStatement pstmt = null;
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			String sql = "select br.id, br.report_name, br.report_type, u.name, br.status,"
					+ "br.status_msg, br.filename, "
					+ "to_char(timezone(?, end_time), 'YYYY-MM-DD HH24:MI:SS') as end_time, "
					+ "extract(epoch from (end_time - start_time)) as duration "
					+ "from background_report br, users u "
					+ "where br.u_id = u.id "
					+ "and br.o_id = ? "
					+ "and (br.p_id = ? or br.p_id = 0) "
					+ "and (u.ident = ? or br.share) "
					+ "order by br.id desc";
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, tz);
			pstmt.setInt(2, oId);
			pstmt.setInt(3, pId);
			pstmt.setString(4, request.getRemoteUser());
			
			log.info("Get background reports: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				BackgroundReport br = new BackgroundReport();
				br.id = rs.getInt("id");
				br.report_name = rs.getString("report_name");
				br.report_type = rs.getString("report_type");
				br.userName = rs.getString("name");
				br.status = rs.getString("status");
				br.status_loc = localisation.getString("c_" + br.status);
				br.status_msg = rs.getString("status_msg");
				br.filename = rs.getString("filename");
				br.completed = rs.getString("end_time");
				br.duration = rs.getInt("duration");
				reports.add(br);
			}
			response = Response.ok(gson.toJson(reports)).build();	
			
		} catch (Exception e) {		
			log.log(Level.SEVERE,"Error: ", e);
			response = Response.serverError().entity(e.getMessage()).build();
		    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(requestName, sd);
		}
		return response;
	}
	
	/*
	 * Create a new background report
	 */
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response createBackgroundReport(
			@Context HttpServletRequest request, 
			@FormParam("report") String sReport,
			@QueryParam("tz") String tz					// Timezone
			) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		String connectionString = "surveyKPI - Create Background Report";
		Response response = null;
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		PreparedStatement pstmtDuplicate = null;
		PreparedStatement pstmt = null;
		
		if(tz == null) {
			tz = "UTC";
		}
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			BackgroundReport br = gson.fromJson(sReport, BackgroundReport.class);
			int uId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			// Start validation
			if(br.params != null) {
				String reportUserString = br.params.get(BackgroundReportsManager.PARAM_USER_ID);
				if(reportUserString != null) {
					int reportUser = 0;
					try {
						reportUser = Integer.valueOf(reportUserString);
					} catch(Exception e) {
						
					}
					if(reportUser > 0) {
						a.isValidUser(sd, request.getRemoteUser(), reportUser);
					}
				}
				
				int sId = GeneralUtilityMethods.getKeyValueInt(BackgroundReportsManager.PARAM_SURVEY_ID, br.params);
				if(sId > 0) {
					boolean superUser = false;
					try {
						superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
					} catch (Exception e) {
					}
					a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
				}
			}
			if(br.pId > 0) {
				a.isValidProject(sd, request.getRemoteUser(), br.pId);
			}
			
			/*
			 * For kontrolid servers a restore can only be initiated by the server owner
			 */
			String serverName = request.getServerName();
			if(serverName != null && serverName.contains("kontrolid") || serverName.equals("localhost")) {
				if(!GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.OWNER_ID)) {
					throw new ApplicationException("Only the server owner can now start a restore.  Contact Kontrolid.");
				}
			}
			/*
			 * Has this report already been requested
			 */
			String sqlDuplicate = "select count(*) from background_report "
					+ "where o_id = ? "
					+ "and u_id = ? "
					+ "and status = 'new' "
					+ "and report_type = ? "
					+ "and params = ?";
			
			pstmtDuplicate = sd.prepareStatement(sqlDuplicate);
			pstmtDuplicate.setInt(1, oId);
			pstmtDuplicate.setInt(2, uId);
			pstmtDuplicate.setString(3, br.report_type);
			pstmtDuplicate.setString(4, gson.toJson(br.params));
			log.info("background report duplicate check: " + pstmtDuplicate.toString());
			
			ResultSet rs = pstmtDuplicate.executeQuery();
			if(rs.next() && rs.getInt(1) > 0) {
				throw new ApplicationException(localisation.getString("rep_ar"));
			}
				
			/*
			 * Save the report
			 */
			String sql = "insert into background_report "
					+ "(o_id, u_id, p_id, status, report_type, report_name, tz, language, params, start_time) "
					+ "values(?, ?, ?, 'new', ?, ?, ?, ?, ?, now())";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setInt(2, uId);
			pstmt.setInt(3, br.pId);
			pstmt.setString(4, br.report_type);
			pstmt.setString(5, br.report_name);
			pstmt.setString(6, tz);
			pstmt.setString(7, locale.getLanguage());
			pstmt.setString(8, gson.toJson(br.params));
			log.info("background report insert " + pstmtDuplicate.toString());
			
			pstmt.executeUpdate();
			
			response = Response.ok().build();		
			
		} catch (ApplicationException e) {		
			response = Response.serverError().entity(e.getMessage()).build();
		    
		} catch (Exception e) {		
			log.log(Level.SEVERE,"Error: ", e);
			response = Response.serverError().entity(e.getMessage()).build();
		    
		} finally {
			try {if (pstmtDuplicate != null) {pstmtDuplicate.close();}} catch (SQLException e) {}
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
}

