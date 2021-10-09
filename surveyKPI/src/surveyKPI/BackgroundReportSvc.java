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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
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
		a = new Authorise(authorisations, null);		
	}

	/*
	 * Create a new background report
	 */
	@POST
	@Consumes("application/json")
	public Response createBackgroundReport(
			@Context HttpServletRequest request, 
			@FormParam("report") String sReport
			) { 
		
		String requestName = "surveyKPI - Create Background Report";
		Response response = null;
		
		System.out.println(sReport);
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		Connection sd = SDDataSource.getConnection(requestName);
		a.isAuthorised(sd, request.getRemoteUser());
		
		PreparedStatement pstmtDuplicate = null;
		PreparedStatement pstmt = null;
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			BackgroundReport br = gson.fromJson(sReport, BackgroundReport.class);
			int uId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			/*
			 * Has this report already been requested
			 */
			String sqlDuplicate = "select count(*) from background_report "
					+ "where o_id = ? "
					+ "and u_id = ? "
					+ "and status = 'new' "
					+ "and report_type = ? "
					+ "and details = ?";
			
			pstmtDuplicate = sd.prepareStatement(sqlDuplicate);
			pstmtDuplicate.setInt(1, oId);
			pstmtDuplicate.setInt(2, uId);
			pstmtDuplicate.setString(3, br.report_type);
			pstmtDuplicate.setString(4, br.details);
			log.info("background report duplicate check: " + pstmtDuplicate.toString());
			
			ResultSet rs = pstmtDuplicate.executeQuery();
			if(rs.next() && rs.getInt(1) > 0) {
				throw new ApplicationException(localisation.getString("rep_ar"));
			}
				
			/*
			 * Save the report
			 */
			String sql = "insert into background_report "
					+ "(o_id, u_id, status, report_type, report_name, details, start_time) "
					+ "values(?, ?, 'new', ?, ?, ?, now())";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setInt(2, uId);
			pstmt.setString(3, br.report_type);
			pstmt.setString(4, br.report_name);
			pstmt.setString(5, br.details);
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
			SDDataSource.closeConnection(requestName, sd);
		}
		
		return response;
	}
}

