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

/*
 * Not used?
 *
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.JsonAuthorisationException;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.model.Survey;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/log")
public class ReportLogs extends Application {

	Authorise a = new Authorise(null, Authorise.ENUM);
	
	private static Logger log =
			 Logger.getLogger(Survey.class.getName());
	
	/*
	 * Post log for user authenticated with a key
	 *
	@POST
	@Produces("application/json")
	@Path("/key/{key}")
	public Response postReportKey(
			@PathParam("key") String key,
			@FormParam("report") String report,
			@Context HttpServletRequest request) {
		
		String user = null;	
		String requester = "surveyKPI-ReportLogs - key";
		Connection connectionSD = SDDataSource.getConnection(requester);
		
		try {
			user = GeneralUtilityMethods.getDynamicUser(connectionSD, key);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			SDDataSource.closeConnection(requester, connectionSD);
		}
		
		if (user == null) {
			log.info("User not found for key");
			throw new JsonAuthorisationException();
		}

		return storeReport(request, report, user);
	}
	
	/*
	 * Post log for user authenticated with credentials
	 *
	@POST
	@Produces("application/json")
	public Response postReportCredentials(
			@Context HttpServletRequest request, 
			@FormParam("report") String assignInput) {
		
		return storeReport(request, assignInput, request.getRemoteUser());
	}
	
	
	
	/*
	 * Store the report
	 *
	public Response storeReport(@Context HttpServletRequest request, 
			String report,
			String userName) { 

		Response response = null;
		
		String requester = "surveyKPI-ReportLogs";
		Connection connectionSD = SDDataSource.getConnection(requester);
		
		// Authorisation not required
			
		PreparedStatement pstmt = null;

		try {
			String sql = null;
			int u_id;

			u_id = GeneralUtilityMethods.getUserId(connectionSD, userName);
			sql = "insert into log_report (u_id, report, upload_time) values(?, ?, now());";
			pstmt = connectionSD.prepareStatement(sql);	
			pstmt.setInt(1, u_id);
			pstmt.setString(2, report);

			pstmt.executeUpdate();
			
			response = Response.ok().build();
			log.info("userevent: " + userName + " : reported device error");	
				
		} catch (Exception e) {		
			response = Response.serverError().build();
			log.log(Level.SEVERE,"Exception", e);
		} finally {
			
			try {if ( pstmt != null ) { pstmt.close(); }} catch (Exception e) {}
			SDDataSource.closeConnection(requester, connectionSD);
		}
		
		return response;
	}
	
}
*/


