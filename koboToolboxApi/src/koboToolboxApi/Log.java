package koboToolboxApi;
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

import model.LogItemDt;
import model.LogsDt;

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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.HourlyLogSummaryItem;
import org.smap.sdal.model.OrgLogSummaryItem;

/*
 * Provides access to collected data
 */
@Path("/v1/log")
public class Log extends Application {
	
	Authorise a = null;
	Authorise aOrg = null;
	
	private static Logger log =
			 Logger.getLogger(Log.class.getName());
	
	public Log() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
		
		 authorisations = new ArrayList<String> ();
		 authorisations.add(Authorise.ORG);
		 aOrg = new Authorise(authorisations, null);
		 
	}
	
	@GET
	@Produces("application/json")
	public Response getLogs(@Context HttpServletRequest request,
			@QueryParam("start") int start,
			@QueryParam("length") int length,
			@QueryParam("limit") int limit,
			@QueryParam("sort") String sort,			// Column Name to sort on
			@QueryParam("dirn") String dirn				// Sort direction, asc || desc
			) { 
		
		String connectionString = "API - get logs";
		Response response = null;
		
		// Authorisation
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		if(dirn == null) {
			dirn = "desc";
		} else {
			dirn = dirn.replace("'", "''");
		}
		if(sort == null) {
			sort = "id";
		}
		if(dirn.equals("desc") && start == 0) {
			start = Integer.MAX_VALUE;
		}
		
		// Limit overrides length which is retained for backwards compatability
		if(limit > 0) {
			length = limit;
		}
		
		PreparedStatement pstmt = null;
		
		try {

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
		
			ArrayList<LogItemDt> logs = getLogEntries(sd, localisation, oId, dirn, start, sort, length, false);
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			response = Response.ok(gson.toJson(logs)).build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * DataTables API version 1 /log
	 * Get log entries
	 */
	@GET
	@Path("/dt")
	@Produces("application/json")
	public Response getDataTableLogs(@Context HttpServletRequest request,
			@QueryParam("draw") int draw,
			@QueryParam("start") int start,
			@QueryParam("length") int length,
			@QueryParam("sort") String sort,			// Column Name to sort on
			@QueryParam("dirn") String dirn				// Sort direction, asc || desc
			) { 
		
		String connectionString = "API - get logs - DataTable";
		Response response = null;
		String user = request.getRemoteUser();
		LogsDt logs = new LogsDt();
		logs.draw = draw;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		String sqlTotal = "select count(*) from log where o_id = ?";
		PreparedStatement pstmtTotal = null;
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		if(dirn == null) {
			dirn = "desc";
		} else {
			dirn = dirn.replace("'", "''");
		}
		if(sort == null) {
			sort = "id";
		}
		if(dirn.equals("desc") && start == 0) {
			start = Integer.MAX_VALUE;
		}
		
		try {
	
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			
			/*
			 * Get total log entries
			 */
			pstmtTotal = sd.prepareStatement(sqlTotal);
			pstmtTotal.setInt(1, oId);
			rs = pstmtTotal.executeQuery();
			if(rs.next()) {
				logs.recordsTotal = rs.getInt(1);
				logs.recordsFiltered = rs.getInt(1);
			}
			rs.close();
			
			logs.data = getLogEntries(sd,localisation, oId, dirn, start, sort, length, true);
						
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			response = Response.ok(gson.toJson(logs)).build();
			
	
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			try {if (pstmtTotal != null) {pstmtTotal.close();	}} catch (SQLException e) {	}
					
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
		
	}
	
	/*
	 * Hourly summary log
	 */
	@GET
	@Path("/hourly/{year}/{month}/{day}")
	@Produces("application/json")
	public Response getDailyLogs(@Context HttpServletRequest request,
			@PathParam("year") int year,
			@PathParam("month") int month,
			@PathParam("day") int day,
			@QueryParam("tz") String tz
			) { 
		
		String connectionString = "API - get summary hourly logs";
		Response response = null;
		
		// Authorisation
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		
		PreparedStatement pstmt = null;
		
		if(tz == null) {
			tz = "UTC";
		}
		
		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
		
			LogManager lm = new LogManager();
			ArrayList<HourlyLogSummaryItem> logs = lm.getSummaryLogEntriesForDay(sd, oId, year, month, day, tz);
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			response = Response.ok(gson.toJson(logs)).build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.ok(e.getMessage()).build();
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * Organisation summary log
	 */
	@GET
	@Path("/organisation/{year}/{month}/{day}")
	@Produces("application/json")
	public Response getDailyOrgLogs(@Context HttpServletRequest request,
			@PathParam("year") int year,
			@PathParam("month") int month,
			@PathParam("day") int day,
			@QueryParam("tz") String tz
			) { 
		
		String connectionString = "API - get summary org logs";
		Response response = null;
		
		// Authorisation
		Connection sd = SDDataSource.getConnection(connectionString);
		aOrg.isAuthorised(sd, request.getRemoteUser());
		
		PreparedStatement pstmt = null;
		
		if(tz == null) {
			tz = "UTC";
		}
		
		try {
			
			LogManager lm = new LogManager();
			ArrayList<OrgLogSummaryItem> logs = lm.getOrgSummaryLogEntriesForDay(sd, year, month, day, tz);
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			response = Response.ok(gson.toJson(logs)).build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.ok(e.getMessage()).build();
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * Get the data
	 */
	private ArrayList<LogItemDt> getLogEntries(
			Connection sd, 
			ResourceBundle localisation,
			int oId,
			String dirn,
			int start,
			String sort,
			int length,
			boolean forHtml) throws SQLException {
		
		ArrayList<LogItemDt> items = new ArrayList<> ();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {

			String sql = "select l.id, l.log_time, l.s_id, s.display_name, l.user_ident, l.event, l.note, l.server "
					+ "from log l "
					+ "left outer join survey s "
					+ "on s.s_id = l.s_id ";
			
			String sqlSelect = "where ";
			if(dirn.equals("asc")) {
				sqlSelect += "l.id > ? ";
			} else {
				sqlSelect += "l.id < ? ";
			}
			
			sqlSelect += "and l.o_id = ? ";
			
			String sqlOrder = "order by l." + sort + " " + dirn + " limit 10000";
			
			pstmt = sd.prepareStatement(sql + sqlSelect + sqlOrder);
			int paramCount = 1;
			pstmt.setInt(paramCount++, start);	
			pstmt.setInt(paramCount++, oId);
			
			log.info("Get data: " + pstmt.toString());
			rs = pstmt.executeQuery();
				
			int index = 0;	
			while (rs.next()) {
					
				if(length > 0 && index >= length) {
					break;
				}
				index++;
					
				LogItemDt li = new LogItemDt();
	
				li.id = rs.getInt("id");
				li.log_time = rs.getTimestamp("log_time");
				li.sId = rs.getInt("s_id");
				String displayName = rs.getString("display_name");
				if(displayName != null) {
					li.sName = GeneralUtilityMethods.getSafeText(displayName, forHtml);
				} else {
					if(li.sId > 0) {
						li.sName = li.sId + " (" + localisation.getString("c_erased") + ")";
					} else {
						li.sName = "";
					}
				}
				li.userIdent = rs.getString("user_ident");
				if(li.userIdent == null) {
					li.userIdent = "";
				}
				li.event = rs.getString("event");
				if(li.event == null) {
					li.event = "";
				}
				li.note = GeneralUtilityMethods.getSafeText(rs.getString("note"), forHtml);
				
				li.server = rs.getString("server");
				if(li.server == null) {
					li.server = "";
				}
						
				items.add(li);
			}
		} finally {
			try {if (rs != null) {rs.close();}} catch (SQLException e) {	}
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
		}
		
		return items;
	}


}

