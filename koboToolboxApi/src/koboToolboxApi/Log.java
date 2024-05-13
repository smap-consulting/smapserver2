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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
import org.smap.sdal.model.LogItemDt;
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
			@QueryParam("dirn") String dirn,			// Sort direction, asc || desc
			@QueryParam("month") int month,
			@QueryParam("year") int year,
			@QueryParam("tz") String tz
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
		if(tz == null || tz.equals("undefined")) {
			tz = "UTC";
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
		
			LogManager lm = new LogManager();
			
			ArrayList<LogItemDt> logs = null;
			if(year > 0 && month > 0) {
				logs = lm.getMonthLogEntries(sd, localisation, oId, year, month, tz, false);
			} else {
				logs = lm.getLogEntries(sd, localisation, oId, dirn, start, sort, length, false);
			}
			
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
	 * Get event counts for all organisations
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
	 * Organisation summary log
	 * Get event counts for all organisations
	 */
	@GET
	@Path("/organisation/{year}/{month}")
	@Produces("application/json")
	public Response getMonthlyOrgLogs(@Context HttpServletRequest request,
			@PathParam("year") int year,
			@PathParam("month") int month,
			@QueryParam("tz") String tz
			) { 
		
		String connectionString = "API - get monthly summary org logs";
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
			ArrayList<OrgLogSummaryItem> logs = lm.getOrgSummaryLogEntriesForDay(sd, year, month, -1, tz);
			
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
}

