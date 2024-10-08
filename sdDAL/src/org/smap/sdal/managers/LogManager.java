package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.HourlyLogSummaryItem;
import org.smap.sdal.model.LogItemDt;
import org.smap.sdal.model.OrgLogSummaryItem;

/*****************************************************************************

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

 ******************************************************************************/

/*
 * Manage the log table
 */
public class LogManager {
	
	private static Logger log =
			 Logger.getLogger(LogManager.class.getName());
	
	// Event types
	public static String API_CSV_VIEW = "API CSV view";
	public static String API_VIEW = "API view";
	public static String API_AUDIT_VIEW = "API audit view";
	public static String API_SINGLE_VIEW = "API single record view";
	public static String ARCHIVE = "archive";
	public static String BLOCK = "block";
	public static String CREATE = "create";
	public static String CASE_MANAGEMENT = "case management";
	public static String CREATE_PDF = "create pdf";
	public static String DASHBOARD_VIEW = "dashboard view";
	public static String DELETE = "delete";
	public static String EMAIL = "email";
	public static String EMAIL_TASK = "email task";
	public static String ERASE = "erase";
	public static String DUPLICATE = "duplicate";
	public static String ERROR = "error";
	public static String SUBMISSION_ERROR = "submission error";
	public static String EXPIRED = "expired";
	public static String GENERATE_REPORT_VIEW = "generate report view";
	public static String INSTANCE_VIEW = "Access instance data for a task";
	public static String LIMIT = "limit";
	public static String MAILOUT = "mailout";
	public static String MAPBOX_REQUEST = "Mapbox Request";
	public static String GOOGLE_REQUEST = "Google Request";
	public static String MAPTILER_REQUEST = "MapTiler Request";
	public static String MOVE_ORGANISATION = "move organisation";
	public static String MOVE_PROJECT = "move project";
	public static String NOTIFICATION = "notification";
	public static String NOTIFICATION_ERROR = "notification error";
	public static String OPTIN = "optin";
	public static String ORGANISATION_UPDATE = "organisation update";
	public static String PROJECT = "project";
	public static String REKOGNITION = "rekognition";
	public static String REMINDER = "reminder";
	public static String ROLE = "role";
	public static String REPLACE = "replace";
	public static String REPORT = "report";
	public static String RESOURCES = "resources";
	public static String RESTORE = "restore";
	public static String SECURITY = "security";
	public static String SUBMISSION = "submissions";
	public static String SMS = "SMS";
	public static String SUBMISSION_ANON = "anonymous submissions";
	public static String SUBMISSION_TASK = "task submissions";
	public static String TASK = "task";
	public static String TRANSCRIBE = "transcribe";
	public static String TRANSCRIBE_MEDICAL = "transcribe_medical";
	public static String TRANSLATE = "translate";
	public static String SENTIMENT = "sentiment";
	public static String USER = "user";
	public static String USER_DETAILS = "user details";
	public static String USER_ACTIVITY_VIEW = "user activity view";
	public static String USER_LOCATION_VIEW = "user location view";
	public static String VIEW = "view";
	
	/*
	 * Write a log entry that includes the survey id
	 */
	public void writeLog(
			Connection sd, 
			int sId,
			String uIdent,
			String event,
			String note,
			int measure,
			String server)  {
		
		String sql = "insert into log ("
				+ "log_time,"
				+ "s_id,"
				+ "o_id,"
				+ "e_id,"
				+ "user_ident,"
				+ "event,"
				+ "note,"
				+ "measure,"
				+ "server) values (now(), ?, ?, (select e_id from organisation where id = ?), ?, ?, ?, ?, ?);";

		PreparedStatement pstmt = null;
		
		try {
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, uIdent);
			if(oId <= 0) {
				 oId = GeneralUtilityMethods.getOrganisationIdForSurvey(sd, sId);
			}

			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, sId);
			pstmt.setInt(2, oId);
			pstmt.setInt(3, oId);
			pstmt.setString(4, uIdent);
			pstmt.setString(5,  event);
			pstmt.setString(6,  note);
			pstmt.setInt(7, measure);
			pstmt.setString(8,  server);
				
			pstmt.executeUpdate();
			
		} catch(Exception e) {
			log.log(Level.SEVERE, "SQL Error", e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	/*
	 * Write a log entry at the organisation level
	 */
	public void writeLogOrganisation(
			Connection sd, 
			int oId,
			String uIdent,
			String event,
			String note,
			int measure)  {
		
		String sql = "insert into log ("
				+ "log_time,"
				+ "s_id,"
				+ "o_id,"
				+ "user_ident,"
				+ "event,"
				+ "note,"
				+ "measure) values (now(), 0, ?, ?, ?, ?, ?);";

		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, oId);
			pstmt.setString(2, uIdent);
			pstmt.setString(3,  event);
			pstmt.setString(4,  note);
			pstmt.setInt(5, measure);
			
			pstmt.executeUpdate();

		} catch(Exception e) {
			log.log(Level.SEVERE, "SQL Error", e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	/*
	 * Get the summary data per hour
	 */
	public ArrayList<HourlyLogSummaryItem> getSummaryLogEntriesForDay(
			Connection sd, 
			int oId,
			int year,
			int month,
			int day,
			String tz) throws SQLException {
		
		ArrayList<HourlyLogSummaryItem> items = new ArrayList<> ();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {

			String sql = "select  count(*) as count, "
					+ "extract(hour from timezone(?, log_time)) as hour, "
					+ "event "
					+ "from log l "
					+ "where l.o_id = ? "
					+ "and timezone(?, l.log_time) >=  ? "
					+ "and timezone(?, l.log_time) < ? "
					+ "group by hour, event "
					+ "order by hour asc";
			
			Timestamp t1 = GeneralUtilityMethods.getTimestampFromParts(year, month, day);
			Timestamp t2 = GeneralUtilityMethods.getTimestampNextDay(t1);
			
			pstmt = sd.prepareStatement(sql);
			int paramCount = 1;
			pstmt.setString(paramCount++, tz);
			pstmt.setInt(paramCount++, oId);	
			pstmt.setString(paramCount++, tz);
			pstmt.setTimestamp(paramCount++, t1);
			pstmt.setString(paramCount++, tz);
			pstmt.setTimestamp(paramCount++, t2);
			
			log.info("Get data: " + pstmt.toString());
			rs = pstmt.executeQuery();
				
			int hour = -1;	
			HourlyLogSummaryItem item = null;
			while (rs.next()) {
			
				int dHour = rs.getInt("hour");
				if(dHour != hour) {
					item = new HourlyLogSummaryItem();
					items.add(item);
					item.hour = dHour;
					hour = dHour;
				}
				item.events.put(rs.getString("event"), rs.getInt("count"));

			}
		} finally {
			try {if (rs != null) {rs.close();}} catch (SQLException e) {	}
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
		}
		
		return items;
	}
	
	/*
	 * Get the summary data per orgaisation
	 */
	public ArrayList<OrgLogSummaryItem> getOrgSummaryLogEntriesForDay(
			Connection sd, 
			int year,
			int month,
			int day,
			String tz) throws SQLException {
		
		ArrayList<OrgLogSummaryItem> items = new ArrayList<> ();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		boolean monthly = false;
		
		if(day < 1) {
			monthly = true;
			day = 1;
		}
		try {

			String sql = "select  count(*) as count, "
					+ "l.event, o.name "
					+ "from log l, organisation o "
					+ "where l.o_id = o.id "
					+ "and timezone(?, l.log_time) >=  ? "
					+ "and timezone(?, l.log_time) < ? "
					+ "group by name, event "
					+ "order by name asc";
			
			Timestamp t1 = GeneralUtilityMethods.getTimestampFromParts(year, month, day);
			Timestamp t2;
			if(monthly) {
				t2 = GeneralUtilityMethods.getTimestampNextMonth(t1);
			} else {
				t2 = GeneralUtilityMethods.getTimestampNextDay(t1);
			}
			
		
			pstmt = sd.prepareStatement(sql);
			int paramCount = 1;
			pstmt.setString(paramCount++, tz);
			pstmt.setTimestamp(paramCount++, t1);
			pstmt.setString(paramCount++, tz);
			pstmt.setTimestamp(paramCount++, t2);
			
			log.info("Get data: " + pstmt.toString());
			rs = pstmt.executeQuery();
				
			String org = null;	
			OrgLogSummaryItem item = null;
			while (rs.next()) {
			
				String dOrg = rs.getString("name");
				if(org == null || !dOrg.equals(org)) {
					item = new OrgLogSummaryItem();
					items.add(item);
					item.organisation = dOrg;
					org = dOrg;
				}
				item.events.put(rs.getString("event"), rs.getInt("count"));

			}
		} finally {
			try {if (rs != null) {rs.close();}} catch (SQLException e) {	}
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
		}
		
		return items;
	}
	
	/*
	 * Get the log entries
	 */
	public ArrayList<LogItemDt> getLogEntries(
			Connection sd, 
			ResourceBundle localisation,
			int oId,
			String dirn,
			int start,
			String sort,
			int length,
			boolean forHtml,
			boolean getNonOrgEntries) throws SQLException {
		
		ArrayList<LogItemDt> items = new ArrayList<> ();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {

			StringBuilder sql = new StringBuilder("select l.id, l.log_time, l.s_id, s.display_name, l.user_ident, l.event, l.note, l.server "
					+ "from log l "
					+ "left outer join survey s "
					+ "on s.s_id = l.s_id "
					+ "where ");
			
			if(dirn.equals("asc")) {
				sql.append("l.id > ? ");
			} else {
				sql.append("l.id < ? ");
			}
			
			if(getNonOrgEntries) {
				sql.append("and (l.o_id = ? or l.o_id = -1) ");
			} else {
				sql.append("and l.o_id = ? ");
			}
			
			sql.append("order by l.").append(sort).append(" ").append(dirn);
			
			pstmt = sd.prepareStatement(sql.toString());
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
				populateLogItem(rs, li, localisation, forHtml);		
				items.add(li);
				
			}
		} finally {
			try {if (rs != null) {rs.close();}} catch (SQLException e) {	}
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
		}
		
		return items;
	}


	/*
	 * Get the log entries for a month
	 */
	public ArrayList<LogItemDt> getMonthLogEntries(
			Connection sd, 
			ResourceBundle localisation,
			int oId,
			int year,
			int month,
			String tz,
			boolean forHtml,
			boolean getNonOrgEntries) throws SQLException {
		
		ArrayList<LogItemDt> items = new ArrayList<> ();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {

			StringBuilder sql = new StringBuilder("select l.id, l.log_time, l.s_id, l.user_ident, l.event, l.note, l.server,"
					+ "(select display_name from survey where ident = s.group_survey_ident) as display_name "
					+ "from log l "
					+ "left outer join survey s "
					+ "on s.s_id = l.s_id "
					+ "where (l.o_id = ? ");
			if(getNonOrgEntries) {
				sql.append("or l.o_id = -1) ");
			} else {
				sql.append(") ");
			}
			sql.append("and timezone(?, l.log_time) >=  ? "
					+ "and timezone(?, l.log_time) < ? "
					+ "order by l.id desc");
			
			Timestamp t1 = GeneralUtilityMethods.getTimestampFromParts(year, month, 1);
			Timestamp t2 = GeneralUtilityMethods.getTimestampNextMonth(t1);
			
			pstmt = sd.prepareStatement(sql.toString());
			int paramCount = 1;
			pstmt.setInt(paramCount++, oId);	
			pstmt.setString(paramCount++, tz);
			pstmt.setTimestamp(paramCount++, t1);
			pstmt.setString(paramCount++, tz);
			pstmt.setTimestamp(paramCount++, t2);
			
			log.info("Get data for month: " + pstmt.toString());
			rs = pstmt.executeQuery();
				
			while (rs.next()) {
					
				LogItemDt li = new LogItemDt();
				populateLogItem(rs, li, localisation, forHtml);		
				items.add(li);
			}
		} finally {
			try {if (rs != null) {rs.close();}} catch (SQLException e) {	}
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
		}
		
		return items;
	}
	
	private void populateLogItem(ResultSet rs, LogItemDt li, ResourceBundle localisation, boolean forHtml) throws SQLException {
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
	}
	
}


