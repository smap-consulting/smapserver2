package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.Case;
import org.smap.sdal.model.CaseCount;
import org.smap.sdal.model.CaseManagementAlert;
import org.smap.sdal.model.CaseManagementSettings;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
 * This class supports access to Case Management information in the database
 * All surveys in a bundle share the same case management set up
 */
public class CaseManager {
	
	private static Logger log =
			 Logger.getLogger(CaseManager.class.getName());

	LogManager lm = new LogManager(); // Application log
	ResourceBundle localisation = null;
	Gson gson;
	
	public CaseManager(ResourceBundle l) {
		localisation = l;
		gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
	}

	/*
	 * Get Case Management settings
	 */
	public CMS getCaseManagementSettings(Connection sd, String groupSurveyIdent) throws SQLException {
		
		PreparedStatement pstmt = null;
		CMS cms = null;
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		try {
			String sql = null;
			ResultSet rs = null;
			
			/*
			 * Get the settings for this survey group
			 */
			CaseManagementSettings settings = new CaseManagementSettings();
			ArrayList<CaseManagementAlert> alerts = new ArrayList<>();
			
			sql = "select id, "
					+ "settings, "
					+ "changed_by as changed_by,"
					+ "changed_ts as changed_ts "
					+ "from cms_setting "
					+ "where group_survey_ident = ? ";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  groupSurveyIdent);
			log.info("Get case management settings: " + pstmt.toString());
			rs = pstmt.executeQuery();
							
			if(rs.next()) {
				String sString = rs.getString("settings");
				
				if(sString != null ) {
					settings = gson.fromJson(sString, CaseManagementSettings.class); 
				} 
			}
			
			if(settings != null) {
				/*
				 * Get the alerts for this survey group
				 * Note the alerts are stored in separate records in the database so
				 * they can be used in queries to find active notification alerts
				 */
				sql = "select id, name, period, filter "
						+ "from cms_alert "
						+ "where group_survey_ident = ? "
						+ "order by name asc";
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1,  groupSurveyIdent);
				
				rs = pstmt.executeQuery();
				while(rs.next()) {
					alerts.add(new CaseManagementAlert(rs.getInt("id"), 
							groupSurveyIdent, rs.getString("name"), 
							rs.getString("period"),
							rs.getString("filter")));
				}
				
				// Create the combined settings object
				cms = new CMS(settings, alerts, groupSurveyIdent);
			}
					    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return cms;
	}

	/*
	 * Create a new Case Management Alert
	 */
	public void createAlert(Connection sd, 
			String user,
			CaseManagementAlert alert) throws Exception {
		
		String sql = "insert into cms_alert (group_survey_ident, name, period, filter, changed_by, changed_ts) " +
				" values (?, ?, ?, ?, ?, now());";
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, alert.group_survey_ident);
			pstmt.setString(2, alert.name);
			pstmt.setString(3,  alert.period);
			pstmt.setString(4,  alert.filter);
			pstmt.setString(5,  user);

			log.info("SQL: " + pstmt.toString());
			pstmt.executeUpdate();
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}
		
		return;

	}
	
	/*
	 * Update a Case Management Setting
	 */
	public void updateAlert(Connection sd, 
			String user,
			CaseManagementAlert alert) throws Exception {
		
		String sql = "update cms_alert "
				+ "set group_survey_ident = ?, "
				+ "name = ?,"
				+ "period = ?,"
				+ "filter = ?,"
				+ "changed_by = ?,"
				+ "changed_ts = now() "
				+ "where id = ?"; 
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setString(1, alert.group_survey_ident);
			pstmt.setString(2, alert.name);
			pstmt.setString(3, alert.period);
			pstmt.setString(4, alert.filter);
			pstmt.setString(5, user);
			pstmt.setInt(6, alert.id);

			log.info("SQL: " + pstmt.toString());
			pstmt.executeUpdate();
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}

	}
	
	/*
	 * Update a Case Management Setting
	 */
	public void updateSettings(Connection sd, 
			String user,
			String group_survey_ident,
			CaseManagementSettings settings) throws Exception {
		
		String sql = "update cms_setting "
				+ "set settings = ?, "
				+ "changed_by = ?,"
				+ "changed_ts = now() "
				+ "where group_survey_ident = ?"; 
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setString(1, gson.toJson(settings));
			pstmt.setString(2, user);
			pstmt.setString(3, group_survey_ident);

			log.info("SQL: " + pstmt.toString());
			int count = pstmt.executeUpdate();
			if(count < 1) {
				try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
				
				sql = "insert into cms_setting "
						+ "(settings, changed_by, group_survey_ident, changed_ts) "
						+ "values(?,?, ?, now())"; 
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, gson.toJson(settings));
				pstmt.setString(2, user);
				pstmt.setString(3, group_survey_ident);
				log.info("SQL: " + pstmt.toString());
				pstmt.executeUpdate();
			}
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}

	}
	
	/*
	 * Delete a case management alert
	 */
	public void deleteAlert(Connection sd, 
			int id) throws Exception {
		
		String sql = "delete from cms_alert where id = ? ";
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setInt(1, id);

			log.info("SQL: " + pstmt.toString());
			pstmt.executeUpdate();
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	/*
	 * Get cases for a user and survey
	 */
	public ArrayList<Case> getCases(
			Connection sd, 
			Connection cResults,
			String sIdent,
			String sName,
			String groupSurveyIdent,
			String user,
			int sId) throws Exception {
		
		PreparedStatement pstmt = null;
			
		ArrayList<Case> cases = new ArrayList<>();
		
		try {
			/*
			 * Return all cases assigned to a user that are to be completed by the provided survey ident
			 * For transition match against the last submitting survey id if _case_survey is null)
			 * Cases should always use the same updateid hence set it to the thread value.  By doing this downloading the latest
			 *  instance of a case to fieldTask will not look like a new case has been created
			 */
			String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, sIdent);
			if(tableName != null) {

				GeneralUtilityMethods.ensureTableCurrent(cResults, tableName, true);		// Temporary - remove 2023

				StringBuilder sql = new StringBuilder("select instanceid, _thread, prikey, instancename, _hrk from ")
						.append(tableName)
						.append(" where not _bad and _assigned = ? and (_case_survey = ? or (_case_survey is null and _s_id = ?)) ");
				pstmt = cResults.prepareStatement(sql.toString());
				pstmt.setString(1, user);
				pstmt.setString(2,  sIdent);
				pstmt.setInt(3, sId);
				log.info("Get cases: " + pstmt.toString());
				ResultSet rs = pstmt.executeQuery();

				while(rs.next()) {
					String title = null;
					int prikey = rs.getInt("prikey");
					String instanceName = rs.getString("instancename");
					String thread = rs.getString("_thread");
					if(thread == null) {
						thread = rs.getString("instanceid");
					}
					String hrk = rs.getString("_hrk");
					title = sName + " - ";
					if(instanceName != null && instanceName.trim().length() > 0) {
						title = title + instanceName;
					} else if(hrk != null && hrk.trim().length() > 0) {
						title = title + hrk;
					} else {
						title = title + String.valueOf(prikey);
					}

					cases.add(new Case(prikey, title, thread));
				}
			}

		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		return cases;
	}
	
	public ArrayList<CaseCount> getOpenClosed(Connection sd, Connection cResults,
			String sIdent, String interval, int intervalCount, String aggregationInterval) throws SQLException {
		
		String table = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, sIdent);
		GeneralUtilityMethods.ensureTableCurrent(cResults, table, true);
		
		StringBuilder cte = new StringBuilder("with days as (select generate_series(")
				.append("date_trunc('day', now()) - '").append(intervalCount).append(" day'::interval,")
				.append("date_trunc('day', now()),")
				.append("'1 day'::interval")
				.append(") as day) ");
		
		StringBuilder sqlOpened = new StringBuilder("select days.day, count(")
				.append(table).append(".prikey) as opened from days left join ")
				.append(table)
				.append(" on date_trunc('day', _thread_created) = days.day ")
				.append("group by 1 order by 1 asc");
		
		StringBuilder sqlClosed = new StringBuilder("select days.day, count(")
				.append(table).append(".prikey) as closed from days left join ")
				.append(table)
				.append(" on date_trunc('day', _case_closed) = days.day ")
				.append("group by 1 order by 1 asc");
		
		ArrayList<CaseCount> cc = new ArrayList<>();
		
		if(table != null) {
			PreparedStatement pstmtOpened = null;
			PreparedStatement pstmtClosed = null;
			
			try {
			
				pstmtOpened = cResults.prepareStatement(cte.toString() + sqlOpened.toString());
				pstmtClosed = cResults.prepareStatement(cte.toString() + sqlClosed.toString());
				log.info("Open: " + pstmtOpened.toString());
				log.info("Closed: " + pstmtClosed.toString());
				ResultSet rs = pstmtOpened.executeQuery();
				ResultSet rsc = pstmtClosed.executeQuery();
				while(rs.next()) {
					rsc.next();
					String day = rs.getString(1);
					String [] dayComp = day.split(" ");
					cc.add(new CaseCount(dayComp[0], rs.getInt(2), rsc.getInt(2)));
				}
			} finally {
				if(pstmtOpened != null) {try {pstmtOpened.close();} catch(Exception e) {}}
				if(pstmtClosed != null) {try {pstmtClosed.close();} catch(Exception e) {}}
			}
		}
		return cc;
	}
	
	/*
	 * Method to assign a record to a user
	 */
	public int assignRecord(Connection sd, 
			Connection cResults, 
			ResourceBundle localisation, 
			String tablename, 
			String instanceId, 
			String assignTo, 
			String type,					// lock || release || assign
			String surveyIdent,
			String note,
			String requestingUser
			) throws SQLException {

		int count = 0;
		
		
		StringBuilder sql = new StringBuilder("update ") 
				.append(tablename) 
				.append(" set _assigned = ?, _case_survey = ? ")
				.append("where _thread = ? ");

		if(assignTo != null && assignTo.equals("_none")) {		// Assigning to no one
			assignTo = null;
			surveyIdent = null;
		}
		
		String thread = GeneralUtilityMethods.getThread(cResults, tablename, instanceId);
		String assignedUser = GeneralUtilityMethods.getAssignedUser(cResults, tablename, instanceId);
		
		String caseSurvey = surveyIdent;
		String details = null;
		if(type.equals("lock")) {
			sql.append("and _assigned is null");		// User can only self assign if no one else is assigned 
			details = localisation.getString("cm_lock");
			details = details.replace("%s1", assignTo);
		} else if(type.equals("release")) {
			assignTo = null;
			caseSurvey = null;
			sql.append("and _assigned = ?");			// User can only release records that they are assigned to
			details = localisation.getString("cm_release") + ": " + (note == null ? "" : note);
		} else {
			if(assignTo != null) {
				details = localisation.getString("assigned_to");
				details = details.replace("%s1", assignTo);
			} else {
				details = localisation.getString("cm_ua");
			}
		}
		
		if(assignTo != null) {
			assignTo = assignTo.trim();
		}
		
		PreparedStatement pstmt = cResults.prepareStatement(sql.toString());
		pstmt.setString(1, assignTo);
		pstmt.setString(2, caseSurvey);
		pstmt.setString(3,thread);
		if(type.equals("release")) {
			pstmt.setString(4,assignedUser);
		}
		log.info("Assign record: " + pstmt.toString());
		
		/*
		 * Write the event before applying the update so that an alert can be sent to the previously assigned user
		 */
		RecordEventManager rem = new RecordEventManager();
		rem.writeEvent(
				sd, 
				cResults, 
				RecordEventManager.ASSIGNED, 
				"success",
				requestingUser, 
				tablename, 
				instanceId, 
				null,				// Change object
				null,	            // Task Object
				null,				// Message Object
				null,				// Notification object
				details, 
				0,				    // sId (don't care legacy)
				null,
				0,				    // Don't need task id if we have an assignment id
				0				    // Assignment id
				);
		
		try {
					
			count = pstmt.executeUpdate();
			
			if(count > 0) {
				// Update the running total of cases maintained in the users table
				UserManager um = new UserManager(null);
				if(assignedUser != null) {
					um.decrementTotalTasks(sd, assignedUser);
				}
				if(assignTo != null) {
					um.incrementTotalTasks(sd, assignTo);
				}
			} else {
				log.info("Error: xxxxxxxxxxxxxx: count is " + count + " : " + pstmt.toString());
			}
			
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
		
		return count;
	}
}
