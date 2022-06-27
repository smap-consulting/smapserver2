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
 * This class supports access to User and Organisation information in the database
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
				sql = "select id, name, period "
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
							rs.getString("period")));
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
			CaseManagementAlert alert, 
			int oId) throws Exception {
		
		String sql = "insert into cms_alert (o_id, group_survey_ident, name, period, changed_by, changed_ts) " +
				" values (?, ?, ?, ?, ?, now());";
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, alert.group_survey_ident);
			pstmt.setString(3, alert.name);
			pstmt.setString(4,  alert.period);
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
			CaseManagementAlert alert, 
			int o_id) throws Exception {
		
		String sql = "update cms_alert "
				+ "set group_survey_ident = ?, "
				+ "name = ?,"
				+ "period = ?,"
				+ "changed_by = ?,"
				+ "changed_ts = now() "
				+ "where o_id = ? "
				+ "and id = ?"; 
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setString(1, alert.group_survey_ident);
			pstmt.setString(2, alert.name);
			pstmt.setString(3, alert.period);
			pstmt.setString(4, user);
			pstmt.setInt(5, o_id);
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
			CaseManagementSettings settings, 
			int o_id) throws Exception {
		
		String sql = "update cms_setting "
				+ "set settings = ?, "
				+ "changed_by = ?,"
				+ "changed_ts = now() "
				+ "where o_id = ? "
				+ "and group_survey_ident = ?"; 
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setString(1, gson.toJson(settings));
			pstmt.setString(2, user);
			pstmt.setInt(3, o_id);
			pstmt.setString(4, group_survey_ident);

			log.info("SQL: " + pstmt.toString());
			int count = pstmt.executeUpdate();
			if(count < 1) {
				try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
				
				sql = "insert into cms_setting "
						+ "(settings, changed_by, o_id, group_survey_ident, changed_ts) "
						+ "values(?,?, ?, ?, now())"; 
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, gson.toJson(settings));
				pstmt.setString(2, user);
				pstmt.setInt(3, o_id);
				pstmt.setString(4, group_survey_ident);
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
			int id, 
			int o_id) throws Exception {
		
		String sql = "delete from cms_alert where id = ? "
				+ "and o_id = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setInt(1, id);
			pstmt.setInt(2, o_id);

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
			int sId,
			String groupSurveyIdent,
			String user) throws Exception {
		
		PreparedStatement pstmt = null;
			
		ArrayList<Case> cases = new ArrayList<>();
		
		try {
			/*
			 * Only return cases which can have a final status
			 */
			CMS cms = getCaseManagementSettings(sd, groupSurveyIdent);
			if(cms != null && cms.settings != null && cms.settings.statusQuestion != null) {
	
				String tableName = GeneralUtilityMethods.getMainResultsTable(sd, cResults, sId);
				if(GeneralUtilityMethods.hasColumn(cResults, tableName, cms.settings.statusQuestion)) {
					
					// Ignore cases that are bad or where the status value is null or if the case has been completed
					StringBuilder sql = new StringBuilder("select instanceid, _thread, prikey, instancename, _hrk from ")
							.append(tableName)
							.append(" where not _bad and _assigned = ? and ")
							.append(cms.settings.statusQuestion)
							.append(" != ?");
					pstmt = cResults.prepareStatement(sql.toString());
					pstmt.setString(1, user);
					pstmt.setString(2,  cms.settings.finalStatus);
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
						if(instanceName != null && instanceName.trim().length() > 0) {
							title = instanceName;
						} else if(hrk != null && hrk.trim().length() > 0) {
							title = hrk;
						} else {
							title = String.valueOf(prikey);
						}
						
						cases.add(new Case(prikey, title, thread));
					}
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
		
		PreparedStatement pstmtOpened = null;
		PreparedStatement pstmtClosed = null;
		
		try {
		
			pstmtOpened = cResults.prepareStatement(cte.toString() + sqlOpened.toString());
			pstmtClosed = cResults.prepareStatement(cte.toString() + sqlClosed.toString());
			
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
		return cc;
	}
}
