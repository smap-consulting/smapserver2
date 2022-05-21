package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.model.CMS;
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
	public CMS getCases(Connection sd, int o_id, String groupSurveyIdent) throws SQLException {
		
		PreparedStatement pstmt = null;
		CMS cms;
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
					+ "where o_id = ? "
					+ "and group_survey_ident = ? ";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, o_id);
			pstmt.setString(2,  groupSurveyIdent);
			log.info("Get case management settings: " + pstmt.toString());
			rs = pstmt.executeQuery();
							
			if(rs.next()) {
				String sString = rs.getString("settings");
				
				if(sString != null ) {
					settings = gson.fromJson(sString, CaseManagementSettings.class); 
				} 
			}
			
			/*
			 * Get the alerts for this survey group
			 * Note the alerts are stored in separate records in the database so
			 * they can be used in queries to find active notification alerts
			 */
			sql = "select id, name, period "
					+ "from cms_alert "
					+ "where o_id = ? "
					+ "and group_survey_ident = ? "
					+ "order by name asc";
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, o_id);
			pstmt.setString(2,  groupSurveyIdent);
			log.info("Get case management alerts: " + pstmt.toString());
			
			rs = pstmt.executeQuery();
			while(rs.next()) {
				alerts.add(new CaseManagementAlert(rs.getInt("id"), 
						groupSurveyIdent, rs.getString("name"), rs.getString("period")));
			}
			
			// Create the combined settings object
			cms = new CMS(settings, alerts, groupSurveyIdent);
					    
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
	 * Delete a case management alert
	 */
	public void deleteAlert(Connection sd, 
			int id, 
			int o_id, 
			String ident) throws Exception {
		
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

}
