package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.model.CMS;

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
	
	public CaseManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Get available roles
	 */
	public ArrayList<CMS> getCases(Connection sd, int o_id, String groupSurveyIdent) throws SQLException {
		PreparedStatement pstmt = null;
		ArrayList<CMS> settings = new ArrayList<CMS> ();
		
		try {
			String sql = null;
			ResultSet rs = null;
			
			/*
			 * Get the case management settings for this organisation
			 */
			sql = "SELECT id, "
					+ "name, "
					+ "type,"
					+ "group_survey_ident,"
					+ "changed_by as changed_by,"
					+ "changed_ts as changed_ts "
					+ "from case_management_setting "
					+ "where o_id = ? "
					+ "and group_survey_ident = ? "
					+ "order by type, name asc";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, o_id);
			pstmt.setString(2,  groupSurveyIdent);
			log.info("Get case management settings: " + pstmt.toString());
			rs = pstmt.executeQuery();
							
			while(rs.next()) {
				settings.add(new CMS(rs.getInt("id"), rs.getString("name"), rs.getString("type"), rs.getString("group_survey_ident"), rs.getString("changed_by")));
			}

					    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return settings;
	}

	/*
	 * Create a new Case Management Setting
	 */
	public void createCMS(Connection sd, 
			CMS cms, 
			int oId, 
			String ident) throws Exception {
		
		String sql = "insert into case_management_setting (o_id, name, type, group_survey_ident, changed_by, changed_ts) " +
				" values (?, ?, ?, ?, ?, now());";
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, cms.name);
			pstmt.setString(3, cms.type);
			pstmt.setString(4, cms.group_survey_ident);
			pstmt.setString(5, ident);

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
	public void updateCMS(Connection sd, 
			CMS cms, 
			int o_id, 
			String ident) throws Exception {
		
		String sql = "update case_management_setting set name = ?, "
				+ "type = ?,"
				+ "group_survey_ident = ?,"
				+ "changed_by = ?,"
				+ "changed_ts = now() "
				+ "where o_id = ? "
				+ "and id = ?"; 
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setString(1, cms.name);
			pstmt.setString(2, cms.type);
			pstmt.setString(3, cms.group_survey_ident);
			pstmt.setString(4, ident);
			pstmt.setInt(5, o_id);
			pstmt.setInt(6, cms.id);

			log.info("SQL: " + pstmt.toString());
			pstmt.executeUpdate();
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}

	}
	
	/*
	 * Delete a Setting
	 */
	public void deleteSetting(Connection sd, 
			int id, 
			int o_id, 
			String ident) throws Exception {
		
		String sql = "delete from case_management_setting where id = ? "
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
