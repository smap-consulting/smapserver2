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
import org.smap.sdal.model.UniqueKey;

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
 * This class supports access to unique information in the database
 * All surveys in a bundle share the same unique key
 */
public class KeyManager {
	
	private static Logger log =
			 Logger.getLogger(KeyManager.class.getName());

	LogManager lm = new LogManager(); // Application log
	ResourceBundle localisation = null;
	Gson gson;
	
	public KeyManager(ResourceBundle l) {
		localisation = l;
		gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
	}

	/*
	 * Get Unique key settings
	 */
	public UniqueKey get(Connection sd, String groupSurveyIdent) throws Exception {
		
		PreparedStatement pstmt = null;
		UniqueKey uk = null;
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		try {
			
			/*
			 * Get the key settings for this survey group
			 */

			
			String sql = "select key, "
					+ "key_policy "
					+ "from cms_setting "
					+ "where group_survey_ident = ? ";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  groupSurveyIdent);
			log.info("Get key settings: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
							
			if(rs.next()) {
				uk = new UniqueKey(rs.getString("key"), rs.getString("key_policy"), groupSurveyIdent);
			}
			
			/*
			 * Handle the legacy situation where the unique key is in the survey definition
			 */
			if(uk == null || uk.key == null) {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				
				sql = "select ident, hrk, key_policy from survey where group_survey_ident = ?";
				pstmt = sd.prepareStatement(sql);		
				pstmt.setString(1,  groupSurveyIdent);
				
				log.info("Get legacy key settings: " + pstmt.toString());
				ResultSet rsLegacy = pstmt.executeQuery();
				
				String keyGroup = null;
				String policyGroup = null;
				String keyFound = null;
				String policyFound = null;
				
				while(rsLegacy.next()) {
					
					String ident = rsLegacy.getString("ident");
					String key = rsLegacy.getString("hrk");
					String policy = rsLegacy.getString("key_policy");
					
					if(keyFound == null) {
						keyFound = key;
						policyFound = policy;
					}
					
					if(groupSurveyIdent.equals(ident)) {
						keyGroup = key;
						policyGroup = policy;
					}
				}
					
				if(keyGroup != null) {
					// Prioritise key from survey that is the first survey in the bundle
					uk = new UniqueKey(keyGroup, policyGroup, groupSurveyIdent);
				} else if(keyFound != null) {
					// Anything we found - in any order no priority
					uk = new UniqueKey(keyFound, policyFound, groupSurveyIdent);
				} else {
					// Still no key - Return default
					uk = new UniqueKey("", "none", groupSurveyIdent);
				}
				
				/*
				 * Save the legacy data so that in future if is available in the cms_settings table
				 */
				update(sd, groupSurveyIdent, uk.key, uk.key_policy, 
						null, GeneralUtilityMethods.getOrganisationIdForSurveyIdent(sd, groupSurveyIdent));
				
			}
			
					    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return uk;
	}
	
	/*
	 * Update a Key Setting
	 */
	public void update(Connection sd, 
			String groupSurveyIdent,
			String key,
			String key_policy, 
			String user,
			int o_id) throws Exception {
		
		String sql = "update cms_setting "
				+ "set key = ?,"
				+ "key_policy = ?, "
				+ "changed_ts = now(), "
				+ "changed_by = ? "
				+ "where o_id = ? "
				+ "and group_survey_ident = ?"; 
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setString(1, key);
			pstmt.setString(2, key_policy);
			pstmt.setString(3, user);
			pstmt.setInt(4, o_id);
			pstmt.setString(5, groupSurveyIdent);

			log.info("SQL: " + pstmt.toString());
			int count = pstmt.executeUpdate();
			if(count < 1) {
				try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
				
				sql = "insert into cms_setting "
						+ "(key, key_policy, changed_by, o_id, group_survey_ident, changed_ts) "
						+ "values(?, ?, ?, ?, ?, now())"; 
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, key);
				pstmt.setString(2, key_policy);
				pstmt.setString(3,  user);
				pstmt.setInt(4, o_id);
				pstmt.setString(5, groupSurveyIdent);
				log.info("Update Key Settings: " + pstmt.toString());
				pstmt.executeUpdate();
			}
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}

	}

}
