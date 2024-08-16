package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.UniqueKey;

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
	
	public KeyManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Get Unique key settings
	 */
	public UniqueKey get(Connection sd, String groupSurveyIdent) throws Exception {
		
		PreparedStatement pstmt = null;
		UniqueKey uk = null;
		
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
						null, GeneralUtilityMethods.getOrganisationIdForSurveyIdent(sd, groupSurveyIdent), true);
				
			}
			
					    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return uk;
	}
	
	/*
	 * Update a Key Setting
	 * The overrideWithNone flag is set to false when a survey without key settings should take the settings of the bundle
	 *  When set to true it can be used to clear a key value, the key editor will set it to true
	 *  Uploading a survey from an XLSForm will set it to false
	 */
	public void update(Connection sd, 
			String groupSurveyIdent,
			String key,
			String key_policy, 
			String user,
			int o_id,
			boolean overrideWithNone) throws Exception {
		
		String sql = "update cms_setting "
				+ "set key = ?,"
				+ "key_policy = ?, "
				+ "changed_ts = now(), "
				+ "changed_by = ? "
				+ "where group_survey_ident = ?"; 
		
		PreparedStatement pstmt = null;
		
		// Initialise
		if(key == null) {
			key = "";
		}
		if(key_policy == null) {
			key_policy = SurveyManager.KP_NONE;
		}
		key = key.trim();
		
		// Confirm that an empty key can override the existing setting
		if(overrideWithNone || !key.equals("")) {
			
			try {
				
				pstmt = sd.prepareStatement(sql);
	
				pstmt.setString(1, key);
				pstmt.setString(2, key_policy);
				pstmt.setString(3, user);
				pstmt.setString(4, groupSurveyIdent);
	
				log.info("SQL: " + pstmt.toString());
				int count = pstmt.executeUpdate();
				if(count < 1) {
					try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
					
					sql = "insert into cms_setting "
							+ "(key, key_policy, changed_by, group_survey_ident, changed_ts) "
							+ "values(?, ?, ?, ?, now())"; 
					pstmt = sd.prepareStatement(sql);
					pstmt.setString(1, key);
					pstmt.setString(2, key_policy);
					pstmt.setString(3,  user);
					pstmt.setString(4, groupSurveyIdent);
					log.info("Update Key Settings: " + pstmt.toString());
					pstmt.executeUpdate();
				}
				
			}  finally {		
				try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
				
			}
		}

	}
	
	/*
	 * Update Existing Records when a key is added
	 * The key is only updated if it is currently null
	 */
	public void updateExistingData(Connection sd,
			Connection cResults,
			String key,
			String groupSurveyIdent,
			String tableName,
			int prikey) throws Exception {
		
		String hrkSql = GeneralUtilityMethods.convertAllxlsNamesToQuery(key, groupSurveyIdent, sd, tableName);
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtAddHrk = null;
		try {
			if(!GeneralUtilityMethods.hasColumn(cResults, tableName, "_hrk")) {
				// This should not be needed as the _hrk column should be in the table if an hrk has been specified for the survey
				log.info("Error:  _hrk being created for table " + tableName + " this column should already be there");
				String sqlAddHrk = "alter table " + tableName + " add column _hrk text;";
				pstmtAddHrk = cResults.prepareStatement(sqlAddHrk);
				pstmtAddHrk.executeUpdate();
			}
			
			StringBuilder sql = new StringBuilder("update ");
			sql.append(tableName)
				.append(" m set _hrk = ")
				.append(hrkSql)
				.append(" where m._hrk is null ");
			if(prikey > 0) {
				sql.append("and m.prikey = ?");
			} 
				
			pstmt = cResults.prepareStatement(sql.toString());
			if(prikey > 0) {
				pstmt.setInt(1, prikey);
			}
			int count = pstmt.executeUpdate();
			log.info("------------- HRK values update: " + count);
			log.info("Applying HRK: " + pstmt.toString());
	
		} finally {
			if(pstmt != null) {try {pstmt.close();}catch(Exception e) {}}
			if(pstmtAddHrk != null) {try {pstmtAddHrk.close();}catch(Exception e) {}}
			
		}
	}

}
