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

package org.smap.sdal.legacy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ResourceBundle;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.HtmlSanitise;

public class JdbcSurveyManager {

	ResourceBundle localisation = null;
	
	// Create new
	PreparedStatement pstmt = null;
	String sql = "insert into survey ("
			+ "s_id, "
			+ "last_updated_time, "
			+ "display_name, "
			+ "p_id, "
			+ "def_lang, "
			+ "class,"
			+ "ident,"
			+ "version,"
			+ "manifest,"
			+ "instance_name,"
			+ "loaded_from_xls,"
			+ "created) "
			+ "values (nextval('s_seq'), now(), ?, ?, ?, ?, ?, ?, ?, ?, ?, now())";
	
	// Update survey ident based on id
	PreparedStatement pstmtUpdate = null;
	String sqlUpdate = "update survey set "
			+ "ident = ? "
			+ "where s_id = ?";
	
	// Retrieve
	PreparedStatement pstmtGetByIdent = null;
	PreparedStatement pstmtGetById = null;
	String sqlGet = "select "
			+ "s_id, "
			+ "display_name, "
			+ "p_id, "
			+ "def_lang, "
			+ "class,"
			+ "ident,"
			+ "version,"
			+ "manifest,"
			+ "instance_name,"
			+ "deleted,"
			+ "hrk,"
			+ "timing_data,"
			+ "audit_location_data,"
			+ "track_changes,"
			+ "hide_on_device,"
			+ "search_local_data,"
			+ "meta,"
			+ "my_reference_data "
			+ "from survey where ";
	String sqlIdentWhere = "ident = ?";
	String sqlIdWhere = "s_id = ?";

	// Check existence
	PreparedStatement pstmtExists = null;
	String sqlExists = "select count(*) from survey where display_name = ? and p_id = ?";
	
	// Get replacement ident
	PreparedStatement pstmtReplacement = null;
	String sqlReplacement = "select new_ident from replacement where old_ident = ?";
	
	/*
	 * Constructor
	 */
	public JdbcSurveyManager(Connection sd, ResourceBundle localisation) throws SQLException {
		this.localisation = localisation;
		
		pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		pstmtUpdate = sd.prepareStatement(sqlUpdate);
		pstmtGetByIdent = sd.prepareStatement(sqlGet + sqlIdentWhere);
		pstmtGetById = sd.prepareStatement(sqlGet + sqlIdWhere);
		pstmtExists = sd.prepareStatement(sqlExists);
		pstmtReplacement = sd.prepareStatement(sqlReplacement);
	}
	
	/*
	 * Store a new survey
	 */
	public void write(Survey s) throws SQLException, ApplicationException {
		pstmt.setString(1, HtmlSanitise.checkCleanName(s.getDisplayName(), localisation) );
		pstmt.setInt(2, s.getProjectId());
		pstmt.setString(3, HtmlSanitise.checkCleanName(s.getDefLang(), localisation));
		pstmt.setString(4, HtmlSanitise.checkCleanName(s.getSurveyClass(), localisation));
		pstmt.setString(5, s.getIdent());
		pstmt.setInt(6, s.getVersion());
		pstmt.setString(7, s.getManifest());
		pstmt.setString(8, s.getInstanceName());
		pstmt.setBoolean(9, s.getLoadedFromXls());
		pstmt.executeUpdate();
					
		ResultSet rs = pstmt.getGeneratedKeys();
		if(rs.next()) {
			s.setId(rs.getInt(1));
			
			String ident = s.getIdent();
			if(ident == null || ident.trim().length() == 0) {
				String surveyName = "s" + s.getProjectId() + "_" + s.getId();
				s.setIdent(surveyName);
				update(s);
			}
		}
	}
	
	/*
	 * Update a survey
	 */
	public void update(Survey s) throws SQLException {
		pstmtUpdate.setString(1, s.getIdent());
		pstmtUpdate.setInt(2, s.getId());
		pstmtUpdate.executeUpdate();
	}
	
	/*
	 * Get a survey using its ident
	 */
	public Survey getByIdent(String ident) throws SQLException {
		
		pstmtGetByIdent.setString(1, ident);		
		Survey s = getSurvey(pstmtGetByIdent);
		
		if(s == null || s.getDeleted()) {
			// Try to get a replacement survey ident
			pstmtReplacement.setString(1, ident);
			ResultSet rs = pstmtReplacement.executeQuery();
			if(rs.next()) {
				pstmtGetByIdent.setString(1, rs.getString(1));
				s = getSurvey(pstmtGetByIdent);
			}
		}
		return s;
	}
	
	/*
	 * Get a survey using its id
	 */
	public Survey getById(int id) throws SQLException {
		pstmtGetById.setInt(1, id);
		return getSurvey(pstmtGetById);
	}
	
	/*
	 * Return true if a survey exists with the specified name and projectId
	 */
	public boolean surveyExists(String name, int projectId) throws SQLException {
		
		boolean exists = false;
		
		pstmtExists.setString(1, name);
		pstmtExists.setInt(2,  projectId);
		ResultSet rs = pstmtExists.executeQuery();
		if(rs.next()) {
			if(rs.getInt(1) > 0) {
				exists = true;
			}
		}
		return exists;
	}
	
	/*
	 * Close statements
	 */
	public void close() {
		try {if(pstmt != null) {pstmt.close();}} catch(Exception e) {};
		try {if(pstmtUpdate != null) {pstmtUpdate.close();}} catch(Exception e) {};
		try {if(pstmtGetByIdent != null) {pstmtGetByIdent.close();}} catch(Exception e) {};
		try {if(pstmtGetById != null) {pstmtGetById.close();}} catch(Exception e) {};
		try {if(pstmtReplacement != null) {pstmtReplacement.close();}} catch(Exception e) {};
	}
	
	/*
	 * Common function to populate a survey object from the database
	 * The query used to find the survey is specified in the prepared statement
	 */
	private Survey getSurvey(PreparedStatement pstmt) throws SQLException {
		Survey s = null;
		
		ResultSet rs = pstmt.executeQuery();
		if(rs.next()) {
			s = new Survey();
			s.setId(rs.getInt(1));
			s.setDisplayName(rs.getString(2));
			s.setProjectId(rs.getInt(3));
			s.setDefLang(rs.getString(4));
			s.setSurveyClass(rs.getString(5));
			s.setIdent(rs.getString(6));
			s.setVersion(rs.getInt(7));
			s.setManifest(rs.getString(8));
			s.setInstanceName(rs.getString(9));
			s.setDeleted(rs.getBoolean(10));
			s.setHrk(rs.getString(11));
			s.setTimingData(rs.getBoolean(12));
			s.setAuditLocationData(rs.getBoolean(13));
			s.setTrackChanges(rs.getBoolean(14));
			s.setHideOnDevice(rs.getBoolean(15));
			s.setSearchLocalData(rs.getBoolean(16));
			s.setMeta(rs.getString(17));
			s.setMyReferenceData(rs.getBoolean(18));
		
		}
		return s;
	}
}
