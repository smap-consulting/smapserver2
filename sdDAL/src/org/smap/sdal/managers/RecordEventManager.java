package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.LanguageItem;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.Question;

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
 * records changes to data records
 */
public class RecordEventManager {
	
	private static Logger log =
			 Logger.getLogger(RecordEventManager.class.getName());
	
	public RecordEventManager() {
		
	}
	
	/*
	 * Save a change
	 * Either specify the HRK or the old and new instanceids
	 *  If the HRK exists and the key policy is merge or replace then it should be used and will provide an invarying index for the change
	 *  Else the old and new instance id's will be used to create a chain of changes.  The existing changes will be indexed
	 *   by the old instanceid so all of these will need to be updated to the new instanceid
	 */
	public void saveChange(Connection sd, 
			String user,
			String tableName, 
			String hrk, 
			String newInstance, 
			String oldInstance,
			String changes,
			int sId) throws SQLException {
		
		String sql = "insert into record_event ("
				+ "key,	"
				+ "instanceid,	"
				+ "details, "
				+ "success, "
				+ "msg, "
				+ "changed_by, "
				+ "change_survey, "
				+ "change_survey_version, "
				+ "event_time) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, now())";
		PreparedStatement pstmt = null;
		
		String sqlSurvey = "select ident, version " 
				+ " from survey " 
				+ " where s_id = ?";
		PreparedStatement pstmtSurvey = null;
		
		// Set key
		String key = null;
		boolean hrkKeyType;
		if(hrk != null) {
			key = hrk;
			 hrkKeyType = true;
		} else {
			key = newInstance;
			hrkKeyType = false;
		}
		
		// Set user id
		int uId = GeneralUtilityMethods.getUserId(sd, user);
		
		// Set survey ident and version
		String sIdent = null;
		int sVersion = 0;
		
		pstmtSurvey = sd.prepareStatement(sqlSurvey);
		pstmtSurvey.setInt(1, sId);
		ResultSet rs = pstmtSurvey.executeQuery();
		if(rs.next()) {
			sIdent = rs.getString(1);
			sVersion = rs.getInt(2);
		}
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, key);
			pstmt.setString(2,  newInstance);
			pstmt.setString(3,  changes);
			pstmt.setBoolean(4,  true);	// success
			pstmt.setString(5,  null);
			pstmt.setInt(6, uId);
			pstmt.setString(7,  sIdent);
			pstmt.setInt(8,  sVersion);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) try{pstmt.close();}catch(Exception e) {};
			if(pstmtSurvey != null) try{pstmtSurvey.close();}catch(Exception e) {};
		}
	}
	

}
