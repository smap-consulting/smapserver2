package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.ResourceBundle;
import java.util.TimeZone;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.AssignFromSurvey;
import org.smap.sdal.model.Assignment;
import org.smap.sdal.model.Location;
import org.smap.sdal.model.Task;
import org.smap.sdal.model.TaskAssignment;
import org.smap.sdal.model.TaskBulkAction;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskGroup;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskProperties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;

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
public class PeopleManager {
	
	private static Logger log =
			 Logger.getLogger(PeopleManager.class.getName());
	
	ResourceBundle localisation = null;
	
	public PeopleManager(ResourceBundle l) {
		localisation = l;
	}
	/*
	 * Get an email key for this user that can be used to unsubscribe
	 * If the person is already unsubscribed then return null
	 * organisation id is recorded in the people table but unsubscription applies across the whole server
	 */
	public String getEmailKey(Connection sd, 
			int oId,
			String email) throws SQLException {
		
		String sql = "select unsubscribed, uuid "
				+ "from people "
				+ "where email = ?";
		PreparedStatement pstmt = null;
		
		String sqlCreate = "insert into people "
				+ "(o_id, email, unsubscribed, uuid) "
				+ "values(?, ?, 'false', ?)";
		PreparedStatement pstmtCreate = null;
		
		String key = null;
		try {
			
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, email);
			
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				boolean unsubscribed = rs.getBoolean(1);
				if(!unsubscribed) {
					key = rs.getString(2);
				}
			} else {
				// Create a key for this email and save it in the people table
				key = UUID.randomUUID().toString();
				pstmtCreate = sd.prepareStatement(sqlCreate);
				pstmtCreate.setInt(1,  oId);
				pstmtCreate.setString(2, email);
				pstmtCreate.setString(3, key);
				pstmtCreate.executeUpdate();
			}


		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			try {if (pstmtCreate != null) {pstmtCreate.close();} } catch (SQLException e) {	}
		}
		
		return key;

	}
	
	public String unsubscribe(Connection sd, 
			String key) throws SQLException, ApplicationException {
		
		String sql = "update people "
				+ "set unsubscribed = true,"
				+ "when_unsubscribed = now() "
				+ "where uuid = ? "
				+ "and not unsubscribed";
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, key);			
			int count = pstmt.executeUpdate();
			if(count == 0) {
				throw new ApplicationException(localisation.getString("c_ns"));
			}

		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return key;

	}

}


