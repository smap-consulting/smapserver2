package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

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
	
	/*
	 * Write a log entry that includes the survey id
	 */
	public String getSubscriptionKey(Connection sd, 
			int oId,
			String email) {
		
		String sql = "select unsubscibed, uuid "
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
				// Create a key for this email
				key = UUID.randomUUID().toString();
				pstmtCreate = sd.prepareStatement(sqlCreate);
				pstmtCreate.setInt(1,  oId);
				pstmtCreate.setString(2, email);
				pstmtCreate.setString(3, key);
				pstmtCreate.executeUpdate();
			}


		} catch(Exception e) {
			log.log(Level.SEVERE, "SQL Error", e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			try {if (pstmtCreate != null) {pstmtCreate.close();} } catch (SQLException e) {	}
		}
		
		return key;

	}
	
	/*
	 * Write a log entry that at the organisation level
	 */
	public void writeLogOrganisation(Connection sd, 
			int oId,
			String uIdent,
			String event,
			String note)  {
		
		String sql = "insert into log ("
				+ "log_time,"
				+ "s_id,"
				+ "o_id,"
				+ "user_ident,"
				+ "event,"
				+ "note) values (now(), 0, ?, ?, ?, ?);";

		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, oId);
			pstmt.setString(2, uIdent);
			pstmt.setString(3,  event);
			pstmt.setString(4,  note);
			
			pstmt.executeUpdate();


		} catch(Exception e) {
			log.log(Level.SEVERE, "SQL Error", e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}

	}
	
}


