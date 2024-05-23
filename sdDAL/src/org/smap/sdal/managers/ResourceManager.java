package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

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
 * Manage usage of costly resources
 */
public class ResourceManager {
	
	private static Logger log =
			 Logger.getLogger(ResourceManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	/*
	 * Get the limit for a resource
	 */
	public int getLimit(Connection sd, int oId, String resource) {
		int limit = 0;
		
		String sql = "select limits "
				+ " from organisation where id = ?";		
		PreparedStatement pstmt = null;
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			log.info("Get limits for an organisation: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String limitString = rs.getString(1);
				log.info("Limit string: " + limitString);
				if(limitString != null) {
					HashMap<String, Integer> limits = gson.fromJson(limitString, 
							new TypeToken<HashMap<String, Integer>>() {}.getType());
					Integer l = limits.get(resource);
					if(l != null) {
						limit = l;
					}
				}
			}
		} catch (Exception e) {
			// Don't throw an error just return 0
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
		}
		return limit;
	}
	
	public int getUsageMeasure(Connection sd, int oId, int month, int year, String resource) throws SQLException {
		
		StringBuilder sb = new StringBuilder("select  sum(measure) as total from log where event = ?")
				.append(" and log_time >=  ? and log_time < ?");
		
		if(oId > 0) {
			sb.append(" and o_id = ?");
		}
		PreparedStatement pstmt = null;
		
		int usage = 0;
		
		try {
			Timestamp t1 = GeneralUtilityMethods.getTimestampFromParts(year, month, 1);
			Timestamp t2 = GeneralUtilityMethods.getTimestampNextMonth(t1);
			
			pstmt = sd.prepareStatement(sb.toString());
			pstmt.setString(1,  resource);
			pstmt.setTimestamp(2, t1);
			pstmt.setTimestamp(3, t2);
			if(oId > 0) {
				pstmt.setInt(4, oId);
			}
			
			log.info("Get resource usage: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
		
			if(rs.next()) {
				usage = rs.getInt("total");
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return usage;
	}
	
	public int getUsageSubmissionsMeasure(Connection sd, int oId, int month, int year) throws SQLException {
		
		String sql = "select  count(*) as total from upload_event ue "
				+ "where ue.db_status = 'success' "
				+ "and ue.o_id = ? "
				+ "and extract(month from ue.upload_time) = ? "
				+ "and extract(year from ue.upload_time) = ? ";
		PreparedStatement pstmt = null;
		
		int usage = 0;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1,  oId);
			pstmt.setInt(2, month);
			pstmt.setInt(3, year);
			
			ResultSet rs = pstmt.executeQuery();
		
			if(rs.next()) {
				usage = rs.getInt("total");
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return usage;
	}
	
	/*
	 * Get the usage in the period
	 */
	public int getUsage(Connection sd, int oId,
			String resource, int month, int year) throws SQLException {
		
		int usage = 0;
		String period = String.valueOf(year) + String.valueOf(month);
			
		String sql = "select period, usage from resource_usage "
				+ "where o_id = ? "
				+ "and resource = ?";
		PreparedStatement pstmt = null;
			
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, resource);
			ResultSet rs = pstmt.executeQuery();
				
			if(rs.next()) {
				String storedPeriod = rs.getString(1);
				if(period.equals(storedPeriod)) {
					// Resource usage table is up to date
					usage = rs.getInt(2);
				} else {
					// A new period begins
					usage = 0;
					deleteUsage(sd, oId, resource, period);
					updateUsage(sd, oId, resource, period, usage);
				}
			} else {
				// Get the usage from the logs
				if(resource.equals(LogManager.SUBMISSION)) {
					usage = getUsageSubmissionsMeasure(sd, oId, month, year);
				} else {
					usage = getUsageMeasure(sd, oId, month, year, resource);
				}
				updateUsage(sd, oId, resource, period, usage);
			}
				
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
		}
	
		return usage;
	}
	
	/*
	 * Check to see if the person can use the resource
	 */
	public boolean canUse(Connection sd, 
			int oId,
			String resource) throws SQLException {
		
		boolean decision = false;
		int limit = getLimit(sd, oId, resource);
		String period = "";
		int usage = 0;
		
		// A limit of 0 for submissions means no restrictions
		if(limit == 0 && resource.equals(LogManager.SUBMISSION)) {
			return true;
		}
		
		if(limit > 0) {
			
			LocalDate d = LocalDate.now();
			int month = d.getMonth().getValue();
			int year = d.getYear();
			period = String.valueOf(year) + String.valueOf(month);
			
			usage = getUsage(sd, oId, resource, month, year);
			decision = usage < limit;
		}
		
		if(!decision) {
			log.info("Usage denied. Period: " + period 
					+ " Resource: " + resource 
					+ " Limit: " + limit 
					+ " Usage: " + usage);
		}
		
		return decision;
	}
	
	/*
	 * Called to record usage of a limited resource
	 */
	public void recordUsage(Connection sd, int oId, int sId, String resource, String msg, 
			String user,
			int usage) throws SQLException {
		
		// Get the period before writing the log in case we are on the cusp of change
		LocalDate d = LocalDate.now();
		int month = d.getMonth().getValue();
		int year = d.getYear();
		String period = String.valueOf(year) + String.valueOf(month);

		// Write the log entry
		if(msg != null) {
			if(sId > 0) {
				lm.writeLog(sd, sId, user, resource, msg, usage, null);
			} else {
				lm.writeLogOrganisation(sd, oId, user, resource, msg, usage);
			}
		}
		
		// Keep temporary store of usage for performance reasons
		updateUsage(sd, oId, resource, period, usage);
	}
	
	/*
	 * Update the usage count
	 */
	private void updateUsage(Connection sd, int oId, String resource, String period, int usage) throws SQLException  {
		
		String sql_u = "update resource_usage set usage = usage + ? "
				+ "where o_id = ? "
				+ "and period = ? "
				+ "and resource = ?";
		
		String sql_c = "insert into resource_usage (usage, o_id, period, resource) values (?, ?, ?, ?) ";
		
		String sql_d = "delete from resource_usage where o_id = ? and resource = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql_u);
			pstmt.setInt(1,  usage);
			pstmt.setInt(2,  oId);
			pstmt.setString(3,  period);
			pstmt.setString(4,  resource);
			
			int count = pstmt.executeUpdate();
			if(count == 0) {
				if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
				// Delete any old period data
				pstmt = sd.prepareStatement(sql_d);
				pstmt.setInt(1, oId);
				pstmt.setString(2, resource);
				pstmt.executeUpdate();
				
				// Insert the new usage
				if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
				pstmt = sd.prepareStatement(sql_c);
				pstmt.setInt(1,  usage);
				pstmt.setInt(2,  oId);
				pstmt.setString(3,  period);
				pstmt.setString(4,  resource);
				pstmt.executeUpdate();
			}
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
		}
	}	
	
	/*
	 * Delete the usage count
	 */
	private void deleteUsage(Connection sd, int oId, String resource, String period) throws SQLException {
		
		String sql = "delete from resource_usage "
				+ "where o_id = ? "
				+ "and resource = ? "
				+ "and period = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1,  oId);
			pstmt.setString(2,  resource);
			pstmt.setString(3,  period);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
		}
	}
	
}


