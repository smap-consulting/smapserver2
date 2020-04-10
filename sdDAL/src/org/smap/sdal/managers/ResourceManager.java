package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Date;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

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
	
	/*
	 * Check to see if the person can use the resource
	 */
	public boolean canUse(Connection sd, 
			int oId,
			String resource) throws SQLException {
		
		boolean decision = false;
		int limit = GeneralUtilityMethods.getLimit(sd, oId, resource);
		System.out.println("xxxxxxxxxxxxxxx " + limit);

		if(limit > 0) {
			LocalDate d = LocalDate.now();
			int month = d.getMonth().getValue();
			int year = d.getYear();
			String period = String.valueOf(year) + String.valueOf(month);
			
			String sql = "select usage from resource_usage "
					+ "where o_id = ? "
					+ "and period = ? "
					+ "and resource = ?";
			PreparedStatement pstmt = null;
			
			int usage = 0;
			try {
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, oId);
				pstmt.setString(2,  period);
				pstmt.setString(3, resource);
				ResultSet rs = pstmt.executeQuery();
				
				if(rs.next()) {
					usage = rs.getInt(1);
					System.out.println("=============" + usage);
				} else {
					// Get the usage from the logs
					usage = GeneralUtilityMethods.getUsageMeasure(sd, oId, month, year, resource);
					updateUsage(sd, oId, resource, period, usage);
				}
				
				System.out.println("----------- " + usage);
				decision = usage < limit;
				
			} finally {
				if(pstmt != null) try {pstmt.close();}catch(Exception e) {}
			}
		}
		
		return decision;
	}
	
	/*
	 * Update the usage count
	 */
	public void updateUsage(Connection sd, int oId, String resource, String period, int usage) throws SQLException  {
		
		String sql_u = "update resource_usage set usage = ? "
				+ "where o_id = ? "
				+ "and period = ? "
				+ "and resource = ?";
		
		String sql_c = "insert into resource_usage (usage, o_id, period, resource) values (?, ?, ?, ?) ";
		
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
}


