package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.SubItemDt;

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
 * Assume emails are case insensitive
 */
public class ContactManager {
	
	private static Logger log =
			 Logger.getLogger(ContactManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	ResourceBundle localisation = null;
	
	public ContactManager(ResourceBundle l) {
		localisation = l;	
	}

	public ArrayList<SubItemDt> getSubscriptions(Connection sd, 
			String user, 
			String tz,
			boolean dt) throws SQLException {
		ArrayList<SubItemDt> data = new ArrayList<> ();
			
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
	
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);			
			
			// Get the data
			String sql = "select id, email, unsubscribed, opted_in, opted_in_sent,"
					+ "name, "
					+ "to_char(timezone(?, when_subscribed), 'YYYY-MM-DD HH24:MI:SS') as when_subscribed, "
					+ "to_char(timezone(?, when_unsubscribed), 'YYYY-MM-DD HH24:MI:SS') as when_unsubscribed, "
					+ "to_char(timezone(?, when_requested_subscribe), 'YYYY-MM-DD HH24:MI:SS') as when_requested_subscribe "
					+ "from people "
					+ "where o_id = ? "
					+ "order by email asc";
			
			pstmt = sd.prepareStatement(sql);
			int paramCount = 1;
			pstmt.setString(paramCount++, tz);
			pstmt.setString(paramCount++, tz);
			pstmt.setString(paramCount++, tz);
			pstmt.setInt(paramCount++, oId);
			
			log.info("Get subscriptions: " + pstmt.toString());
			rs = pstmt.executeQuery();
				
			while (rs.next()) {
					
				SubItemDt item = new SubItemDt();

				item.id = rs.getInt("id");
				item.email = GeneralUtilityMethods.getSafeText(rs.getString("email"), dt);
				item.name = GeneralUtilityMethods.getSafeText(rs.getString("name"), dt);
				if(item.name == null) {
					item.name = "";
				}
				
				/*
				 * Get status
				 */
				String status = "";
				String status_loc = "";
				boolean unsubscribed = rs.getBoolean("unsubscribed");
				boolean optedin = rs.getBoolean("opted_in");
				item.time_changed = "";
				if(unsubscribed) {
					status = "unsubscribed";
					status_loc = localisation.getString("c_unsubscribed");
					item.time_changed = rs.getString("when_unsubscribed");
						
				} else if(optedin) {
					status = "subscribed";
					status_loc = localisation.getString("c_s2");
					item.time_changed = rs.getString("when_subscribed");
				} else {
					String optedInSent = rs.getString("opted_in_sent");
					if(optedInSent != null) {
						status = "pending";
						status_loc = localisation.getString("c_pending");
						item.time_changed = rs.getString("when_requested_subscribe");
					} else {
						status = "new";
						status_loc = localisation.getString("c_new");
					}
				}
				if(item.time_changed != null) {
					item.time_changed = item.time_changed.replaceAll("\\.[0-9]+", ""); // Remove milliseconds
				} else {
					item.time_changed = "";
				}
				item.status = status;
				if(dt) {
					item.status_loc = status_loc;
				}
				
				data.add(item);
			}
						
			
	
		} finally {
			
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			
		}
		
		return data;
	}

}


