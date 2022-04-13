package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.PointEntry;
import org.smap.sdal.model.Role;


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
public class UserLocationManager {

	private static Logger log =
			Logger.getLogger(UserLocationManager.class.getName());
	private static ResourceBundle localisation;
	private String tz;

	public UserLocationManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}

	/*
	 * Get latest location information
	 */
	public String getUserLocations(
			Connection sd,
			int pId,
			int start_key,
			int rec_limit,
			String remoteUser,
			boolean forDashboard
			) throws SQLException {
		
		StringBuffer message = new StringBuffer("");
		JSONObject jo = new JSONObject();
		JSONArray columns = new JSONArray();
		JSONArray types = new JSONArray();
		JSONObject jTotals = new JSONObject();

		int maxRec = 0;
		int recCount = 0;	
		int idx = 1;		// Prepared statment index

			PreparedStatement pstmt = null;

			int totalCount = 0;

			try {

				int oId = GeneralUtilityMethods.getOrganisationId(sd, remoteUser);

				// As per change request: https://github.com/nap2000/prop-smapserver/issues/32
				// If the user only has the 'view data" security group they will be restricted to seeing other users with the same security role
				/*
				 * Two cases
				 * 1) The user has no security role -> They can only see other users who do not have a security role
				 * 2) The user can only see other users who have the same security roles
				 */
				ArrayList<Role> roles = null;
				ArrayList<Integer> roleids = new ArrayList<> ();
				boolean viewDataOnly = GeneralUtilityMethods.isOnlyViewData(sd, remoteUser);
				if(viewDataOnly) {					
					int uId = GeneralUtilityMethods.getUserId(sd, remoteUser);
					UserManager um = new UserManager(localisation);
					roles = um.getUserRoles(sd, uId);
				}

				if(forDashboard) {
					jo.put("totals", jTotals);
					jTotals.put("start_key", start_key);
				}

				// Get the number of records
				StringBuilder sqlCount = new StringBuilder("select count(*) ");
				if(viewDataOnly && roles != null && roles.size() > 0) {
					sqlCount.append("from last_refresh lr, users u, user_role ur ");
				} else {
					sqlCount.append("from last_refresh lr, users u ");
				}
				sqlCount.append("where u.o_id = ? "
						+ "and lr.o_id = ? "
						+ "and lr.geo_point is not null "
						+ "and (st_x(geo_point) != 0 or st_y(geo_point) != 0) "
						+ "and lr.user_ident = u.ident ");

				if(pId > 0) {
					sqlCount.append(" and lr.user_ident in "
							+ "(select ident from users ux, user_project upx "
							+ "where ux.id = upx.u_id "
							+ "and upx.p_id = ?) ");
				}				
				if(viewDataOnly) {					
					if(roles == null || roles.size() == 0) {
						sqlCount.append(" and u.id not in (select u_id from user_role ur, users u2 where ur.u_id = u2.id and u2.o_id = ?) ");
					} else {
						sqlCount.append(" and ur.u_id = u.id and ur.r_id = any (?) ");
						
						for(Role r : roles) {		// Create an array of role ids for the any clause
							roleids.add(r.id);
						}
					}
				}
				
				pstmt = sd.prepareStatement(sqlCount.toString());	
				pstmt.setInt(idx++, oId);
				pstmt.setInt(idx++, oId);
				if(pId > 0) {
					pstmt.setInt(idx++, pId);
				}
				if(viewDataOnly) {	
					if(roles == null || roles.size() == 0) {
						pstmt.setInt(idx++, oId);
					} else {
						pstmt.setArray(idx++, sd.createArrayOf("integer", roleids.toArray(new Integer[roleids.size()])));
					}
				}
				log.info("Get the number of records: " + pstmt.toString());	
				ResultSet resultSet = pstmt.executeQuery();
				if(resultSet.next()) {
					totalCount = resultSet.getInt(1);
					jTotals.put("total_count", totalCount);
				}
				if(forDashboard && rec_limit > 0) {
					jTotals.put("rec_limit", rec_limit);
				}
				/*
				 * Get the data
				 */
				StringBuilder sqlData = new StringBuilder("select "
						+ "lr.id,"
						+ "lr.user_ident, "
						+ "timezone(?, lr.refresh_time) as refresh_time, "
						+ "lr.refresh_time as utc_time,"  
						+ "ST_AsGeoJSON(lr.geo_point) as geo_point ");
				if(viewDataOnly && roles != null && roles.size() > 0) {
					sqlData.append("from last_refresh lr, users u, user_role ur ");
				} else {
					sqlData.append("from last_refresh lr, users u ");
				}
				sqlData.append("where u.o_id = ? "
						+ "and lr.geo_point is not null "
						+ "and (st_x(geo_point) != 0 or st_y(geo_point) != 0) "
						+ "and lr.o_id = ? "
						+ "and lr.user_ident = u.ident ");

				if(pId > 0) {
					sqlData.append(" and lr.user_ident in "
							+ "(select ident from users ux, user_project upx "
							+ "where ux.id = upx.u_id "
							+ "and upx.p_id = ?) ");
				}
				if(start_key > 0) {
					sqlData.append(" and lr.id < " ).append(start_key);
				}
				if(viewDataOnly) {				
					if(roles == null || roles.size() == 0) {
						sqlData.append(" and u.id not in (select u_id from user_role ur, users u2 where ur.u_id = u2.id and u2.o_id = ?) ");
					} else {
						sqlData.append(" and ur.u_id = u.id and ur.r_id = any (?) ");
					}
				}
				sqlData.append("order by lr.id desc ");

				if(rec_limit > 0) {
					sqlData.append("limit ").append(rec_limit);
				}
				// Get the prepared statement to retrieve data
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				try {if (resultSet != null) {resultSet.close();}} catch (SQLException e) {}
				idx = 1;

				pstmt = sd.prepareStatement(sqlData.toString());
				pstmt.setString(idx++, tz);
				pstmt.setInt(idx++, oId);
				pstmt.setInt(idx++, oId);
				if(pId > 0) {
					pstmt.setInt(idx++, pId);
				}

				if(viewDataOnly) {	
					if(roles == null || roles.size() == 0) {
						pstmt.setInt(idx++, oId);
					} else {
						pstmt.setArray(idx++, sd.createArrayOf("integer", roleids.toArray(new Integer[roleids.size()])));
					}
				}

				// Request the data
				log.info("Get Usage Data: " + pstmt.toString());
				resultSet = pstmt.executeQuery();

				JSONArray ja = new JSONArray();
				while (resultSet.next()) {

					JSONObject jr = new JSONObject();
					JSONObject jp = new JSONObject();

					JSONObject jg = null;
					String geomValue = resultSet.getString("geo_point");	
					if(geomValue != null) {	
						jg = new JSONObject(geomValue);
					}

					jp.put("prikey", resultSet.getString("id"));
					jp.put(localisation.getString("mf_u"), resultSet.getString("user_ident"));
					jp.put(localisation.getString("u_ref_time"), resultSet.getString("refresh_time"));
					if(forDashboard) {
						jp.put("_label", resultSet.getString("user_ident"));	// For labels
						
						Timestamp refreshWhen = resultSet.getTimestamp("utc_time");
						Timestamp now = new Timestamp(new java.util.Date().getTime());
						
						// Set an integer value that can be used to colour the output depending on time since refresh
						// Based on requirements for half hour intervals get a count of the number of half hours since the last refresh
						long v = (now.getTime() - refreshWhen.getTime()) / 1800000;
						
						if(v >= 4) {
							jp.put("value", 4);	// More than 120 mins - red
						} else if(v >= 3) {
							jp.put("value", 3);	// More than 90 mins - orange
						} else if(v >= 2) {
							jp.put("value", 2);	// More than 60 mins - yellow
						} else{
							jp.put("value", 1);	// Less than 1 hour - green
						}
								
					}
					jr.put("type", "Feature");

					jr.put("geometry", jg);
					jr.put("properties", jp);
					ja.put(jr);

					maxRec = resultSet.getInt("id");	
					recCount++;
				}

				/*
				 * Add columns and types
				 */
				if(forDashboard) {
					columns.put("prikey");
					columns.put(localisation.getString("mf_u"));
					columns.put(localisation.getString("u_ref_time"));
	
					types.put("integer");
					types.put("string");
					types.put("dateTime");

					sqlCount.append(" and lr.id < ").append(maxRec);
	
					try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
					try {if (resultSet != null) {resultSet.close();}} catch (SQLException e) {}
					idx = 1;
					pstmt = sd.prepareStatement(sqlCount.toString());	
					pstmt.setInt(idx++, oId);
					pstmt.setInt(idx++, oId);
					if(pId > 0) {
						pstmt.setInt(idx++, pId);
					}
					if(viewDataOnly) {	
						if(roles == null || roles.size() == 0) {
							pstmt.setInt(idx++, oId);
						} else {
							pstmt.setArray(idx++, sd.createArrayOf("integer", roleids.toArray(new Integer[roleids.size()])));
						}
					}
					resultSet = pstmt.executeQuery();
					if(resultSet.next()) {
						jTotals.put("more_recs", resultSet.getInt(1));
					}
					jTotals.put("max_rec", maxRec);
					jTotals.put("returned_count", recCount);
				}

				jo.put("type", "FeatureCollection");
				jo.put("features", ja);
				if(forDashboard) {
					jo.put("cols", columns);
					jo.put("types", types);
					jo.put("formName", "User Locations");
				}

			} catch (SQLException e) {

				String msg = e.getMessage();
				if(msg.contains("does not exist") && !msg.contains("column") && !msg.contains("operator")) {	// Don't do a stack dump if the table did not exist that just means no one has submitted results yet
					// Don't do a stack dump if the table did not exist that just means no one has submitted results yet
				} else {
					message.append(msg);
					log.log(Level.SEVERE, message.toString(), e);
				}

			} catch (Exception e) {
				log.log(Level.SEVERE, message.toString(), e);
				message.append(e.getMessage());
			} finally {

				try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}

			}

		try {
			jo.put("message", message);
		} catch (Exception e) {
		}

		return jo.toString();
	}
	
	/*
	 * Update the user trail
	 */
	public void recordUserTrail(Connection sd, int userId, String deviceId, List<PointEntry> userTrail) throws SQLException {
		
		if(userTrail != null) {
			String sqlTrail = "insert into user_trail (" +
					"u_id, " +
					"device_id, " +			
					"the_geom," +		// keep this
					"event_time" +
					") " +
					"values(?, ?, ST_GeomFromText(?, 4326), ?);";
			PreparedStatement pstmt = null;
			try {
				pstmt = sd.prepareStatement(sqlTrail);
				pstmt.setInt(1, userId);
				pstmt.setString(2, deviceId);
				for(PointEntry pe : userTrail) {
	
					pstmt.setString(3, "POINT(" + pe.lon + " " + pe.lat + ")");
					
					if(pe.time == 0) {
						log.info("Error time is zero ######### --------+++++++-----------+++++++------------ " + pstmt.toString());
						// Seting to now
						pstmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));		// Hack
					} else {						
						pstmt.setTimestamp(4, new Timestamp(pe.time));
					}
					pstmt.executeUpdate();
				}
			} finally {
				if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
			}

		}	
	}
	
	/*
	 * Log a refresh
	 */
	public void recordRefresh(Connection sd, int oId, String user, Double lat, Double lon, 
			long deviceTime, String hostname, String deviceid, String appVersion, boolean updateLog) throws SQLException {

		StringBuilder sql = new StringBuilder("update last_refresh "
				+ "set refresh_time = now(), "
				+ "geo_point =  ST_GeomFromText('POINT(' || ? || ' ' || ? ||')', 4326) ");
		
		if(deviceTime > 0) {
				sql.append(",device_time = ? ");
		}
		if(deviceid != null) {
			sql.append(", deviceid = ? ");
		}
		if(appVersion != null) {
			sql.append(", appversion = ? ");
		}
		sql.append("where o_id = ? "
				+ "and user_ident = ?");

		String sqlInsert = "insert into last_refresh "
				+ "(o_id, user_ident, refresh_time, geo_point, device_time, deviceid, appversion) "
				+ "values(?, ?, now(),  ST_GeomFromText('POINT(' || ? || ' ' || ? ||')', 4326), ?, ?, ?)";
		
		String sqlInsertLog = "insert into last_refresh_log "
				+ "(o_id, user_ident, refresh_time, geo_point, device_time, deviceid, appversion) "
				+ "values(?, ?, now(), ST_GeomFromText('POINT(' || ? || ' ' || ? ||')', 4326), ?, ?, ?)";
		
		PreparedStatement pstmt = null;

		
		if(user != null) {
			try {
				int idx = 1;
				Timestamp deviceTimeStamp = new Timestamp(deviceTime);
				
				pstmt = sd.prepareStatement(sql.toString());
				pstmt.setDouble(idx++, lon);
				pstmt.setDouble(idx++, lat);
				if(deviceTime > 0) {

					pstmt.setTimestamp(idx++, deviceTimeStamp);
				}
				if(deviceid != null) {
					pstmt.setString(idx++,  deviceid);
				}
				if(appVersion != null) {
					pstmt.setString(idx++,  appVersion);
				}
				pstmt.setInt(idx++, oId);
				pstmt.setString(idx++,  user);
				int count = pstmt.executeUpdate();
				
				if (count == 0) {
					try {pstmt.close();} catch (Exception e) {};
					pstmt = sd.prepareStatement(sqlInsert);
					pstmt.setInt(1, oId);
					pstmt.setString(2, user);
					pstmt.setDouble(3, lon);
					pstmt.setDouble(4, lat);
					pstmt.setTimestamp(5,  deviceTimeStamp);
					pstmt.setString(6,  deviceid);
					pstmt.setString(7,  appVersion);
					pstmt.executeUpdate();
				}
	
				// Write to the log
				if(updateLog) {
					try {pstmt.close();} catch (Exception e) {};
					pstmt = sd.prepareStatement(sqlInsertLog);
					pstmt.setInt(1, oId);
					pstmt.setString(2, user);
					if(GeneralUtilityMethods.isLocationServer(hostname)) {
						log.info("Is location server setting location");
						pstmt.setDouble(3, lon);
						pstmt.setDouble(4, lat);
					} else {
						pstmt.setDouble(3, 0.0);
						pstmt.setDouble(4, 0.0);
					}
					pstmt.setTimestamp(5,  deviceTimeStamp);
					pstmt.setString(6,  deviceid);
					pstmt.setString(7,  appVersion);
					pstmt.executeUpdate();
				}
				
			} finally {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			}
		}
	}
}
