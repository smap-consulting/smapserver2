package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.model.DataItemChange;
import org.smap.sdal.model.DataItemChangeEvent;
import org.smap.sdal.model.SubmissionMessage;
import org.smap.sdal.model.TaskEventChange;
import org.smap.sdal.model.TaskItemChange;

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
	 * Get laest location information
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

			PreparedStatement pstmt = null;

			int totalCount = 0;

			try {

				int oId = GeneralUtilityMethods.getOrganisationId(sd, remoteUser);

				if(forDashboard) {
					jo.put("totals", jTotals);
					jTotals.put("start_key", start_key);
				}

				// Get the number of records
				StringBuffer sqlCount = new StringBuffer("select count(*) "
						+ "from last_refresh lr, users u "
						+ "where u.o_id = ? "
						+ "and lr.o_id = ? "
						+ "and lr.geo_point != null "
						+ "and lr.user_ident = u.ident ");

				if(pId > 0) {
					sqlCount.append(" and lr.user_ident in "
							+ "(select ident from users ux, user_project upx "
							+ "where ux.id = upx.u_id "
							+ "and upx.p_id = ?)");
				}
				
				pstmt = sd.prepareStatement(sqlCount.toString());	
				pstmt.setInt(1, oId);
				pstmt.setInt(2, oId);
				if(pId > 0) {
					pstmt.setInt(3, pId);
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

				StringBuffer sqlData = new StringBuffer("select "
						+ "lr.id,"
						+ "lr.user_ident, "
						+ "timezone(?, lr.refresh_time) as refresh_time, "
						+ "lr.refresh_time as utc_time,"  
						+ "ST_AsGeoJSON(lr.geo_point) as geo_point "
						+ "from last_refresh lr, users u "
						+ "where u.o_id = ? "
						+ "and lr.geo_point != null "
						+ "and lr.o_id = ? "
						+ "and lr.user_ident = u.ident ");

				if(pId > 0) {
					sqlData.append(" and lr.user_ident in "
							+ "(select ident from users ux, user_project upx "
							+ "where ux.id = upx.u_id "
							+ "and upx.p_id = ?)");
				}
				if(start_key > 0) {
					sqlData.append(" and lr.id < ").append(start_key);
				}
				sqlData.append("order by lr.id desc");

				if(rec_limit > 0) {
					sqlData.append(" limit ").append(rec_limit);
				}
				// Get the prepared statement to retrieve data
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sqlData.toString());
				pstmt.setString(1, tz);
				pstmt.setInt(2, oId);
				pstmt.setInt(3, oId);
				if(pId > 0) {
					pstmt.setInt(4, pId);
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
						long v = (now.getTime() - refreshWhen.getTime()) / 3600;
						if(v > 4) {
							jp.put("value", 0);
						} else if(v > 3) {
							jp.put("value", 1);
						} else if(v > 2) {
							jp.put("value", 2);
						} else if(v > 1) {
							jp.put("value", 3);
						} else {
							jp.put("value", 4);
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
					pstmt = sd.prepareStatement(sqlCount.toString());	
					pstmt.setInt(1, oId);
					pstmt.setInt(2, oId);
					if(pId > 0) {
						pstmt.setInt(3, pId);
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
				if(msg.contains("does not exist") && !msg.contains("column")) {	// Don't do a stack dump if the table did not exist that just means no one has submitted results yet
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
}
