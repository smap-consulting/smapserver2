package org.smap.sdal.managers;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.constants.SmapQuestionTypes;
import org.smap.sdal.model.AuditItem;
import org.smap.sdal.model.GeoPoint;
import org.smap.sdal.model.KeyFilter;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.RoleColumnFilter;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.SqlParam;
import org.smap.sdal.model.TableColumn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * 
 * SMAP is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * SMAP is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * SMAP. If not, see <http://www.gnu.org/licenses/>.
 * 
 ******************************************************************************/

/*
 * Manage access to submissions
 */
public class SubmissionsManager {

	private static Logger log = Logger.getLogger(SubmissionsManager.class.getName());
	private static ResourceBundle localisation;
	String tz;
	
	public SubmissionsManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}

	public String getWhereClause(boolean user, int dateId, Date startDate, Date endDate) {
		
		StringBuffer whereClause =  new StringBuffer("");
		if(user) {
			whereClause.append(getJoin(whereClause));
			whereClause.append("user_name = ?");
		}
		
		// RBAC
		whereClause.append(getJoin(whereClause));
		whereClause.append(GeneralUtilityMethods.getSurveyRBACUploadEvent());
		
		// Date Filter
		StringBuffer sqlFilter = new StringBuffer("");						
		if(dateId > 0 && dateId < 5) {
			String dateName = null;
			if(dateId == 1) {
				dateName = "upload_time";
			} else if(dateId == 2) {
				dateName = "start_time";
			} else if(dateId == 3) {
				dateName = "end_time";
			} else if(dateId == 4) {
				dateName = "scheduled_start";
			}
			
			// Add start and end dates
			String sqlRestrictToDateRange = GeneralUtilityMethods.getDateRange(startDate, endDate, dateName);
			if(sqlRestrictToDateRange.trim().length() > 0) {
				if(sqlFilter.length() > 0) {
					sqlFilter.append(" and ");
				}
				sqlFilter.append(sqlRestrictToDateRange);
			}
		}
		if(sqlFilter.length() > 0) {
			whereClause.append(getJoin(whereClause));
			whereClause.append(sqlFilter);	
		}
		
		return whereClause.toString();
	}
	
	public PreparedStatement getSubmissionsStatement(
			Connection sd, 
			int rec_limit, 
			int start_key, 
			String whereClause,
			String user,
			String requestingUser,
			int dateId,
			Date startDate,
			Date endDate) throws SQLException {
		
		String sqlLimit = "";
		if(rec_limit > 0) {
			sqlLimit = "limit " + rec_limit;
		}
		
		StringBuffer sqlPage = new StringBuffer("");
		if(start_key > 0) {
			sqlPage.append(" and ue_id < ").append(start_key);
		}	
		
		// Get columns for main select
		StringBuffer sql2 = new StringBuffer("select ");	
		sql2.append("ue.ue_id, "
				+ "ue.survey_name, "
				+ "ue.s_id, "
				+ "s.ident, "
				+ "ue.user_name, "
				+ "s.original_ident, "
				+ "ue.instanceid, "
				+ "to_char(timezone(?, upload_time), 'YYYY-MM-DD HH24:MI:SS') as upload_time,"
				+ "ue.location, "
				+ "p.name as project_name, "
				+ "ue.survey_notes,"
				+ "ue.instance_name, "
				+ "to_char(timezone(?, ue.start_time), 'YYYY-MM-DD HH24:MI:SS') as start_time,"
				+ "to_char(timezone(?, ue.end_time), 'YYYY-MM-DD HH24:MI:SS') as end_time,"
				+ "ue.imei,"
				+ "to_char(timezone(?, ue.scheduled_start), 'YYYY-MM-DD HH24:MI:SS') as scheduled_start"
				+ " ");
		sql2.append("from upload_event ue ");
		sql2.append("left outer join survey s on ue.s_id = s.s_id ");
		sql2.append("left outer join project p on ue.p_id = p.id ");
		
		sql2.append(whereClause);
		if(sqlPage.length() > 0) {
			sql2.append(sqlPage);
		}
		sql2.append(" order by ue_id desc ").append(sqlLimit);
		
		PreparedStatement pstmt = sd.prepareStatement(sql2.toString());
		
		/*
		 * Set prepared statement values
		 */
		int attribIdx = 1;
		
		pstmt.setString(attribIdx++, tz);	// upload time
		pstmt.setString(attribIdx++, tz);	// start time
		pstmt.setString(attribIdx++, tz);	// end time
		pstmt.setString(attribIdx++, tz);	// scheduled start
		if(user != null) {					// Filter on user ident
			pstmt.setString(attribIdx++, user);
		}
		pstmt.setString(attribIdx++, requestingUser);		// For RBAC
		
		
		// dates
		if(dateId > 0 && dateId < 5) {
			if(startDate != null) {
				pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.startOfDay(startDate, tz));
			}
			if(endDate != null) {
				pstmt.setTimestamp(attribIdx++, GeneralUtilityMethods.endOfDay(endDate, tz));
			}
		}
		
		return pstmt;
		
	}
	
	public JSONObject getRecord(ResultSet resultSet, 
			boolean isGeoJson, 
			boolean incMergedLocation,
			boolean getUser) throws NumberFormatException, JSONException, SQLException {
		JSONObject jr = new JSONObject();
		JSONObject jp = null;
		
		if(isGeoJson) {
			jr.put("type", "Feature");
			jp = new JSONObject();
		} else {
			jp = jr;
		}

		jp.put("prikey", resultSet.getString("ue_id"));									// prikey
		jp.put(localisation.getString("a_name"), resultSet.getString("survey_name"));		// survey name
		jp.put("s_id", resultSet.getString("s_id"));										// survey id
		
		// Get the ident of the currently active survey version
		String ident = resultSet.getString("original_ident");
		if(ident == null || ident.trim().length() == 0) {
			ident = resultSet.getString("ident");
		}
		jp.put("survey_ident", ident);								// survey ident
		jp.put("instanceid", resultSet.getString("instanceid"));							// instanceId
		jp.put(localisation.getString("a_device"), resultSet.getString("imei"));
		jp.put(localisation.getString("a_ut"), resultSet.getString("upload_time"));
		jp.put(localisation.getString("ar_project"), resultSet.getString("project_name"));
		jp.put(localisation.getString("a_sn"), resultSet.getString("survey_notes"));
		jp.put(localisation.getString("a_in"), resultSet.getString("instance_name"));
		jp.put(localisation.getString("a_st"), resultSet.getString("start_time"));
		jp.put(localisation.getString("a_et"), resultSet.getString("end_time"));
		jp.put(localisation.getString("a_sched"), resultSet.getString("scheduled_start"));
		jp.put(localisation.getString("a_sched"), resultSet.getString("scheduled_start"));
		if(getUser) {
			jp.put(localisation.getString("a_user"), resultSet.getString("user_name"));
		}
		
		String location = resultSet.getString("location");
		if(location != null) {							// For map
			
			String[] coords = location.split(" ");
			if(coords.length == 2) {
				Double lon = Double.parseDouble(coords[0]);
				Double lat = Double.parseDouble(coords[1]);
				
				if(isGeoJson) {
					JSONObject jg = null;
					JSONArray jCoords = new JSONArray();
				
					jCoords.put(lon);
					jCoords.put(lat);
					jg = new JSONObject();
					jg.put("type", "Point");
					jg.put("coordinates", jCoords);
					jr.put("geometry", jg);
				} else {
					jp.put("lon", lon);
					jp.put("lat", lat);
				}
			}
		}
		if(incMergedLocation) {
			jp.put(localisation.getString("a_l"), location);		// For table	
		}
		if(isGeoJson) {
			jr.put("properties", jp);
		}
		
		return jr;
	}

	private String getJoin(StringBuffer whereClause) {
		if(whereClause.length() == 0) {
			return "where ";
		} else {
			return " and ";
		}
	}
}
