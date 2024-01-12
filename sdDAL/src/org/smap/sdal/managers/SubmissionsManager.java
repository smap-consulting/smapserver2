package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.GroupDetails;

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
	LogManager lm = new LogManager();		// Application log
	
	public SubmissionsManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}

	public String getWhereClause(String user, int oId, int dateId, Date startDate, Date endDate, 
			int stopat, String survey_ident) {
		
		StringBuffer whereClause =  new StringBuffer("");
		
		whereClause.append(getJoin(whereClause));		// Only return submissions that made it to the results table
		whereClause.append("results_db_applied");
		
		whereClause.append(getJoin(whereClause));		// Only return submissions that made it to the results table
		whereClause.append("ue.status = 'success'");
		
		if(oId > 0) {
			whereClause.append(getJoin(whereClause));
			whereClause.append("ue.o_id = ?");
		}
		
		if(user != null) {
			whereClause.append(getJoin(whereClause));
			whereClause.append("ue.user_name = ?");
		}
		
		if(stopat > 0) {
			whereClause.append(getJoin(whereClause));
			whereClause.append("ue.ue_id > ?");
		}
		
		if(survey_ident != null) {
			whereClause.append(getJoin(whereClause));
			whereClause.append("ue.ident = ?");
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
	
	/*
	 * Get the submissions statement
	 */
	public PreparedStatement getSubmissionsStatement(
			Connection sd, 
			int rec_limit, 
			int start_key, 
			String whereClause,
			String user,
			int oId,
			String requestingUser,
			int dateId,
			Date startDate,
			Date endDate,
			int stopat,
			String survey_ident) throws SQLException {
		
		String sqlLimit = "";
		if(rec_limit > 0) {
			sqlLimit = "limit " + rec_limit;
		}
		
		StringBuffer sqlPage = new StringBuffer("");
		if(start_key > 0) {
			sqlPage.append(" and ue.ue_id < ").append(start_key);
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
		if(oId > 0) {					// Filter on organisation id
			pstmt.setInt(attribIdx++, oId);
		}
		if(user != null) {					// Filter on user ident
			pstmt.setString(attribIdx++, user);
		}
		if(stopat > 0) {
			pstmt.setInt(attribIdx++, stopat);
		}
		if(survey_ident != null) {
			pstmt.setString(attribIdx++, survey_ident);
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
			boolean getUser,
			boolean links,
			String urlprefix) throws NumberFormatException, JSONException, SQLException {
		JSONObject jr = new JSONObject();
		JSONObject jl = null;	// links
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
		
		// Get the instance id
		String instanceId = resultSet.getString("instanceid");
		
		jp.put("survey_ident", ident);								// survey ident
		jp.put("instanceid", resultSet.getString("instanceid"));							// instanceId
		jp.put(localisation.getString("a_device"), resultSet.getString("imei"));
		jp.put(localisation.getString("a_ut"), resultSet.getString("upload_time"));
		jp.put(localisation.getString("ar_project"), resultSet.getString("project_name"));
		jp.put(localisation.getString("a_sn"), resultSet.getString("survey_notes"));
		jp.put(localisation.getString("a_in"), resultSet.getString("instance_name"));
		jp.put(localisation.getString("a_id"), instanceId);
		jp.put(localisation.getString("a_st"), resultSet.getString("start_time"));
		jp.put(localisation.getString("a_et"), resultSet.getString("end_time"));
		jp.put(localisation.getString("a_sched"), resultSet.getString("scheduled_start"));
		jp.put(localisation.getString("a_sched"), resultSet.getString("scheduled_start"));
		if(getUser) {
			jp.put(localisation.getString("a_user"), resultSet.getString("user_name"));
		}
		
		String location = resultSet.getString("location");
		if(location != null) {							// For map
			
			try {
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
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		if(incMergedLocation) {
			jp.put(localisation.getString("a_l"), location);		// For table	
		}
		
		if(links) {
			jl = new JSONObject();
			
			// Link to data structure used by tasks
			jl.put("data", GeneralUtilityMethods.getInitialDataLink(
					urlprefix, 
					ident, 
					"survey",
					0,		// task id - not needed for survey data
					instanceId));
			
			// Link to pdf
			jl.put("pdf", GeneralUtilityMethods.getPdfLink(
					urlprefix, 
					ident, 
					instanceId,
					tz));
			
			// Link to webform
			jl.put("webform", GeneralUtilityMethods.getWebformLink(
					urlprefix, 
					ident, 
					instanceId));
			
			jp.put("links", jl);
		}
		
		if(isGeoJson) {
			jr.put("properties", jp);
		}
		
		return jr;
	}

	/*
	 * Restore a survey contents from original files
	 */
	public void restore(Connection sd, 
			Connection connectionRel,
			ResourceBundle localisation , String tz, 
			HashMap<String, String> params,
			int oId,
			int uId) throws Exception {
		
		int sId = GeneralUtilityMethods.getKeyValueInt(BackgroundReportsManager.PARAM_SURVEY_ID, params);
		String uIdent = GeneralUtilityMethods.getUserIdent(sd, uId);
		
		lm.writeLog(sd, sId, uIdent, LogManager.RESTORE, "Restore results", 0, null);
		
		if(sId > 0) {

			String sql = null;				
			PreparedStatement pstmt = null;
			PreparedStatement pstmtReset = null;
			PreparedStatement pstmtUnpublish = null;
			
			Statement stmtRel = null;
			try {
				
				// Mark columns as unpublished		
				String sqlUnpublish = "update question set published = 'false' where f_id in (select f_id from form where s_id = ?)";
				pstmtUnpublish = sd.prepareStatement(sqlUnpublish);		
				
				String sqlResetLoadFlag = "update upload_event "
						+ "set results_db_applied = 'false',"
						+ "db_status = null, "
						+ "db_reason = null "
						+ "where ident = ?";
				pstmtReset = sd.prepareStatement(sqlResetLoadFlag);
				
				/*
				 * Get the surveys and tables that are part of the group that this survey belongs to
				 */
				SurveyManager sm = new SurveyManager(localisation, "UTC");
				String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
				ArrayList<GroupDetails> surveys = sm.getAccessibleGroupSurveys(sd, groupSurveyIdent, uIdent, false);
				ArrayList<String> tableList = sm.getGroupTables(sd, groupSurveyIdent, oId, uIdent, sId);
				
				/*
				 * Delete data from each form ready for reload
				 */
				for(String tableName : tableList) {				

					sql = "drop TABLE " + tableName + ";";
					log.info("################################# Delete table contents and drop table prior to restore: " + sql);
					
					try {if (stmtRel != null) {stmtRel.close();}} catch (SQLException e) {}
					stmtRel = connectionRel.createStatement();
					try {
						stmtRel.executeUpdate(sql);
					} catch (Exception e) {
						log.info("Error deleting table: " + e.getMessage());
					}
					log.info("userevent: " + uIdent + " : delete results : " + tableName + " in survey : "+ sId); 
				}
					
				/*
				 * Mark questions as unpublished
				 */
				connectionRel.setAutoCommit(false);
				for(GroupDetails gd : surveys) {
					pstmtUnpublish.setInt(1, gd.sId);
					log.info("set unpublished " + pstmtUnpublish.toString());
					pstmtUnpublish.executeUpdate();
				}
				
				/*
				 * Reload the surveys
				 */
				ExternalFileManager efm = new ExternalFileManager(localisation);
				connectionRel.setAutoCommit(false);
				for(GroupDetails gd : surveys) {
					// restore backed up files from s3 of raw data
					GeneralUtilityMethods.restoreUploadedFiles(gd.surveyIdent, "uploadedSurveys");			
					pstmtReset.setString(1, gd.surveyIdent);			// Initiate reset of go faster flag
					log.info("Restoring survey2 " + pstmtReset.toString());
					pstmtReset.executeUpdate();
					
					// Force regeneration of any dynamic CSV files that this survey links to
					efm.linkerChanged(sd, gd.sId);	// deprecated
				}
				connectionRel.commit();
				
			} finally {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				try {if (pstmtReset != null) {pstmtReset.close();}} catch (SQLException e) {}
				try {if (pstmtUnpublish != null) {pstmtUnpublish.close();}} catch (SQLException e) {}
				try {if (stmtRel != null) {stmtRel.close();}} catch (SQLException e) {}
			

				try {connectionRel.setAutoCommit(true);} catch (Exception e) {}
				
			}
		}
	}
	
	private String getJoin(StringBuffer whereClause) {
		if(whereClause.length() == 0) {
			return "where ";
		} else {
			return " and ";
		}
	}
}
