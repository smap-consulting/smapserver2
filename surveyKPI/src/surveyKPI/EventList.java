package surveyKPI;

/*
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

*/

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.Organisation;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/eventList")
public class EventList extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(EventList.class.getName());


	public EventList() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
		
	}

	/*
	 * Retry a notification
	 */
	@GET
	@Path("/retry/{messageId}")
	public Response retry(@Context HttpServletRequest request,
			@PathParam("messageId") int messageId,
			@PathParam("notificationId") int notificationId
			) {
		
		Response response = null;
		String connectionString = "surveyKPI-EventList - retry";
		
		String user = request.getRemoteUser();
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, user);
		if(messageId != 0) {
			a.isValidMessage(sd, request.getRemoteUser(), messageId);
		}
		
		String sqlNot = "delete from notification_log where message_id = ?";
		PreparedStatement pstmtNot = null;
		
		String sqlMsg = "update message set processed_time = null where id = ?";
		PreparedStatement pstmtMsg = null;
		
		try {
			
			// Delete notification
			pstmtNot = sd.prepareStatement(sqlNot);
			pstmtNot.setInt(1,messageId);
			pstmtNot.executeUpdate();
			
			// Set message as unprocessed
			pstmtMsg = sd.prepareStatement(sqlMsg);
			pstmtMsg.setInt(1,messageId);
			pstmtMsg.executeUpdate();
			
			
		} catch (SQLException e) {
				
			log.log(Level.SEVERE, "SQL Exception", e);
		
		} finally {
			try {if (pstmtNot != null) {pstmtNot.close();}} catch (SQLException e) {}
			try {if (pstmtMsg != null) {pstmtMsg.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * Retry an optin message
	 */
	@GET
	@Path("/optin_retry/{id}")
	public Response retry(@Context HttpServletRequest request,
			@PathParam("id") int id
			) {
		
		Response response = null;
		
		String connectionString = "surveyKPI-optin - retry";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		if(id != 0) {
			a.isValidOptin(sd, request.getRemoteUser(), id);
		}
		
		String sql = "select email, uuid from people "
				+ "where o_id = ? "
				+ "and id = ?";
		PreparedStatement pstmt = null;
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);			

			MessagingManager mm = new MessagingManager(localisation);
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, oId);
			EmailServer emailServer = UtilityMethodsEmail.getEmailServer(sd, localisation, null, request.getRemoteUser(), oId);
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setInt(2, id);
			
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				mm.sendOptinEmail(sd, 
						oId, 
						rs.getString(1),  		// email
						organisation.getAdminEmail(), 
						emailServer, 
						rs.getString(2),		// email key 
						"https", 
						request.getServerName(),
						true);
			}
			
		} catch (Exception e) {
				
			log.log(Level.SEVERE, "SQL Exception", e);
		
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * Retry a submission
	 */
	@GET
	@Path("/submission_retry/{surveyId}")
	public Response submissionRetry(@Context HttpServletRequest request,
			@PathParam("surveyId") String surveyId
			) throws SQLException {
		
		Response response = null;
		
		String connectionString = "surveyKPI-submission - retry";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// Get the survey ident
		String surveyIdent = null;
		try {
			int sId = Integer.parseInt(surveyId);
			surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
		} catch(Exception e) {
			// Assume we were passed an ident
			surveyIdent = surveyId;
		}
		
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(),e);
		}
		
		a.isValidSurveyIdent(sd, request.getRemoteUser(), surveyIdent, false, superUser);
	
		String sql1 = "update upload_event "
				+ "set results_db_applied = 'false',"
				+ "db_status = null,"
				+ "db_reason = null "
				+ "where db_status = 'error' "
				+ "and db_reason not like 'Duplicate survey:%' "
				+ "and ident = ? ";
		PreparedStatement pstmt = null;
		
		try {			

			sd.setAutoCommit(false);
			
			// Update the flag that indicates if the record has been applied (performance related flag)
			pstmt = sd.prepareStatement(sql1);
			pstmt.setString(1, surveyIdent);
			log.info("Restore: " + pstmt.toString());
			int count1 = pstmt.executeUpdate();
			log.info("Number of records: " + count1);
			
			sd.commit();
			
			response = Response.ok().build();
			
		} catch (Exception e) {
			sd.rollback();	
			log.log(Level.SEVERE, "SQL Exception", e);
			 response = Response.serverError().entity(e.getMessage()).build();
			 
		} finally {
			sd.setAutoCommit(true);
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	// Respond with JSON 
	@GET
	@Produces("application/json")
	@Path("/{projectId}/{sName}")
	public String getEvents(@Context HttpServletRequest request, 
			@PathParam("projectId") int projectId,
			@PathParam("sName") String sName,
			@QueryParam("hide_errors") boolean hideErrors,
			@QueryParam("hide_duplicates") boolean hideDuplicates,
			@QueryParam("hide_merged") boolean hideMerged,
			@QueryParam("hide_success") boolean hideSuccess,
			@QueryParam("hide_not_loaded") boolean hideNotLoaded,
			@QueryParam("hide_upload_errors") boolean hideUploadErrors,
			@QueryParam("is_forward") boolean is_forward,
			@QueryParam("ignore_old_issues") boolean ignoreOld,
			@QueryParam("start_key") int start_key,
			@QueryParam("rec_limit") int rec_limit) {
		
		log.info("Get events, Project id: " + projectId + " Survey id: " + sName);
		HashMap<Integer, String> surveyNames = new HashMap<Integer, String> ();
		
		String user = request.getRemoteUser();
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-EventList");
		a.isAuthorised(sd, user);
		if(projectId != 0) {
			a.isValidProject(sd, request.getRemoteUser(), projectId);
		}
		// End Authorisation
		
		if(rec_limit == 0) {
			rec_limit = 200;	// Default for number of records to return
		}
		String filter = "";
		if(start_key > 0) {
			filter = " AND ue.ue_id < ? ";
		}
		
		JSONObject jo = new JSONObject();
		
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			
			// Record limiting
			JSONObject jTotals = new JSONObject();
			jo.put("totals", jTotals);
			jTotals.put("start_key", start_key);
			jTotals.put("rec_limit", rec_limit);
			jTotals.put("more_recs", 0);	// Default
			
			String sql = null;
			if(sName == null || sName.equals("_all")) {
				String projSelect = "";
				if(projectId != 0) {	// set to 0 to get all available projects
					projSelect = "AND up.p_id = ? ";
				}
				sql = "SELECT ue.ue_id, ue.s_id, ue.upload_time, ue.user_name, ue.imei, ue.file_name, ue.survey_name, ue.location, "
						+ "ue.db_status as db_status, ue.db_reason as db_reason, "
						+ "ue.ident,"
						+"ue.status as upload_status, ue.reason as upload_reason, ue.incomplete "
						+ "from upload_event ue "
						+ "inner join user_project up "
						+ "on ue.p_id = up.p_id "
						+ "inner join users u "
						+ "on up.u_id = u.id "
						+ "inner join project p "
						+ "on up.p_id = p.id "
						+ "where u.ident = ? "
						+ "and p.o_id = ? "
						+ getObsoleteFilter(ignoreOld)
						+ projSelect
						+ filter
						+ " order by ue.ue_id desc;";
				pstmt = sd.prepareStatement(sql);
				int argIdx = 1;
				pstmt.setString(argIdx++, user);
				pstmt.setInt(argIdx++, oId);
				if(projectId != 0) {
					pstmt.setInt(argIdx++, projectId);
				}
				if(start_key > 0) {
					pstmt.setInt(argIdx++, start_key);
				}
				
			} else {
				// get the survey ident
				String sIdent = null;
				try {
					int sId = Integer.parseInt(sName);
					sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
				} catch(Exception e) {
					// Assume we were passed an ident
					sIdent = sName;
				}
				sql = "SELECT ue.ue_id, ue.s_id, ue.upload_time, ue.user_name, ue.imei, ue.file_name, ue.survey_name, ue.location, " +
						"ue.db_status as db_status, ue.db_reason as db_reason, " +
						"ue.ident, " +
						"ue.status as upload_status, ue.reason as upload_reason, ue.incomplete " +
						"FROM upload_event ue " +
						"inner join user_project up " +
						"on ue.p_id = up.p_id " +
						"inner join users u " +
						"on up.u_id = u.id " +
						"where u.ident = ? " +
						"and ue.ident = ? " +
						"and up.p_id = ? " +
						getObsoleteFilter(ignoreOld) +
						filter +
						" ORDER BY ue.ue_id desc;";
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, user);
				pstmt.setString(2, sIdent);
				pstmt.setInt(3, projectId);
				if(start_key > 0) {
					pstmt.setInt(4, start_key);
				}
			}

			 log.info("Events List: " + pstmt.toString());

			 resultSet = pstmt.executeQuery();
			 JSONArray ja = new JSONArray();	
			 int countRecords = 0;
			 int maxRec = 0;
			 while (resultSet.next()) {
				 String status = resultSet.getString("db_status");
				 String reason = resultSet.getString("db_reason");
				 String upload_status = resultSet.getString("upload_status");
				 String upload_reason = resultSet.getString("upload_reason");
				 
				 if(
						 (status == null && !hideNotLoaded) ||
						 (status != null && !hideSuccess && status.equals("success")) ||
						 (status != null && !hideErrors && status.equals("error") && (reason == null || !reason.startsWith("Duplicate survey:"))) ||
						 (status != null && !hideDuplicates && status.equals("error") && (reason != null && reason.startsWith("Duplicate survey:")) ||
						 (status != null && !hideMerged && status.equals("merged")) ||
						 (upload_status != null && upload_status.equals("error") && !hideUploadErrors)
						 )) {
					
					
					// Only return max limit
					if(countRecords++ >= rec_limit) {
						// We have at least one more record than we want to return
						jTotals.put("more_recs", 1);	// Ideally we should record the number of records still to be returned
						countRecords--;					// Set to the number of records actually returned
						break;
					}
					
					JSONObject jr = new JSONObject();
					jr.put("type", "Feature");
					
					// Add Geometry
					JSONObject jg = null;
					String geom = resultSet.getString("location");					 
					if(geom != null) {
						log.info("Geom: " + geom);
						JSONArray jCoords = new JSONArray();
						if(geom.contains("line:")) {
							geom = geom.replace("line:", "");	// Remove the line: from a geocompound location
						}
						String[] coords = geom.split(" ");
						if(coords.length == 2) {
							coords[0] = coords[0].replace(";", "");    // hack these coords with semicolons are probably coming from compound widgets
							coords[1] = coords[1].replace(";", "");
							log.info("Coords: "  + coords[0] + " : " + coords[1]);
							try {
								jCoords.put(Double.parseDouble(coords[0]));
								jCoords.put(Double.parseDouble(coords[1]));
							} catch (Exception e) {
								log.log(Level.SEVERE, geom, e);
							}
							jg = new JSONObject();
							jg.put("type", "Point");
							jg.put("coordinates", jCoords);
							jr.put("geometry", jg);
						}
					}
					
					// Add the properties
					JSONObject jp = new JSONObject();
					jp.put("ue_id", resultSet.getInt("ue_id"));
					jp.put("upload_time", resultSet.getString("upload_time"));
					jp.put("user_name", resultSet.getString("user_name"));
					jp.put("file_name", resultSet.getString("file_name"));
					int sId = resultSet.getInt("s_id");
					String nm = surveyNames.get(sId);
					if(nm == null) {
						nm = GeneralUtilityMethods.getSurveyName(sd, sId);
						if(nm == null) { // erased survey
							nm = resultSet.getString("survey_name");
						}
					}
					jp.put("survey_name", nm);
					jp.put("imei", resultSet.getString("imei"));
					jp.put("ident", resultSet.getString("ident"));
					jp.put("complete", (resultSet.getBoolean("incomplete")) ? "c_no" : "c_yes");
					if(upload_status != null && upload_status.equals("error")) {
						jp.put("status", "upload error");
						jp.put("reason", upload_reason);
					} else {
						if(status == null) {					// Not loaded by subscriber
								status = "not_loaded";
								reason = "Not added to database";
						}
						jp.put("status", status);
						jp.put("reason", reason);
					}
					jr.put("properties", jp);
					ja.put(jr);
					
					maxRec = resultSet.getInt("ue_id");
				 }
				 
				 jTotals.put("max_rec", maxRec);
				 jTotals.put("returned_count", countRecords);
				 String uploadTime = resultSet.getString("upload_time");
				 String aUploadTime [] = uploadTime.split(" ");
				 jTotals.put("to_date", aUploadTime[0]);
				 if(countRecords == 1) {	 
					 if(aUploadTime.length > 1) {
						 jTotals.put("from_date", aUploadTime[0]);
					 }
				 }
				 
				 jo.put("type", "FeatureCollection");
				 jo.put("features", ja);
			 }
			 

		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Exception", e);
		} catch (JSONException e) {
			log.log(Level.SEVERE, "JSON Exception", e);
		} finally {
			try {
				if(resultSet != null) {
					resultSet.close();
				}
			} catch (SQLException e) {
			
			}
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-EventList", sd);
		}
		
		return jo.toString();
	}

	/*
	 * Get the individual notification events
	 */
	@GET
	@Produces("application/json")
	@Path("/notifications/{projectId}/{sName}")
	public String getNotificationEvents(@Context HttpServletRequest request, 
			@PathParam("projectId") int projectId,
			@PathParam("sName") String sName,
			@QueryParam("hide_errors") boolean hideErrors,
			@QueryParam("hide_success") boolean hideSuccess,
			@QueryParam("start_key") int start_key,
			@QueryParam("rec_limit") int rec_limit,
			@QueryParam("ignore_old_issues") boolean ignoreOld) {
		
		String connectionString = "surveyKPI - EventList - noifications";
		
		String user = request.getRemoteUser();
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, user);
		// End Authorisation
		
		if(rec_limit == 0) {
			rec_limit = 200;	// Default for number of records to return
		}
		String filter = "";
		if(start_key > 0) {
			filter = " and n.id < ? ";
		}
		
		int sId = 0;
		if(sName != null && !sName.equals("_all")) {
			sId = Integer.parseInt(sName);
		}
		
		JSONObject jo = new JSONObject();

		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		try {
			// Record limiting
			JSONObject jTotals = new JSONObject();
			jo.put("totals", jTotals);
			jTotals.put("start_key", start_key);
			jTotals.put("rec_limit", rec_limit);
			jTotals.put("more_recs", 0);	// Default
			
			String sql = null;
			String projSurveySelect = "";
			if(sId > 0) {
				projSurveySelect = "and n.s_id = ? ";
			} else if(projectId > 0) {
				projSurveySelect = "and n.p_id = ? ";
			}
				
			sql = "SELECT n.id, n.status, n.notify_details, n.status_details, n.event_time, n.message_id, type " +
					"from notification_log n, users u " +
					"where u.ident = ? " +
					"and u.o_id = n.o_id " +
					getObsoleteFilterNotifications(ignoreOld) +
					filter +
					projSurveySelect +
					" ORDER BY n.id desc";
		
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, user);
			int argIdx = 2;
			if(start_key > 0) {
				pstmt.setInt(argIdx++, start_key);
			}
			if(sId > 0) {
				pstmt.setInt(argIdx++, sId);
			} else if(projectId > 0) {
				pstmt.setInt(argIdx++, projectId);
			}
			
			log.info("Notification Events List: " + pstmt.toString());

			 resultSet = pstmt.executeQuery();
			 JSONArray ja = new JSONArray();	
			 int countRecords = 0;
			 int maxRec = 0;
			 while (resultSet.next()) {
				 String status = resultSet.getString("status");
				 if(
						 (status != null && !hideSuccess && status.toLowerCase().equals("success")) ||
						 (status != null && !hideErrors && status.toLowerCase().equals("error")) 
						
						 ) {
					
					// Only return max limit
					if(countRecords++ >= rec_limit) {
						// We have at least one more record than we want to return
						jTotals.put("more_recs", 1);	// Ideally we should record the number of records still to be returned
						countRecords--;					// Set to the number of records actually returned
						break;
					}
					
					JSONObject jr = new JSONObject();
					jr.put("type", "Feature");
					
					// Add the properties
					JSONObject jp = new JSONObject();
					jp.put("id", resultSet.getInt("id")); 
					jp.put("notify_details", resultSet.getString("notify_details"));
					jp.put("status", resultSet.getString("status"));
					jp.put("status_details", resultSet.getString("status_details"));
					jp.put("event_time", resultSet.getString("event_time"));
					jp.put("message_id", resultSet.getString("message_id"));
					jp.put("type", resultSet.getString("type"));
					jr.put("properties", jp);
					ja.put(jr);
					
					maxRec = resultSet.getInt("id");
				 }
				 
				 jTotals.put("max_rec", maxRec);
				 jTotals.put("returned_count", countRecords);
				 String eventTime = resultSet.getString("event_time");
				 String aEventTime [] = eventTime.split(" ");
				 jTotals.put("to_date", aEventTime[0]);
				 if(countRecords == 1) {	 
					 if(aEventTime.length > 1) {
						 jTotals.put("from_date", aEventTime[0]);
					 }
				 }
				 
				 jo.put("type", "FeatureCollection");
				 jo.put("features", ja);
			 }
			 

		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Exception", e);
		} catch (JSONException e) {
			log.log(Level.SEVERE, "JSON Exception", e);
		} finally {
			
			try {if(resultSet != null) {resultSet.close();}	} catch (SQLException e) {}
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return jo.toString();
	}


	/*
	 * Get the individual optin events
	 */
	@GET
	@Produces("application/json")
	@Path("/optin")
	public String getOptinEvents(@Context HttpServletRequest request, 
			@QueryParam("hide_errors") boolean hideErrors,
			@QueryParam("hide_success") boolean hideSuccess,
			@QueryParam("start_key") int start_key,
			@QueryParam("rec_limit") int rec_limit) {
		
		String connectionString = "surveyKPI - EventList - Optin";
		String user = request.getRemoteUser();
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, user);
		// End Authorisation
		
		if(rec_limit == 0) {
			rec_limit = 200;	// Default for number of records to return
		}
		String filter = "";
		if(start_key > 0) {
			filter = " and p.id < ? ";
		}
		
		JSONObject jo = new JSONObject();

		String sqlPending = "select count(*) from pending_message "
				+ "where o_id = ? "
				+ "and email = ? "
				+ "and processed_time is null";
		PreparedStatement pstmtPending = null;
		ResultSet rsPending = null;
		
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		try {
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			pstmtPending = sd.prepareStatement(sqlPending);
			pstmtPending.setInt(1, oId);
			
			// Record limiting
			JSONObject jTotals = new JSONObject();
			jo.put("totals", jTotals);
			jTotals.put("start_key", start_key);
			jTotals.put("rec_limit", rec_limit);
			jTotals.put("more_recs", 0);	// Default
				
			String sql = "select p.id, "
					+ "p.opted_in_status as status, "
					+ "p.email, "
					+ "p.opted_in_status_msg as status_details, "
					+ "p.opted_in_sent as event_time, "
					+ "p.opted_in_count "
					+ "from people p "
					+ "where p.o_id = ? "
					+ "and not p.opted_in "
					+ "and not p.unsubscribed "
					+ filter
					+ " order by p.id desc";
		
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			int argIdx = 2;
			if(start_key > 0) {
				pstmt.setInt(argIdx++, start_key);
			}
			
			log.info("Optin Events List: " + pstmt.toString());

			 resultSet = pstmt.executeQuery();
			 JSONArray ja = new JSONArray();	
			 int countRecords = 0;
			 int maxRec = 0;
			 while (resultSet.next()) {
				 String status = resultSet.getString("status");
				 if(
						 (status != null && !hideSuccess && status.toLowerCase().equals("success")) ||
						 (status != null && !hideErrors && status.toLowerCase().equals("error")) 
						
						 ) {
					
					// Only return max limit
					if(countRecords++ >= rec_limit) {
						// We have at least one more record than we want to return
						jTotals.put("more_recs", 1);	// Ideally we should record the number of records still to be returned
						countRecords--;					// Set to the number of records actually returned
						break;
					}
					
					JSONObject jr = new JSONObject();
					jr.put("type", "Feature");
					
					// Add the properties
					JSONObject jp = new JSONObject();
					jp.put("id", resultSet.getInt("id")); 
					
					String email = resultSet.getString("email");
					jp.put("email", email);
					jp.put("status", resultSet.getString("status"));
					jp.put("status_details", resultSet.getString("status_details"));
					jp.put("event_time", resultSet.getString("event_time"));
					jp.put("opted_in_count", resultSet.getString("opted_in_count"));				
					
					pstmtPending.setString(2, email);
					rsPending = pstmtPending.executeQuery();
					int count = 0;
					if(rsPending.next()) {
						count = rsPending.getInt(1);
					}
					jp.put("pending_count", count);
					
					jr.put("properties", jp);
					ja.put(jr);
					
					maxRec = resultSet.getInt("id");
				 }
				 
				 jTotals.put("max_rec", maxRec);
				 jTotals.put("returned_count", countRecords);
				 String eventTime = resultSet.getString("event_time");
				 if(eventTime != null) {
					 String aEventTime [] = eventTime.split(" ");
					 jTotals.put("to_date", aEventTime[0]);
					 if(countRecords == 1) {	 
						 if(aEventTime.length > 1) {
							 jTotals.put("from_date", aEventTime[0]);
						 }
					 }
				 }
				 
				 jo.put("type", "FeatureCollection");
				 jo.put("features", ja);
			 }
			 

		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
		} finally {
			
			try {if(resultSet != null) {resultSet.close();}	} catch (SQLException e) {}
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtPending != null) {pstmtPending.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return jo.toString();
	}
	
	private class StatusTotal {
		String key;
		String dest;
		int success = 0;
		int errors = 0;
		int duplicates = 0;
		int merged = 0;
		int notLoaded = 0;
		int uploadErrors = 0;
	}
	
	// Get totals for notifications
	@GET
	@Produces("application/json")
	@Path("/notifications/{projectId}/{sName}/totals")
	public String getNotificationTotals(@Context HttpServletRequest request, 
			@PathParam("projectId") int pId,
			@PathParam("sName") String sName,
			@QueryParam("hide_errors") boolean hideErrors,
			@QueryParam("hide_success") boolean hideSuccess,
			@QueryParam("ignore_old_issues") boolean ignoreOld
			) {
		
		String connectionString = "EventList - Get Notification Totals";
		String user = request.getRemoteUser();
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, user);
		// End Authorisation
		
		HashMap<String,StatusTotal> sList = new HashMap<String,StatusTotal> ();
		JSONObject jo = new JSONObject();
		
		PreparedStatement pstmt = null;

		int sId = 0;
		if(sName != null && !sName.equals("_all")) {
			sId = Integer.parseInt(sName);
		}
		
		try {
			if(!hideSuccess) {
				addNotificationTotals("success", user, pstmt, sd,	sList, pId, sId, false); 
			}
			if(!hideErrors) {
				addNotificationTotals("error", user, pstmt, sd, sList, pId, sId, ignoreOld); 
			}
			
			
			ArrayList<String> totals = new ArrayList<String> ();
			for (String uniqueTotal : sList.keySet()) {
				totals.add(uniqueTotal);
			}
			
			// Create JSON array
			JSONArray ja = new JSONArray();	
			StatusTotal st = sList.get("notifications");
			
			JSONObject jr = new JSONObject();
			jr.put("type", "Feature");
					
			// Add the properties
			JSONObject jp = new JSONObject();
			jp.put("key", "notifications");
			if(!hideSuccess) {
				jp.put("success", st.success);
			}
			if(!hideErrors) {
				jp.put("errors", st.errors);
			}
			jr.put("properties", jp);
			ja.put(jr);
				 
			jo.put("type", "FeatureCollection");
			jo.put("features", ja);

		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Exception", e);
		} catch (JSONException e) {
			log.log(Level.SEVERE, "JSON Exception", e);
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return jo.toString();
	}
	
	// Get totals for opt in message
	@GET
	@Produces("application/json")
	@Path("/optin/totals")
	public String getOptinTotals(@Context HttpServletRequest request, 
			@QueryParam("hide_errors") boolean hideErrors,
			@QueryParam("hide_success") boolean hideSuccess
			) {
		
		String connectionString = "EventList - Get Optin Totals";
		String user = request.getRemoteUser();
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, user);
		// End Authorisation
		
		HashMap<String,StatusTotal> sList = new HashMap<String,StatusTotal> ();
		JSONObject jo = new JSONObject();
		
		PreparedStatement pstmt = null;
		
		try {
			if(!hideSuccess) {
				addOptinTotals("success", user, pstmt, sd,	sList); 
			}
			if(!hideErrors) {
				addOptinTotals("error", user, pstmt, sd, sList); 
			}
			
			
			ArrayList<String> totals = new ArrayList<String> ();
			for (String uniqueTotal : sList.keySet()) {
				totals.add(uniqueTotal);
			}
			
			// Create JSON array
			JSONArray ja = new JSONArray();	
			StatusTotal st = sList.get("optin");
			
			JSONObject jr = new JSONObject();
			jr.put("type", "Feature");
					
			// Add the properties
			JSONObject jp = new JSONObject();
			jp.put("key", "optin");
			if(!hideSuccess) {
				jp.put("success", st.success);
			}
			if(!hideErrors) {
				jp.put("errors", st.errors);
			}
			jr.put("properties", jp);
			ja.put(jr);
				 
			jo.put("type", "FeatureCollection");
			jo.put("features", ja);

		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return jo.toString();
	}
	
	// Get totals for events
	@GET
	@Produces("application/json")
	@Path("/{projectId}/{sName}/totals")
	public String getEventTotals(@Context HttpServletRequest request, 
			@PathParam("projectId") int projectId,
			@PathParam("sName") String sName,
			@QueryParam("hide_errors") boolean hideErrors,
			@QueryParam("hide_duplicates") boolean hideDuplicates,
			@QueryParam("hide_merged") boolean hideMerged,
			@QueryParam("hide_success") boolean hideSuccess,
			@QueryParam("hide_not_loaded") boolean hideNotLoaded,
			@QueryParam("hide_upload_errors") boolean hideUploadErrors,
			@QueryParam("groupby") String groupby,
			@QueryParam("is_forward") boolean is_forward,
			@QueryParam("ignore_old_issues") boolean ignoreOld) {
		
		String user = request.getRemoteUser();
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-EventList");
		a.isAuthorised(sd, user);
		if(projectId != 0) {	// 0 is not a valid project but represents all projects
			a.isValidProject(sd, request.getRemoteUser(), projectId);
		}
		// End Authorisation
		
		HashMap<String,StatusTotal> sList = new HashMap<String,StatusTotal> ();
		JSONObject jo = new JSONObject();

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			
			// Get the survey ident
			String surveyIdent = null;
			try {
				int sId = Integer.parseInt(sName);
				surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			} catch(Exception e) {
				// Assume we were passed an ident
				surveyIdent = sName;
			}
			
			if(!hideSuccess) {
				addStatusTotals("success", sName, surveyIdent, projectId, user, sd,	groupby, sList, is_forward, oId, false);	// Always all records 
			}
			if(!hideErrors) {
				addStatusTotals("errors", sName, surveyIdent, projectId, user, sd,	groupby, sList, is_forward, oId, ignoreOld);		// Ignore old errors 
			}
			if(!hideDuplicates) {
				addStatusTotals("duplicates", sName, surveyIdent, projectId, user, sd,	groupby, sList, is_forward, oId, ignoreOld); 
			}
			if(!hideMerged) {
				addStatusTotals("merged", sName, surveyIdent, projectId, user, sd,	groupby, sList, is_forward, oId, ignoreOld); 
			}
			if(!hideNotLoaded) {
				addStatusTotals("not_loaded", sName, surveyIdent, projectId, user, sd, groupby, sList, is_forward, oId, ignoreOld); 
			}
			if(!hideUploadErrors) {
				addStatusTotals("upload_errors", sName, surveyIdent, projectId, user, sd, groupby, sList, is_forward, oId, ignoreOld); 
			}
			
			
			ArrayList<String> totals = new ArrayList<String> ();
			for (String uniqueTotal : sList.keySet()) {
				totals.add(uniqueTotal);
			}
		
			// Sort the list of keys 
			List<String> sortedTotals = totals.subList(0, totals.size());
			Collections.sort(sortedTotals);
			// If the totals are grouped by day, week or month then sort in descending order
			if(groupby != null && (groupby.equals("day") || groupby.equals("week") || groupby.equals("month"))) {
				Collections.reverse(sortedTotals);
			}
			
			// Create JSON array
			JSONArray ja = new JSONArray();			 
			for (int i = 0; i < sortedTotals.size(); i++) {
				String key = sortedTotals.get(i);
				StatusTotal st = sList.get(key);
				
				JSONObject jr = new JSONObject();
				jr.put("type", "Feature");
					
				// Add the properties
				JSONObject jp = new JSONObject();
				jp.put("key", st.key);
				jp.put("dest", st.dest);
				if(!hideSuccess) {
					jp.put("success", st.success);
				}
				if(!hideErrors) {
					jp.put("errors", st.errors);
				}
				if(!hideDuplicates) {
					jp.put("duplicates", st.duplicates);
				}
				if(!hideMerged) {
					jp.put("merged", st.merged);
				}
				if(!hideNotLoaded) {
					jp.put("not_loaded", st.notLoaded);
				}
				if(!hideUploadErrors) {
					jp.put("upload_errors", st.uploadErrors);
				}
				jr.put("properties", jp);
				ja.put(jr);
			}
				 
			jo.put("type", "FeatureCollection");
			jo.put("features", ja);

		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Exception", e);
		} catch (JSONException e) {
			log.log(Level.SEVERE, "JSON Exception", e);
		} finally {

			SDDataSource.closeConnection("surveyKPI-EventList", sd);
		}
		
		return jo.toString();
	}
	
	private void addStatusTotals(
			String status, 
			String sName, 
			String surveyIdent,
			int projectId,
			String user,
			Connection sd,
			String groupby,
			HashMap<String,StatusTotal> sList,
			boolean isForward,
			int oId,
			boolean ignoreOld) throws SQLException {
		
		PreparedStatement pstmt = null;
		
		try {
			
			String selectStatus = null;
			if(status.equals("success")) {
				selectStatus = "AND ue.db_status = 'success' ";
			} else if(status.equals("errors")) {
				selectStatus = "AND (coalesce(ue.db_status,'') = 'error' AND coalesce(ue.db_reason,'') not like 'Duplicate survey:%') ";
			} else if(status.equals("not_loaded")) {
				selectStatus = "AND ue.status != 'error' and ue.db_status is null ";
			} else if(status.equals("duplicates")) {
				selectStatus = "AND ue.db_status = 'error' AND ue.db_reason like 'Duplicate survey:%' ";
			} else if(status.equals("merged")) {
				selectStatus = "AND ue.db_status = 'merged' ";
			} else if(status.equals("upload_errors")) {
				selectStatus = "AND ue.status = 'error' ";
			}
			
			String sql = null;
			if(sName == null || sName.equals("_all")) {
				String projSelect = "";
				if(projectId != 0) {	// set to 0 to get all available projects
					projSelect = " AND up.p_id = ? ";
				}
				String aggregate;
				String getDest;
				if(isForward) {
					aggregate = "ue.ident, se.dest";
					getDest = ",se.dest ";
				} else {
					aggregate = "ue.ident";
					getDest = "";
				}
				
				sql = "SELECT count(*), ue.ident "
						+ getDest
						+ "from upload_event ue "
						+ "inner join user_project up "
						+ "on ue.p_id = up.p_id "
						+ "inner join users u "
						+ "on up.u_id = u.id "
						+ "inner join project p "
						+ "on up.p_id = p.id "
						+ "where u.ident = ? "
						+ "and p.o_id = ? "
						+ getObsoleteFilter(ignoreOld)
						+ selectStatus
						+ projSelect
						+ "GROUP BY " + aggregate
						+ " ORDER BY " + aggregate + " desc";
	
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, user);
				pstmt.setInt(2, oId);
				if(projectId != 0) {
					pstmt.setInt(3, projectId);
				}
			} else if(groupby == null || groupby.equals("device")) {
				
				String aggregate;
				String getDest;
				if(isForward) {
					aggregate = "ue.imei, se.dest";
					getDest = ",se.dest ";
				} else {
					aggregate = "ue.imei";
					getDest = "";
				}
				
				sql = "SELECT count(*), ue.imei "
						+ getDest
						+ "from upload_event ue "
						+ "inner join user_project up "
						+ "on ue.p_id = up.p_id "
						+ "inner join users u "
						+ "on up.u_id = u.id "
						+ "inner join project p "
						+ "on up.p_id = p.id "
						+ "where u.ident = ? "
						+ "and ue.ident = ? "
						+ "and up.p_id = ? "
						+ "and p.o_id = ? "
						+ getObsoleteFilter(ignoreOld)
						+ selectStatus
						+ " group by " + aggregate
						+ " order by " + aggregate + " asc";
				
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, user);
				pstmt.setString(2, surveyIdent);
				pstmt.setInt(3, projectId);
				pstmt.setInt(4, oId);
				
			} else if(groupby.equals("month")) {
				
				String aggregate = "extract(year from upload_time) || '-' || lpad(cast(extract(month from upload_time) as varchar), 2, '0')";
				
				if(isForward) {
	
					aggregate += ",se.dest ";
				} 
				
				sql = "SELECT count(*), " 
						+ aggregate
						+ " from upload_event ue "
						+ "inner join user_project up "
						+ "on ue.p_id = up.p_id "
						+ "inner join users u "
						+ "on up.u_id = u.id "
						+ "inner join project p "
						+ "on up.p_id = p.id "
						+ "where u.ident = ? "
						+ "and ue.ident = ? "
						+ "and up.p_id = ? "
						+ "and p.o_id = ? "
						+ getObsoleteFilter(ignoreOld)
						+ selectStatus
						+ " group by " + aggregate
						+ " order by " + aggregate + " desc";
				
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, user);
				pstmt.setString(2, surveyIdent);
				pstmt.setInt(3, projectId);
				pstmt.setInt(4, oId);
			} else if(groupby.equals("week")) {			
	
				String aggregate = "extract(year from upload_time) || '-' || lpad(cast(extract(week from upload_time) as varchar), 2, '0')";
				if(isForward) {			
					aggregate += ", se.dest ";
				} 
				
				sql = "SELECT count(*), " 
						+ aggregate
						+ " from upload_event ue "
						+ "inner join user_project up "
						+ "on ue.p_id = up.p_id "
						+ "inner join users u "
						+ "on up.u_id = u.id "
						+ "inner join project p "
						+ "on up.p_id = p.id "
						+ "where u.ident = ? "
						+ "and ue.ident = ? "
						+ "and up.p_id = ? "
						+ "and p.o_id = ? "
						+ getObsoleteFilter(ignoreOld)
						+ selectStatus
						+ " group by " + aggregate
						+ " order by " + aggregate + " desc";
				
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, user);
				pstmt.setString(2, surveyIdent);
				pstmt.setInt(3, projectId);
				pstmt.setInt(4, oId);
				
			} else if(groupby.equals("day")) {
				
				String aggregate = "extract(year from upload_time) || '-' || "
						+ "lpad(cast(extract(month from upload_time) as varchar), 2, '0') || '-' || "
						+ "lpad(cast(extract(day from upload_time) as varchar), 2, '0') ";	
				if(isForward) {
					aggregate += ",se.dest ";
				}
				
				sql = "SELECT count(*), " 
						+ aggregate
						+ " from upload_event ue "
						+ "inner join user_project up "
						+ "on ue.p_id = up.p_id "
						+ "inner join users u "
						+ "on up.u_id = u.id "
						+ "inner join project p "
						+ "on up.p_id = p.id "
						+ "where u.ident = ? "
						+ "and ue.ident = ? "
						+ "and up.p_id = ? "
						+ "and p.o_id = ? "
						+ getObsoleteFilter(ignoreOld)
						+ selectStatus
						+ " group by " + aggregate
						+ " order by " + aggregate + " desc";
				
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, user);
				pstmt.setString(2, surveyIdent);
				pstmt.setInt(3, projectId);
				pstmt.setInt(4, oId);
			}
	
			log.info("Get totals for events: " + pstmt.toString());
	
			 ResultSet resultSet = pstmt.executeQuery();
			 while (resultSet.next()) {
				 int count = resultSet.getInt(1);
				 String key = resultSet.getString(2);
				 String dest = "";
				 if(isForward) {
					 dest = resultSet.getString(3);
				 }
				 
				 if(sName == null || sName.equals("_all")) {
					 // Convert survey ident to display name
					 String sIdent = key;
					 String nm = GeneralUtilityMethods.getSurveyNameFromIdent(sd, sIdent);
					 if(nm != null) {
						 key = nm;
					 }
				 }
				 StatusTotal st = sList.get(key + dest);
				
				 if(st == null) {
					 st = new StatusTotal();
					 sList.put(key + dest, st);
					 st.key = key;
					 st.dest = dest;
				 }
				 if(status.equals("success")) {
					 st.success = count;
				 } else if(status.equals("errors")) {
					 st.errors = count;
				 } else if(status.equals("duplicates")) {
					 st.duplicates = count;
				 } else if(status.equals("merged")) {
					 st.merged = count;
				 } else if(status.equals("not_loaded")) {
					 st.notLoaded = count;
				 } else if(status.equals("upload_errors")) {
					 st.uploadErrors = count;
				 }
			 }
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
	}
	
	private void addNotificationTotals(String status, 
			String user,
			PreparedStatement pstmt, 
			Connection sd,
			HashMap<String,StatusTotal> sList,
			int pId,
			int sId,
			boolean ignoreOld) throws SQLException {
		
		String sql = null;
		
		String filter = "";
		if(sId > 0) {		// surveyId is always a stronger filter than projectId
			filter = "and n.s_id = ? ";
		} else if(pId > 0) {				
			filter = "and n.p_id = ? ";
		}
			
		sql = "SELECT count(*) " +
				"from notification_log n, users u " +
				"where u.ident = ? " +
				"and n.o_id = u.o_id " +
				"and n.status = ? " +
				getObsoleteFilterNotifications(ignoreOld) +
				filter;

		try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		pstmt = sd.prepareStatement(sql);
		pstmt.setString(1, user);			
		pstmt.setString(2, status);
		if(sId > 0) {
			pstmt.setInt(3, sId);
		} else if(pId > 0) {
			pstmt.setInt(3, pId);
		}

		log.info("Notification totals: " + pstmt.toString());
		ResultSet resultSet = pstmt.executeQuery();
		if (resultSet.next()) {
			int count = resultSet.getInt(1);
			 
			StatusTotal st = sList.get("notifications");			
			if(st == null) {
				st = new StatusTotal();
				sList.put("notifications", st);
				st.key = "notifications";
			 }
			 if(status.equals("success")) {
				 st.success = count;
			 } else if(status.equals("error")) {
				 st.errors = count;
			 } 
		 }
	}
	
	private void addOptinTotals(String status, 
			String user,
			PreparedStatement pstmt, 
			Connection sd,
			HashMap<String,StatusTotal> sList) throws SQLException {
		
		String sql = null;
			
		sql = "SELECT count(*) "
				+ "from people p, users u "
				+ "where u.ident = ? "
				+ "and p.o_id = u.o_id "
				+ "and p.opted_in_status = ? "
				+ "and not p.unsubscribed "
				+ "and not opted_in ";

		try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		pstmt = sd.prepareStatement(sql);
		pstmt.setString(1, user);			
		pstmt.setString(2, status);

		log.info("Optin totals: " + pstmt.toString());
		ResultSet resultSet = pstmt.executeQuery();
		if (resultSet.next()) {
			int count = resultSet.getInt(1);
			 
			StatusTotal st = sList.get("optin");			
			if(st == null) {
				st = new StatusTotal();
				sList.put("optin", st);
				st.key = "optin";
			 }
			 if(status.equals("success")) {
				 st.success = count;
			 } else if(status.equals("error")) {
				 st.errors = count;
			 } 
		 }
	}
	
	// Get forms
	@GET
	@Produces("application/json")
	@Path("/{projectId}/{sName}/forms")
	public String getEventForms(@Context HttpServletRequest request, 
			@PathParam("projectId") int projectId,
			@PathParam("sName") String sName) {

		int sId = -1;
		
		if(sName.equals("_all")) {
			
		} else {
			sId = Integer.parseInt(sName);
		}
		
		String user = request.getRemoteUser();
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-EventList");
		a.isAuthorised(sd, user);
		if(projectId != 0) {
			a.isValidProject(sd, request.getRemoteUser(), projectId);
		}
		// End Authorisation
		
		
		JSONArray ja = new JSONArray();
		PreparedStatement pstmt = null;

		try {
			
			String sql = null;
			ResultSet rs = null;
			
			if(sId == -1) {
				sql = "select u.name, u.ident, s.display_name, s.version, fd.form_ident, fd.form_version, fd.device_id "
						+ "from survey s inner join project p on s.p_id = p.id and p.id = ? "
						+ "inner join user_project up on p.id = up.p_id "
						+ "inner join users u on u.id = up.u_id "  
						+ "left outer join form_downloads fd on fd.u_id = u.id "
						+ "and fd.form_ident = s.ident "
						+ "where u.temporary = 'false' "
						+ "and s.deleted = 'false' "
						+ "and s.blocked = 'false' "
						+ "order by u.name, s.display_name asc";

			} else {
				sql = "select u.name, u.ident, s.display_name, s.version, fd.form_ident, fd.form_version, fd.device_id "
						+ "from survey s inner join project p on s.p_id = p.id and p.id = ? "
						+ "inner join user_project up on p.id = up.p_id "
						+ "inner join users u on u.id = up.u_id "  
						+ "left outer join form_downloads fd on fd.u_id = u.id "
						+ "and fd.form_ident = s.ident "
						+ "where u.temporary = 'false' "
						+ "and s.deleted = 'false' "
						+ "and s.blocked = 'false' "
						+ "and s.s_id = ? "
						+ "order by u.name, s.display_name asc";
			
			}
			
			pstmt = sd.prepareStatement(sql);
			if(sId == -1) {
				pstmt.setInt(1, projectId);
			} else {
				pstmt.setInt(1, projectId);
				pstmt.setInt(2, sId);
			}
			log.info("Get form downloads: " + pstmt.toString());
			rs = pstmt.executeQuery();
			
	
			while(rs.next()) {
				JSONObject jr = new JSONObject();
				
				jr.put("u_name", rs.getString(1));
				jr.put("u_ident", rs.getString(2));
				jr.put("survey_name", rs.getString(3));
				jr.put("survey_version", rs.getString(4));
				
				String dl_survey = rs.getString(5);
				if(dl_survey == null) {
					jr.put("no_download", true);
				}
				
				String dl_version = rs.getString(6);
				jr.put("download_version", (dl_version != null) ? dl_version : "");
				
				String dl_device = rs.getString(7);
				jr.put("device_id", (dl_device != null) ? dl_device : "");
			
				ja.put(jr);
			}
			

		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Exception", e); 
		} catch (JSONException e) {
			log.log(Level.SEVERE, "JSON Exception", e);
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-EventList", sd);
		}
		
		return ja.toString();
	}

	/*
	 * Return a filter to ignore records submitted more than 100 days previously
	 */
	private String getObsoleteFilter(boolean ignoreOld) {
		String filter = "";
		if(ignoreOld) {
			filter = " and upload_time > now() - interval '100 days' ";
		}
		return filter;
	}
	/*
	 * Return a filter to ignore notifications submitted more than 100 days previously
	 */
	private String getObsoleteFilterNotifications(boolean ignoreOld) {
		String filter = "";
		if(ignoreOld) {
			filter = " and event_time > now() - interval '100 days' ";
		}
		return filter;
	}
}

