package org.smap.sdal.managers;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.internet.InternetAddress;
import javax.servlet.http.HttpServletRequest;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.AssignFromSurvey;
import org.smap.sdal.model.AssignmentServerDefn;
import org.smap.sdal.model.CreateTaskResp;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.EmailTaskMessage;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.KeyValueTask;
import org.smap.sdal.model.Line;
import org.smap.sdal.model.Location;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.NameValue;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Point;
import org.smap.sdal.model.Polygon;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.SubmissionMessage;
import org.smap.sdal.model.SubscriptionStatus;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TaskAddressSettings;
import org.smap.sdal.model.TaskAssignmentPair;
import org.smap.sdal.model.TaskBulkAction;
import org.smap.sdal.model.TaskEmailDetails;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskGroup;
import org.smap.sdal.model.TaskItemChange;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskProperties;
import org.smap.sdal.model.TaskServerDefn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
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
 * Manage the table that stores details on tasks
 */
public class TaskManager {

	private static Logger log =
			Logger.getLogger(TaskManager.class.getName());
	
	LogManager lm = new LogManager(); // Application log

	private ResourceBundle localisation = null;
	private String tz;
	
	public static String SURVEY_DATA_SOURCE = "survey";
	public static String TASK_DATA_SOURCE = "task";
	public static String NO_DATA_SOURCE = "none";
	
	public static final String STATUS_T_NEW = "new";
	public static final String STATUS_T_ACCEPTED = "accepted";
    public static final String STATUS_T_REJECTED = "rejected";
    public static final String STATUS_T_SUBMITTED = "submitted";
    public static final String STATUS_T_CANCELLED = "cancelled";
    public static final String STATUS_T_PENDING = "pending";
    public static final String STATUS_T_UNSENT = "unsent";
    public static final String STATUS_T_DELETED = "deleted";
    public static final String STATUS_T_BLOCKED = "blocked";
    
    public static final String STATUS_T_COMPLETE = "complete";
    public static final String STATUS_T_LATE = "late";	// Pseudo status = accepted and overdue
    
	private String fullStatusList[] = {		// late, complete are not included as a real status
			STATUS_T_NEW, 
			STATUS_T_ACCEPTED, 
			STATUS_T_UNSENT, 
			"unsubscribed", 
			STATUS_T_SUBMITTED, 
			STATUS_T_REJECTED, 
			STATUS_T_CANCELLED, 
			STATUS_T_DELETED,
			STATUS_T_PENDING,
			"error",
			STATUS_T_BLOCKED
			};
	
	public class TaskInstanceData {
		public int prikey = 0;						// data from submission
		public String ident = null;					// Identifier, from results, of person or role to be assigned
		public String location = null;				// data from submission
		public String address = null;				// data from submission
		public String locationTrigger = null;		// data from task set up
		public String instanceName = null;			// data from task look up
		public Timestamp taskStart = null;			// Start time
	}

	public TaskManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}
	
	/*
	 * Get the current task groups
	 */
	public ArrayList<TaskGroup> getTaskGroups(Connection sd, int projectId) throws Exception {

		String sql = "select tg_id, name, address_params, p_id, rule, "
				+ "source_s_id, target_s_id, email_details "
				+ "from task_group where p_id = ? order by tg_id asc;";
		PreparedStatement pstmt = null;

		String sqlTotal = "select tg.tg_id, count(t.id)  "
				+ "from task_group tg "
				+ "left outer join tasks t "
				+ "on tg.tg_id = t.tg_id " 
				+ "left outer join assignments a "
				+ "on a.task_id = t.id " 
				+ "and a.status != 'deleted' "
				+ "and a.status != 'cancelled' "
				+ "where tg.p_id = ? "
				+ "group by tg.tg_id "
				+ "order by tg.tg_id asc;";
		PreparedStatement pstmtTotal = null;

		String sqlComplete = "select tg.tg_id, count(a.id)  "
				+ "from task_group tg "
				+ "left outer join tasks t "
				+ "on tg.tg_id = t.tg_id " 
				+ "left outer join assignments a "
				+ "on a.task_id = t.id " 
				+ "and a.status = 'submitted' "
				+ "where tg.p_id = ? "
				+ "group by tg.tg_id "
				+ "order by tg.tg_id asc;";
		PreparedStatement pstmtComplete = null;

		ArrayList<TaskGroup> taskgroups = new ArrayList<TaskGroup> ();

		try {

			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, projectId);
			log.info("Get task groups: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();

			pstmtTotal = sd.prepareStatement(sqlTotal);	
			pstmtTotal.setInt(1, projectId);
			log.info("Get task groups totals: " + pstmtTotal.toString());
			ResultSet rsTotal = pstmtTotal.executeQuery();

			pstmtComplete = sd.prepareStatement(sqlComplete);	
			pstmtComplete.setInt(1, projectId);
			log.info("Get task groups totals complete: " + pstmtComplete.toString());
			ResultSet rsComplete = pstmtComplete.executeQuery();

			while (rs.next()) {
				TaskGroup tg = new TaskGroup();

				tg.tg_id = rs.getInt(1);
				tg.name = rs.getString(2);
				tg.address_params = rs.getString(3);
				tg.p_id = rs.getInt(4);
				tg.rule = new Gson().fromJson(rs.getString(5), AssignFromSurvey.class);
				tg.source_s_id = rs.getInt(6);
				tg.target_s_id = rs.getInt(7);
				tg.emaildetails = new Gson().fromJson(rs.getString(8), TaskEmailDetails.class);
				

				if(rsTotal.next()) {
					int tg_id = rsTotal.getInt(1);
					if(tg.tg_id == tg_id) {
						tg.totalTasks = rsTotal.getInt(2);
					} else {
						throw new Exception("Total tasks id mismatch");
					}
				}

				if(rsComplete.next()) {
					int tg_id = rsComplete.getInt(1);
					if(tg.tg_id == tg_id) {
						tg.completeTasks = rsComplete.getInt(2);
					} else {
						throw new Exception("Total tasks id mismatch");
					}
				}
				taskgroups.add(tg);
			}


		} catch(Exception e) {
			throw(e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			try {if (pstmtTotal != null) {pstmtTotal.close();} } catch (SQLException e) { }
			try {if (pstmtComplete != null) {pstmtComplete.close();} } catch (SQLException e) {	}
		}

		return taskgroups;
	}

	/*
	 * Get  details for a task group
	 */
	public TaskGroup getTaskGroupDetails(Connection sd, int tgId) throws Exception {

		String sql = "select tg_id, name, address_params, p_id, rule, source_s_id, target_s_id "
				+ "from task_group where tg_id = ?;";
		PreparedStatement pstmt = null;

		TaskGroup tg = new TaskGroup ();

		try {

			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, tgId);
			log.info("Get task group details: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();



			if (rs.next()) {

				tg.tg_id = rs.getInt(1);
				tg.name = rs.getString(2);
				tg.address_params = rs.getString(3);
				tg.p_id = rs.getInt(4);
				tg.rule = new Gson().fromJson(rs.getString(5), AssignFromSurvey.class);
				tg.source_s_id = rs.getInt(6);
				tg.target_s_id = rs.getInt(7);

			}


		} catch(Exception e) {
			throw(e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}

		}

		return tg;
	}

	/*
	 * Get tasks
	 */
	public TaskListGeoJson getTasks(
			Connection sd, 
			String urlprefix,
			int oId,			// only required if tgId is not set
			int tgId, 		// Presumably this has been security checked as being in correct organisation
			int taskId,
			int aId,
			boolean completed,
			int userId,
			String incStatus,
			String period,
			int start,		// First task id to return
			int limit,		// Maximum number of tasks to return
			String sort,		// Data to sort on
			String dirn		// Direction of sort asc || desc
			) throws Exception {
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		StringBuffer sql = new StringBuffer("select t.id as t_id, "
				+ "t.title as name,"
				+ "timezone(?, t.schedule_at) as schedule_at,"
				+ "timezone(?, t.schedule_finish) as schedule_finish,"
				+ "timezone(?, t.schedule_at) + interval '1 hour' as default_finish,"
				+ "t.location_trigger as location_trigger,"
				+ "t.location_group as location_group,"
				+ "t.location_name as location_name,"
				+ "t.update_id as update_id,"
				+ "t.initial_data as initial_data,"
				+ "t.initial_data_source as initial_data_source,"
				+ "t.address as address,"
				+ "t.guidance as guidance,"
				+ "t.repeat as repeat,"
				+ "t.repeat_count as repeat_count,"
				+ "t.survey_ident,"
				+ "t.survey_name as survey_name,"
				+ "t.deleted,"
				+ "t.complete_all,"
				+ "t.assign_auto,"
				+ "t.tg_id,"
				+ "tg.name as tg_name,"
				+ "s.blocked as blocked,"
				+ "s.ident as form_ident,"
				+ "a.id as assignment_id,"
				+ "a.status as status,"
				+ "a.assignee,"
				+ "a.assignee_name,"
				+ "a.comment,"
				+ "a.email,"
				+ "a.action_link,"
				+ "u.ident as assignee_ident, "				// Get current user ident for notification
				+ "ST_AsGeoJSON(t.geo_point) as geom, "
				+ "ST_AsText(t.geo_point) as wkt, "
				+ "ST_x(t.geo_point) as lon,"
				+ "ST_Y(t.geo_point) as lat,"
				+ "t.show_dist "
				+ "from tasks t "
				+ "join survey s "
				+ "on t.survey_ident = s.ident "
				+ "join task_group tg "
				+ "on t.tg_id = tg.tg_id "
				+ "left outer join assignments a "
				+ "on a.task_id = t.id " 
				+ "left outer join users u "
				+ "on a.assignee = u.id");
		
		if(taskId > 0) {				// Restrict by taskId
			sql.append(" where t.id = ?");
		} else if(tgId > 0) {		// Restrict by taskGroupId
			sql.append(" where t.tg_id = ?");
		} else if(aId > 0) {		// Restrict by assignment id
			sql.append(" where a.id = ?");
		} else {
			sql.append( " where t.p_id in (select id from project where o_id = ?)");
		}
		
		// Restrict by start task id
		if(start > 0) {
			sql.append(" and t.id >= ?");
		}
		
		// Restrict by period
		if(period != null) {	
			if(period.equals("week")) {
				sql.append(" and t.schedule_at > now() - interval '7 days' ");
			} else if(period.equals("month")) {
				sql.append(" and t.schedule_at > now() - interval '30 days' ");
			}
		}
		
		ArrayList<String> statusList = new ArrayList<String> ();
		String sqlStatus = "and a.status = any (?) ";		// No need to include unassigned tasks
		boolean wantLate = false;
		boolean wantAccepted = false;
		if(incStatus != null) {
			String [] incStatusArray = incStatus.split(",");
			for(String status : incStatusArray) {
				if(status.trim().equals(STATUS_T_LATE)) {
					wantLate = true;
				} else if(status.trim().equals(STATUS_T_ACCEPTED)) {
					wantAccepted = true;
				}
				for(String statusRef : fullStatusList) {
					if(status.trim().equals(statusRef)) {
						statusList.add(statusRef);
						if(statusRef.equals("new")) {
							sqlStatus = "and (a.status is null or a.status = any (?)) ";
						}
						break;
					}
				}
			}
			if(wantLate && !wantAccepted) {
				statusList.add(STATUS_T_ACCEPTED);	// Add accepted back into the status list
			}
		} 
		
		if(statusList.size() > 0) {	
			sql.append(sqlStatus);
		} else {
			sql.append(" and (a.status is null or a.status != 'deleted') ");	// default
		}
		
		// Add order by
		if(dirn == null || !dirn.equals("desc")) {
			dirn = "asc";
		} else {
			dirn = "desc";
		}
		sql.append(" order by");
		if(sort != null) {
			if(sort.equals("scheduled")) {
				sql.append(" t.schedule_at::timestamp(0) ").append(dirn).append(", t.id ");
			} else {
				sql.append(" t.id ").append(dirn);
			}
		} else {
			sql.append(" t.id ").append(dirn);
		}
		sql.append(", a.id ");
		
		PreparedStatement pstmt = null;
		TaskListGeoJson tl = new TaskListGeoJson();
	
		try {

			pstmt = sd.prepareStatement(sql.toString());	
			
			/*
			 * Set the parameters
			 */
			int paramIdx = 1;
			pstmt.setString(paramIdx++, tz);		// Timezones
			pstmt.setString(paramIdx++, tz);
			pstmt.setString(paramIdx++, tz);
			
			if(taskId > 0) {
				pstmt.setInt(paramIdx++, taskId);
			} else if(tgId > 0) {						// Task group or organisation
				pstmt.setInt(paramIdx++, tgId);
			} else if(aId > 0) {							// Assignment id
				pstmt.setInt(paramIdx++, aId);
			} else {
				pstmt.setInt(paramIdx++, oId);
			}
			
			if(start > 0) {						// Starting task
				pstmt.setInt(paramIdx++, start);
			}
			
			if(statusList.size() > 0) {			// Task Status
				pstmt.setArray(paramIdx++, sd.createArrayOf("text", statusList.toArray(new String[statusList.size()])));
			}
			
			// Get the data
			log.info("Get tasks: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			JsonParser parser = new JsonParser();
			int index = 0;
			long now = Calendar.getInstance().getTime().getTime();
			while (rs.next()) {

				String status = rs.getString("status");
				boolean deleted = rs.getBoolean("deleted");
				int assignee = rs.getInt("assignee"); 
				Timestamp to = rs.getTimestamp("schedule_finish");
				
				// If we don't want accepted but do want late tasks then filter on date
				if(wantLate && !wantAccepted) {
					if(status.equals(STATUS_T_ACCEPTED) && (to == null || to.getTime() > now)) {
						continue;
					} 
				} else if(wantAccepted && !wantLate) {
					if(to != null) {
						if(status.equals(STATUS_T_ACCEPTED) && (to.getTime() < now)) {
							continue;
						}
					}
				}
				
				// Adjust status
				if(deleted && status == null) {
					status = STATUS_T_DELETED;
				} else if(status == null) {
					status = "new";
				} else if(assignee < 0 && !status.equals(STATUS_T_CANCELLED)  && !status.equals(STATUS_T_SUBMITTED)) {
					status = "new";
				} else if(status.equals(STATUS_T_ACCEPTED) && (to != null && to.getTime() < now)) {
					status = STATUS_T_LATE;
				}
				
				// Ignore any tasks that are not required
				if(!completed && (status.equals(STATUS_T_SUBMITTED) || status.equals(STATUS_T_COMPLETE))) {
					continue;
				}
				if(userId > 0 && userId != assignee) {	// Assigned to a specific user only
					continue;
				}
				if(userId < 0 && assignee > 0) {		// Unassigned only
					continue;
				}

				TaskFeature tf = new TaskFeature();
				tf.properties = new TaskProperties();

				tf.properties.id = rs.getInt("t_id");
				tf.properties.a_id = rs.getInt("assignment_id");
				tf.properties.name = rs.getString("name");
				tf.properties.from = rs.getTimestamp("schedule_at");
				tf.properties.to = to;
				tf.properties.status = status;	
				tf.properties.survey_ident = rs.getString("survey_ident");
				tf.properties.form_id = GeneralUtilityMethods.getSurveyId(sd, tf.properties.survey_ident);	// Deprecate - should remove all usage of survey id
				tf.properties.survey_name = rs.getString("survey_name");
				tf.properties.action_link = rs.getString("action_link");
				tf.properties.blocked = rs.getBoolean("blocked");
				tf.properties.assignee = rs.getInt("assignee");
				tf.properties.assignee_name = rs.getString("assignee_name");
				tf.properties.comment = rs.getString("comment");
				tf.properties.emails = rs.getString("email");
				tf.properties.assignee_ident = rs.getString("assignee_ident");
				tf.properties.location_trigger = rs.getString("location_trigger");
				tf.properties.location_group = rs.getString("location_group");
				tf.properties.location_name = rs.getString("location_name");
				tf.properties.update_id = rs.getString("update_id");
				tf.properties.address = rs.getString("address");
				tf.properties.guidance = rs.getString("guidance");
				if(tf.properties.guidance == null) {
					if(tf.properties.address != null) {
						Type addressType = new TypeToken<ArrayList<NameValue>>() {}.getType();
						ArrayList<NameValue> address = gson.fromJson(tf.properties.address, addressType);
						StringBuffer guidance = new StringBuffer("");
						for(NameValue nv : address) {
							if(guidance.length() > 0) {
								guidance.append(", ");
							}
							guidance.append(nv.value);
						}
						tf.properties.address = guidance.toString();
					}
					
				}
				tf.properties.repeat = rs.getBoolean("repeat");
				tf.properties.repeat_count = rs.getInt("repeat_count");
				tf.geometry = parser.parse(rs.getString("geom")).getAsJsonObject();
				tf.properties.complete_all = rs.getBoolean("complete_all");
				tf.properties.assign_auto = rs.getBoolean("assign_auto");
				tf.properties.tg_id = rs.getInt("tg_id");
				tf.properties.tg_name = rs.getString("tg_name");
				tf.properties.initial_data_source = rs.getString("initial_data_source");
				tf.properties.show_dist = rs.getInt("show_dist");

				tf.properties.lat = rs.getDouble("lat");
				tf.properties.lon = rs.getDouble("lon");
				
				/*
				 * Add the task data
				 * Embed the data if the request is for a single task
				 * Otherwise set a link
				 */			
				if(taskId != 0  && tf.properties.initial_data_source != null) {
					if(tf.properties.initial_data_source.equals("survey")) {
						tf.properties.initial_data = null;		// TODO Get instance data from survey record					
					} else if(tf.properties.initial_data_source.equals("task")) {
						String initial_data = rs.getString("initial_data");
						if(initial_data != null) {
							tf.properties.initial_data = gson.fromJson(initial_data, Instance.class);
						}
					} else {
						tf.properties.initial_data = null;
					}
				}
				
				// Add links
				tf.links = new HashMap<String, String> ();
				tf.links.put("detail", urlprefix + "/api/v1/tasks/" + tf.properties.id);
				tf.links.put("webform", GeneralUtilityMethods.getWebformLink(
						urlprefix, 
						tf.properties.survey_ident, 
						tf.properties.initial_data_source,
						tf.properties.a_id,
						tf.properties.id,
						tf.properties.update_id));
				tf.links.put("xml_data", GeneralUtilityMethods.getInitialXmlDataLink(
						urlprefix, 
						tf.properties.survey_ident, 
						tf.properties.initial_data_source,
						tf.properties.id,
						tf.properties.update_id));
				tf.links.put("json_data", GeneralUtilityMethods.getInitialJsonDataLink(
						urlprefix, 
						tf.properties.survey_ident, 
						tf.properties.initial_data_source,
						tf.properties.id,
						tf.properties.update_id));
				
				tl.features.add(tf);
				
				index++;
				if (limit > 0 && index >= limit) {
					break;
				}
			}


		} catch(Exception e) {
			throw(e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}

		}

		return tl;
	}
	
	/*
	 * Get unassigned tasks
	 * Only get tasks if the task group is set to auto allocate
	 */
	public TaskListGeoJson getUnassignedTasks(
			Connection sd, 
			int oId,	
			int userId,
			int limit,		// Maximum number of tasks to return
			String userName
			) throws Exception {
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		StringBuffer sql = new StringBuffer("select t.id as t_id, "
				+ "t.title as name,"
				+ "timezone(?, t.schedule_at) as schedule_at,"
				+ "timezone(?, t.schedule_finish) as schedule_finish,"
				+ "timezone(?, t.schedule_at) + interval '1 hour' as default_finish,"
				+ "t.location_trigger as location_trigger,"
				+ "t.location_group as location_group,"
				+ "t.location_name as location_name,"
				+ "t.update_id as update_id,"
				+ "t.initial_data as initial_data,"
				+ "t.initial_data_source as initial_data_source,"
				+ "t.address as address,"
				+ "t.guidance as guidance,"
				+ "t.repeat as repeat,"
				+ "t.repeat_count as repeat_count,"
				+ "t.survey_ident,"
				+ "t.survey_name as survey_name,"
				+ "t.complete_all,"
				+ "t.assign_auto,"
				+ "t.tg_id,"
				+ "tg.name as tg_name,"
				+ "tg.p_id as p_id,"
				+ "s.blocked as blocked,"
				+ "s.ident as form_ident,"
				+ "s.version as form_version,"
				+ "a.id as assignment_id,"
				+ "a.status as status,"
				+ "a.assignee,"
				+ "a.assignee_name,"
				+ "a.comment,"
				+ "a.email,"
				+ "a.action_link,"
				+ "u.ident as assignee_ident, "				// Get current user ident for notification
				+ "ST_AsGeoJSON(t.geo_point) as geom, "
				+ "ST_AsText(t.geo_point) as wkt, "
				+ "ST_x(t.geo_point) as lon,"
				+ "ST_Y(t.geo_point) as lat,"
				+ "t.show_dist "
				+ "from tasks t "
				+ "join survey s "
				+ "on t.survey_ident = s.ident "
				+ "join task_group tg "
				+ "on t.tg_id = tg.tg_id "
				+ "left outer join assignments a "
				+ "on a.task_id = t.id " 
				+ "left outer join users u "
				+ "on a.assignee = u.id");
		
		sql.append(" where t.p_id in (select id from project where o_id = ?)");
		sql.append(" and t.p_id in (select p_id from user_project where u_id = ?)");
		sql.append(" and (a.status is null or a.status = 'new' or assignee < 0) ");
		sql.append(" and not t.deleted ");
		sql.append(" and t.assign_auto ");
		sql.append(" and a.id not in (select a_id from task_rejected where ident = ?) ");
		sql.append(" order by t.schedule_at::timestamp(0) ").append("asc").append(", t.id ");	
		sql.append(", a.id ");
		
		PreparedStatement pstmt = null;
		TaskListGeoJson tl = new TaskListGeoJson();
	
		try {

			pstmt = sd.prepareStatement(sql.toString());	
			
			/*
			 * Set the parameters
			 */
			int paramIdx = 1;
			pstmt.setString(paramIdx++, tz);		// Timezones
			pstmt.setString(paramIdx++, tz);
			pstmt.setString(paramIdx++, tz);			
			pstmt.setInt(paramIdx++, oId);
			pstmt.setInt(paramIdx++, userId);
			pstmt.setString(paramIdx++, userName);
			
			// Get the data
			log.info("Get unassignd tasks: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			JsonParser parser = new JsonParser();
			int index = 0;
			while (rs.next()) {

				String status = rs.getString("status");
				int assignee = rs.getInt("assignee"); 
				Timestamp to = rs.getTimestamp("schedule_finish");
				// Remove default finish, a task can be set without a finish
				//if(to == null) {
				//	to = rs.getTimestamp("default_finish");
				//}
				
				// Adjust status
				if(status == null) {
					status = "new";
				} else if(assignee < 0) {
					status = "new";
				} 

				TaskFeature tf = new TaskFeature();
				tf.properties = new TaskProperties();

				tf.properties.id = rs.getInt("t_id");
				tf.properties.a_id = rs.getInt("assignment_id");
				tf.properties.name = rs.getString("name");
				tf.properties.from = rs.getTimestamp("schedule_at");
				tf.properties.to = to;
				tf.properties.status = status;	
				tf.properties.survey_ident = rs.getString("survey_ident");
				tf.properties.form_id = GeneralUtilityMethods.getSurveyId(sd, tf.properties.survey_ident);	// Deprecate - should remove all usage of survey id
				tf.properties.form_version = rs.getString("form_version");
				tf.properties.survey_name = rs.getString("survey_name");
				tf.properties.action_link = rs.getString("action_link");
				tf.properties.blocked = rs.getBoolean("blocked");
				tf.properties.assignee = rs.getInt("assignee");
				tf.properties.assignee_name = rs.getString("assignee_name");
				tf.properties.comment = rs.getString("comment");
				tf.properties.emails = rs.getString("email");
				tf.properties.assignee_ident = rs.getString("assignee_ident");
				tf.properties.location_trigger = rs.getString("location_trigger");
				tf.properties.location_group = rs.getString("location_group");
				tf.properties.location_name = rs.getString("location_name");
				tf.properties.update_id = rs.getString("update_id");
				tf.properties.address = rs.getString("address");
				tf.properties.guidance = rs.getString("guidance");
				if(tf.properties.guidance == null) {
					if(tf.properties.address != null) {
						Type addressType = new TypeToken<ArrayList<NameValue>>() {}.getType();
						ArrayList<NameValue> address = gson.fromJson(tf.properties.address, addressType);
						StringBuffer guidance = new StringBuffer("");
						for(NameValue nv : address) {
							if(guidance.length() > 0) {
								guidance.append(", ");
							}
							guidance.append(nv.value);
						}
						tf.properties.address = guidance.toString();
					}
					
				}
				tf.properties.repeat = rs.getBoolean("repeat");
				tf.properties.repeat_count = rs.getInt("repeat_count");
				tf.geometry = parser.parse(rs.getString("geom")).getAsJsonObject();
				tf.properties.complete_all = rs.getBoolean("complete_all");
				tf.properties.assign_auto = rs.getBoolean("assign_auto");
				tf.properties.tg_id = rs.getInt("tg_id");
				tf.properties.tg_name = rs.getString("tg_name");
				tf.properties.p_id = rs.getInt("p_id");
				tf.properties.initial_data_source = rs.getString("initial_data_source");
				tf.properties.show_dist = rs.getInt("show_dist");

				tf.properties.lat = rs.getDouble("lat");
				tf.properties.lon = rs.getDouble("lon");
				
				tl.features.add(tf);
				
				index++;
				if (limit > 0 && index >= limit) {
					break;
				}
			}


		} catch(Exception e) {
			throw(e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}

		}

		return tl;
	}

	/*
	 * Save the tasks for the specified task group
	 */
	public void writeTaskList(
			Connection sd, 
			Connection cResults,
			ArrayList<TaskServerDefn> tl,
			int tgId,
			String urlPrefix,
			boolean updateResources,
			int oId,
			boolean autosendEmails,
			String remoteUser,
			boolean temporaryUser,
			boolean preserveInitialData) throws Exception {

		HashMap<String, String> userIdents = new HashMap<>();

		for(TaskServerDefn tsd : tl) {
			writeTask(sd, cResults, tgId, tsd, urlPrefix, updateResources, 
					oId, autosendEmails, remoteUser, temporaryUser, urlPrefix, preserveInitialData);
			for(AssignmentServerDefn asd : tsd.assignments) {
				if(asd.assignee_ident != null) {
					userIdents.put(asd.assignee_ident, asd.assignee_ident);
				}
			}
		}

		// Create a notification to alert the new user of the change to the task details
		for(String user : userIdents.keySet()) {
			MessagingManager mm = new MessagingManager(localisation);
			mm.userChange(sd, user);
		}	

	}

	/*
	 * Get the current locations available for an organisation
	 */
	public ArrayList<Location>  getLocations(Connection sd, int oId) throws SQLException {

		String sql = "select id, locn_group, locn_type, uid, name, "
				+ "ST_x(the_geom) as lon,"		// keep the_geom
				+ "ST_Y(the_geom) as lat "		// keep the_geom
				+ "from locations "
				+ "where o_id = ? "
				+ "order by locn_group asc, name asc;";
		PreparedStatement pstmt = null;
		ArrayList<Location> locations = new ArrayList<Location> ();

		try {

			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, oId);

			log.info("Get locations: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				Location locn = new Location();

				locn.id = rs.getInt("id");
				locn.group = rs.getString("locn_group");
				locn.type = rs.getString("locn_type");
				locn.uid = rs.getString("uid");
				locn.name = rs.getString("name");
				locn.lon = rs.getDouble("lon"); 
				locn.lat = rs.getDouble("lat"); 

				locations.add(locn);
			}


		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}

		return locations;

	}
	
	/*
	 * Get the current locations available for an organisation
	 */
	public Location  getLocation(Connection sd, int oId, String group, String name) throws SQLException {

		String sql = "select id, locn_group, locn_type, uid, name, "
				+ "ST_x(the_geom) as lon,"		// keep the_geom
				+ "ST_Y(the_geom) as lat "		// keep the_geom
				+ "from locations "
				+ "where o_id = ? "
				+ "and locn_group = ? "
				+ "and name = ?";
		PreparedStatement pstmt = null;
		Location locn = null;

		try {

			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, oId);
			pstmt.setString(2, group);
			pstmt.setString(3,  name);

			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				locn = new Location();

				locn.id = rs.getInt("id");
				locn.group = rs.getString("locn_group");
				locn.type = rs.getString("locn_type");
				locn.uid = rs.getString("uid");
				locn.name = rs.getString("name");
				locn.lon = rs.getDouble("lon"); 
				locn.lat = rs.getDouble("lat"); 

			}


		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}

		return locn;

	}

	/*
	 * Save a list of locations replacing the existing ones
	 */
	public void saveLocations(Connection sd, 
			ArrayList<Location> tags,
			int oId) throws SQLException {


		String sqlDelete = "delete from locations "
				+ "where o_id = ?";
		PreparedStatement pstmtDelete = null;

		String sql = "insert into locations (o_id, locn_group, locn_type, uid, name, the_geom) "		// keep the_geom
				+ "values (?, ?, ?, ?, ?, ST_GeomFromText(?, 4326));";
		PreparedStatement pstmt = null;
		
		try {

			log.info("Set autocommit false");
			sd.setAutoCommit(false);

			// Remove existing data
			pstmtDelete = sd.prepareStatement(sqlDelete);
			pstmtDelete.setInt(1, oId);
			pstmtDelete.executeUpdate();

			// Add new data
			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, oId);
			for(int i = 0; i < tags.size(); i++) {

				Location t = tags.get(i);

				pstmt.setString(2, t.group);
				pstmt.setString(3, t.type);
				pstmt.setString(4, t.uid);
				pstmt.setString(5, t.name);

				Point newPoint = new Point(t.lon, t.lat);
				pstmt.setString(6, newPoint.getAsText());
				
				pstmt.executeUpdate();
			}
			sd.commit();
		} catch(Exception e) {
			sd.rollback();
			throw(e);
		} finally {
			log.info("Set autocommit true");
			sd.setAutoCommit(true);
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			try {if (pstmtDelete != null) {pstmtDelete.close();} } catch (SQLException e) {	}
		}


	}

	/*
	 * Check the task group rules and add any new tasks based on this submission
	 */
	public void updateTasksForSubmission(
			Connection sd, 
			Connection cResults,
			String source_s_ident, 
			String hostname,
			String instanceId,
			int pId,
			String pName,
			String remoteUser,
			boolean temporaryUser) throws Exception {

		String sqlGetRules = "select tg_id, name, rule, address_params, target_s_id, complete_all, assign_auto from task_group where source_s_id = ?;";
		PreparedStatement pstmtGetRules = null;

		SurveyManager sm = new SurveyManager(localisation, "UTC");
		Survey survey = null;

		
		try {
			int source_s_id = GeneralUtilityMethods.getSurveyId(sd, source_s_ident);
			pstmtGetRules = sd.prepareStatement(sqlGetRules);
			pstmtGetRules.setInt(1, source_s_id);
			log.info("Get task rules: " + pstmtGetRules.toString());

			ResultSet rs = pstmtGetRules.executeQuery();
			while(rs.next()) {
				
				if(survey == null) {
					// Get the forms - this is required by the test filter
					survey = sm.getById(sd, cResults, remoteUser, temporaryUser, source_s_id, true, "", 
							instanceId, false, false, true, false, true, "real", 
							false, 
							false, 
							true, 		// Set super user true so that roles are ignored
							"geojson",
							false,		// Do not test in child surveys (at least not yet)
							false,		// launched only
							false		// Don't merge set value into default values
							);	
				}

				int tgId = rs.getInt(1);
				String tgName = rs.getString(2);
				String asString = rs.getString(3);
				try {
					AssignFromSurvey as = new Gson().fromJson(asString, AssignFromSurvey.class);
					if(as.add_future) {
						String addressString = rs.getString(4);
						ArrayList<TaskAddressSettings> address = null;
						if(addressString != null) {
							address = new Gson().fromJson(addressString, new TypeToken<ArrayList<TaskAddressSettings>>() {}.getType());
						}
						int target_s_id = rs.getInt(5);
						String target_s_ident = GeneralUtilityMethods.getSurveyIdent(sd, target_s_id);
						boolean complete_all = rs.getBoolean(6);
						boolean assign_auto = rs.getBoolean(7);
						
						log.info("Assign Survey String: " + rs.getString(3));
						log.info("userevent: matching rule: " + as.task_group_name + " for survey: " + source_s_id);	// For log
		
						/*
						 * Check filter to see if this rule should be fired
						 * In addition avoid permanent loops of tasks being reassigned after completion
						 *   Don't fire if form to be updated is the same one that has been submitted 
						 */
						boolean fires = false;
						
						if(as.filter != null && as.filter.advanced != null) {
							fires = GeneralUtilityMethods.testFilter(sd, cResults, remoteUser, localisation, survey, as.filter.advanced, instanceId, tz, "Tasks");
							if(!fires) {
								log.info("Rule not fired as filter criteria not met: " + as.filter.advanced);
							}
						} else {
							fires = true;
						}
						
						if(fires && source_s_id == target_s_id) {
							log.info("Rule fired however target survey id = source id (" + source_s_id + ")");
						}
						
		
						if(fires) {
							log.info("userevent: rule fires: " + (as.filter == null ? "no filter" : "yes filter") + " for survey: " + source_s_id + 
									" task survey: " + target_s_id);
							TaskInstanceData tid = getTaskInstanceData(sd, cResults, 
									source_s_id, instanceId, as, address); // Get data from new submission
							
							Survey sourceSurvey = null;
							if(as.prepopulate) {
								// Get the source survey definition so we can get the source data
								sourceSurvey = sm.getById(
										sd, cResults, 
										remoteUser, 
										temporaryUser, 
										source_s_id, 
										true, 		// full
										null, 		// basepath
										null, 		// instance id
										false, 		// get results
										false, 		// generate dummy values
										true, 		// get property questions
										false, 		// get soft deleted
										true, 		// get HRK
										null, 		// get external options
										false, 		// get change history
										false, 		// get roles
										true,		// superuser 
										null, 		// geomformat
										false, 		// reference surveys
										false,		// only get launched
										false		// Don't merge set value into default values
										);
							}
							writeTaskCreatedFromSurveyResults(sd, cResults, as, hostname, tgId, tgName, pId, pName, sourceSurvey, 
									target_s_ident, tid, instanceId, true, remoteUser, temporaryUser, complete_all,
									assign_auto, as.repeat);  // Write to the database
						}
					}
				} catch (Exception e) {
					log.info("Assignment: " + asString);
					lm.writeLog(sd, source_s_id, "subscriber", LogManager.TASK, e.getMessage(), 0, null);
					log.log(Level.SEVERE, e.getMessage(), e);
				}
			}

		} finally {

			try {if (pstmtGetRules != null) {pstmtGetRules.close();} } catch (SQLException e) {	}

		}

	}

	/*
	 * Write the task into the task table
	 */
	public void writeTaskCreatedFromSurveyResults(
			Connection sd,
			Connection cResults,
			AssignFromSurvey as,
			String hostname,
			int tgId,
			String tgName,
			int pId,
			String pName,
			Survey sourceSurvey,				// Set if we need to get the instance data from the survey
			String target_s_ident,
			TaskInstanceData tid,			// data from submission
			String updateId,
			boolean autosendEmails,
			String remoteUser,
			boolean temporaryUser,
			boolean complete_all,
			boolean assign_auto,
			boolean repeat
			) throws Exception {

		PreparedStatement pstmtAssign = null;
		PreparedStatement pstmtRoles = null;
		PreparedStatement pstmtRoles2 = null;
		PreparedStatement pstmt = null;
		
		String title = null;
		if(tid.instanceName == null || tid.instanceName.trim().length() == 0) {
			title = tgName + " : " + as.project_name + " : " + as.survey_name;
		} else {
			title = tgName + " : " + tid.instanceName;
		}

		String location = tid.location;
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

		try {

			/*
			 * Set data to be updated
			 */
			String initialDataSource = null;
			String initialData = null;
			if(as.update_results) {
				initialDataSource = TaskManager.SURVEY_DATA_SOURCE;
			} else if(as.prepopulate) {
				initialDataSource = TaskManager.TASK_DATA_SOURCE;
				SurveyManager sm = new SurveyManager(localisation, tz);	
				
				ArrayList<Instance> instances = sm.getInstances(
						sd,
						cResults,
						sourceSurvey,
						sourceSurvey.getFirstForm(),
						0,
						null,
						updateId,
						sm,
						false);		// Meta data
				
				// There should only be one instance at the top level
				initialData = gson.toJson(instances.get(0), Instance.class);
			} else {
				initialDataSource = TaskManager.NO_DATA_SOURCE;
			}

			/*
			 * Location
			 */
			// Default is 0,0
			Point defPoint = new Point(0.0, 0.0);
			String taskPoint = defPoint.getAsText();
			
			if(location != null) {		
				if(location.toLowerCase().contains("point")) {
					Point p = gson.fromJson(location, Point.class);
					taskPoint = p.getAsText();
				} else if(location.toLowerCase().contains("linestring")) {
					log.info("Starts with linestring: " + tid.location.split(" ").length);
					if(location.split(" ").length < 3) {	// Convert to point if there is only one location in the line
						location = location.replaceFirst("LINESTRING", "POINT");
					}
				
				} else if(location.toLowerCase().contains("polygon")) {
					Polygon p = gson.fromJson(location, Polygon.class);
					// Get the centroid of the first shape
					if(p.coordinates != null && p.coordinates.size() > 0) {
						ArrayList<ArrayList<Double>> shape = p.coordinates.get(0);
						if(shape.size() > 0) {
							int pointCount = 0;
							Double lon = 0.0;
							Double lat = 0.0;
							for(int i = 0; i < shape.size(); i++) {
								ArrayList<Double> points = shape.get(i);
								if(points.size() > 1) {
									lon += points.get(0);
									lat += points.get(1);
									pointCount++;
								}
							}
							if(pointCount > 0) {
								lon = lon / pointCount;
								lat = lat / pointCount;
							}
							Point newPoint = new Point(lon, lat);
							taskPoint = newPoint.getAsText();
						}
					}
				} else if(location.toLowerCase().contains("linestring")) {
					Line l = gson.fromJson(location, Line.class);
					// Get the centroid of the line
					if(l.coordinates != null && l.coordinates.size() > 0) {
						int pointCount = 0;
						Double lon = 0.0;
						Double lat = 0.0;
						for(int i = 0; i < l.coordinates.size(); i++) {
							ArrayList<Double> points = l.coordinates.get(i);
							if(points.size() > 1) {
								lon += points.get(0);
								lat += points.get(1);
								pointCount++;
							}
						}
						if(pointCount > 0) {
							lon = lon / pointCount;
							lat = lat / pointCount;
						}
						Point newPoint = new Point(lon, lat);
						taskPoint = newPoint.getAsText();
					}
				}
			}
			
			/*
			 * Start and finish time
			 */
			Timestamp taskStart = getTaskStartTime(as, tid.taskStart);
			Timestamp taskFinish = getTaskFinishTime(as, taskStart);

			/*
			 * Write the task to the database
			 */
			pstmt = getInsertTaskStatement(sd);
			insertTask(
					pstmt,	
					pId,
					pName,
					tgId,
					tgName,
					title,
					target_s_ident,
					taskPoint,
					updateId,
					tid.address,
					taskStart,
					taskFinish,
					tid.locationTrigger,
					null,
					null,
					false,
					complete_all,
					assign_auto,
					null,
					initialDataSource,
					initialData,
					as.show_dist);

			/*
			 * Assign the user to the new task
			 */
			int userId = as.user_id;
			int roleId = as.role_id;
			log.info("Assign user: userId: "  + userId + " roleId: " + roleId + " tid.ident: " + tid.ident);
			int fixedRoleId = as.fixed_role_id;
			int oId = GeneralUtilityMethods.getOrganisationIdForSurveyIdent(sd, target_s_ident);
			
			// Assign to people dependent on data from a form
			String emails = as.emails;
			if(tid.ident != null) {
			
				log.info("Assign Ident: " + tid.ident);
				if(as.user_id == -2) {
					userId = GeneralUtilityMethods.getUserIdOrgCheck(sd, tid.ident, oId);   // Its a user ident
				} else if (as.role_id == -2){
					roleId = GeneralUtilityMethods.getRoleId(sd, tid.ident, oId);   // Its a role name
				} else {
					// Append emails as data
					emails = combineEmails(emails, tid.ident);
				}
			}
			
			ResultSet rsKeys = pstmt.getGeneratedKeys();
			if(rsKeys.next()) {
				int taskId = rsKeys.getInt(1);
				
				pstmtAssign = getInsertAssignmentStatement(sd);
				pstmtRoles = getRoles(sd);
				pstmtRoles2 = getRoles2(sd);
				
				applyAllAssignments(
						sd, 
						cResults,
						pstmtRoles, 
						pstmtRoles2, 
						pstmtAssign, 
						tgId,
						taskId,
						userId, 
						roleId, 
						fixedRoleId,
						emails,
						oId,
						pId,
						target_s_ident,
						updateId,
						autosendEmails,
						remoteUser,
						temporaryUser,
						initialDataSource,
						updateId,
						title,
						taskStart,
						taskFinish,
						assign_auto,
						repeat);
			}
			
			if(rsKeys != null) try{ rsKeys.close(); } catch(SQLException e) {};		

		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtAssign != null) try { pstmtAssign.close(); } catch(SQLException e) {};
			if(pstmtRoles != null) try {	pstmtRoles.close(); } catch(SQLException e) {};
			if(pstmtRoles2 != null) try {	pstmtRoles2.close(); } catch(SQLException e) {};
		}
	}

	/*
	 * Get the instance data from a submission that is relevant for assigning a task
	 *  location
	 *  address
	 *  location Trigger
	 */
	public TaskInstanceData getTaskInstanceData (Connection sd, 
			Connection cResults, 
			int sId, 
			String instanceId,
			AssignFromSurvey as,
			ArrayList<TaskAddressSettings> address) throws Exception {
		
		TaskInstanceData tid = new TaskInstanceData();
		
		PreparedStatement pstmt = null;		
		int addressCount = 0;
		int colIdx = 1;
		ArrayList<KeyValueTask> guidance = new ArrayList<> ();
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		
		try {
			Form topForm = GeneralUtilityMethods.getTopLevelForm(sd, sId);

			StringBuffer sql = new StringBuffer("select prikey, instancename");
			
			String geomColumn = GeneralUtilityMethods.getGeomColumnFromForm(sd, sId, topForm.id);
			if(geomColumn != null) {
				sql.append(", ST_AsGeoJson(" + geomColumn + ")");
			}
			if(as.assign_data != null && as.assign_data.trim().length() > 0) {
				SqlFrag frag = new SqlFrag();
				frag.addSqlFragment(as.assign_data, false, localisation, 0);
				sql.append(",").append(frag.sql.toString()).append(" as _assign_key");;
			}
			
			if(address != null) {
				for(TaskAddressSettings a : address) {
					if(a.selected && !a.isMedia) {
						if(GeneralUtilityMethods.hasColumn(cResults, topForm.tableName, a.name)) {
							sql.append(",");
							sql.append(a.name);
							addressCount++;
						}					
					}
				}
			}
			// Add start date column
			boolean getTaskStartValue = false;
			if(as.taskStart != -1 && as.taskStart != 0) {
				String name = null;
				if(as.taskStart > 0) {
					Question q = GeneralUtilityMethods.getQuestion(sd, as.taskStart);
					if(q != null) {
						name = q.name;
					}
				} else {
					MetaItem mi = GeneralUtilityMethods.getPreloadDetails(sd, sId, as.taskStart);
					if(mi != null) {
						name = mi.columnName;
					}
				}
				if(name != null) {
					getTaskStartValue = true;
					sql.append(",").append(topForm.tableName).append(".").append(name).append(" as taskstart");
				}
			}
			
			sql.append(" from ");
			sql.append(topForm.tableName);
			sql.append(" where instanceid = ?");
			
			pstmt = cResults.prepareStatement(sql.toString());
			pstmt.setString(1, instanceId);
			log.info("Get instance task data: " + pstmt.toString());

			ResultSet rsData = pstmt.executeQuery();
			if(rsData.next()) {
				tid.prikey = rsData.getInt(colIdx++);
				tid.instanceName = rsData.getString(colIdx++);
				if(as.assign_data != null && as.assign_data.trim().length() > 0) {
					tid.ident = rsData.getString("_assign_key");
				}
				if(geomColumn != null) {
					tid.location = rsData.getString(colIdx++);
				}
				if(addressCount > 0) {
					for(int i = 0; i < addressCount; i++) {
						guidance.add(new KeyValueTask(address.get(i).name, rsData.getString(colIdx++)));
					}
					tid.address = gson.toJson(guidance);
				}
				if(getTaskStartValue) {
					tid.taskStart = rsData.getTimestamp("taskstart");
				}

			}

		} finally {
			
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
		}
		return tid;
	}


	/*
	 * Create a new task
	 */
	public CreateTaskResp writeTask(
			Connection sd, 
			Connection cResults,
			int tgId,
			TaskServerDefn tsd,
			String urlPrefix,
			boolean updateResources,
			int oId,
			boolean autosendEmails,
			String remoteUser,
			boolean temporaryUser,
			String urlprefix,
			boolean preserveInitialData
			) throws Exception {

		CreateTaskResp resp = new CreateTaskResp();
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtInsert = null;

		String sqlGetSurveyIdentFromName = "select ident from survey where display_name = ? and p_id = ? and deleted = 'false'";
		PreparedStatement pstmtGetSurveyIdent = sd.prepareStatement(sqlGetSurveyIdentFromName);

		String sqlGetAssigneeId = "select u.id from users u, user_project up "
				+ "where u.ident = ? "
				+ "and u.id = up.u_id "
				+ "and up.p_id = ?";
		PreparedStatement pstmtGetAssigneeId = sd.prepareStatement(sqlGetAssigneeId);

		String sqlHasLocationTrigger = "select count(*) from locations where o_id = ? and uid = ? and locn_type = 'nfc'";
		PreparedStatement pstmtHasLocationTrigger = null;

		try {
			
			// Get the project for this task group
			int pId = GeneralUtilityMethods.getProjectIdFromTaskGroup(sd, tgId);
			String pName = GeneralUtilityMethods.getProjectName(sd, pId);
			String tgName = GeneralUtilityMethods.getTaskGroupName(sd, tgId);

			// 1. Update the existing task if it is being updated
			if(tsd.id > 0) {
				// Throw an exception if the task has been deleted
				for(AssignmentServerDefn asd : tsd.assignments) {
					if(isTaskDeleted(sd, tsd.id, asd.a_id)) {
						throw new Exception("Task has been deleted and cannot be edited");
					}
				}
			}

			// 2. Get the survey ident, if only the form name is specified
			if(tsd.survey_ident == null) {
				if(tsd.survey_name == null) {
					throw new Exception("Missing survey Name");
				} else {
					pstmtGetSurveyIdent.setString(1, tsd.survey_name);
					pstmtGetSurveyIdent.setInt(2, pId);
					log.info("Get survey id: " + pstmtGetSurveyIdent.toString());
					ResultSet rs = pstmtGetSurveyIdent.executeQuery();
					if(rs.next()) {
						tsd.survey_ident = rs.getString(1);
					} else {
						throw new Exception("Form not found: " + tsd.survey_name);
					}
				}
			}

			/*
			 *   Get the assignee id, only if the assignee ident is specified
			 */
			for(AssignmentServerDefn asd : tsd.assignments) {
				if(asd.assignee <= 0 && asd.assignee_ident != null && asd.assignee_ident.trim().length() > 0) {
	
					pstmtGetAssigneeId.setString(1, asd.assignee_ident);
					pstmtGetAssigneeId.setInt(2, pId);
					log.info("Get user id: " + pstmtGetAssigneeId.toString());
					ResultSet rs = pstmtGetAssigneeId.executeQuery();
					if(rs.next()) {
						asd.assignee = rs.getInt(1);
					} else {
						throw new Exception("Assignee not found: " + asd.assignee_ident);
					}
				}
			}

			String targetSurveyIdent = tsd.survey_ident;
			
			/*
			 * Set the task name to the survey name if it has not been set
			 */
			if(tsd.name == null || tsd.name.isEmpty()) {
				if(tsd.survey_name != null) {
					tsd.name = tsd.survey_name;
				} else {
					tsd.name = GeneralUtilityMethods.getSurveyNameFromIdent(sd, tsd.survey_ident);
				}
			}
			
			/*
			 * 4. Location and GPS 
			 * if a location name is specified
			 *      if location exists update it
			 *      if location does not exist create it
			 *      if location has UID and task does not then update task with UID
			 *      if location has GPS and task does not then update task with GPS
			 */
			if(tsd.location_group != null && tsd.location_group.trim().length() > 0  &&
					tsd.location_name != null && tsd.location_name.trim().length() > 0 ) {
				
				Location locn = getLocation(sd, oId, tsd.location_group, tsd.location_name);
				if(locn == null) {
					// Create location
					GeneralUtilityMethods.createLocation(sd, oId, tsd.location_group, tsd.location_trigger, tsd.location_name, tsd.lon, tsd.lat);
				} else {
					// Update Location
					GeneralUtilityMethods.updateLocation(sd, oId, tsd.location_group, tsd.location_trigger, tsd.location_name, tsd.lon, tsd.lat);
					// If task does not have UID then use the locations
					if(tsd.location_trigger == null || tsd.location_trigger.trim().length() == 0) {
						tsd.location_trigger = locn.uid;
					}
					// If task does not have GPS coordinates then use the locations
					if(tsd.lat == 0.0 && tsd.lon == 0.0) {
						tsd.lat = locn.lat;
						tsd.lon = locn.lon;
					}
				}
			}
			
			String sLon = String.valueOf(tsd.lon);
			String sLat = String.valueOf(tsd.lat);
			if(sLon.equals("0.0")) {
				sLon = "0";
			}
			if(sLat.equals("0.0")) {
				sLat = "0";
			}
			String gpsCoords = "POINT(" + tsd.lon + " " + tsd.lat + ")";
			
			/*
			 * 5. Write the task to the database
			 */
			if(tsd.from == null) {
				Calendar cal = Calendar.getInstance();
				tsd.from = new Timestamp(cal.getTime().getTime());
			}
			
			int taskId = tsd.id;
			Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String initial_data = null;
			if(tsd.initial_data != null) {
				initial_data = gson.toJson(tsd.initial_data);	
			}
			
			if(tsd.id > 0) {
				pstmt = getUpdateTaskStatement(sd, preserveInitialData);
				updateTask(
						pstmt,
						tsd.id,
						tgId,
						tsd.name,
						tsd.survey_ident,
						gpsCoords,
						tsd.address,
						tsd.from,
						tsd.to,
						tsd.location_trigger,
						tsd.location_group,
						tsd.location_name,
						tsd.repeat,
						tsd.complete_all,
						tsd.assign_auto,
						tsd.guidance,
						tsd.initial_data_source,
						initial_data,
						tsd.show_dist,
						preserveInitialData);
			} else {
				pstmt = getInsertTaskStatement(sd);
				insertTask(
						pstmt,
						pId,
						pName,
						tgId,
						tgName,
						tsd.name,
						tsd.survey_ident,
						gpsCoords,
						tsd.update_id,
						tsd.address,
						tsd.from,
						tsd.to,
						tsd.location_trigger,
						tsd.location_group,
						tsd.location_name,
						tsd.repeat,
						tsd.complete_all,
						tsd.assign_auto,
						tsd.guidance,
						tsd.initial_data_source,
						initial_data,
						tsd.show_dist);
				ResultSet rsKeys = pstmt.getGeneratedKeys();
				if(rsKeys.next()) {
					taskId = rsKeys.getInt(1);
				}
			}
			
			/*
			 * if a location name is specified
			 *      if location exists update it
			 *      if location does not exist create it
			 *      
			 * Update or create a location resource
			 */
			if(tsd.save_type != null) {
				if(tsd.save_type.equals("nl")) {
					
				} else if(tsd.save_type.equals("ul")) {
					GeneralUtilityMethods.updateLocation(sd, oId, tsd.location_group, tsd.location_trigger, tsd.location_name, tsd.lon, tsd.lat);
				}
			}
			/*
			 * 6. Assign the user to the task
			 */ 	
			for(AssignmentServerDefn asd : tsd.assignments) {
				if(asd.a_id > 0) {
					pstmtInsert = getInsertAssignmentStatement(sd);
					updateAssignment(sd, 
							cResults,
							pstmtInsert, 
							asd.assignee, 
							asd.email, 
							asd.status, 
							tgId,
							taskId, 
							asd.a_id,
							oId,
							pId,
							targetSurveyIdent,
							autosendEmails,
							remoteUser,
							temporaryUser,
							tsd.initial_data_source,
							tsd.update_id,
							tsd.name,
							tsd.from,
							tsd.to,
							tsd.assign_auto,
							tsd.repeat);
				} else {
					
					/*
					 * If a user was unassigned from a task that previously did not have
					 * anyone assigned to it then we need to mark that task as cancelled in the
					 * RecordEventManager
					 * Update the Record Event Manager
					 */
					RecordEventManager rem = new RecordEventManager();
					rem.writeTaskStatusEvent(
							sd, 
							cResults,
							tsd.id,
							0,
							STATUS_T_CANCELLED,
							null,			
							tsd.name,
							tsd.assign_auto);
					
					pstmtInsert = getInsertAssignmentStatement(sd);
					applyAllAssignments(
							sd, 
							cResults,
							null, 
							null, 
							pstmtInsert, 
							tgId,
							taskId,
							asd.assignee, 
							0,		// Role changes not supported from task properties edit 
							0,
							asd.email,
							oId,
							pId,
							targetSurveyIdent,
							null,
							autosendEmails,
							remoteUser,
							temporaryUser,
							tsd.initial_data_source,
							tsd.update_id,
							tsd.name,
							tsd.from,
							tsd.to,
							tsd.assign_auto,
							tsd.repeat);
					
				}

				if(asd.assignee > 0) {
					// Create a notification to alert the new user of the change to the task details
					String userIdent = GeneralUtilityMethods.getUserIdent(sd, asd.assignee);
					MessagingManager mm = new MessagingManager(localisation);
					mm.userChange(sd, userIdent);
				}
				
			}

			/*
			 * 7. Create a location_trigger resource to match the trigger in the loaded task
			 *    This allows a location trigger to be created when it is loaded with a task from an xls file
			 * 
			 */
			if(updateResources) {
				if(tsd.location_trigger != null && tsd.location_trigger.trim().length() > 0) {
					pstmtHasLocationTrigger = sd.prepareStatement(sqlHasLocationTrigger);
					pstmtHasLocationTrigger.setInt(1, oId);
					pstmtHasLocationTrigger.setString(2, tsd.location_trigger);
					ResultSet rs = pstmtHasLocationTrigger.executeQuery();
					if(rs.next()) {
						int count = rs.getInt(1);
						if(count == 0) {
							GeneralUtilityMethods.createLocation(sd, oId, "tg", 
									tsd.location_trigger, tsd.name, 0.0, 0.0);
						}
					}
				}
			}
			
			/*
			 * Get the link to the task to return to the calling program
			 */
			TaskListGeoJson t = getTasks(
					sd, 
					urlprefix,
					0,		// Organisation id 
					0, 		// task group id
					taskId,
					0,		// Assignment id
					true, 
					0,		// userId 
					null, 
					null,	// period 
					0,		// start 
					0,		// limit
					null,	// sort
					null);	// sort direction
			
			if(t != null && t.features.size() > 0) {
				resp = new CreateTaskResp();
				resp.task = t.features.get(0).properties;
				resp.links.put("webform", GeneralUtilityMethods.getWebformLink(
						urlprefix, 
						resp.task.survey_ident, 
						tsd.initial_data_source,
						resp.task.a_id,
						resp.task.id,
						resp.task.update_id));
			}

		} finally {
			if(pstmt != null) try {pstmt.close(); } catch(SQLException e) {};
			if(pstmtInsert != null) try {pstmtInsert.close(); } catch(SQLException e) {};
			if(pstmtGetSurveyIdent != null) try {pstmtGetSurveyIdent.close(); } catch(SQLException e) {};
			if(pstmtGetAssigneeId != null) try {pstmtGetAssigneeId.close(); } catch(SQLException e) {};
			if(pstmtHasLocationTrigger != null) try {pstmtHasLocationTrigger.close(); } catch(SQLException e) {};
		}
		
		return resp;

	}

	/*
	 * Apply an action to multiple tasks
	 */
	public void applyBulkAction(
			HttpServletRequest request, 
			Connection sd, 
			Connection cResults,
			String remoteUser,
			int tgId, 
			int pId, 
			TaskBulkAction action) throws Exception {

		String sqlGetAssignedUsers = "select distinct ident from users where temporary = false and id in "
				+ "(select a.assignee from assignments a, tasks t "
				+ "where a.task_id = t.id and t.p_id = ? and a.id in ("; 

		String sqlDeleteAssignedTemporaryUsers = "delete from users where temporary = true and id in "
				+ "(select a.assignee from assignments a, tasks t "
				+ "where a.task_id = t.id and t.p_id = ? and a.id in ("; 

		String deleteTaskSql = "update tasks t set deleted = 'true', deleted_at = now() "
				+ "where t.p_id = ? "		// Authorisation
				+ "and (select count(*) from assignments a where a.task_id = t.id and a.status != 'deleted' and a.status != 'cancelled'"
				+ "and a.status != 'new') = 0  "
				+ "and t.id in (";		
		String deleteAssignmentsSql = "update assignments set status = 'cancelled', cancelled_date = now() "
				+ "where task_id in (select id from tasks t, task_group tg where t.tg_id = tg.tg_id and tg.p_id = ?) "		// Authorisation
				+ "and (status = 'new' or status = 'accepted' or status = 'unsent' or status = 'error') "
				+ "and id in (";
		String acceptedAssignmentsSql = "update assignments set status = 'accepted', cancelled_date = null "
				+ "where task_id in (select id from tasks t, task_group tg where t.tg_id = tg.tg_id and tg.p_id = ?) "		// Authorisation
				+ "and id in (";
		
		String assignSql = "update assignments set assignee = ?, assigned_date = now(), assignee_name = (select name from users where id = ?) "
				+ "where task_id in (select id from tasks t, task_group tg where t.tg_id = tg.tg_id and tg.p_id = ?) "		// Authorisation
				+ "and id in (";
		String sqlGetUnassigned = "select id from tasks "
				+ "where id not in (select task_id from assignments where status != 'new'	) "
				+ "and id in (";
		String sqlCreateAssignments = "insert into assignments (assignee, status, task_id, assigned_date, assignee_name) "
				+ "values(?, 'accepted', ?, now(), (select name from users where id = ?))";

		String sqlEmailDetails = "select a.id, a.status, a.assignee_name, a.email, a.action_link, "
				+ "t.survey_ident, t.update_id, t.assign_auto "
				+ "from assignments a, tasks t "
				+ "where a.task_id = t.id "
				+ "and a.task_id in (select id from tasks t, task_group tg where t.tg_id = tg.tg_id and tg.p_id = ?) "
				+ "and (a.status = 'unsent' or a.status = 'accepted' or a.status = 'blocked') "
				+ "and a.id in (";
		
		String deleteNewAssignmentsSql = "update assignments set status = 'cancelled', cancelled_date = now() "
				+ "where task_id in (select id from tasks t, task_group tg where t.tg_id = tg.tg_id and tg.p_id = ?) "		// Authorisation
				+ "and (status = 'new') "
				+ "and id in (";
		
		String deleteHardNewAssignmentsSql = "delete from assignments "
				+ "where task_id in (select id from tasks t, task_group tg where t.tg_id = tg.tg_id and tg.p_id = ? and not t.assign_auto) "	
				+ "and (status = 'new') "
				+ "and id in (";
		
		String setStatusSql = "update assignments set status = ? where id = ? ";
		
		String whereTasksSql = "";
		String whereAssignmentsSql = "";
		boolean hasAssignments = false;

		PreparedStatement pstmt = null;
		PreparedStatement pstmtGetUnassigned = null;
		PreparedStatement pstmtCreateAssignments = null;
		PreparedStatement pstmtDelTempUsers = null;
		PreparedStatement pstmtGetUsers = null;
		PreparedStatement pstmtEmailDetails = null;
		PreparedStatement pstmtSetStatus = null;
		PreparedStatement pstmtDeleteNewAssignments = null;
		PreparedStatement pstmtDeleteHardNewAssignments = null;

		RecordEventManager rem = new RecordEventManager();
		
		try {

			if(action.tasks.size() == 0) {
				throw new Exception(localisation.getString("msg_no_tasks"));
			} 
			
			pstmtSetStatus = sd.prepareStatement(setStatusSql);
			
			boolean emailAction = false;		// Set a flag if this action is intended to send an email to a person
			if(action.action.equals("email_unsent")) {
				emailAction = true;
			}
			
			// Create a task / Assignment hierarchy
			HashMap<Integer, ArrayList<Integer>> hierarchyHash = new HashMap<> ();
			for(TaskAssignmentPair pair : action.tasks) {
				ArrayList<Integer> a = hierarchyHash.get(pair.taskId);
				if(a == null) {
					a = new ArrayList<Integer> ();				
					hierarchyHash.put(pair.taskId, a);
				}
				if(pair.assignmentId > 0) {
					a.add(pair.assignmentId);
				}
			}
			List<Integer> taskList = new ArrayList<Integer>(hierarchyHash.keySet());
			
			// Create a select list for affected tasks and assignments
			for(Integer taskId : taskList) {
				if(whereTasksSql.length() > 0) {
					whereTasksSql += ",";
				}
				whereTasksSql += taskId.toString();
				
				ArrayList<Integer> a = hierarchyHash.get(taskId);
				hasAssignments = (a.size() > 0);
				for(Integer assignmentId : a) {
					if(assignmentId > 0) {
						if(whereAssignmentsSql.length() > 0) {
							whereAssignmentsSql += ",";
						}
						whereAssignmentsSql += assignmentId.toString();
					} 
				}
				
			}
			whereTasksSql += ")";
			whereAssignmentsSql += ")";


			// Notify currently assigned users that are being modified
			MessagingManager mm = new MessagingManager(localisation);
			if(hasAssignments && !emailAction) {				
				pstmtGetUsers = sd.prepareStatement(sqlGetAssignedUsers + whereAssignmentsSql + ")");
				pstmtGetUsers.setInt(1, pId);
				log.info("Notify currently assigned users: " + pstmtGetUsers.toString());
				ResultSet rsNot = pstmtGetUsers.executeQuery();
				while(rsNot.next()) {
					mm.userChange(sd, rsNot.getString(1));
				}
			}

			String updateAssigned = null;
			String updateStatus = null;
			String updateName = null;
			
			if(action.action.equals("delete")) {

				if(hasAssignments) {
					// Delete temporary users
					pstmtDelTempUsers = sd.prepareStatement(sqlDeleteAssignedTemporaryUsers + whereAssignmentsSql + ")");
					pstmtDelTempUsers.setInt(1, pId);
					log.info("Del temp users created for tasks to be deleted: " + pstmtDelTempUsers.toString());
					pstmtDelTempUsers.executeUpdate();
					
					// Set assignments to deleted
					pstmt = sd.prepareStatement(deleteAssignmentsSql + whereAssignmentsSql);
					pstmt.setInt(1, pId);
					log.info("Delete assignments: " + pstmt.toString());
					pstmt.executeUpdate();
				}
				
				// Delete unassigned tasks and tasks that have only a single assignment
				if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
				pstmt = sd.prepareStatement(deleteTaskSql + whereTasksSql);
				pstmt.setInt(1, pId);
				log.info("Delete unassigned and singly assigned tasks: " + pstmt.toString());
				pstmt.executeUpdate();
				
				updateAssigned = null;
				updateStatus = "cancelled";
				updateName = null;

			} else if(action.action.equals("status")) {

				if(hasAssignments) {
					
					// Set assignments to accepted
					pstmt = sd.prepareStatement(acceptedAssignmentsSql + whereAssignmentsSql);
					pstmt.setInt(1, pId);
					log.info("Set assgignments accepted: " + pstmt.toString());
					pstmt.executeUpdate();
				}
				
				updateAssigned = null;
				updateStatus = "accepted";
				updateName = null;

			} else if(action.action.equals("assign")) {

				// Get tasks that have not had an assignment created
				pstmtGetUnassigned = sd.prepareStatement(sqlGetUnassigned + whereTasksSql);
				pstmtCreateAssignments = sd.prepareCall(sqlCreateAssignments);
				pstmtDeleteNewAssignments = sd.prepareStatement(deleteNewAssignmentsSql + whereAssignmentsSql);
				pstmtDeleteHardNewAssignments = sd.prepareStatement(deleteHardNewAssignmentsSql + whereAssignmentsSql);
				ResultSet rs = pstmtGetUnassigned.executeQuery();
				while (rs.next()) {
					// Create the first assignment for this task
					pstmtCreateAssignments.setInt(1, action.userId);
					pstmtCreateAssignments.setInt(2, rs.getInt(1));
					pstmtCreateAssignments.setInt(3, action.userId);
					log.info("Create assignment: " + pstmtCreateAssignments.toString());
					pstmtCreateAssignments.executeUpdate();
				}
				
				/*
				 * First do a hard delete of new tasks that are not auto assign
				 * Set remaining auto assign tasks to cancelled
				 */
				pstmtDeleteHardNewAssignments.setInt(1, pId);
				log.info("Delete new assignments: " + pstmtDeleteHardNewAssignments.toString());
				pstmtDeleteHardNewAssignments.executeUpdate();
				
				pstmtDeleteNewAssignments.setInt(1, pId);
				log.info("Delete new assignments: " + pstmtDeleteNewAssignments.toString());
				pstmtDeleteNewAssignments.executeUpdate();
				
				// Update assignments
				if(hasAssignments) {
					if(action.userId >= 0) {
						pstmt = sd.prepareStatement(assignSql + whereAssignmentsSql);
						pstmt.setInt(1,action.userId);
						pstmt.setInt(2,action.userId);
						pstmt.setInt(3, pId);				
						log.info("Update Assignments: " + pstmt.toString());
						pstmt.executeUpdate();
					} else {
						// Set assignments to deleted
						pstmt = sd.prepareStatement(deleteAssignmentsSql + whereAssignmentsSql);
						pstmt.setInt(1, pId);
						log.info("Delete assignments: " + pstmt.toString());
						pstmt.executeUpdate();
					}
				}

				// Notify the user who has been assigned the tasks
				String userIdent = GeneralUtilityMethods.getUserIdent(sd, action.userId);
				mm.userChange(sd, userIdent);
				
				updateAssigned = userIdent;
				updateStatus = "accepted";
				updateName = null;
				
			} else if(emailAction) {
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
				if(!GeneralUtilityMethods.emailTaskBlocked(sd, oId)) {			
					
					Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
					
					// 1. Get tasks and loop
					pstmtEmailDetails = sd.prepareStatement(sqlEmailDetails + whereAssignmentsSql);
					pstmtEmailDetails.setInt(1, pId);
					log.info("Get email tasks: " + pstmtEmailDetails.toString());
					
					ResultSet rs = pstmtEmailDetails.executeQuery();
					while(rs.next()) {
					
						int aId = rs.getInt("id");
						String status = rs.getString("status");
						String email = rs.getString("email");
						String actionLink = rs.getString("action_link");
						String surveyIdent = rs.getString("survey_ident");
						int sId = GeneralUtilityMethods.getSurveyId(sd, surveyIdent);
						String instanceId = rs.getString("update_id");
						
						if(action.action.equals("email_unsent") && !status.equals("unsent") && !status.equals("blocked")) {
							log.info("Ignoring task with status " + status + " when sending unsent");
							continue;
						}
						
						// Set email status to pending
						pstmtSetStatus.setString(1, STATUS_T_PENDING);
						pstmtSetStatus.setInt(2, aId);
						pstmtSetStatus.executeUpdate();
						
						TaskManager tm = new TaskManager(localisation, tz);
						TaskEmailDetails ted = tm.getEmailDetails(sd, tgId);
						
						// Create a submission message (The task may or may not have come from a submission)
						EmailTaskMessage taskMsg = new EmailTaskMessage(
								sId,
								pId,
								aId,
								instanceId,			
								ted.from,
								ted.subject, 
								ted.content,
								"task",
								email,			
								"email",
								request.getRemoteUser(),
								false,		// Not a temporary user if request.getRemoteUser() works
								actionLink);
						mm.createMessage(sd, oId, "email_task", "", gson.toJson(taskMsg));					
					}
				} else {
					throw new Exception(localisation.getString("email_b"));
				}
				
				updateAssigned = null;
				updateStatus = STATUS_T_PENDING;
				updateName = null;
			}
			
			/*
			 * Update the Record Event Manager
			 */
			for(Integer taskId : taskList) {
				ArrayList<Integer> a = hierarchyHash.get(taskId);
				for(Integer assignmentId : a) {
					if(assignmentId > 0) {
						rem.writeTaskStatusEvent(
								sd, 
								cResults,
								(int) taskId,
								assignmentId,
								updateStatus,
								updateAssigned,
								updateName,
								false);
					}
				}
			}

		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtGetUnassigned != null) try {pstmtGetUnassigned.close(); } catch(SQLException e) {};
			if(pstmtCreateAssignments != null) try {pstmtCreateAssignments.close(); } catch(SQLException e) {};	
			if(pstmtDelTempUsers != null) try {pstmtDelTempUsers.close(); } catch(SQLException e) {};	
			if(pstmtGetUsers != null) try {pstmtGetUsers.close(); } catch(SQLException e) {};	
			if(pstmtEmailDetails != null) try {pstmtEmailDetails.close(); } catch(SQLException e) {};	
			if(pstmtSetStatus != null) try {pstmtSetStatus.close(); } catch(SQLException e) {};	
			if(pstmtDeleteNewAssignments != null) try {pstmtDeleteNewAssignments.close(); } catch(SQLException e) {};
			if(pstmtDeleteHardNewAssignments != null) try {pstmtDeleteHardNewAssignments.close(); } catch(SQLException e) {};
		}

	}

	/*
	 * Update a task start date and time
	 */
	public void updateWhen(Connection sd, int pId, int taskId, 
			Timestamp from,
			Timestamp to) throws Exception {

		String sql = "update tasks set schedule_at = ?, schedule_finish = ? where p_id = ? and id = ?;";
		PreparedStatement pstmt = null;

		try {

			pstmt = sd.prepareStatement(sql);
			pstmt.setTimestamp(1, from);
			pstmt.setTimestamp(2, to);
			pstmt.setInt(3, pId);
			pstmt.setInt(4, taskId);

			log.info("Update start date and time: " + pstmt.toString());
			pstmt.executeUpdate();

			// Create a notification of the change
			MessagingManager mm = new MessagingManager(localisation);
			mm.taskChange(sd, taskId);

		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};

		}		
	}

	/*
	 * Delete the tasks in a task group
	 */
	public void deleteTasksInTaskGroup(Connection sd, int tgId) throws SQLException {

		String sqlUsers = "delete from users where temporary = true and id in "
				+ "(select assignee from assignments where task_id in "
				+ "(select id from tasks where tg_id = ?))"; 
		PreparedStatement pstmtUsers = null;

		String sql = "update tasks set deleted = 'true', deleted_at = now() where tg_id = ?"; 
		PreparedStatement pstmt = null;
		
		String sqlAssignments = "update assignments set status = 'cancelled', cancelled_date = now() where task_id in "
				+ "(select id from tasks where tg_id = ?)";

		String sqlGetUsers = "select distinct ident from users where temporary = false and id in "
				+ "(select assignee from assignments where task_id in "
				+ "(select id from tasks where tg_id = ?))"; 
		PreparedStatement pstmtGetUsers = null;

		try {

			// Delete any temporary users created for this task
			pstmtUsers = sd.prepareStatement(sqlUsers);
			pstmtUsers.setInt(1, tgId);

			log.info("Delete temporary user: " + pstmtUsers.toString());
			pstmtUsers.executeUpdate();

			// Notify users whose task has been deleted
			MessagingManager mm = new MessagingManager(localisation);
			pstmtGetUsers = sd.prepareStatement(sqlGetUsers);
			pstmtGetUsers.setInt(1, tgId);
			log.info("Get task users: " + pstmtGetUsers.toString());
			ResultSet rs = pstmtGetUsers.executeQuery();
			while (rs.next()) {
				mm.userChange(sd, rs.getString(1));
			}			

			// Delete the tasks
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, tgId);
			log.info("Delete tasks in task group: " + pstmt.toString());
			pstmt.executeUpdate();
			
			// Delete the assignments
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			pstmt = sd.prepareStatement(sqlAssignments);
			pstmt.setInt(1, tgId);
			log.info("Delete assignments in task group: " + pstmt.toString());
			pstmt.executeUpdate();

		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtUsers != null) try {	pstmtUsers.close(); } catch(SQLException e) {};
			if(pstmtGetUsers != null) try {	pstmtGetUsers.close(); } catch(SQLException e) {};
		}		
	}
	
	/*
	 * Delete a task
	 */
	public void deleteTask(Connection sd, Connection cResults, int taskId) throws SQLException {
		
		String sql = "update tasks set deleted = 'true', deleted_at = now() where id = ?"; 
		PreparedStatement pstmt = null;
		
		String sqlAssignments = "select a.id, t.title from assignments a, tasks t "
				+ "where a.task_id = ? "
				+ "and a.task_id = t.id "
				+ "and (a.status = 'new' "
				+ "or a.status = 'accepted' "
				+ "or a.status = 'unsent' "
				+ "or a.status = 'pending' "
				+ "or a.status = 'blocked') ";
		
		PreparedStatement pstmtAssignments = null;
		
		try {
			// Delete the task
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, taskId);
			log.info("Delete tasks in task group: " + pstmt.toString());
			pstmt.executeUpdate();
				
			// Cancel the assignments
			pstmtAssignments = sd.prepareStatement(sqlAssignments);
			pstmtAssignments.setInt(1, taskId);
			ResultSet rs = pstmtAssignments.executeQuery();
			while (rs.next()) {
				int aId = rs.getInt(1);
				String taskName = rs.getString(2);
				cancelAssignment(
						sd, 
						cResults,
						taskId,
						aId,
						taskName,
						false);
			}
			
		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtAssignments != null) try {	pstmtAssignments.close(); } catch(SQLException e) {};
		}
	}
	
	/*
	 * If a user has completed a task and the task is set to only require one assignment
	 * to be completed then delete the other assignments
	 */
	public void cancelOtherAssignments(
			Connection sd, 
			Connection cResults,
			int assignmentId) throws SQLException {

		String sqlGetTask = "select a.task_id, t.complete_all, t.title "
				+ "from assignments a, tasks t "
				+ "where a.task_id = t.id "
				+ "and a.id = ?";
		PreparedStatement pstmtGetTask = null;
		
		String sql = "select id from assignments a "
				+ "where a.task_id = ? "
				+ "and a.id != ? "
				+ "and (a.status = 'new' "
				+ "or a.status = 'accepted' "
				+ "or a.status = 'unsent' "
				+ "or a.status = 'pending' "
				+ "or a.status = 'blocked') ";
		PreparedStatement pstmt = null;

		try {

			// 1. Get the task info
			pstmtGetTask = sd.prepareStatement(sqlGetTask);
			pstmtGetTask.setInt(1, assignmentId);
			ResultSet rs = pstmtGetTask.executeQuery();
			
			if(rs.next()) {
				int taskId = rs.getInt(1);
				boolean complete_all = rs.getBoolean(2);
				String taskName = rs.getString(3);
				
				if(!complete_all) {
					// 2. Cancel the assignments
					pstmt = sd.prepareStatement(sql);
					pstmt.setInt(1, taskId);
					pstmt.setInt(2, assignmentId);
					log.info("Get other asignments: " + pstmt.toString());
					ResultSet rs2 = pstmt.executeQuery();
		
					while (rs2.next()) {
						int aId = rs2.getInt(1);
						if(aId != assignmentId) {
							log.info("Cancelling other assignment");
							cancelAssignment(
									sd, 
									cResults,
									taskId,
									aId,
									taskName,
									false);
						}
					}
				}
			}

			
		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtGetTask != null) try {	pstmtGetTask.close(); } catch(SQLException e) {};
		}		
	}
	
	/*
	 * Utility function to cancel an assignment
	 */
	public void cancelAssignment(
			Connection sd,
			Connection cResults,
			int taskId,
			int assignmentId,
			String updateName,
			boolean assign_auto) throws SQLException {
		
		String sqlUsers = "delete from users where temporary = true and id in "
				+ "(select assignee from assignments where id = ?) ";
		PreparedStatement pstmtUsers = null;
		
		String sqlGetUsers = "select distinct ident from users where temporary = false and id in "
				+ "(select assignee from assignments where id = ?) ";
		PreparedStatement pstmtGetUsers = null;
		
		String sqlAssignments = "update assignments set status = 'cancelled', cancelled_date = now() "
				+ "where id = ?";
		PreparedStatement pstmtCancel = null;
		
		try {
		
			// 1. Delete any temporary users created for this task
			pstmtUsers = sd.prepareStatement(sqlUsers);
			pstmtUsers.setInt(1, assignmentId);
			log.info("Delete temporary user for assignment: " + pstmtUsers.toString());
			pstmtUsers.executeUpdate();
			
			// 2. Notify users whose task has been deleted
			MessagingManager mm = new MessagingManager(localisation);
			pstmtGetUsers = sd.prepareStatement(sqlGetUsers);
			pstmtGetUsers.setInt(1, assignmentId);
			log.info("Get task users: " + pstmtGetUsers.toString());
			ResultSet rs = pstmtGetUsers.executeQuery();
			while (rs.next()) {
				mm.userChange(sd, rs.getString(1));
			}			
			
			// 3. Cancel the assignments
			pstmtCancel = sd.prepareStatement(sqlAssignments);
			pstmtCancel.setInt(1, assignmentId);
			log.info("Cancel Assignment: " + pstmtCancel.toString());
			pstmtCancel.executeUpdate();
			
			// Update Record Event Manager
			RecordEventManager rem = new RecordEventManager();
			rem.writeTaskStatusEvent(
					sd, 
					cResults,
					taskId,
					assignmentId,
					STATUS_T_CANCELLED,
					null,
					updateName,
					assign_auto);
		} finally {
			if(pstmtUsers != null) try {	pstmtUsers.close(); } catch(SQLException e) {};
			if(pstmtGetUsers != null) try {	pstmtGetUsers.close(); } catch(SQLException e) {};
			if(pstmtCancel != null) try {	pstmtCancel.close(); } catch(SQLException e) {};
		}
	}
	
	public Timestamp getTaskStartTime(AssignFromSurvey as, Timestamp taskStart) {
		
		if(as.taskStart == -1 || taskStart == null) {									
			taskStart = new Timestamp(System.currentTimeMillis());
		} 
		
		if(as.taskAfter > 0) {
			Calendar cal = Calendar.getInstance();
			cal.setTime(taskStart);
			if(as.taskUnits.equals("days")) {
				cal.add(Calendar.DAY_OF_WEEK, as.taskAfter);
			} else if(as.taskUnits.equals("hours")) {
				cal.add(Calendar.HOUR_OF_DAY, as.taskAfter);
			} else if(as.taskUnits.equals("minutes")) {
				cal.add(Calendar.MINUTE, as.taskAfter);
			}
			taskStart.setTime(cal.getTime().getTime());
		}	
		
		return taskStart;
	}
	
	public Timestamp getTaskFinishTime(AssignFromSurvey as, Timestamp taskStart) {
		Timestamp taskFinish = null;
							
		if(as.taskDuration > 0) {
			taskFinish = new Timestamp(taskStart.getTime());	
			Calendar cal = Calendar.getInstance();
			cal.setTime(taskFinish);
			if(as.durationUnits.equals("days")) {
				cal.add(Calendar.DAY_OF_WEEK, as.taskDuration);
			} else if(as.durationUnits.equals("hours")) {
				cal.add(Calendar.HOUR_OF_DAY, as.taskDuration);
			} else if(as.durationUnits.equals("minutes")) {
				cal.add(Calendar.MINUTE, as.taskDuration);
			}
			taskFinish.setTime(cal.getTime().getTime());
		}
		return taskFinish;
	}
	
	/*
	 * Two functions for inserting a task
	 *   1. Return a prepared statement
	 *   2. Apply the insert
	 */
	
	/*
	 * Get the preparedStatement for inserting a task
	 */
	public PreparedStatement getInsertTaskStatement(Connection sd) throws SQLException {

		String sql = "insert into tasks ("
				+ "p_id, "
				+ "p_name, "
				+ "tg_id, "
				+ "tg_name, "
				+ "title, "
				+ "survey_ident, "
				+ "survey_name, "
				+ "geo_point,"
				+ "update_id,"
				+ "address,"
				+ "schedule_at,"
				+ "schedule_finish,"
				+ "location_trigger,"
				+ "location_group,"
				+ "location_name,"
				+ "repeat,"
				+ "complete_all,"
				+ "assign_auto,"
				+ "guidance,"
				+ "initial_data_source,"
				+ "initial_data,"
				+ "show_dist) "
				+ "values ("
				+ "?, "		// p_id
				+ "?, "		// p_name
				+ "?, "		// tg_id
				+ "?, "		// tg_name
				+ "?, "		// title
				+ "?, "		// Survey ident
				+ "(select display_name from survey where ident = ?), "		// Survey name
				+ "ST_GeomFromText(?, 4326), "	// geo_point
				+ "?, "		// update_id
				+ "?, "		// address
				+ "?,"		// schedule_at
				+ "?,"		// schedule_finish
				+ "?,"		// location_trigger
				+ "?,"		// location_group
				+ "?,"		// location_name
				+ "?,"		// repeat
				+ "?,"		// complete_all
				+ "?,"		// assign_auto
				+ "?,"		// guidance
				+ "?,"		// initial_data_source
				+ "?,"		// initial_data	
				+ "?)";		// show_dist
		
		return sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		
	}
	/*
	 * Insert a task
	 */
	public int insertTask(
			PreparedStatement pstmt,
			int pId,
			String pName,
			int tgId,
			String tgName,
			String title,
			String target_s_ident,
			String location,
			String targetInstanceId,
			String address,
			Timestamp taskStart,
			Timestamp taskFinish,
			String locationTrigger,
			String locationGroup,
			String locationName,
			boolean repeat,
			boolean complete_all,
			boolean assign_auto,
			String guidance,
			String initial_data_source,
			String initial_data,
			int show_dist) throws SQLException {
		
		// If the initial data source is not a survey then there is nothing to update
		if(initial_data_source == null || !initial_data_source.equals("survey")) {
			targetInstanceId = null;
		}
				
		pstmt.setInt(1, pId);
		pstmt.setString(2,  pName);
		pstmt.setInt(3,  tgId);
		pstmt.setString(4,  tgName);
		pstmt.setString(5,  title);
		pstmt.setString(6, target_s_ident);	
		pstmt.setString(7, target_s_ident);			// For survey name			
		pstmt.setString(8, location);				// geopoint
		pstmt.setString(9, targetInstanceId);		// update id
		pstmt.setString(10, address);
		pstmt.setTimestamp(11, taskStart);
		pstmt.setTimestamp(12, taskFinish);
		pstmt.setString(13, locationTrigger);
		pstmt.setString(14, locationGroup);
		pstmt.setString(15, locationName);
		pstmt.setBoolean(16, repeat);
		pstmt.setBoolean(17, complete_all);	
		pstmt.setBoolean(18, assign_auto);	
		pstmt.setString(19, guidance);	
		pstmt.setString(20, initial_data_source);
		pstmt.setString(21, initial_data);	
		pstmt.setInt(22, show_dist);	

		log.info("Create a new task: " + pstmt.toString());
		return(pstmt.executeUpdate());
		
	}
	
	/*
	 * Get the preparedStatement for updating a task
	 */
	public PreparedStatement getUpdateTaskStatement(Connection sd, boolean preserveInitialData) throws SQLException {

		StringBuilder sql = new StringBuilder("update tasks set "
				+ "title = ?, "
				+ "survey_ident = ?, "
				+ "survey_name = (select display_name from survey where ident = ?), "
				+ "geo_point = ST_GeomFromText(?, 4326),"
				+ "address = ?,"
				+ "schedule_at = ?,"
				+ "schedule_finish = ?,"
				+ "location_trigger = ?,"
				+ "location_group = ?,"
				+ "location_name = ?,"
				+ "repeat = ?,"
				+ "complete_all = ?,"
				+ "assign_auto = ?,"
				+ "guidance = ?,"
				+ "initial_data_source = ?,");
		if(!preserveInitialData) {
			sql.append("initial_data = ?, ");
		}
		sql.append("show_dist = ? "
				+ "where id = ? "
				+ "and tg_id = ?");		// authorisation
		
		return sd.prepareStatement(sql.toString());
	}
	/*
	 * Update a task
	 */
	public int updateTask(
			PreparedStatement pstmt,	
			int tId,
			int tgId,
			String title,
			String target_s_ident,
			String location,
			String address,
			Timestamp taskStart,
			Timestamp taskFinish,
			String locationTrigger,
			String locationGroup,
			String locationName,
			boolean repeat,
			boolean complete_all,
			boolean assign_auto,
			String guidance,
			String initial_data_source,
			String initial_data,
			int show_dist,
			boolean preserveInitialData) throws SQLException {
		
		int idx = 1;
		pstmt.setString(idx++, title);
		pstmt.setString(idx++,  target_s_ident);
		pstmt.setString(idx++,  target_s_ident);			// To set survey name				
		pstmt.setString(idx++, location);			// geopoint
		pstmt.setString(idx++, address);
		pstmt.setTimestamp(idx++, taskStart);
		pstmt.setTimestamp(idx++, taskFinish);
		pstmt.setString(idx++, locationTrigger);
		pstmt.setString(idx++, locationGroup);
		pstmt.setString(idx++, locationName);
		pstmt.setBoolean(idx++, repeat);	
		pstmt.setBoolean(idx++, complete_all);	
		pstmt.setBoolean(idx++, assign_auto);	
		pstmt.setString(idx++, guidance);
		pstmt.setString(idx++, initial_data_source);
		if(!preserveInitialData) {
			pstmt.setString(idx++, initial_data);
		}		
		pstmt.setInt(idx++, show_dist);
		pstmt.setInt(idx++, tId);
		pstmt.setInt(idx++, tgId);

		log.info("Update a task: " + pstmt.toString());
		return(pstmt.executeUpdate());
	}
	
	/*
	 * Two functions for inserting an assignment
	 *   1. Return a prepared statement
	 *   2. Apply the insert
	 */
	
	/*
	 * Get the preparedStatement
	 */
	public PreparedStatement getInsertAssignmentStatement(Connection sd) throws SQLException {

		String sql = "insert into assignments ("
				+ "assignee, "
				+ "assignee_name,"
				+ "email,"
				+ "status, "
				+ "task_id,"
				+ "assigned_date) "
				+ "values (?, ?, ?, ?, ?, now())";
		
		return sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
	}
	
	/*
	 * Insert an assignment
	 */
	public int insertAssignment(
			Connection sd,
			Connection cResults,
			Gson gson,
			PreparedStatement pstmt,
			String name,					// Task name
			int assignee,
			String assigneeIdent,
			String email,
			String status,
			int task_id,
			String updateId,
			String sIdent,
			String remoteUser,
			Date scheduledAt,
			Date scheduledFinish,
			boolean assign_auto) throws SQLException {
		
		/*
		 * Delete any "new" assignments that have been set
		 */
		deleteNewAssignments(sd, task_id);
		pstmt.setInt(1, assignee);
		if(email != null) {
			pstmt.setString(2, email);
		} else {
			pstmt.setString(2,  assigneeIdent);
		}
		pstmt.setString(3,  email);	
		pstmt.setString(4,  status);	
		pstmt.setInt(5,  task_id);

		log.info("Create a new assignment: " + pstmt.toString());
		pstmt.executeUpdate();
		
		ResultSet rs = pstmt.getGeneratedKeys();
		int aId = 0;
		if (rs.next()){
			aId = rs.getInt(1);
		}
		rs.close();
		
		/*
		 * Record the record event if this task applies to a record
		 */
		if(updateId != null) {
			String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, sIdent);
			if(tableName != null) {
				log.info("Record event: " + sIdent + " : " + tableName);
				String assigned = null;
				if(email != null) {
					assigned = email;
				} else {
					assigned = GeneralUtilityMethods.getUserIdent(sd, assignee);
				}
				TaskItemChange tic = new TaskItemChange(0, aId, name, status, assigned, null, 
						scheduledAt, 
						scheduledFinish,
						assign_auto);
				RecordEventManager rem = new RecordEventManager();
				rem.writeEvent(
						sd, 
						cResults, 
						RecordEventManager.TASK, 
						status,
						remoteUser, 
						tableName, 
						updateId, 
						null,				// Change object
						gson.toJson(tic),	// Task Object
						null,				// Notification object
						localisation.getString("t_tc"), 
						0,				// sId (don't care legacy)
						sIdent,
						0,				// Don't need task id if we have an assignment id
						aId				// Assignment id
						);
				
				
			}
		} 
		
		return aId;
	}
	
	/*
	 * Two functions for inserting an assignment
	 *   1. Return a prepared statement
	 *   2. Apply the insert
	 */
	
	/*
	 * Update an assignment
	 */
	public void updateAssignment(
			Connection sd,
			Connection cResults,
			PreparedStatement pstmtInsert,
			int assignee,
			String email,
			String status,
			int tgId,
			int task_id,
			int a_id,
			int oId,
			int pId,
			String targetSurveyIdent,
			boolean autosendEmails,
			String remoteUser,
			boolean temporaryUser,
			String initialDataSource,
			String update_id,
			String task_name,
			Date scheduledAt,
			Date scheduledFinish,
			boolean assign_auto,
			boolean persist) throws Exception {
		
		String sql = "select assignee, email from assignments where id = ?";
		PreparedStatement pstmtGetExisting = null;
		boolean insertAssignment = false;
		try {
			// 1.  If assignee id or emails is set then get existing assignee and email and cancel if the assignee has changed
			pstmtGetExisting = sd.prepareStatement(sql);
			pstmtGetExisting.setInt(1, a_id);

			ResultSet rs = pstmtGetExisting.executeQuery();
			if(rs.next()) {
				int existingAssignee = rs.getInt(1);
				String existingEmail = rs.getString(2);

				// Notify currently assigned user
				if(existingAssignee != assignee) {
					cancelAssignment(
							sd, 
							cResults,
							task_id,
							a_id,
							task_name,
							false);
				}

				// Set flag indicating that new assignee should be set
				// A new assignment is required if the assignee has changed
				if((existingAssignee != assignee) ||
						(email != null && !email.equals(existingEmail)) ||
						(email == null && existingEmail != null)) {						
					insertAssignment = true;
				}
			}				
		
			// 2.  Insert new assignment
			if(insertAssignment) {
				applyAllAssignments(
						sd, 
						cResults,
						null, 
						null, 
						pstmtInsert, 
						tgId,
						task_id,
						assignee, 
						0,		// Role changes not supported from task properties edit 
						0,
						email,
						oId,
						pId,
						targetSurveyIdent,
						null,
						autosendEmails,
						remoteUser,
						temporaryUser,
						initialDataSource,
						update_id,
						task_name,
						scheduledAt,
						scheduledFinish,
						assign_auto,
						persist);
			} 
		} finally {
			if(pstmtGetExisting != null) {try {pstmtGetExisting.close();} catch(Exception e){}}
		}
		
	}
	
	
	/*
	 * Create all assignments for specific user id, role or emails
	 */
	public void applyAllAssignments(
			Connection sd,
			Connection cResults,
			PreparedStatement pstmtRoles, 
			PreparedStatement pstmtRoles2,
			PreparedStatement pstmtAssign, 
			int tgId,
			int taskId,
			int userId, 
			int roleId,
			int fixedRoleId,
			String emails,
			int oId,
			int pId,
			String sIdent,
			String targetInstanceId,
			boolean autosendEmails,
			String remoteUser,			// For autosend of emails
			boolean temporaryUser,
			String initialDataSource,
			String update_id,
			String task_name,
			Date scheduledAt,
			Date scheduledFinish,
			boolean assign_auto,
			boolean persist
			) throws Exception {

		String status = "accepted";
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		if(userId > 0) {		// Assign the user to the new task

			String userIdent = GeneralUtilityMethods.getUserIdent(sd, userId);
			insertAssignment(sd, cResults, gson, pstmtAssign, task_name, userId, userIdent, null, status, taskId, update_id, sIdent, 
					remoteUser, scheduledAt, scheduledFinish,
					assign_auto);
			
			// Notify the user of their new assignment		
			MessagingManager mm = new MessagingManager(localisation);
			mm.userChange(sd, userIdent);	

		} else if(roleId > 0) {		// Assign all users with the current role

			ResultSet rsRoles = null;
			if(fixedRoleId > 0) {
				pstmtRoles2.setInt(1, roleId);
				pstmtRoles2.setInt(2, fixedRoleId);
				log.info("Get roles2: " + pstmtRoles2.toString());
				rsRoles = pstmtRoles2.executeQuery();
			} else {
				pstmtRoles.setInt(1, roleId);
				log.info("Get roles: " + pstmtRoles.toString());
				rsRoles = pstmtRoles.executeQuery();
			}	
			
			int count = 0;
			while(rsRoles.next()) {
				count++;
		
				String userIdent = GeneralUtilityMethods.getUserIdent(sd, rsRoles.getInt(1));
				insertAssignment(sd, cResults, gson, pstmtAssign, task_name, 
						rsRoles.getInt(1), userIdent, null, status, taskId, update_id, sIdent, 
						remoteUser, scheduledAt, scheduledFinish,
						assign_auto);
				
				// Notify the user of their new assignment
				
				MessagingManager mm = new MessagingManager(localisation);
				mm.userChange(sd, userIdent);	
			}
			if(count == 0) {
				log.info("No matching users found");
			}
		} else if(emails != null && emails.length() > 0) {
			boolean emailTaskBlocked = GeneralUtilityMethods.emailTaskBlocked(sd, oId);
			String [] emailArray = emails.split(",");
			
			if(emailTaskBlocked) { 
				status = STATUS_T_BLOCKED;
			} else if(autosendEmails) {
				status = STATUS_T_PENDING;
			} else {
				status = STATUS_T_UNSENT;
			}
			
			TaskManager tm = new TaskManager(localisation, tz);
			TaskEmailDetails ted = tm.getEmailDetails(sd, tgId);
			
			// Add the task name to the email subject
			if(ted.subject == null) {
				ted.subject = "";
			}
			ted.subject = ted.subject.trim();
			if(ted.subject.length() > 0) {
				ted.subject += " - ";
			}
			ted.subject += task_name;
			
			// Create an action this should be (mostly) identical for all emails
			ActionManager am = new ActionManager(localisation, tz);
			Action action = new Action("task");
			action.surveyIdent = sIdent;
			action.pId = pId;
			action.single = !persist;
			if(initialDataSource != null) {
				if(initialDataSource.equals("task")) {
					action.taskKey = taskId;
				} else if(initialDataSource.equals("survey")) {
					if(targetInstanceId != null && targetInstanceId.trim().length() > 0) {
						action.datakey = "instanceid";
						action.datakeyvalue = targetInstanceId;
					}
				}
			} else {
				if(targetInstanceId != null && targetInstanceId.trim().length() > 0) {
					action.datakey = "instanceid";
					action.datakeyvalue = targetInstanceId;
				}
			}
			
			for(String email : emailArray) {
				
				action.email = email;
				
				// Create the assignment
				int aId = insertAssignment(sd, cResults, gson, pstmtAssign, task_name, 0, null, email, status, taskId, update_id, sIdent, 
						remoteUser, scheduledAt, scheduledFinish,
						assign_auto);
				// Create a temporary user embedding the assignment id in the action link, get the link to that user
				action.assignmentId = aId;
				log.info("######## Creating action link inside task manager: " + action.single);
				String link = am.getLink(sd, action, oId, true);	// For email tasks it is always a single submission
				
				// Update the assignment with the link to the action
				setAssignmentLink(sd, aId, link);
				
				/*
				 * Send the email if it is not blocked
				 */
				if(!emailTaskBlocked) {
					
					MessagingManager mm = new MessagingManager(localisation);
					if(autosendEmails) {
						// Create a submission message (The task may or may not have come from a submission)
						int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
						EmailTaskMessage taskMsg = new EmailTaskMessage(
								sId,
								pId,
								aId,
								targetInstanceId,			
								ted.from,
								ted.subject, 
								ted.content,
								"task",
								email,			
								"email",
								remoteUser,
								temporaryUser,
								link);
						mm.createMessage(sd, oId, "email_task", "", gson.toJson(taskMsg));
					}
				}
			}
		} else {
			
			// Set the assignment to unassigned
			String userIdent = null;
			insertAssignment(sd, cResults, gson, pstmtAssign, task_name, userId, userIdent, null, "new", taskId, update_id, sIdent, 
					remoteUser, scheduledAt, scheduledFinish, assign_auto);
			
		}
	}
	
	public PreparedStatement getRoles(Connection sd) throws SQLException {
		String sql = "select u_id from user_role where r_id = ?";
		return sd.prepareStatement(sql);
	}
	
	public PreparedStatement getRoles2(Connection sd) throws SQLException {
		String sql = "select u_id from user_role where r_id = ? and u_id in "
				+ "(select u_id from user_role where r_id = ?)";
		return sd.prepareStatement(sql);
	}
	
	public boolean isTaskDeleted(Connection sd, int t_id, int a_id) throws SQLException {
		boolean isDeleted = false;
		String sqlAssignment = "select count(*) from assignments where id = ? and (status = 'deleted' or status = 'cancelled')";
		String sqlTasks = "select count(*) from tasks where id = ? and deleted";
		PreparedStatement pstmt = null;
		try {
			if(a_id > 0) {
				pstmt = sd.prepareStatement(sqlAssignment);		// Check assignment
				pstmt.setInt(1,a_id);
			} else {
				// Check task
				pstmt = sd.prepareStatement(sqlTasks);			// Check task
				pstmt.setInt(1, t_id);
			}
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				if(rs.getInt(1) > 0) {
					isDeleted = true;
				}
			}
		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
		}
		return isDeleted;
	}
	
	/*
	 * Process an email task
	 */
	public void emailTask(
			Connection sd, 
			Connection cResults, 
			Organisation organisation,
			EmailTaskMessage msg,
			int messageId,
			String user,
			String basePath,
			String scheme,
			String server,
			String topic,
			boolean createPending) throws Exception {
		
		String docURL = null;
		String filePath = null;
		String filename = "instance";
		
		boolean writeToMonitor = true;
		MessagingManager mm = new MessagingManager(localisation);
		
		boolean generateBlank =  (msg.instanceId == null) ? true : false;	// If false only show selected options
		SurveyManager sm = new SurveyManager(localisation, "UTC");
		Survey survey = sm.getById(sd, cResults, msg.user, msg.temporaryUser, msg.sId, true, basePath, 
				msg.instanceId, true, generateBlank, true, false, true, "real", 
				false, false, true, "geojson",
				false,		// Do not include child surveys (at least not yet)
				false,		// launched only
				false		// Don't merge set value into default values
				);
		
		try {
			
			// Notification log
			String error_details = null;
			String notify_details = null;
			String status = null;
			boolean unsubscribed = false;
			
			if(organisation.email_task) {
				/*
				 * Add details from the survey to the subject and email content
				 */
				log.info("xxxxxxxxxxxxx1: " + msg.content);
				if(survey != null) {
					msg.subject = sm.fillStringTemplate(survey, msg.subject);
					msg.content = sm.fillStringTemplate(survey, msg.content);
				}
				log.info("xxxxxxxxxxxxx2: " + msg.content);
				TextManager tm = new TextManager(localisation, tz);
				ArrayList<String> text = new ArrayList<> ();
				text.add(msg.subject);
				text.add(msg.content);
				tm.createTextOutput(sd,
							cResults,
							text,
							basePath, 
							msg.user,
							survey,
							"none",
							organisation.id);
				msg.subject = text.get(0);
				msg.content = text.get(1);
				log.info("xxxxxxxxxxxxx3: " + msg.content);
				
				docURL = "/webForm" + msg.actionLink;
					
				/*
				 * Send document to target
				 */
				status = "success";				// Notification log
				notify_details = null;			// Notification log
				error_details = null;				// Notification log
				unsubscribed = false;
				if(msg.target.equals("email")) {
					EmailServer emailServer = UtilityMethodsEmail.getSmtpHost(sd, null, msg.user, organisation.id);
					if(emailServer.smtpHost != null && emailServer.smtpHost.trim().length() > 0) {
						if(UtilityMethodsEmail.isValidEmail(msg.email)) {
								
							log.info("userevent: " + msg.user + " sending email of '" + docURL + "' to " + msg.email);
							
							// Set the subject
							String subject = "";
							if(msg.subject != null && msg.subject.trim().length() > 0) {
								subject = msg.subject;
							} else {
								if(server != null && server.contains("smap")) {
									subject = "Smap ";
								}
								subject += localisation.getString("c_notify");
							}
							
							String from = "smap";
							if(msg.from != null && msg.from.trim().length() > 0) {
								from = msg.from;
							}
							StringBuilder content = null;
							if(msg.content != null && msg.content.trim().length() > 0) {
								content = new StringBuilder(msg.content);
							} else {
								content = new StringBuilder(organisation.default_email_content);
							}
							
							// task message
							String taskMsg  = localisation.getString("email_task");
							taskMsg = taskMsg.replaceAll("%s1", server);
							
							// Add the survey link and task message
							if(docURL != null) {
								content.append("<br />")
									.append(taskMsg)
									.append(" <a href=\"").append(scheme + "://")
									.append(server)
									.append(docURL)
									.append("\">")
									.append(localisation.getString("ar_survey"))
									.append("</a>");
							}

							notify_details = localisation.getString("msg_et");
							if(msg.email != null) {
								notify_details = notify_details.replaceAll("%s1", msg.email);
							}
							if(docURL != null) {
								notify_details = notify_details.replaceAll("%s2", docURL);
							}
							if(survey != null && survey.displayName != null) {
								notify_details = notify_details.replaceAll("%s3", survey.displayName);
							}
							if(survey != null && survey.projectName != null) {
								notify_details = notify_details.replaceAll("%s4", survey.projectName);
							}
							
							log.info("+++ emailing task to: " + msg.email + " docUrl: " + docURL + 
									" from: " + from + 
									" subject: " + subject +
									" smtp_host: " + emailServer.smtpHost +
									" email_domain: " + emailServer.emailDomain);
							try {
								EmailManager em = new EmailManager();
								PeopleManager peopleMgr = new PeopleManager(localisation);
								InternetAddress[] emailArray = InternetAddress.parse(msg.email);
								
								for(InternetAddress ia : emailArray) {	
									SubscriptionStatus subStatus = peopleMgr.getEmailKey(sd, organisation.id, ia.getAddress());							
									if(subStatus.unsubscribed) {
										unsubscribed = true;
										setAssignmentStatus(sd, msg.aId, "unsubscribed");
									} else {
										if(subStatus.optedIn || !organisation.send_optin) {
											log.info("Send email: " + msg.email + " : " + docURL);
													
											em.sendEmailHtml(
													ia.getAddress(),  
													"bcc", 
													subject, 
													content, 
													filePath,
													filename,
													emailServer,
													server,
													subStatus.emailKey,
													localisation,
													null,
													organisation.getAdminEmail(),
													organisation.getEmailFooter());
											
											setAssignmentStatus(sd, msg.aId, "accepted");
											
											lm.writeLog(sd, 
													survey.id, 
													ia.getAddress(), 
													LogManager.EMAIL_TASK, 
													localisation.getString("mo_sent"), 0, null);

										} else {
											/*
											 * User needs to opt in before email can be sent
											 * Move message to pending messages and send opt in message if needed
											 */ 
											mm.saveToPending(sd, organisation.id, ia.getAddress(), topic, null, 
													msg, 
													null,
													subStatus.optedInSent,
													organisation.getAdminEmail(),
													emailServer,
													subStatus.emailKey,
													createPending,
													scheme,
													server,
													messageId);
										}
									}
								}
							} catch(Exception e) {
								status = "error";
								error_details = e.getMessage();
								setAssignmentStatus(sd, msg.aId, "error");
							}
						} else {
							log.log(Level.INFO, "Info: List of email recipients is empty");
							lm.writeLog(sd, msg.sId, "subscriber", LogManager.EMAIL, localisation.getString("email_nr"), 0, null);
							writeToMonitor = false;
						}
					} else {
						status = "error";
						error_details = "smtp_host not set";
						log.log(Level.SEVERE, "Error: Task Manager, Attempt to do email notification but email server not set");
					}
					
				}  else {
					status = "error";
					error_details = "Invalid target: " + msg.target;
					log.log(Level.SEVERE, "Error: Invalid target" + msg.target);
				}
			} else {
				status = "error";
				error_details = localisation.getString("susp_email_tasks");
				log.log(Level.SEVERE, "Error: notification services suspended");
			}
			
			// Write log message
			if(writeToMonitor) {
				if(unsubscribed) {
					error_details += localisation.getString("c_unsubscribed") + ": " + msg.email;
				}
				NotificationManager nm = new NotificationManager(localisation);
				nm.writeToLog(sd, organisation.id, msg.pId, msg.sId, notify_details, status, 
						error_details, messageId);
				
			}
		} finally {
			//
			
		}
	}
	

	private void setAssignmentStatus(Connection sd, int aId, String status) throws SQLException {
		String sql = "update assignments set status = ? where id = ? ";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, status);
			pstmt.setInt(2, aId);
			log.info("Set assignment status: " + pstmt.toString());
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
	
	private void setAssignmentLink(Connection sd, int aId, String link) throws SQLException {
		String sql = "update assignments set action_link = ? where id = ? ";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, link);
			pstmt.setInt(2, aId);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
	
	/*
	 * Convert TaskProperties from a TaskFeature to a TaskSeverDefn
	 * TaskPoperties are the old deprecated way of representing a task and an assignment as a single object
	 * Now the 1 to many relationship between task and assignments is managed using TaskServerDefn
	 */
	public TaskServerDefn convertTaskFeature(TaskFeature tf) {
		TaskServerDefn tsd = new TaskServerDefn();
		tsd.id = tf.properties.id;
		tsd.name = tf.properties.name;
		tsd.address = tf.properties.address;
		tsd.survey_ident = tf.properties.survey_ident;
		tsd.survey_name = tf.properties.survey_name;
		tsd.from = tf.properties.from;
		tsd.to = tf.properties.to;
		tsd.guidance = tf.properties.guidance;
		tsd.initial_data = tf.properties.initial_data;
		tsd.show_dist = tf.properties.show_dist;
		tsd.initial_data_source = tf.properties.initial_data_source;
		tsd.repeat = tf.properties.repeat;
		tsd.complete_all = tf.properties.complete_all;
		tsd.assign_auto = tf.properties.assign_auto;
		tsd.update_id = tf.properties.update_id;
		tsd.lon = tf.properties.lon;
		tsd.lat = tf.properties.lat;
		tsd.location_trigger = tf.properties.location_trigger;
		tsd.location_group = tf.properties.location_group;
		tsd.location_name = tf.properties.location_name;
		tsd.save_type = tf.properties.save_type;
		
		if(tf.properties.emails != null && tf.properties.emails.trim().length() > 0) {
			String [] emailArray = tf.properties.emails.split(",");
			for(String email : emailArray) {
				AssignmentServerDefn asd = new AssignmentServerDefn();
				asd.a_id = tf.properties.a_id;
				asd.assignee = tf.properties.assignee;
				asd.assignee_name = tf.properties.assignee_name;
				asd.email = email;
				tsd.assignments.add(asd);
			}
		} else {
			AssignmentServerDefn asd = new AssignmentServerDefn();
			asd.a_id = tf.properties.a_id;
			asd.assignee = tf.properties.assignee;
			asd.assignee_name = tf.properties.assignee_name;
			asd.email = tf.properties.emails;
			tsd.assignments.add(asd);
		}
		
		return tsd;
		
	}
	
	/*
	 * Update the email details for a task group
	 */
	public void updateEmailDetails(Connection sd, int pId, int tgId, TaskEmailDetails ted) throws SQLException {
		String sql = "update task_group set email_details = ? where p_id = ? and tg_id = ?";
		PreparedStatement pstmt = null;
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, gson.toJson(ted));
			pstmt.setInt(2, pId);
			pstmt.setInt(3, tgId);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
	}
	
	/*
	 * Update the email details for a task group
	 */
	public TaskEmailDetails getEmailDetails(Connection sd, int tgId) throws SQLException {
		String sql = "select email_details from task_group where tg_id = ?";
		PreparedStatement pstmt = null;
	
		TaskEmailDetails ted = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1,  tgId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				ted = new Gson().fromJson(rs.getString(1), TaskEmailDetails.class);
			}
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
		if(ted == null) {
			ted = new TaskEmailDetails();
		}
		return ted;
	}
	
	/*
	 * Add data based emails to fixed emails
	 * Emails may be space separated or comma separated
	 */
	public String combineEmails(String fixedEmails, String dataEmails) {
		if(fixedEmails == null) {
			fixedEmails = "";
		}
		if(dataEmails == null) {
			dataEmails = "";
		}
		
		String emails = fixedEmails;
		if(emails.length() > 0) {
			emails += ",";
		}
		emails += dataEmails;
		emails = emails.trim().replaceAll("\\s", " ").replace(" ", ",");
		emails = emails.replaceAll("[,]+", ",");
		return emails;
		
	}
	
	/*
	 * return an instance containing the instance data attached to the task
	 * Note this is not the same as TaskInstanceData which is meta data for a task
	 * This is form data that needs to initialise a task
	 */
	public Instance getInstance(Connection sd, int taskId) throws SQLException {

		String sql = "select initial_data " 
				+ "from tasks "
				+ "where id = ? ";

		Instance instance = new Instance();
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, taskId);
			ResultSet resultSet = pstmt.executeQuery();

			if (resultSet.next()) {
				String iString = resultSet.getString(1);
				if(iString != null) {
					Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
					instance = gson.fromJson(iString, Instance.class);
				}
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}

		return instance;
	}
	
	public String fillStringTaskTemplate(TaskProperties task, SubmissionMessage msg, String in) {
		String out = in;

		StringBuffer taskUrl = new StringBuffer("");
		taskUrl.append(msg.scheme).append("://").append(msg.server).append("/webForm");
		if(task.action_link != null) {
			taskUrl.append(task.action_link);
		} else {
			taskUrl.append("/").append(task.survey_ident).append("?assignment_id=").append(task.a_id);
			if(task.update_id != null) {
				taskUrl.append("&datakey=instanceid&datakeyvalue=").append(task.update_id);
			}
		}
	
		if(out != null && task != null) {
			out = out.replaceAll("\\$\\{task_webform\\}", taskUrl.toString());
			out = out.replaceAll("\\$\\{assignee_name\\}", task.assignee_name);
			out = out.replaceAll("\\$\\{scheduled\\}", task.from.toString());

		} else {
			log.info("Could not fill task template details for: " + out + " : " + ((task == null) ? "task is null" : "task not null" ));
		}

		return out;
	}
	
	/*
	 * Create a task group if it does not exist or return the existing task group id
	 * Task groups have unique names within a project
	 */
	public int createTaskGroup(Connection sd, 
			String taskGroupName, 
			int projectId,
			String addressParams,
			String rule,
			int sourceSurveyId,
			int targetSurveyId,
			int dlDist,
			boolean complete_all,
			boolean assign_auto,
			boolean useExisting			// If set and there is an existing task group with the same name return its id, otherwise throw an exception
			) throws Exception {
		
		int taskGroupId = 0;
		
		PreparedStatement pstmtUniqueTg = null;
		PreparedStatement pstmtTaskGroup = null;
		ResultSet rsKeys = null;
		
		try {
			/*
			 * Check that a task group of this name does not already exist
			 * This would be better implemented as a constraint on the database but existing customers probably have task
			 *  groups with duplicate names
			 */
			String checkUniqueTg = "select tg_id from task_group where name = ? and p_id = ?;";
			pstmtUniqueTg = sd.prepareStatement(checkUniqueTg);
			pstmtUniqueTg.setString(1, taskGroupName);
			pstmtUniqueTg.setInt(2, projectId);
			log.info("Check uniqueness of task group name in project: " + pstmtUniqueTg.toString());
			ResultSet rs = pstmtUniqueTg.executeQuery();
	
			if(rs.next()) {
				taskGroupId = rs.getInt(1);
				if(taskGroupId > 0 && !useExisting) {
					throw new Exception("Task Group Name " + taskGroupName + " already Exists");
				}
			}
		
			// Create a task group if one was not found
			if(taskGroupId == 0) {
				String tgSql = "insert into task_group ( "
						+ "name, "
						+ "p_id, "
						+ "address_params,"
						+ "rule,"
						+ "source_s_id,"
						+ "target_s_id,"
						+ "dl_dist,"
						+ "complete_all,"
						+ "assign_auto) "
						+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?);";
		
				pstmtTaskGroup = sd.prepareStatement(tgSql, Statement.RETURN_GENERATED_KEYS);
				pstmtTaskGroup.setString(1, taskGroupName);
				pstmtTaskGroup.setInt(2, projectId);
				pstmtTaskGroup.setString(3, addressParams);
				pstmtTaskGroup.setString(4, rule);
				pstmtTaskGroup.setInt(5, sourceSurveyId);
				pstmtTaskGroup.setInt(6, targetSurveyId);
				pstmtTaskGroup.setInt(7, dlDist);
				pstmtTaskGroup.setBoolean(8, complete_all);
				pstmtTaskGroup.setBoolean(9, assign_auto);
				log.info("Insert into task group: " + pstmtTaskGroup.toString());
				pstmtTaskGroup.execute();
		
				rsKeys = pstmtTaskGroup.getGeneratedKeys();
				if(rsKeys.next()) {
					taskGroupId = rsKeys.getInt(1);
				}
			}
		} finally {
			if(pstmtUniqueTg != null) try {	pstmtUniqueTg.close(); } catch(SQLException e) {};
			if(pstmtTaskGroup != null) try {	pstmtTaskGroup.close(); } catch(SQLException e) {};
		}
		return taskGroupId;
	}
	
	private void deleteNewAssignments(Connection sd, int taskId) throws SQLException {
		String sql = "delete from assignments where task_id = ? and status = 'new'";
		PreparedStatement pstmt= null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1,  taskId);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
	}
	
	public void setTaskStatusCancelled(Connection sd, int assignmentId) throws SQLException {

		PreparedStatement pstmt = null;

		String sql = "update assignments set status = 'cancelled', cancelled_date = now() "
				+ "where id = ? ";

		try {

			pstmt = sd.prepareStatement(sql);
			
			if(assignmentId > 0) {
				pstmt.setInt(1, assignmentId);
				log.info("Updating assignment status: " + pstmt.toString());
				pstmt.executeUpdate();

		
			}


		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}


		}
	}
}

