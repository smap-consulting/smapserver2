package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
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
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.EmailTaskMessage;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValueTask;
import org.smap.sdal.model.Location;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TaskAddressSettings;
import org.smap.sdal.model.TaskAssignmentPair;
import org.smap.sdal.model.TaskBulkAction;
import org.smap.sdal.model.TaskEmailDetails;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskGroup;
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
	
	private String fullStatusList[] = {
			"new", 
			"accepted", 
			"unsent", 
			"unsubscribed", 
			"submitted", 
			"rejected", 
			"cancelled", 
			"deleted",
			"pending",
			"error"
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

	public TaskManager(ResourceBundle l) {
		localisation = l;
	}
	
	/*
	 * Get the current task groups
	 */
	public ArrayList<TaskGroup> getTaskGroups(Connection sd, int projectId) throws Exception {

		String sql = "select tg_id, name, address_params, p_id, rule, source_s_id, target_s_id, email_details "
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
				tg.rule = rs.getString(5);
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
				tg.rule = rs.getString(5);
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
	 * Get the tasks for the specified task group
	 */
	public TaskListGeoJson getTasks(Connection sd, 
			int taskGroupId, 
			boolean completed,
			int userId,
			String incStatus,
			String period) throws Exception {

		String sql1 = "select t.id as t_id, "
				+ "t.title as name,"
				+ "t.schedule_at as schedule_at,"
				+ "t.schedule_finish as schedule_finish,"
				+ "t.schedule_at + interval '1 hour' as default_finish,"
				+ "t.location_trigger as location_trigger,"
				+ "t.update_id as update_id,"
				+ "t.initial_data as initial_data,"
				+ "t.address as address,"
				+ "t.guidance as guidance,"
				+ "t.repeat as repeat,"
				+ "t.repeat_count as repeat_count,"
				+ "t.url as url,"
				+ "t.form_id,"
				+ "t.survey_name as form_name,"
				+ "t.deleted,"
				+ "t.complete_all,"
				+ "s.blocked as blocked,"
				+ "s.ident as form_ident,"
				+ "a.id as assignment_id,"
				+ "a.status as status,"
				+ "a.assignee,"
				+ "a.assignee_name,"
				+ "a.email,"
				+ "a.action_link,"
				+ "u.ident as assignee_ident, "				// Get current user ident for notification
				+ "ST_AsGeoJSON(t.geo_point) as geom, "
				+ "ST_AsText(t.geo_point) as wkt "
				+ "from tasks t "
				+ "join survey s "
				+ "on t.form_id = s.s_id "
				+ "left outer join assignments a "
				+ "on a.task_id = t.id " 
				+ "left outer join users u "
				+ "on a.assignee = u.id "
				+ "where t.tg_id = ? "
				+ "and a.status = any (?) ";
		
		if(period == null) {
			period = "all";
		}
		String sql2 = null;
		if(period.equals("all")) {
			sql2 = "";
		} else if(period.equals("week")) {
			sql2 = "and t.schedule_at > now() - interval '7 days'";
		} else if(period.equals("month")) {
			sql2 = "and t.schedule_at > now() - interval '30 days'";
		}
		String sql3 = "order by t.schedule_at desc, t.id, a.id desc;";
		PreparedStatement pstmt = null;

		TaskListGeoJson tl = new TaskListGeoJson();

		// Create the list of status values to return
		ArrayList<String> statusList = new ArrayList<String> ();
		if(incStatus == null) {
			for(String status : fullStatusList) {
				statusList.add(status);
			}
		} else {
			String [] incStatusArray = incStatus.split(",");
			for(String status : incStatusArray) {
				for(String statusRef : fullStatusList) {
					if(status.trim().equals(statusRef)) {
						statusList.add(statusRef);
						break;
					}
				}
			}
		}
		
		try {

			pstmt = sd.prepareStatement(sql1 + sql2 + sql3);	
			pstmt.setInt(1, taskGroupId);
			pstmt.setArray(2, sd.createArrayOf("text", statusList.toArray(new String[statusList.size()])));		
			log.info("Get tasks: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();

			JsonParser parser = new JsonParser();
			while (rs.next()) {

				String status = rs.getString("status");
				boolean deleted = rs.getBoolean("deleted");
				if(deleted && status == null) {
					status = "deleted";
				} else if(status == null) {
					status = "new";
				}

				int assignee = rs.getInt("assignee"); 

				// Ignore any tasks that are not required
				if(!completed && (status.equals("submitted") || status.equals("complete"))) {
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
				tf.properties.to = rs.getTimestamp("schedule_finish");
				if(tf.properties.to == null) {
					tf.properties.to = rs.getTimestamp("default_finish");
				}
				tf.properties.status = status;	
				tf.properties.form_id = rs.getInt("form_id");
				tf.properties.form_ident = rs.getString("form_ident");
				tf.properties.form_name = rs.getString("form_name");
				tf.properties.url = rs.getString("url");
				tf.properties.action_link = rs.getString("action_link");
				tf.properties.blocked = rs.getBoolean("blocked");
				tf.properties.assignee = rs.getInt("assignee");
				tf.properties.assignee_name = rs.getString("assignee_name");
				tf.properties.emails = rs.getString("email");
				tf.properties.assignee_ident = rs.getString("assignee_ident");
				tf.properties.location_trigger = rs.getString("location_trigger");
				tf.properties.update_id = rs.getString("update_id");
				tf.properties.initial_data = rs.getString("initial_data");
				tf.properties.address = rs.getString("address");
				tf.properties.guidance = rs.getString("guidance");
				tf.properties.repeat = rs.getBoolean("repeat");
				tf.properties.repeat_count = rs.getInt("repeat_count");
				tf.geometry = parser.parse(rs.getString("geom")).getAsJsonObject();
				tf.properties.complete_all = rs.getBoolean("complete_all");

				tl.features.add(tf);
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
	public void writeTaskList(Connection sd, 
			ArrayList<TaskServerDefn> tl,
			int pId,
			String pName,
			int tgId,
			String tgName,
			String urlPrefix,
			boolean updateResources,
			int oId,
			boolean autosendEmails,
			String remoteUser) throws Exception {

		HashMap<String, String> userIdents = new HashMap<>();

		for(TaskServerDefn tsd : tl) {
			writeTask(sd, pId, pName, tgId, tgName, tsd, urlPrefix, updateResources, oId, autosendEmails, remoteUser);
			for(AssignmentServerDefn asd : tsd.assignments)
			if(asd.assignee_ident != null) {
				userIdents.put(asd.assignee_ident, asd.assignee_ident);
			}
		}

		// Create a notification to alert the new user of the change to the task details
		for(String user : userIdents.keySet()) {
			MessagingManager mm = new MessagingManager();
			mm.userChange(sd, user);
		}	

	}

	/*
	 * Get the current locations available for an organisation
	 */
	public ArrayList<Location>  getLocations(Connection sd, 
			int oId) throws SQLException {

		String sql = "select id, locn_group, locn_type, uid, name from locations where o_id = ? order by id asc;";
		PreparedStatement pstmt = null;
		ArrayList<Location> locations = new ArrayList<Location> ();

		try {

			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, oId);

			log.info("Get locations: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				Location locn = new Location();

				locn.id = rs.getInt(1);
				locn.group = rs.getString(2);
				locn.type = rs.getString(3);
				locn.uid = rs.getString(4);
				locn.name = rs.getString(5);

				locations.add(locn);
			}


		} catch(Exception e) {
			throw(e);
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}

		return locations;

	}

	/*
	 * Save a list of locations replacing the existing ones
	 */
	public void saveLocations(Connection sd, 
			ArrayList<Location> tags,
			int oId) throws SQLException {


		String sqlDelete = "delete from locations where o_id = ?;";
		PreparedStatement pstmtDelete = null;

		String sql = "insert into locations (o_id, locn_group, locn_type, uid, name) values (?, ?, ?, ?, ?);";
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
			int source_s_id, 
			String hostname,
			String instanceId,
			int pId,
			String pName,
			String remoteUser) throws Exception {

		String sqlGetRules = "select tg_id, name, rule, address_params, target_s_id from task_group where source_s_id = ?;";
		PreparedStatement pstmtGetRules = null;

		SurveyManager sm = new SurveyManager(localisation);
		Survey survey = null;

		
		try {

			pstmtGetRules = sd.prepareStatement(sqlGetRules);
			pstmtGetRules.setInt(1, source_s_id);
			log.info("Get task rules: " + pstmtGetRules.toString());

			ResultSet rs = pstmtGetRules.executeQuery();
			while(rs.next()) {
				
				if(survey == null) {
					// Get the forms - this is required by the test filter
					survey = sm.getById(sd, cResults, remoteUser, source_s_id, true, "", 
							instanceId, false, false, true, false, true, "real", 
							false, false, false, "geojson");
				}

				int tgId = rs.getInt(1);
				String tgName = rs.getString(2);
				AssignFromSurvey as = new Gson().fromJson(rs.getString(3), AssignFromSurvey.class);
				String addressString = rs.getString(4);
				ArrayList<TaskAddressSettings> address = null;
				if(addressString != null) {
					address = new Gson().fromJson(addressString, new TypeToken<ArrayList<TaskAddressSettings>>() {}.getType());
				}
				int target_s_id = rs.getInt(5);

				log.info("Assign Survey String: " + rs.getString(3));
				log.info("userevent: matching rule: " + as.task_group_name + " for survey: " + source_s_id);	// For log

				/*
				 * Check filter to see if this rule should be fired
				 * In addition avoid permanent loops of tasks being reassigned after completion
				 *   Don't fire if form to be updated is the same one that has been submitted 
				 */
				boolean fires = false;
				if(as.add_future  && source_s_id != target_s_id) {
					if(as.filter != null && as.filter.advanced != null) {
						fires = GeneralUtilityMethods.testFilter(cResults, localisation, survey, as.filter.advanced, instanceId);
						if(!fires) {
							log.info("Rule not fired as filter criteria not met: " + as.filter.advanced);
						}
					} else {
						fires = true;
					}
				} else {
					log.info("Rule not fired as target = source");
				}

				if(fires) {
					log.info("userevent: rule fires: " + (as.filter == null ? "no filter" : "yes filter") + " for survey: " + source_s_id + 
							" task survey: " + target_s_id);
					TaskInstanceData tid = getTaskInstanceData(sd, cResults, 
							source_s_id, instanceId, as, address); // Get data from new submission
					writeTaskCreatedFromSurveyResults(sd, as, hostname, tgId, tgName, pId, pName, source_s_id, 
							target_s_id, tid, instanceId, true, remoteUser);  // Write to the database
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
			AssignFromSurvey as,
			String hostname,
			int tgId,
			String tgName,
			int pId,
			String pName,
			int source_s_id,
			int target_s_id,
			TaskInstanceData tid,			// data from submission
			String instanceId,
			boolean autosendEmails,
			String remoteUser
			) throws Exception {



		PreparedStatement pstmtAssign = null;
		PreparedStatement pstmtRoles = null;
		PreparedStatement pstmtRoles2 = null;
		PreparedStatement pstmt = null;
		
		String title = null;
		if(tid.instanceName == null || tid.instanceName.trim().length() == 0) {
			title = as.project_name + " : " + as.survey_name;
		} else {
			title = tid.instanceName;
		}

		String location = tid.location;

		try {

			String targetSurveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, target_s_id);
			String formUrl = "http://" + hostname + "/formXML?key=" + targetSurveyIdent;
			String initial_data_url = null;
			String targetInstanceId = null;

			/*
			 * Set data to be updated
			 */
			if(as.update_results) {
				initial_data_url = "http://" + hostname + "/instanceXML/" + 
						targetSurveyIdent + "/0?key=prikey&keyval=" + tid.prikey;					// deprecated
				targetInstanceId = instanceId;										// New way to identify existing records to be updated
			}

			/*
			 * Location
			 */
			if(location == null) {
				location = "POINT(0 0)";
			} else if(location.startsWith("LINESTRING")) {
				log.info("Starts with linestring: " + tid.location.split(" ").length);
				if(location.split(" ").length < 3) {	// Convert to point if there is only one location in the line
					location = location.replaceFirst("LINESTRING", "POINT");
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
					target_s_id,
					formUrl,
					initial_data_url,
					location,
					targetInstanceId,
					tid.address,
					taskStart,
					taskFinish,
					tid.locationTrigger,
					false,
					null,
					instanceId);

			/*
			 * Assign the user to the new task
			 */
			int userId = as.user_id;
			int roleId = as.role_id;
			log.info("Assign user: userId: "  + userId + " roleId: " + roleId + " tid.ident: " + tid.ident);
			int fixedRoleId = as.fixed_role_id;
			int oId = GeneralUtilityMethods.getOrganisationId(sd, null, target_s_id);
			
			// Assign to people dependent on data from a form
			if(tid.ident != null) {
			
				log.info("Assign Ident: " + tid.ident);
				if(as.user_id == -2) {
					userId = GeneralUtilityMethods.getUserIdOrgCheck(sd, tid.ident, oId);   // Its a user ident
				} else {
					roleId = GeneralUtilityMethods.getRoleId(sd, tid.ident, oId);   // Its a role name
				}
			}
			
			ResultSet rsKeys = pstmt.getGeneratedKeys();
			if(rsKeys.next()) {
				int taskId = rsKeys.getInt(1);
				
				pstmtAssign = getInsertAssignmentStatement(sd, as.emails == null);
				pstmtRoles = getRoles(sd);
				pstmtRoles2 = getRoles2(sd);
				
				applyAllAssignments(
						sd, 
						pstmtRoles, 
						pstmtRoles2, 
						pstmtAssign, 
						tgId,
						taskId,
						userId, 
						roleId, 
						fixedRoleId,
						as.emails,
						oId,
						pId,
						targetSurveyIdent,
						tid,
						autosendEmails,
						remoteUser,
						instanceId);
			}
			if(rsKeys != null) try{ rsKeys.close(); } catch(SQLException e) {};		

		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtAssign != null) try {	pstmtAssign.close(); } catch(SQLException e) {};
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
			
			boolean hasGeom = GeneralUtilityMethods.hasColumn(cResults, topForm.tableName, "the_geom");
			if(hasGeom) {
				sql.append(", ST_AsText(the_geom)");
			}
			if(as.assign_data != null && as.assign_data.trim().length() > 0) {
				SqlFrag frag = new SqlFrag();
				frag.addSqlFragment(as.assign_data, false, localisation);
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
				if(hasGeom) {
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
	public void writeTask(Connection sd, 
			int pId, 
			String pName,
			int tgId,
			String tgName,
			TaskServerDefn tsd,
			String urlPrefix,
			boolean updateResources,
			int oId,
			boolean autosendEmails,
			String remoteUser
			) throws Exception {

		PreparedStatement pstmt = null;
		PreparedStatement pstmtAssign = null;
		PreparedStatement pstmtInsert = null;

		String sqlGetFormIdFromName = "select s_id from survey where display_name = ? and p_id = ? and deleted = 'false'";
		PreparedStatement pstmtGetFormId = sd.prepareStatement(sqlGetFormIdFromName);

		String sqlGetAssigneeId = "select u.id from users u, user_project up "
				+ "where u.ident = ? "
				+ "and u.id = up.u_id "
				+ "and up.p_id = ?";
		PreparedStatement pstmtGetAssigneeId = sd.prepareStatement(sqlGetAssigneeId);

		String sqlHasLocationTrigger = "select count(*) from locations where o_id = ? and uid = ? and locn_type = 'nfc';";
		PreparedStatement pstmtHasLocationTrigger = null;

		String sqlUpdateLocationTrigger = "insert into locations (o_id, locn_group, locn_type, uid, name) values (?, 'tg', 'nfc', ?, ?);";
		PreparedStatement pstmtUpdateLocationTrigger = null;

		try {

			// 1. Update the existing task if it is being updated
			if(tsd.id > 0) {
				// Throw an exception if the task has been deleted
				for(AssignmentServerDefn asd : tsd.assignments) {
					if(isTaskDeleted(sd, tsd.id, asd.a_id)) {
						throw new Exception("Task has been deleted and cannot be edited");
					}
				}
			}

			// 2. Get the form id, if only the form name is specified
			if(tsd.form_id <= 0) {
				if(tsd.form_name == null) {
					throw new Exception("Missing form Name");
				} else {
					pstmtGetFormId.setString(1, tsd.form_name);
					pstmtGetFormId.setInt(2, pId);
					log.info("Get survey id: " + pstmtGetFormId.toString());
					ResultSet rs = pstmtGetFormId.executeQuery();
					if(rs.next()) {
						tsd.form_id = rs.getInt(1);
					} else {
						throw new Exception("Form not found: " + tsd.form_name);
					}
				}
			}

			/*
			 *   Get the assignee id, if only the assignee ident is specified
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

			String targetSurveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, tsd.form_id);
			String webformUrl = urlPrefix + "/webForm/" + targetSurveyIdent;

			/*
			 * 4. Location
			 */
			String sLon = String.valueOf(tsd.lon);
			String sLat = String.valueOf(tsd.lat);
			if(sLon.equals("0.0")) {
				sLon = "0";
			}
			if(sLat.equals("0.0")) {
				sLat = "0";
			}
			String location = "POINT(" + tsd.lon + " " + tsd.lat + ")";

			/*
			 * 5. Write the task to the database
			 */
			if(tsd.from == null) {
				Calendar cal = Calendar.getInstance();
				tsd.from = new Timestamp(cal.getTime().getTime());
			}
			
			int taskId = tsd.id;
			if(tsd.id > 0) {
				pstmt = getUpdateTaskStatement(sd);
				updateTask(
						pstmt,
						tsd.id,
						tgId,
						tsd.name,
						tsd.form_id,
						webformUrl,
						location,
						tsd.address,
						tsd.from,
						tsd.to,
						tsd.location_trigger,
						tsd.repeat,
						tsd.guidance);
			} else {
				pstmt = getInsertTaskStatement(sd);
				insertTask(
						pstmt,
						pId,
						pName,
						tgId,
						tgName,
						tsd.name,
						tsd.form_id,
						webformUrl,
						tsd.initial_data,
						location,
						tsd.update_id,
						tsd.address,
						tsd.from,
						tsd.to,
						tsd.location_trigger,
						tsd.repeat,
						tsd.guidance,
						tsd.instance_id);
				ResultSet rsKeys = pstmt.getGeneratedKeys();
				if(rsKeys.next()) {
					taskId = rsKeys.getInt(1);
				}
			}

			/*
			 * 6. Assign the user to the task
			 */ 	
			for(AssignmentServerDefn asd : tsd.assignments) {
				if(asd.a_id > 0) {
					pstmtInsert = getInsertAssignmentStatement(sd, asd.email == null);
					pstmtAssign = getUpdateAssignmentStatement(sd);
					updateAssignment(sd, pstmtAssign, pstmtInsert, 
							asd.assignee, asd.email, "accepted", 
							tgId,
							taskId, 
							asd.a_id,
							oId,
							pId,
							targetSurveyIdent,
							autosendEmails,
							remoteUser,
							tsd.instance_id);
				} else {
					pstmtInsert = getInsertAssignmentStatement(sd, asd.email == null);
					applyAllAssignments(
							sd, 
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
							tsd.instance_id);
				}
				
				if(asd.assignee > 0) {
					// Create a notification to alert the new user of the change to the task details
					String userIdent = GeneralUtilityMethods.getUserIdent(sd, asd.assignee);
					MessagingManager mm = new MessagingManager();
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
							pstmtUpdateLocationTrigger = sd.prepareStatement(sqlUpdateLocationTrigger);
							pstmtUpdateLocationTrigger.setInt(1, oId);
							pstmtUpdateLocationTrigger.setString(2, tsd.location_trigger);
							pstmtUpdateLocationTrigger.setString(3, tsd.name);
							log.info("Adding NFC resource: " + pstmtUpdateLocationTrigger.toString());
							pstmtUpdateLocationTrigger.executeUpdate();
						}
					}
				}
			}

		} finally {
			if(pstmt != null) try {pstmt.close(); } catch(SQLException e) {};
			if(pstmtAssign != null) try {pstmtAssign.close(); } catch(SQLException e) {};
			if(pstmtInsert != null) try {pstmtInsert.close(); } catch(SQLException e) {};
			if(pstmtGetFormId != null) try {pstmtGetFormId.close(); } catch(SQLException e) {};
			if(pstmtGetAssigneeId != null) try {pstmtGetAssigneeId.close(); } catch(SQLException e) {};
			if(pstmtHasLocationTrigger != null) try {pstmtHasLocationTrigger.close(); } catch(SQLException e) {};
			if(pstmtUpdateLocationTrigger != null) try {pstmtUpdateLocationTrigger.close(); } catch(SQLException e) {};
		}

	}

	/*
	 * Apply an action to multiple tasks
	 */
	public void applyBulkAction(HttpServletRequest request, Connection sd, 
			int tgId, int pId, TaskBulkAction action) throws Exception {

		String sqlGetAssignedUsers = "select distinct ident from users where temporary = false and id in "
				+ "(select a.assignee from assignments a, tasks t "
				+ "where a.task_id = t.id and t.p_id = ? and a.id in ("; 

		String sqlDeleteAssignedTemporaryUsers = "delete from users where temporary = true and id in "
				+ "(select a.assignee from assignments a, tasks t "
				+ "where a.task_id = t.id and t.p_id = ? and a.id in ("; 

		String deleteTaskSql = "update tasks t set deleted = 'true', deleted_at = now() "
				+ "where t.p_id = ? "		// Authorisation
				+ "and (select count(*) from assignments a where a.task_id = t.id and a.status != 'deleted' and a.status != 'cancelled') = 0  "
				+ "and t.id in (";		
		String deleteAssignmentsSql = "update assignments set status = 'cancelled', cancelled_date = now() "
				+ "where task_id in (select task_id from tasks where p_id = ?) "		// Authorisation
				+ "and (status = 'new' or status = 'accepted' or status = 'unsent' or status = 'error') "
				+ "and id in (";
		
		String assignSql = "update assignments set assignee = ?, assigned_date = now() "
				+ "where task_id in (select task_id from tasks where p_id = ?) "		// Authorisation
				+ "and id in (";
		String sqlGetUnassigned = "select id from tasks "
				+ "where id not in (select task_id from assignments) "
				+ "and id in (";
		String sqlCreateAssignments = "insert into assignments (assignee, status, task_id, assigned_date) "
				+ "values(?, 'accepted', ?, now())";

		String sqlEmailDetails = "select a.id, a.status, a.assignee_name, a.email, a.action_link, "
				+ "t.form_id, t.update_id "
				+ "from assignments a, tasks t "
				+ "where a.task_id = t.id "
				+ "and a.task_id in (select task_id from tasks where p_id = ?) "
				+ "and (a.status = 'unsent' or a.status = 'accepted') "
				+ "and a.id in (";
		
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

		try {

			if(action.tasks.size() == 0) {
				throw new Exception("No tasks");
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
			MessagingManager mm = new MessagingManager();
			if(hasAssignments && !emailAction) {				
				pstmtGetUsers = sd.prepareStatement(sqlGetAssignedUsers + whereAssignmentsSql + ")");
				pstmtGetUsers.setInt(1, pId);
				log.info("Notify currently assigned users: " + pstmtGetUsers.toString());
				ResultSet rsNot = pstmtGetUsers.executeQuery();
				while(rsNot.next()) {
					mm.userChange(sd, rsNot.getString(1));
				}
			}

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

			} else if(action.action.equals("assign")) {

				// Get tasks that have not had an assignment created
				pstmtGetUnassigned = sd.prepareStatement(sqlGetUnassigned + whereTasksSql);
				pstmtCreateAssignments = sd.prepareCall(sqlCreateAssignments);
				ResultSet rs = pstmtGetUnassigned.executeQuery();
				while (rs.next()) {
					// Create the first assignment for this task
					pstmtCreateAssignments.setInt(1, action.userId);
					pstmtCreateAssignments.setInt(2, rs.getInt(1));
					log.info("Create assignment: " + pstmtCreateAssignments.toString());
					pstmtCreateAssignments.executeUpdate();
				}
				// Update assignments
				if(hasAssignments) {
					if(action.userId >= 0) {
						pstmt = sd.prepareStatement(assignSql + whereAssignmentsSql);
						pstmt.setInt(1,action.userId);
						pstmt.setInt(2, pId);				
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
			} else if(emailAction) {
				
				String basePath = GeneralUtilityMethods.getBasePath(request);
				String urlprefix = "https://" + request.getServerName() + "/";
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser(), 0);
				Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
				
				// 1. Get tasks and loop
				pstmtEmailDetails = sd.prepareStatement(sqlEmailDetails + whereAssignmentsSql);
				pstmtEmailDetails.setInt(1, pId);
				log.info("Get email tasks: " + pstmtEmailDetails.toString());
				
				ResultSet rs = pstmtEmailDetails.executeQuery();
				while(rs.next()) {
				
					// "select a.status, a.assignee_name, a.email, a.temp_user_id, t.form_id "
					int aId = rs.getInt("id");
					String status = rs.getString("status");
					String email = rs.getString("email");
					String actionLink = rs.getString("action_link");
					int sId = rs.getInt("form_id");
					String instanceId = rs.getString("update_id");
					
					if(action.action.equals("email_unsent") && !status.equals("unsent")) {
						log.info("Ignoring task with status " + status + " when sending unsent");
						continue;
					}
					
					// Set email status to pending
					pstmtSetStatus.setString(1, "pending");
					pstmtSetStatus.setInt(2, aId);
					pstmtSetStatus.executeUpdate();
					
					TaskManager tm = new TaskManager(localisation);
					TaskEmailDetails ted = tm.getEmailDetails(sd, tgId);
					
					// Create a submission message (The task may or may not have come from a submission)
					EmailTaskMessage taskMsg = new EmailTaskMessage(
							sId,
							pId,
							aId,
							instanceId,			
							"from someone",
							"subject is x", 
							"content is y",
							"task",
							email,			
							"email",
							urlprefix,
							actionLink);
					mm.createMessage(sd, oId, "email_task", "", gson.toJson(taskMsg));
					
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
			MessagingManager mm = new MessagingManager();
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
			MessagingManager mm = new MessagingManager();
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
	 * Delete tasks that reference a specific updateId
	 */
	public void deleteTaskforUpdateId(Connection sd, int sId, String updateId, String user) throws SQLException {

		String sqlGetUsers = "select distinct ident from users where temporary = false and id in "
				+ "(select a.assignee from assignments a, tasks t "
				+ "where t.update_id = ? "
				+ "and a.status = 'accepted' "
				+ "and a.task_id = t.id)"; 
		PreparedStatement pstmtGetUsers = null;

		String sqlTempUsers = "delete from users where temporary = true and id in "
				+ "(select a.assignee from assignments a, tasks t "
				+ "where t.update_id = ? "
				+ "and a.status = 'accepted' "
				+ "and a.task_id = t.id)"; 
		PreparedStatement pstmtTempUsers = null;
		
		String sqlAssignments = "update assignments set status = 'cancelled', cancelled_date = now() "
				+ "where status = 'accepted' "
				+ "and task_id in (select id from tasks where update_id = ?)";
		PreparedStatement pstmt = null;

		try {

			// Delete any temporary users created for this task
			pstmtTempUsers = sd.prepareStatement(sqlTempUsers);
			pstmtTempUsers.setString(1, updateId);

			log.info("Delete temporary user: " + pstmtTempUsers.toString());
			pstmtTempUsers.executeUpdate();

			// Notify users whose task has been deleted
			MessagingManager mm = new MessagingManager();
			pstmtGetUsers = sd.prepareStatement(sqlGetUsers);
			pstmtGetUsers.setString(1, updateId);

			log.info("Get task users: " + pstmtGetUsers.toString());
			ResultSet rs = pstmtGetUsers.executeQuery();
			while (rs.next()) {
				mm.userChange(sd, rs.getString(1));
			}			
			
			// Delete the assignments
			pstmt = sd.prepareStatement(sqlAssignments);
			pstmt.setString(1, updateId);
			log.info("Delete assignments that reference an update id: " + pstmt.toString());
			pstmt.executeUpdate();
			

		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtTempUsers != null) try {	pstmtTempUsers.close(); } catch(SQLException e) {};
			if(pstmtGetUsers != null) try {	pstmtGetUsers.close(); } catch(SQLException e) {};
		}		
	}
	
	public Timestamp getTaskStartTime(AssignFromSurvey as, Timestamp taskStart) {
		
		if(as.taskStart == -1) {									
			taskStart = new Timestamp(System.currentTimeMillis());
		} else {
			if(taskStart == null) {
				taskStart = new Timestamp(System.currentTimeMillis());
			}
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
				+ "form_id, "
				+ "survey_name, "
				+ "url, "
				+ "initial_data,"
				+ "geo_point,"
				+ "update_id,"
				+ "address,"
				+ "schedule_at,"
				+ "schedule_finish,"
				+ "location_trigger,"
				+ "repeat,"
				+ "guidance,"
				+ "instance_id) "
				+ "values ("
				+ "?, "		// p_id
				+ "?, "		// p_name
				+ "?, "		// tg_id
				+ "?, "		// tg_name
				+ "?, "		// title
				+ "?, "		// form_id
				+ "(select display_name from survey where s_id = ?), "		// Survey name
				+ "?, "		// url
				+ "?, "		// initial_data	
				+ "ST_GeomFromText(?, 4326), "	// geo_point
				+ "?, "		// update_id
				+ "?, "		// address
				+ "?,"		// schedule_at
				+ "?,"		// schedule_finish
				+ "?,"		// location_trigger
				+ "?,"		// repeat
				+ "?,"		// guidance
				+ "?)";		// instanceId
		
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
			int target_s_id,
			String formUrl,
			String initial_data_url,
			String location,
			String targetInstanceId,
			String address,
			Timestamp taskStart,
			Timestamp taskFinish,
			String locationTrigger,
			boolean repeat,
			String guidance,
			String instanceId) throws SQLException {
		
		pstmt.setInt(1, pId);
		pstmt.setString(2,  pName);
		pstmt.setInt(3,  tgId);
		pstmt.setString(4,  tgName);
		pstmt.setString(5,  title);
		pstmt.setInt(6, target_s_id);			// form id
		pstmt.setInt(7, target_s_id);			// For survey name
		pstmt.setString(8, formUrl);				
		pstmt.setString(9, initial_data_url);	// initial_data
		pstmt.setString(10, location);			// geopoint
		pstmt.setString(11, targetInstanceId);	// update id
		pstmt.setString(12, address);
		pstmt.setTimestamp(13, taskStart);
		pstmt.setTimestamp(14, taskFinish);
		pstmt.setString(15, locationTrigger);
		pstmt.setBoolean(16, repeat);	
		pstmt.setString(17, guidance);	
		pstmt.setString(18, instanceId);

		log.info("Create a new task: " + pstmt.toString());
		return(pstmt.executeUpdate());
	}
	
	/*
	 * Get the preparedStatement for updating a task
	 */
	public PreparedStatement getUpdateTaskStatement(Connection sd) throws SQLException {

		String sql = "update tasks set "
				+ "title = ?, "
				+ "form_id = ?, "
				+ "survey_name = (select display_name from survey where s_id = ?), "
				+ "url = ?, "
				+ "geo_point = ST_GeomFromText(?, 4326),"
				+ "address = ?,"
				+ "schedule_at = ?,"
				+ "schedule_finish = ?,"
				+ "location_trigger = ?,"
				+ "repeat = ?,"
				+ "guidance = ? "
				+ "where id = ? "
				+ "and tg_id = ?";		// authorisation
		
		return sd.prepareStatement(sql);
	}
	/*
	 * Insert a task
	 */
	public int updateTask(
			PreparedStatement pstmt,	
			int tId,
			int tgId,
			String title,
			int target_s_id,
			String formUrl,
			String location,
			String address,
			Timestamp taskStart,
			Timestamp taskFinish,
			String locationTrigger,
			boolean repeat,
			String guidance) throws SQLException {
		
		pstmt.setString(1, title);
		pstmt.setInt(2,  target_s_id);
		pstmt.setInt(3,  target_s_id);			// To set suvey name
		pstmt.setString(4, formUrl);					
		pstmt.setString(5, location);			// geopoint
		pstmt.setString(6, address);
		pstmt.setTimestamp(7, taskStart);
		pstmt.setTimestamp(8, taskFinish);
		pstmt.setString(9, locationTrigger);
		pstmt.setBoolean(10, repeat);	
		pstmt.setString(11, guidance);
		pstmt.setInt(12, tId);
		pstmt.setInt(13, tgId);

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
	public PreparedStatement getInsertAssignmentStatement(Connection sd, boolean internal) throws SQLException {

		String sql1 = "insert into assignments ("
				+ "assignee, "
				+ "assignee_name,"
				+ "email,"
				+ "status, "
				+ "task_id,"
				+ "assigned_date) "
				+ "values (?, ";
		String sql2_internal = "(select name from users where id = ?)";
		String sql2_email = "?";
		String sql3 = ", ?, ?, ?, now())";
		
		String sql = null;
		if(internal) {
			sql = sql1 + sql2_internal +sql3;
		} else {
			sql = sql1 + sql2_email +sql3;
		}
		return sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
	}
	
	/*
	 * Insert an assignment
	 */
	public int insertAssignment(
			PreparedStatement pstmt,
			int assignee,
			String email,
			String status,
			int task_id) throws SQLException {
		
		pstmt.setInt(1, assignee);
		if(email != null) {
			pstmt.setString(2, email);
		} else {
			pstmt.setInt(2,  assignee);
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
		
		return aId;
	}
	
	/*
	 * Two functions for inserting an assignment
	 *   1. Return a prepared statement
	 *   2. Apply the insert
	 */
	
	/*
	 * Get the preparedStatement
	 */
	public PreparedStatement getUpdateAssignmentStatement(Connection sd) throws SQLException {

		String sql = "update assignments "
				+ "set assignee = ?, "
				+ "assignee_name = (select name from users where id = ?),"
				+ "status = ?,"
				+ "assigned_date = now() "
				+ "where task_id = ? "
				+ "and id = ?";
		
		return sd.prepareStatement(sql);
	}
	
	/*
	 * Update an assignment
	 */
	public void updateAssignment(
			Connection sd,
			PreparedStatement pstmtAssign,
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
			String instanceId) throws Exception {
		
		String sql = "select assignee, email from assignments where id = ?";
		PreparedStatement pstmtGetExisting = null;
		boolean assignmentCancelled = false;
		try {
			// 1.  If assignee id or emails is set then get existing assignee and email and cancel if the assignee has changed
			if(assignee > 0 || email != null) {
				pstmtGetExisting = sd.prepareStatement(sql);
				pstmtGetExisting.setInt(1, a_id);
				
				ResultSet rs = pstmtGetExisting.executeQuery();
				if(rs.next()) {
					int existingAssignee = rs.getInt(1);
					String existingEmail = rs.getString(2);
					
					if((assignee > 0 && existingAssignee != assignee) ||
							(email != null && !email.equals(existingEmail))) {
						cancelAssignment(sd, a_id, assignee);
						assignmentCancelled = true;
					}
				}				
				
			}
		
			// 2.  If assignment has been cancelled then insert new assignment
			if(assignmentCancelled) {
				applyAllAssignments(
						sd, 
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
						instanceId);
			} else {
				// Else apply update
				pstmtAssign.setInt(1, assignee);
				pstmtAssign.setInt(2,  assignee);
				pstmtAssign.setString(3,  status);			// default the status to accepted for new assignments
				pstmtAssign.setInt(4,  task_id);
				pstmtAssign.setInt(5, a_id);
				
				log.info("Update an assignment: " + pstmtAssign.toString());
				pstmtAssign.executeUpdate();
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
			TaskInstanceData tid,
			boolean autosendEmails,
			String remoteUser,			// For autosend of emails
			String instanceId			// For autosend of emails
			) throws Exception {

		String status = "accepted";
		
		if(userId > 0) {		// Assign the user to the new task

			insertAssignment(pstmtAssign, userId, null, status, taskId);
			
			// Notify the user of their new assignment
			String userIdent = GeneralUtilityMethods.getUserIdent(sd, userId);
			MessagingManager mm = new MessagingManager();
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
		
				insertAssignment(pstmtAssign, rsRoles.getInt(1), null, status, taskId);
				
				// Notify the user of their new assignment
				String userIdent = GeneralUtilityMethods.getUserIdent(sd, rsRoles.getInt(1));
				MessagingManager mm = new MessagingManager();
				mm.userChange(sd, userIdent);	
			}
			if(count == 0) {
				log.info("No matching users found");
			}
		} else if(emails != null && emails.length() > 0) {
			String [] emailArray = emails.split(",");
			if(autosendEmails) {
				status = "pending";
			} else {
				status = "unsent";
			}
			
			TaskManager tm = new TaskManager(localisation);
			TaskEmailDetails ted = tm.getEmailDetails(sd, tgId);
			
			// Create an action this should be (mostly) identical for all emails
			ActionManager am = new ActionManager();
			Action action = new Action("task");
			action.surveyIdent = sIdent;
			action.pId = pId;
			if(tid != null && tid.prikey > 0) {
				action.datakey = "prikey";
				action.datakeyvalue = String.valueOf(tid.prikey);
			}
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			for(String email : emailArray) {
				
				// Create the assignment
				int aId = insertAssignment(pstmtAssign, 0, email, status, taskId);
				
				// Create a temporary user embedding the assignment id in the action link, get the link to that user
				action.assignmentId = aId;
				String link = am.getLink(sd, action, oId, true);
				
				// Update the assignment with the link to the action
				setAssignmentLink(sd, aId, link);
				MessagingManager mm = new MessagingManager();
				if(autosendEmails) {
					// Create a submission message (The task may or may not have come from a submission)
					int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
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
							remoteUser,
							link);
					mm.createMessage(sd, oId, "email_task", "", gson.toJson(taskMsg));
				}
			}
		} else {
			log.info("No matching assignments found");
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
	 * Process a notification
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
			String server) throws Exception {
		
		String docURL = null;
		String filePath = null;
		String filename = "instance";
		
		boolean writeToMonitor = true;
		
		HashMap<String, String> sentEndPoints = new HashMap<> ();
		
		PreparedStatement pstmtGetSMSUrl = null;
		
		PreparedStatement pstmtNotificationLog = null;
		String sqlNotificationLog = "insert into notification_log " +
				"(o_id, p_id, s_id, notify_details, status, status_details, event_time, message_id) " +
				"values( ?, ?,?, ?, ?, ?, now(), ?); ";
		
		// Time Zone
		int utcOffset = 0;	
		LocalDateTime dt = LocalDateTime.now();
		if(organisation.timeZone != null) {
			try {
				ZoneId zone = ZoneId.of(organisation.timeZone);
			    ZonedDateTime zdt = dt.atZone(zone);
			    ZoneOffset offset = zdt.getOffset();
			    utcOffset = offset.getTotalSeconds() / 60;
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		
		boolean generateBlank =  (msg.instanceId == null) ? true : false;	// If false only show selected options
		SurveyManager sm = new SurveyManager(localisation);
		Survey survey = sm.getById(sd, cResults, msg.user, msg.sId, true, basePath, 
				msg.instanceId, true, generateBlank, true, false, true, "real", 
				false, false, true, "geojson");
		
		try {
			
			pstmtNotificationLog = sd.prepareStatement(sqlNotificationLog);
			
			/*
			 * Add details from the survey to the subject and email content
			 */
			if(survey != null) {
				msg.subject = sm.fillStringTemplate(survey, msg.subject);
				msg.content = sm.fillStringTemplate(survey, msg.content);
			}
			TextManager tm = new TextManager(localisation);
			ArrayList<String> text = new ArrayList<> ();
			text.add(msg.subject);
			text.add(msg.content);
			tm.createTextOutput(sd,
						cResults,
						text,
						basePath, 
						msg.user,
						survey,
						utcOffset,
						"none",
						organisation.id);
			msg.subject = text.get(0);
			msg.content = text.get(1);
			
			docURL = "/webForm" + msg.actionLink;
				
			/*
			 * Send document to target
			 */
			String status = "success";				// Notification log
			String notify_details = null;			// Notification log
			String error_details = null;				// Notification log
			boolean unsubscribed = false;
			if(msg.target.equals("email")) {
				EmailServer emailServer = UtilityMethodsEmail.getSmtpHost(sd, null, msg.user);
				if(emailServer.smtpHost != null && emailServer.smtpHost.trim().length() > 0) {
					if(GeneralUtilityMethods.isValidEmail(msg.email)) {
							
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
						String content = null;
						if(msg.content != null && msg.content.trim().length() > 0) {
							content = msg.content;
						} else {
							content = organisation.default_email_content;
						}
						
						notify_details = "Sending task email to: " + msg.email + " containing link " + docURL;
						
						log.info("+++ emailing task to: " + msg.email + " docUrl: " + docURL + 
								" from: " + from + 
								" subject: " + subject +
								" smtp_host: " + emailServer.smtpHost +
								" email_domain: " + emailServer.emailDomain);
						try {
							EmailManager em = new EmailManager();
							PeopleManager peopleMgr = new PeopleManager(localisation);
							InternetAddress[] emailArray = InternetAddress.parse(msg.email);
							String emailKey = null;
							
							for(InternetAddress ia : emailArray) {								
								emailKey = peopleMgr.getEmailKey(sd, organisation.id, ia.getAddress());							
								if(emailKey == null) {
									unsubscribed = true;
									setAssignmentStatus(sd, msg.aId, "unsubscribed");
								} else {
									System.out.println("Send email: " + msg.email + " : " + docURL);
									em.sendEmail(
											ia.getAddress(), 
											null, 
											"notify", 
											subject, 
											content,
											from,		
											null, 
											null, 
											null, 
											docURL, 
											filePath,
											filename,
											organisation.getAdminEmail(), 
											emailServer,
											scheme,
											server,
											emailKey,
											localisation);
									setAssignmentStatus(sd, msg.aId, "accepted");
								}
							}
						} catch(Exception e) {
							status = "error";
							error_details = e.getMessage();
							setAssignmentStatus(sd, msg.aId, "error");
						}
					} else {
						log.log(Level.INFO, "Info: List of email recipients is empty");
						lm.writeLog(sd, msg.sId, "subscriber", "email", localisation.getString("email_nr"));
						writeToMonitor = false;
					}
				} else {
					status = "error";
					error_details = "smtp_host not set";
					log.log(Level.SEVERE, "Error: Attempt to do email notification but email server not set");
				}
				
			} else if(msg.target.equals("sms")) {   // SMS URL notification - SMS message is posted to an arbitrary URL 
				
				// Get the URL to use in sending the SMS
				String sql = "select s.sms_url "
						+ "from server s";
				
				String sms_url = null;
				pstmtGetSMSUrl = sd.prepareStatement(sql);
				ResultSet rs = pstmtGetSMSUrl.executeQuery();
				if(rs.next()) {
					sms_url = rs.getString("sms_url");	
				}
				
				if(sms_url != null) {
					ArrayList<String> smsList = null;
					ArrayList<String> responseList = new ArrayList<> ();
					smsList.add(msg.email);
					
					if(smsList.size() > 0) {
						SMSManager smsUrlMgr = new SMSManager();
						for(String sms : smsList) {
							
							if(sentEndPoints.get(sms) == null) {
								log.info("userevent: " + msg.user + " sending sms of '" + msg.content + "' to " + sms);
								responseList.add(smsUrlMgr.sendSMSUrl(sms_url, sms, msg.content));
								sentEndPoints.put(sms, sms);
							} else {
								log.info("Duplicate phone number: " + sms);
							}
							
						} 
					} else {
						log.info("No phone numbers to send to");
						writeToMonitor = false;
					}
					
					notify_details = "Sending sms " + smsList.toString() 
							+ ((docURL == null || docURL.equals("null")) ? "" :" containing link " + docURL)
							+ " with response " + responseList.toString();
					
				} else {
					status = "error";
					error_details = "SMS URL not set";
					log.log(Level.SEVERE, "Error: Attempt to do SMS notification but SMS URL not set");
				}
	
				
			} else {
				status = "error";
				error_details = "Invalid target: " + msg.target;
				log.log(Level.SEVERE, "Error: Invalid target" + msg.target);
			}
			
			// Write log message
			if(writeToMonitor) {
				if(unsubscribed) {
					error_details += localisation.getString("c_unsubscribed") + ": " + msg.email;
				}
				pstmtNotificationLog.setInt(1, organisation.id);
				pstmtNotificationLog.setInt(2, msg.pId);
				pstmtNotificationLog.setInt(3, msg.sId);
				pstmtNotificationLog.setString(4, notify_details);
				pstmtNotificationLog.setString(5, status);
				pstmtNotificationLog.setString(6, error_details);
				pstmtNotificationLog.setInt(7, messageId);
				
				pstmtNotificationLog.executeUpdate();
			}
		} finally {
			try {if (pstmtNotificationLog != null) {pstmtNotificationLog.close();}} catch (SQLException e) {}
			try {if (pstmtGetSMSUrl != null) {pstmtGetSMSUrl.close();}} catch (SQLException e) {}
			
		}
	}
	

	private void setAssignmentStatus(Connection sd, int aId, String status) throws SQLException {
		String sql = "update assignments set status = ? where id = ? ";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, status);
			pstmt.setInt(2, aId);
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
	
	private void cancelAssignment(Connection sd, int aId, int assignee) throws SQLException {
		String sql = "update assignments set status = 'cancelled' where id = ? ";
		PreparedStatement pstmt = null;
		
		String sqlGetAssignedUsers = "select distinct ident from users where temporary = false and id in "
				+ "(select a.assignee from assignments a where a.id = ?) ";
		PreparedStatement pstmtGetUsers = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, aId);
			pstmt.executeUpdate();
		
			// Notify currently assigned users that are being modified
			MessagingManager mm = new MessagingManager();
			if(assignee > 0) {				
				pstmtGetUsers = sd.prepareStatement(sqlGetAssignedUsers);
				pstmtGetUsers.setInt(1, assignee);
						
				ResultSet rsNot = pstmtGetUsers.executeQuery();
				while(rsNot.next()) {
					mm.userChange(sd, rsNot.getString(1));
				}
			}

		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
			if(pstmtGetUsers != null) {try {pstmtGetUsers.close();} catch(Exception e) {}}
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
		tsd.form_id = tf.properties.form_id;
		tsd.form_name = tf.properties.form_name;
		tsd.from = tf.properties.from;
		tsd.to = tf.properties.to;
		tsd.guidance = tf.properties.guidance;
		tsd.initial_data = tf.properties.initial_data;
		tsd.instance_id = tf.properties.instance_id;
		tsd.repeat = tf.properties.repeat;
		tsd.update_id = tf.properties.update_id;
		tsd.lon = tf.properties.lon;
		tsd.lat = tf.properties.lat;
		
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
		return ted;
	}
}

