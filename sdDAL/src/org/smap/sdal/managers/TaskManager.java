package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import javax.ws.rs.core.Request;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.AssignFromSurvey;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.KeyValueTask;
import org.smap.sdal.model.Location;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TaskAddressSettings;
import org.smap.sdal.model.TaskBulkAction;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskGroup;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskProperties;
import org.smap.sdal.model.User;
import org.smap.sdal.model.UserGroup;

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
	
	private class TaskInstanceData {
		int prikey = 0;						// data from submission
		String ident = null;					// Identifier of person or role to be assigned
		String location = null;				// data from submission
		String address = null;				// data from submission
		String locationTrigger = null;		// data from task set up
		String instanceName = null;			// data from task look up
		Timestamp taskStart = null;			// Start time
	}
	


	public TaskManager(ResourceBundle l) {
		localisation = l;
	}
	
	/*
	 * Get the current task groups
	 */
	public ArrayList<TaskGroup> getTaskGroups(Connection sd, int projectId) throws Exception {

		String sql = "select tg_id, name, address_params, p_id, rule, source_s_id, target_s_id "
				+ "from task_group where p_id = ? order by tg_id asc;";
		PreparedStatement pstmt = null;

		String sqlTotal = "select tg.tg_id, count(t.id)  "
				+ "from task_group tg "
				+ "left outer join tasks t "
				+ "on tg.tg_id = t.tg_id " 
				+ "left outer join assignments a "
				+ "on a.task_id = t.id " 
				+ "and a.status != 'deleted' "
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
			int userId) throws Exception {

		String sql = "select t.id as t_id, "
				+ "t.title as name,"
				+ "t.schedule_at as schedule_at,"
				+ "t.schedule_finish as schedule_finish,"
				+ "t.schedule_at + interval '1 hour' as default_finish,"
				+ "t.location_trigger as location_trigger,"
				+ "t.update_id as update_id,"
				+ "t.initial_data as initial_data,"
				+ "t.address as address,"
				+ "t.guidance as guidance,"
				+ "t.email as email,"
				+ "t.repeat as repeat,"
				+ "t.repeat_count as repeat_count,"
				+ "t.url as url,"
				+ "t.form_id,"
				+ "t.survey_name as form_name,"
				+ "s.blocked as blocked,"
				+ "s.ident as form_ident,"
				+ "a.status as status,"
				+ "a.assignee,"
				+ "a.assignee_name,"
				+ "u.ident as assignee_ident, "				// Get current user ident for notification
				+ "ST_AsGeoJSON(t.geo_point) as geom, "
				+ "ST_AsText(t.geo_point) as wkt "
				+ "from tasks t "
				+ "left outer join users u "
				+ "on t.assignee = u.id "
				+ "join survey s "
				+ "on t.form_id = s.s_id "
				+ "left outer join assignments a "
				+ "on a.task_id = t.id " 
				+ "where t.tg_id = ? "
				+ "order by t.schedule_at desc, t.id desc;";
		PreparedStatement pstmt = null;

		TaskListGeoJson tl = new TaskListGeoJson();

		try {

			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, taskGroupId);
			log.info("Get tasks: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();

			JsonParser parser = new JsonParser();
			while (rs.next()) {

				String status = rs.getString("status");
				if(status == null) {
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
				tf.properties.blocked = rs.getBoolean("blocked");
				tf.properties.assignee = rs.getInt("assignee");
				tf.properties.assignee_name = rs.getString("assignee_name");
				tf.properties.assignee_ident = rs.getString("assignee_ident");
				tf.properties.location_trigger = rs.getString("location_trigger");
				tf.properties.update_id = rs.getString("update_id");
				tf.properties.initial_data = rs.getString("initial_data");
				tf.properties.address = rs.getString("address");
				tf.properties.guidance = rs.getString("guidance");
				tf.properties.email = rs.getString("email");
				tf.properties.repeat = rs.getBoolean("repeat");
				tf.properties.repeat_count = rs.getInt("repeat_count");
				tf.geometry = parser.parse(rs.getString("geom")).getAsJsonObject();
				tf.properties.location = rs.getString("wkt");

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
			TaskListGeoJson tl,
			int pId,
			String pName,
			int tgId,
			String tgName,
			String urlPrefix,
			boolean updateResources,
			int oId) throws Exception {

		HashMap<String, String> userIdents = new HashMap<>();

		ArrayList<TaskFeature> features = tl.features;
		for(int i = 0; i < features.size(); i++) {
			TaskFeature tf = features.get(i);
			writeTask(sd, pId, pName, tgId, tgName, tf, urlPrefix, updateResources, oId);
			if(tf.properties.assignee_ident != null) {
				userIdents.put(tf.properties.assignee_ident, tf.properties.assignee_ident);
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

		String sqlGetRules = "select tg_id, tg_name, rule, address_params, target_s_id from task_group where source_s_id = ?;";
		PreparedStatement pstmtGetRules = null;

		SurveyManager sm = new SurveyManager(localisation);
		Survey survey = null;

		
		try {

			pstmtGetRules = sd.prepareStatement(sqlGetRules);
			pstmtGetRules.setInt(1, source_s_id);

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
				AssignFromSurvey as = new Gson().fromJson(rs.getString(2), AssignFromSurvey.class);
				String addressString = rs.getString(4);
				ArrayList<TaskAddressSettings> address = null;
				if(addressString != null) {
					address = new Gson().fromJson(addressString, new TypeToken<ArrayList<TaskAddressSettings>>() {}.getType());
				}
				int target_s_id = rs.getInt(4);

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
							target_s_id, tid, instanceId);  // Write to the database
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
			String instanceId	
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

			pstmtAssign = getInsertAssignmentStatement(sd);
			pstmtRoles = getRoles(sd);
			pstmtRoles2 = getRoles2(sd);
			
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
					null);

			/*
			 * Assign the user to the new task
			 */
			int userId = as.user_id;
			int roleId = as.role_id;
			log.info("Assign user: userId: "  + userId + " roleId: " + roleId + " tid.ident: " + tid.ident);
			int fixedRoleId = as.fixed_role_id;
			int oId = GeneralUtilityMethods.getOrganisationId(sd, null, target_s_id);
			if(tid.ident != null) {
			
				log.info("Assign Ident: " + tid.ident);
				if(as.user_id == -2) {
					userId = GeneralUtilityMethods.getUserIdOrgCheck(sd, tid.ident, oId);   // Its a user ident
				} else {
					roleId = GeneralUtilityMethods.getRoleId(sd, tid.ident, oId);   // Its a role name
				}
			}
			
			if(userId > 0 || roleId > 0) {
				ResultSet rsKeys = pstmt.getGeneratedKeys();
				if(rsKeys.next()) {
					int taskId = rsKeys.getInt(1);
					applyAllAssignments(sd, pstmtRoles, pstmtRoles2, pstmtAssign, taskId,userId, roleId, fixedRoleId);
				}
				if(rsKeys != null) try{ rsKeys.close(); } catch(SQLException e) {};

			} else {
				log.info("xxxxxxx No matching role");
			}

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
			TaskFeature tf,
			String urlPrefix,
			boolean updateResources,
			int oId	
			) throws Exception {

		PreparedStatement pstmt = null;
		PreparedStatement pstmtAssign = null;

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

			// 1. Delete the existing task if it is being updated
			if(tf.properties.id > 0) {
				deleteTask(sd, tf.properties.id, pId);		// Delete the task
			}

			// 2. Get the form id, if only the form name is specified
			if(tf.properties.form_id <= 0) {
				if(tf.properties.form_name == null) {
					throw new Exception("Missing form Name");
				} else {
					pstmtGetFormId.setString(1, tf.properties.form_name);
					pstmtGetFormId.setInt(2, pId);
					log.info("Get survey id: " + pstmtGetFormId.toString());
					ResultSet rs = pstmtGetFormId.executeQuery();
					if(rs.next()) {
						tf.properties.form_id = rs.getInt(1);
					} else {
						throw new Exception("Form not found: " + tf.properties.form_name);
					}
				}
			}

			/*
			 * 3. If a temporary user is to be created then create the user
			 *   Else Get the assignee id, if only the assignee ident is specified
			 */
			String tempUserId = null;
			if(tf.properties.generate_user) {
				tempUserId = GeneralUtilityMethods.createTempUser(
						sd,
						oId,
						tf.properties.email, 
						tf.properties.assignee_name, 
						pId,
						tf);

			} else if(tf.properties.assignee <= 0 && tf.properties.assignee_ident != null && tf.properties.assignee_ident.trim().length() > 0) {

				pstmtGetAssigneeId.setString(1, tf.properties.assignee_ident);
				pstmtGetAssigneeId.setInt(2, pId);
				log.info("Get user id: " + pstmtGetAssigneeId.toString());
				ResultSet rs = pstmtGetAssigneeId.executeQuery();
				if(rs.next()) {
					tf.properties.assignee = rs.getInt(1);
				} else {
					throw new Exception("Assignee not found: " + tf.properties.assignee_ident);
				}
			}

			String targetSurveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, tf.properties.form_id);
			String webformUrl = null;
			if(tf.properties.generate_user) {

				webformUrl = urlPrefix + "/webForm/id/" + tempUserId + 
						"/" + targetSurveyIdent;
			} else {
				webformUrl = urlPrefix + "/webForm/" + targetSurveyIdent;
			}

			/*
			 * 4. Location
			 */
			String location = tf.properties.location;
			if(location == null) {
				location = "POINT(0 0)";
			} else if(location.startsWith("LINESTRING")) {
				log.info("Starts with linestring: " + tf.properties.location.split(" ").length);
				if(location.split(" ").length < 3) {	// Convert to point if there is only one location in the line
					location = location.replaceFirst("LINESTRING", "POINT");
				}
			}	 

			/*
			 * 5. Write the task to the database
			 */
			if(tf.properties.from == null) {
				Calendar cal = Calendar.getInstance();
				cal.add(Calendar.HOUR, 7 * 24);
				tf.properties.from = new Timestamp(cal.getTime().getTime());
			}
			pstmt = getInsertTaskStatement(sd);
			insertTask(
					pstmt,
					pId,
					pName,
					tgId,
					tgName,
					tf.properties.name,
					tf.properties.form_id,
					webformUrl,
					tf.properties.initial_data,
					location,
					tf.properties.update_id,
					tf.properties.address,
					tf.properties.from,
					tf.properties.to,
					tf.properties.location_trigger,
					tf.properties.repeat,
					tf.properties.guidance);

			/*
			 * 6. Assign the user to the new task
			 */
			if(tf.properties.assignee > 0) {

				ResultSet keys = pstmt.getGeneratedKeys();
				if(keys.next()) {
					int taskId = keys.getInt(1);

					pstmtAssign = getInsertAssignmentStatement(sd);
					insertAssignment(pstmtAssign, tf.properties.assignee, "accepted", taskId);

					// Create a notification to alert the new user of the change to the task details
					String userIdent = GeneralUtilityMethods.getUserIdent(sd, tf.properties.assignee);
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
				if(tf.properties.location_trigger != null && tf.properties.location_trigger.trim().length() > 0) {
					pstmtHasLocationTrigger = sd.prepareStatement(sqlHasLocationTrigger);
					pstmtHasLocationTrigger.setInt(1, oId);
					pstmtHasLocationTrigger.setString(2, tf.properties.location_trigger);
					ResultSet rs = pstmtHasLocationTrigger.executeQuery();
					if(rs.next()) {
						int count = rs.getInt(1);
						if(count == 0) {
							pstmtUpdateLocationTrigger = sd.prepareStatement(sqlUpdateLocationTrigger);
							pstmtUpdateLocationTrigger.setInt(1, oId);
							pstmtUpdateLocationTrigger.setString(2,tf.properties.location_trigger);
							pstmtUpdateLocationTrigger.setString(3,tf.properties.name);
							log.info("Adding NFC resource: " + pstmtUpdateLocationTrigger.toString());
							pstmtUpdateLocationTrigger.executeUpdate();
						}
					}
				}
			}

		} finally {
			if(pstmt != null) try {pstmt.close(); } catch(SQLException e) {};
			if(pstmtAssign != null) try {pstmtAssign.close(); } catch(SQLException e) {};
			if(pstmtGetFormId != null) try {pstmtGetFormId.close(); } catch(SQLException e) {};
			if(pstmtGetAssigneeId != null) try {pstmtGetAssigneeId.close(); } catch(SQLException e) {};
			if(pstmtHasLocationTrigger != null) try {pstmtHasLocationTrigger.close(); } catch(SQLException e) {};
			if(pstmtUpdateLocationTrigger != null) try {pstmtUpdateLocationTrigger.close(); } catch(SQLException e) {};
		}

	}

	/*
	 * Apply an action to multiple tasks
	 */
	public void applyBulkAction(Connection sd, int pId, TaskBulkAction action) throws Exception {

		String sqlGetUsers = "select distinct ident from users where temporary = false and id in "
				+ "(select a.assignee from assignments a, tasks t "
				+ "where a.task_id = t.id "
				+ "and t.p_id = ? "
				+ "and a.task_id in ("; 

		String sqlUsers = "delete from users where temporary = true and id in "
				+ "(select a.assignee from assignments a, tasks t "
				+ "where a.task_id = t.id "
				+ "and t.p_id = ? "
				+ "and a.task_id in (";  

		String deleteSql = "delete from tasks where p_id = ? and id in (";
		String assignSql = "update assignments set assignee = ? "
				+ "where task_id in (select task_id from tasks where p_id = ?) "		// Authorisation
				+ "and task_id in (";
		String sqlGetUnassigned = "select id from tasks "
				+ "where id not in (select task_id from assignments) "
				+ "and id in (";
		String sqlCreateAssignments = "insert into assignments (assignee, status, task_id, assigned_date) "
				+ "values(?, 'accepted', ?, now())";
		String sqlDeleteAssignments = "delete from assignments where task_id in (";
		String whereSql = "";


		PreparedStatement pstmt = null;
		PreparedStatement pstmtGetUnassigned = null;
		PreparedStatement pstmtCreateAssignments = null;
		PreparedStatement pstmtDelTempUsers = null;
		PreparedStatement pstmtGetUsers = null;

		try {

			if(action.taskIds.size() == 0) {
				throw new Exception("No tasks");
			} 

			for(int i = 0; i < action.taskIds.size(); i++) {
				if(i > 0) {
					whereSql += ",";
				}
				whereSql += action.taskIds.get(i).toString();
			}
			whereSql += ")";

			// Notify users currently assigned to tasks that are being modified
			MessagingManager mm = new MessagingManager();
			pstmtGetUsers = sd.prepareStatement(sqlGetUsers + whereSql + ")");
			pstmtGetUsers.setInt(1, pId);
			log.info("Notify affected users: " + pstmtGetUsers.toString());
			ResultSet rsNot = pstmtGetUsers.executeQuery();
			while(rsNot.next()) {
				mm.userChange(sd, rsNot.getString(1));
			}


			if(action.action.equals("delete")) {

				// Delete temporary users
				pstmtDelTempUsers = sd.prepareStatement(sqlUsers + whereSql + ")");
				pstmtDelTempUsers.setInt(1, pId);
				log.info("Del temp users created for tasks to be deleted: " + pstmtDelTempUsers.toString());
				pstmtDelTempUsers.executeUpdate();

				pstmt = sd.prepareStatement(deleteSql + whereSql);
				pstmt.setInt(1, pId);
				log.info("Bulk update: " + pstmt.toString());
				pstmt.executeUpdate();

			} else if(action.action.equals("assign")) {

				// Get tasks that have not had an assignment created
				pstmtGetUnassigned = sd.prepareStatement(sqlGetUnassigned + whereSql);
				pstmtCreateAssignments = sd.prepareCall(sqlCreateAssignments);
				log.info("Getting unassigned tasks: " + pstmtGetUnassigned.toString());
				ResultSet rs = pstmtGetUnassigned.executeQuery();
				while (rs.next()) {
					// Create the first assignment for this task
					pstmtCreateAssignments.setInt(1, action.userId);
					pstmtCreateAssignments.setInt(2, rs.getInt(1));
					log.info("Create assignment: " + pstmtCreateAssignments.toString());
					pstmtCreateAssignments.executeUpdate();
				}
				// Update assignments
				if(action.userId >= 0) {
					pstmt = sd.prepareStatement(assignSql + whereSql);
					pstmt.setInt(1,action.userId);
					pstmt.setInt(2, pId);				
					log.info("Bulk update: " + pstmt.toString());
					pstmt.executeUpdate();
				} else {
					pstmt = sd.prepareStatement(sqlDeleteAssignments + whereSql);				
					log.info("Bulk update: " + pstmt.toString());
					pstmt.executeUpdate();
				}

				// Notify the user who has been assigned the tasks
				String userIdent = GeneralUtilityMethods.getUserIdent(sd, action.userId);
				mm.userChange(sd, userIdent);
			}

		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtGetUnassigned != null) try {pstmtGetUnassigned.close(); } catch(SQLException e) {};
			if(pstmtCreateAssignments != null) try {pstmtCreateAssignments.close(); } catch(SQLException e) {};	
			if(pstmtDelTempUsers != null) try {pstmtDelTempUsers.close(); } catch(SQLException e) {};	
			if(pstmtGetUsers != null) try {pstmtGetUsers.close(); } catch(SQLException e) {};	
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

		String sql = "delete from tasks where tg_id = ?"; 
		PreparedStatement pstmt = null;

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

			// Delete the task group
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, tgId);

			log.info("Delete tasks in task group: " + pstmt.toString());
			pstmt.executeUpdate();

		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtUsers != null) try {	pstmtUsers.close(); } catch(SQLException e) {};
			if(pstmtGetUsers != null) try {	pstmtGetUsers.close(); } catch(SQLException e) {};
		}		
	}

	/*
	 * Delete an individual task
	 */
	public void deleteTask(Connection sd, int tId, int pId) throws SQLException {

		String sqlGetUsers = "select distinct ident from users where temporary = false and id in "
				+ "(select a.assignee from assignments a, tasks t where a.task_id = ? "
				+ "and a.task_id = t.id "
				+ "and t.p_id = ?)"; 
		PreparedStatement pstmtGetUsers = null;

		String sqlTempUsers = "delete from users where temporary = true and id in "
				+ "(select a.assignee from assignments a, tasks t where a.task_id = ? "
				+ "and a.task_id = t.id "
				+ "and t.p_id = ?)"; 
		PreparedStatement pstmtTempUsers = null;

		String sql = "delete from tasks where id = ? and p_id = ?"; 
		PreparedStatement pstmt = null;

		try {

			// Delete any temporary users created for this task
			pstmtTempUsers = sd.prepareStatement(sqlTempUsers);
			pstmtTempUsers.setInt(1, tId);
			pstmtTempUsers.setInt(2, pId);

			log.info("Delete temporary user: " + pstmtTempUsers.toString());
			pstmtTempUsers.executeUpdate();

			// Notify users whose task has been deleted
			MessagingManager mm = new MessagingManager();
			pstmtGetUsers = sd.prepareStatement(sqlGetUsers);
			pstmtGetUsers.setInt(1, tId);
			pstmtGetUsers.setInt(2, pId);

			log.info("Get task users: " + pstmtGetUsers.toString());
			ResultSet rs = pstmtGetUsers.executeQuery();
			while (rs.next()) {
				mm.userChange(sd, rs.getString(1));
			}			

			// Delete the task
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, tId);
			pstmt.setInt(2, pId);

			log.info("Delete task: " + pstmt.toString());
			pstmt.executeUpdate();

		} finally {
			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtTempUsers != null) try {	pstmtTempUsers.close(); } catch(SQLException e) {};
			if(pstmtGetUsers != null) try {	pstmtGetUsers.close(); } catch(SQLException e) {};
		}		
	}
	
	/*
	 * Delete tasks that reference a specific updateId
	 */
	public void deleteTaskforUpdateId(Connection sd, int sId, String updateId, String user) throws SQLException {

		String sqlGetUsers = "select distinct ident from users where temporary = false and id in "
				+ "(select a.assignee from assignments a, tasks t where t.update_id = ? "
				+ "and a.task_id = t.id)"; 
		PreparedStatement pstmtGetUsers = null;

		String sqlTempUsers = "delete from users where temporary = true and id in "
				+ "(select a.assignee from assignments a, tasks t where t.update_id = ? "
				+ "and a.task_id = t.id)"; 
		PreparedStatement pstmtTempUsers = null;

		String sql = "delete from tasks where update_id = ?"; 
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

			// Delete the task
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, updateId);
		
			log.info("Delete task: " + pstmt.toString());
			int count = pstmt.executeUpdate();
			if(count > 0) {
				String msg = localisation.getString("lm_del_task_for_update_id");
				msg = msg.replaceFirst("%s1", String.valueOf(count));
				msg = msg.replaceFirst("%s2", updateId);
				lm.writeLog(sd, sId, user, LogManager.DELETE, msg);
			}

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
	 * Get the preparedStatement
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
				+ "guidance) "
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
				+ "?)";		// guidance
		
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
			String guidance) throws SQLException {
		
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
		pstmt.setBoolean(16, repeat);	// repeat
		pstmt.setString(17, guidance);	// guidance

		log.info("Create a new task: " + pstmt.toString());
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
				+ "status, "
				+ "task_id,"
				+ "assigned_date) "
				+ "values (?, (select name from users where id = ?), ?, ?, now())";
		
		return sd.prepareStatement(sql);
	}
	
	/*
	 * Insert an assignment
	 */
	public int insertAssignment(
			PreparedStatement pstmt,
			int assignee,
			String status,
			int task_id) throws SQLException {
		
		pstmt.setInt(1, assignee);
		pstmt.setInt(2,  assignee);
		pstmt.setString(3,  status);			// default the status to accepted for new assignments
		pstmt.setInt(4,  task_id);

		log.info("Create a new assignment: " + pstmt.toString());
		return(pstmt.executeUpdate());
	}
	
	/*
	 * Create all assignments for specific user id or a role
	 */
	public void applyAllAssignments(
			Connection sd,
			PreparedStatement pstmtRoles, 
			PreparedStatement pstmtRoles2,
			PreparedStatement pstmtAssign, 
			int taskId,
			int userId, 
			int roleId,
			int fixedRoleId) throws SQLException {

		String status = "accepted";
		
		if(userId > 0) {		// Assign the user to the new task

			insertAssignment(pstmtAssign, userId, status, taskId);
			
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
		
				insertAssignment(pstmtAssign, rsRoles.getInt(1), status, taskId);
				
				// Notify the user of their new assignment
				String userIdent = GeneralUtilityMethods.getUserIdent(sd, rsRoles.getInt(1));
				MessagingManager mm = new MessagingManager();
				mm.userChange(sd, userIdent);	
			}
			if(count == 0) {
				log.info("No matching users found");
			}
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
}

