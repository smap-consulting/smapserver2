package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
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
public class RecordEventManager {
	
	private static Logger log =
			 Logger.getLogger(RecordEventManager.class.getName());
	
	public static String CREATED = "created";
	public static String CHANGES = "changes";
	public static String TASK = "task";
	public static String ALERT = "alert";
	public static String ASSIGNED = "assigned";
	public static String NOTIFICATION = "notification";
	
	public static String STATUS_SUCCESS = "success";
	public static String STATUS_NEW = "new";
	
	public RecordEventManager() {
		
	}
	
	/*
	 * Save a change
	 */
	public void writeEvent(
			Connection sd, 
			Connection cResults,
			String event,
			String status,
			String user,
			String tableName, 
			String newInstance, 
			String changes,
			String task,
			String notification,
			String description,
			int sId,						// legacy
			String sIdent,
			int taskId,
			int assignmentId
			) throws SQLException {
		
		String sql = "insert into record_event ("
				+ "key,	"
				+ "table_name, "
				+ "event,"
				+ "status, "
				+ "instanceid,	"
				+ "changes, "
				+ "task, "
				+ "notification, "
				+ "description, "
				+ "success, "
				+ "msg, "
				+ "changed_by, "
				+ "change_survey, "
				+ "change_survey_version, "
				+ "task_id, "
				+ "assignment_id, "
				+ "event_time) "
				+ "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())";
		PreparedStatement pstmt = null;
		
		String sqlSurvey = "select version " 
				+ " from survey " 
				+ " where ident = ?";
		PreparedStatement pstmtSurvey = null;
		
		// Don't use the actual instance id as it will change with every update
		String key = GeneralUtilityMethods.getThread(cResults, tableName, newInstance);
		
		// Set user id
		int uId = GeneralUtilityMethods.getUserId(sd, user);
		
		// Set survey ident and version
		int sVersion = 0;
		if(sIdent == null) {
			sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
		}
		
		pstmtSurvey = sd.prepareStatement(sqlSurvey);
		pstmtSurvey.setString(1, sIdent);
		ResultSet rs = pstmtSurvey.executeQuery();
		if(rs.next()) {
			sVersion = rs.getInt(1);
		}
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, key);
			pstmt.setString(2,  tableName);
			pstmt.setString(3,  event);
			pstmt.setString(4,  status);
			pstmt.setString(5,  newInstance);
			pstmt.setString(6,  changes);
			pstmt.setString(7,  task);
			pstmt.setString(8,  notification);
			pstmt.setString(9,  description);
			pstmt.setBoolean(10,  true);	// success
			pstmt.setString(11,  null);
			pstmt.setInt(12, uId);
			pstmt.setString(13,  sIdent);
			pstmt.setInt(14,  sVersion);
			pstmt.setInt(15,  taskId);
			pstmt.setInt(16,  assignmentId);
			pstmt.executeUpdate();
			
			/*
			 * Alert the user previously assigned to this record and the new user
			 */
			if(!event.equals(RecordEventManager.NOTIFICATION)) {
				MessagingManager mm = new MessagingManager(null);	// Assume no messages will require localisation!
				String assignedUser = GeneralUtilityMethods.getAssignedUser(cResults, tableName, key);
				
				// Notify the currently assigned user
				if(assignedUser != null) {
					mm.userChange(sd, assignedUser);
				}
				// An assigned event - notify the newly assigned user
				if(event.equals(RecordEventManager.ASSIGNED) && user != null && (assignedUser == null || !assignedUser.equals(user))) {
					mm.userChange(sd, user);
				}
			}
			
		} finally {
			if(pstmt != null) try{pstmt.close();}catch(Exception e) {};
			if(pstmtSurvey != null) try{pstmtSurvey.close();}catch(Exception e) {};
		}
	}
	
	/*
	 * Save a change to task status 
	 */
	public void writeTaskStatusEvent(
			Connection sd, 
			Connection cResults,
			int taskId,
			int assignmentId,
			String status,
			String assigned,
			String taskName,
			boolean assign_auto
			) throws SQLException {
		
		String sqlUsingAssignment = "select t.update_id, f.table_name, t.id, t.schedule_at, t.schedule_finish "
				+ "from assignments a, tasks t, form f, survey s "
				+ "where t.survey_ident = s.ident "
				+ "and f.s_id = s.s_id "
				+ "and a.task_id = t.id "
				+ "and f.parentform = 0 "
				+ "and a.id = ?";
		String sqlUsingTask = "select t.update_id, f.table_name, t.id, t.schedule_at, t.schedule_finish "
				+ "from tasks t, form f, survey s "
				+ "where t.survey_ident = s.ident "
				+ "and f.s_id = s.s_id "
				+ "and f.parentform = 0 "
				+ "and t.id = ?";
		
		PreparedStatement pstmt = null;
		
		String sqlGetUsingAssignment = "select task, status from record_event "
				+ "where key = ? "
				+ "and event = 'task' "
				+ "and assignment_id = ?";
		
		String sqlGetUsingTask = "select task, status from record_event "
				+ "where key = ? "
				+ "and event = 'task' "
				+ "and task_id = ?";
		PreparedStatement pstmtGet = null;
		
		String sqlSetUsingAssignment = "update record_event "
				+ "set task = ?, "
				+ "assignment_id = ?, "
				+ "status = ? "
				+ "where key = ? "
				+ "and event = 'task' "
				+ "and assignment_id = ?";
		
		String sqlSetUsingTask = "update record_event "
				+ "set task = ?, "
				+ "assignment_id = ?, "
				+ "status = ? "
				+ "where key = ? "
				+ "and event = 'task' "
				+ "and task_id = ?";
		PreparedStatement pstmtSet = null;
		
		try {
			/*
			 * Get record information from the task
			 */
			
			if(assignmentId > 0) {
				pstmt = sd.prepareStatement(sqlUsingAssignment);
				pstmt.setInt(1, assignmentId);
			} else {
				pstmt = sd.prepareStatement(sqlUsingTask);
				pstmt.setInt(1, taskId);
			}
			log.info("Get task info: " + pstmt.toString());
			
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
				
				String updateId = rs.getString(1);
				String  tableName = rs.getString(2);
				taskId = rs.getInt(3);
				Timestamp scheduleAt = rs.getTimestamp(4);
				Timestamp scheduleFinish = rs.getTimestamp(5);
				
				if(updateId != null) {
					// Get the current Task Item Change and update it
					String key = GeneralUtilityMethods.getThread(cResults, tableName, updateId);
					if(key != null) {
						if(assignmentId > 0) {
							pstmtGet = sd.prepareStatement(sqlGetUsingAssignment);
							pstmtGet.setString(1,  key);
							pstmtGet.setInt(2,  assignmentId);
						} else {
							pstmtGet = sd.prepareStatement(sqlGetUsingTask);
							pstmtGet.setString(1,  key);
							pstmtGet.setInt(2, taskId);
						}
	
						log.info("Set autocommit false");
						sd.setAutoCommit(false);
						ResultSet rs2 = pstmtGet.executeQuery();
						if(rs2.next()) {
							String itemString = rs2.getString(1);
							String oldStatus = rs2.getString(2);
							if(itemString != null) {
								TaskItemChange tic = gson.fromJson(itemString, TaskItemChange.class);
								tic.taskEvents.add(new TaskEventChange(taskName, status, assigned, 
										null,		// comment
										scheduleAt,
										scheduleFinish,
										assign_auto
										));
								
								if(status == null) {
									status = oldStatus;
								}
								// Write the changed event back to the database
								if(assignmentId > 0) {
									pstmtSet = sd.prepareStatement(sqlSetUsingAssignment);
									pstmtSet.setInt(5, assignmentId);
								} else {
									pstmtSet = sd.prepareStatement(sqlSetUsingTask);
									pstmtSet.setInt(5, taskId);
								}
								pstmtSet.setString(1,gson.toJson(tic));
								pstmtSet.setInt(2, assignmentId);
								pstmtSet.setString(3, status);
								pstmtSet.setString(4, key);
								pstmtSet.executeUpdate();
							}	
						}
					}
					
					log.info("Set autocommit true");
					sd.setAutoCommit(true);
				}
			}
				
		} catch (Exception e) {
			try {sd.setAutoCommit(true);} catch(Exception ex) {};
			throw(e);
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
			if(pstmtGet != null) {try{pstmtGet.close();}catch(Exception e) {}}
			if(pstmtSet != null) {try{pstmtSet.close();}catch(Exception e) {}}
		}
	}
	
	/*
	 * Get a list of event changes for a thread
	 */
	public ArrayList<DataItemChangeEvent> getChangeEvents(Connection sd, String tz, String tableName, String key) throws SQLException {
		
		ArrayList<DataItemChangeEvent> events = new ArrayList<DataItemChangeEvent> ();
		
		String sql = "select "
				+ "event, "
				+ "status,"
				+ "changes,"
				+ "task, "
				+ "notification, "
				+ "description, "
				+ "changed_by, "
				+ "change_survey, "
				+ "change_survey_version, "
				+ "to_char(timezone(?, event_time), 'YYYY-MM-DD HH24:MI:SS') as event_time "
				+ "from record_event "
				+ "where table_name = ?"
				+ "and key = ?"
				+ "order by event_time desc";
		PreparedStatement pstmt = null;
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, tz);
			pstmt.setString(2, tableName);
			pstmt.setString(3, key);
			log.info("Get changes: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				DataItemChangeEvent event = new DataItemChangeEvent();
				event.event = rs.getString("event");
				event.userName = GeneralUtilityMethods.getUserName(sd, rs.getInt("changed_by"));
				
				String changes = rs.getString("changes");
				if(changes != null) {
					event.changes = gson.fromJson(changes, new TypeToken<ArrayList<DataItemChange>>() {}.getType());
				}
				String task = rs.getString("task");
				long scheduledFinish = 0;
				if(task != null) {
					event.task = gson.fromJson(task, TaskItemChange.class);
					if(event.task.taskEvents != null) {
						TaskEventChange lastEvent = event.task.taskEvents.get(event.task.taskEvents.size() - 1);
						if(lastEvent != null && lastEvent.schedule_finish != null) {
							scheduledFinish = lastEvent.schedule_finish.getTime();
						}
					}
				}
				event.status = rs.getString("status");
				if(scheduledFinish > 0) {
					if(event.status.equals(TaskManager.STATUS_T_ACCEPTED) && (scheduledFinish < Calendar.getInstance().getTime().getTime())) {
						event.status = TaskManager.STATUS_T_LATE;
					}
				}
				
				String notification = rs.getString("notification");
				if(notification != null) {
					event.notification = gson.fromJson(notification, SubmissionMessage.class);
				}
				
				event.description = rs.getString("description");
				
				String sIdent = rs.getString("change_survey");
				if(sIdent != null) {				
					event.surveyName = GeneralUtilityMethods.getSurveyNameFromIdent(sd, sIdent);
					event.surveyVersion = rs.getInt("change_survey_version");
				}
				
				event.eventTime = rs.getString("event_time");
				event.tz = tz;
				
				events.add(event);
				
			}
		} finally {
			if(pstmt != null) try{pstmt.close();}catch(Exception e) {};
		}
		
		return events;
	}
	
	public void deleteTableEvents(Connection sd, String tableName) throws SQLException {
		
		String sql = "delete from record_event where table_name = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, tableName);
			log.info("Delete record events for a table: " + pstmt.toString());
			pstmt.executeUpdate();
			
		} finally {
			if(pstmt != null) try{pstmt.close();}catch(Exception e) {};
		}
		
	}

}
