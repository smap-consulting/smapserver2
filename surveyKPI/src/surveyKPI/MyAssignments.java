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
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.JsonAuthorisationException;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.SystemException;
import org.smap.sdal.managers.AssignmentsManager;
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.UserLocationManager;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TaskCompletionInfo;
import org.smap.sdal.model.TaskResponse;
import org.smap.sdal.model.TaskResponseAssignment;
import org.smap.sdal.model.TrTask;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.sql.*;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Returns tasks for the user that made the request
 * This entry point should be used only by calls from fieldTask or API calls
 * It will be set up to use basic authentication
 * No checking is done on the use of Ajax for posts
 */
@Path("/myassignments")
public class MyAssignments extends Application {

	Authorise a = new Authorise(null, Authorise.ENUM);

	private static Logger log = Logger.getLogger(Survey.class.getName());

	LogManager lm = new LogManager(); // Application log

	/*
	 * Get assignments for user authenticated with credentials
	 */
	@GET
	@Produces("application/json")

	public Response getTasksCredentials(@Context HttpServletRequest request,
			@QueryParam("noprojects") boolean noProjects, 
			@QueryParam("orgs") boolean getOrgs,
			@QueryParam("linked") boolean getLinkedRefDefns, 
			@QueryParam("manifests") boolean getManifests)
			throws SQLException, ApplicationException {
		AssignmentsManager am = new AssignmentsManager();
		return am.getTasks(request, request.getRemoteUser(), noProjects, getOrgs, 
				getLinkedRefDefns, getManifests, true);
	}

	/*
	 * Get assignments for user authenticated with a key
	 */
	@GET
	@Produces("application/json")
	@Path("/key/{key}")
	public Response getTaskskey(@PathParam("key") String key, @QueryParam("projects") boolean noProjects,
			@QueryParam("orgs") boolean getOrgs, @QueryParam("linked") boolean getLinkedRefDefns,
			@QueryParam("manifests") boolean getManifests, @Context HttpServletRequest request) throws SQLException, ApplicationException {

		log.info("webserviceevent : getTaskskey");
		String connection = "surveyKPI-getaskskey";

		String user = null;
		Connection sd = SDDataSource.getConnection(connection);

		try {
			user = GeneralUtilityMethods.getDynamicUser(sd, key);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			SDDataSource.closeConnection(connection, sd);
		}

		if (user == null) {
			log.info("User not found for key");
			throw new JsonAuthorisationException();
		}
		AssignmentsManager am = new AssignmentsManager();
		return am.getTasks(request, user, noProjects, getOrgs, getLinkedRefDefns, getManifests, false);
	}

	/*
	 * Post assignments for user authenticated with a key
	 */
	@POST
	@Produces("application/json")
	@Path("/key/{key}")
	public Response updateTasksKey(@PathParam("key") String key, @FormParam("assignInput") String assignInput,
			@Context HttpServletRequest request) {

		// Check for Ajax not done as this is a service called from fieldTask
		
		log.info("webserviceevent : updateTasksKey");
		String connection = "surveyKPI-UpdateTasksKey";

		String user = null;
		Connection sd = SDDataSource.getConnection(connection);

		try {
			user = GeneralUtilityMethods.getDynamicUser(sd, key);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			SDDataSource.closeConnection(connection, sd);
		}

		if (user == null) {
			log.info("User not found for key");
			throw new JsonAuthorisationException();
		}

		return updateTasks(request, assignInput, user);
	}

	/*
	 * Update assignments for user authenticated with credentials
	 */
	@POST
	@Produces("application/json")
	public Response updateTasksCredentials(@Context HttpServletRequest request,
			@FormParam("assignInput") String assignInput) {

		log.info("webserviceevent : updateAssignments");
		return updateTasks(request, assignInput, request.getRemoteUser());
	}

	/*
	 * Reject assignments for user authenticated with credentials
	 */
	@POST
	@Path("/update_status")
	@Produces("application/json")
	public Response rejectTaskCredentials(@FormParam("assignment") String assignment,
			@Context HttpServletRequest request) {

		AssignmentsManager am = new AssignmentsManager();
		return am.updateStatusToRejected(request, assignment);

	}

	/*
	 * Update the task assignment
	 */
	public Response updateTasks(@Context HttpServletRequest request, String assignInput, String userName) {

		Response response = null;
		String connectionString = "surveyKPI-MyAssignments - updateTasks";

		Connection sd = SDDataSource.getConnection(connectionString);
		AssignmentsManager am = new AssignmentsManager();

		// Authorisation not required as a user can only update their own assignments

		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm").create();
		TaskResponse tr = gson.fromJson(assignInput, TaskResponse.class);

		// TODO that the status is valid (A different range of status values depending
		// on the role of the user)
		PreparedStatement pstmtSetDeleted = null;
		PreparedStatement pstmtSetUpdatedRejected = null;
		PreparedStatement pstmtSetUpdatedNotRejected = null;
		PreparedStatement pstmtTasks = null;
		PreparedStatement pstmtEvents = null;
		PreparedStatement pstmtUpdateId = null;
		PreparedStatement pstmtUnassignedRejected = null;

		Connection cResults = ResultsDataSource.getConnection(connectionString);
		try {

			if (tr == null) {
				throw new ApplicationException("Task information was not set");
			}
			log.info("Device:" + tr.deviceId + " for user " + userName);

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			CaseManager cm = new CaseManager(localisation);

			pstmtSetDeleted = am.getPreparedStatementSetDeleted(sd);
			pstmtSetUpdatedRejected = am.getPreparedStatementSetUpdatedRejected(sd);
			pstmtSetUpdatedNotRejected = am.getPreparedStatementSetUpdatedNotRejected(sd);
			pstmtEvents = am.getPreparedStatementEvents(sd);

			sd.setAutoCommit(false);
			for (TaskResponseAssignment ta : tr.taskAssignments) {

				if (ta.task != null && ta.task.type != null && ta.task.type.equals("case")) {
					if (ta.assignment.assignment_status != null && ta.assignment.assignment_status.equals("rejected")) {
						String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults,
								ta.task.form_id);
						cm.assignRecord(sd, cResults, localisation, tableName, ta.task.update_id,
								request.getRemoteUser(), "release", null, ta.assignment.task_comment,
								request.getRemoteUser());
					}
				} else if (ta.assignment.assignment_id > 0) {
					log.info("Task Assignment: " + ta.assignment.assignment_status);

					if (ta.task == null) {
						ta.task = new TrTask();
						ta.task.id = GeneralUtilityMethods.getTaskId(sd, ta.assignment.assignment_id);
					}
					am.updateAssignment(sd, cResults, localisation, pstmtSetDeleted, pstmtSetUpdatedRejected,
							pstmtSetUpdatedNotRejected, pstmtEvents, gson, userName, ta.task.id,
							ta.assignment.assignment_id, ta.assignment.assignment_status, ta.assignment.task_comment);

					/*
					 * Set the update id if it is not already set and this is a completed task
					 */
					if (ta.assignment.uuid != null && ta.assignment.assignment_status != null
							&& ta.assignment.assignment_status.equals("submitted")) {

						String sqlUpdateId = "update tasks set update_id = ? " + "where id = ? "
								+ "and update_id is null";
						pstmtUpdateId = sd.prepareStatement(sqlUpdateId);

						pstmtUpdateId.setString(1, ta.assignment.uuid);
						pstmtUpdateId.setInt(2, ta.task.id);
						log.info("+++++++++++++++ Updating task updateId: " + pstmtUpdateId.toString());
						pstmtUpdateId.executeUpdate();
					}

				} else {
					log.info("Error: assignment id is zero");
				}
			}

			int userId = GeneralUtilityMethods.getUserId(sd, userName);

			/*
			 * Record task information for any submitted tasks
			 */
			if (tr.taskCompletionInfo != null) {
				String sqlTasks = "insert into task_completion (" + "u_id, " + "device_id, " + "form_ident, "
						+ "form_version, " + "uuid," + "the_geom," + // keep this
						"completion_time" + ") " + "values(?, ?, ?, ?, ?, ST_GeomFromText(?, 4326), ?)";
				pstmtTasks = sd.prepareStatement(sqlTasks);

				pstmtTasks.setInt(1, userId);
				pstmtTasks.setString(2, tr.deviceId);
				for (TaskCompletionInfo tci : tr.taskCompletionInfo) {

					pstmtTasks.setString(3, tci.ident);
					pstmtTasks.setInt(4, tci.version);
					pstmtTasks.setString(5, tci.uuid);
					pstmtTasks.setString(6, "POINT(" + tci.lon + " " + tci.lat + ")");
					pstmtTasks.setTimestamp(7, new Timestamp(tci.actFinish));

					log.info("Insert task: " + pstmtTasks.toString());
					pstmtTasks.executeUpdate();

				}

			}

			/*
			 * Record user trail information
			 */
			UserLocationManager ulm = new UserLocationManager(localisation, "UTC");
			ulm.recordUserTrail(sd, userId, tr.deviceId, tr.userTrail);
			
			if (!sd.getAutoCommit()) {
				sd.commit();
			}

			response = Response.ok().build();
			log.info("Assignments updated");

		} catch (ApplicationException e) {
			throw new SystemException(e.getMessage());
		} catch (Exception e) {
			response = Response.serverError().build();
			log.log(Level.SEVERE, "Exception", e);
			try {sd.rollback();} catch (Exception ex) {
				log.log(Level.SEVERE, "", ex);
			}
		} finally {

			try {
				if (!sd.getAutoCommit()) {
					sd.setAutoCommit(true);
				}
			} catch (Exception e) {

			}

			try {
				if (pstmtSetDeleted != null) {
					pstmtSetDeleted.close();
				}
			} catch (Exception e) {
			}
			try {
				if (pstmtSetUpdatedRejected != null) {
					pstmtSetUpdatedRejected.close();
				}
			} catch (Exception e) {
			}
			try {
				if (pstmtSetUpdatedNotRejected != null) {
					pstmtSetUpdatedNotRejected.close();
				}
			} catch (Exception e) {
			}
			try {
				if (pstmtTasks != null) {
					pstmtTasks.close();
				}
			} catch (Exception e) {
			}
			try {
				if (pstmtEvents != null) {
					pstmtEvents.close();
				}
			} catch (Exception e) {
			}
			try {
				if (pstmtUpdateId != null) {
					pstmtUpdateId.close();
				}
			} catch (Exception e) {
			}
			try {
				if (pstmtUnassignedRejected != null) {
					pstmtUnassignedRejected.close();
				}
			} catch (Exception e) {
			}

			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}

		return response;
	}

}
