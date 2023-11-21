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
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.managers.ExternalFileManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.RecordEventManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.SurveyTableManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.managers.TranslationManager;
import org.smap.sdal.managers.UserLocationManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Case;
import org.smap.sdal.model.CustomUserReference;
import org.smap.sdal.model.GeometryString;
import org.smap.sdal.model.KeyValueTask;
import org.smap.sdal.model.MediaFile;
import org.smap.sdal.model.ManifestValue;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskLocation;
import org.smap.sdal.model.TaskUpdate;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import taskModel.FieldTaskSettings;
import taskModel.FormLocator;
import taskModel.ReferenceSurvey;
import taskModel.TaskCompletionInfo;
import taskModel.TaskResponse;
import taskModel.TaskResponseAssignment;
import taskModel.TrAssignment;
import taskModel.TrTask;

import java.io.File;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Returns tasks for the user that made the request
 */
@Path("/myassignments")
public class MyAssignments extends Application {

	Authorise a = new Authorise(null, Authorise.ENUM);

	private static Logger log =
			Logger.getLogger(Survey.class.getName());
	
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
			@QueryParam("manifests") boolean getManifests
			) throws SQLException {
		log.info("webserviceevent : getTasksCredentials");
		return getTasks(request, request.getRemoteUser(), noProjects, getOrgs, getLinkedRefDefns, getManifests);
	}

	/*
	 * Get assignments for user authenticated with a key
	 */
	@GET
	@Produces("application/json")
	@Path("/key/{key}")
	public Response getTaskskey(
			@PathParam("key") String key,
			@QueryParam("projects") boolean noProjects,
			@QueryParam("orgs") boolean getOrgs,
			@QueryParam("linked") boolean getLinkedRefDefns,
			@QueryParam("manifests") boolean getManifests,
			@Context HttpServletRequest request) throws SQLException {

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
		return getTasks(request, user, noProjects, getOrgs, getLinkedRefDefns, getManifests);
	}

	/*
	 * Post assignments for user authenticated with a key
	 */
	@POST
	@Produces("application/json")
	@Path("/key/{key}")
	public Response updateTasksKey(
			@PathParam("key") String key,
			@FormParam("assignInput") String assignInput,
			@Context HttpServletRequest request) {

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
	public Response updateTasksCredentials(
			@Context HttpServletRequest request, 
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
	public Response rejectTaskCredentials(
			@FormParam("assignment") String assignment,
			@Context HttpServletRequest request) {

		Response response = null;
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm").create();
		TaskUpdate tu = gson.fromJson(assignment, TaskUpdate.class);
		log.info("webserviceevent : update assignment status: " + tu.assignment_id);
		
		String connectionString = "surveyKPI-MyAssignments-reject";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
			
		PreparedStatement pstmtSetDeleted = null;
		PreparedStatement pstmtSetUpdatedRejected = null;
		PreparedStatement pstmtSetUpdatedNotRejected = null;
		PreparedStatement pstmtEvents = null;
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
		
			if(tu.type != null && tu.type.equals("case")) {
				CaseManager cm = new CaseManager(localisation);
				String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, tu.sIdent);
				cm.assignRecord(sd, cResults, localisation, tableName, tu.uuid, request.getRemoteUser(), "release", null, tu.task_comment);
			} else {
				
				pstmtSetDeleted = getPreparedStatementSetDeleted(sd);
				pstmtSetUpdatedRejected = getPreparedStatementSetUpdatedRejected(sd);
				pstmtSetUpdatedNotRejected = getPreparedStatementSetUpdatedNotRejected(sd);
				pstmtEvents = getPreparedStatementEvents(sd);
				
				int taskId = GeneralUtilityMethods.getTaskId(sd, tu.assignment_id);
				updateAssignment(
						sd,
						cResults,
						localisation, 
						pstmtSetDeleted, 
						pstmtSetUpdatedRejected,
						pstmtSetUpdatedNotRejected,
						pstmtEvents,
						gson,
						request.getRemoteUser(),
						taskId,
						tu.assignment_id,
						tu.assignment_status,
						tu.task_comment);
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			response = Response.serverError().build();
		} finally {
			try {if ( pstmtSetDeleted != null ) { pstmtSetDeleted.close(); }} catch (Exception e) {}
			try {if ( pstmtSetUpdatedRejected != null ) { pstmtSetUpdatedRejected.close(); }} catch (Exception e) {}
			try {if ( pstmtSetUpdatedNotRejected != null ) { pstmtSetUpdatedNotRejected.close(); }} catch (Exception e) {}
			try {if ( pstmtEvents != null ) { pstmtEvents.close(); }} catch (Exception e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}
		
		return response;
	}

	/*
	 * Return the list of tasks allocated to the requesting user
	 */
	public Response getTasks(HttpServletRequest request, String userIdent, boolean noProjects, 
			boolean getOrgs, boolean getLinkedRefDefns, boolean getManifests) throws SQLException {

		Response response = null;

		String connectionString = "surveyKPI-getTasks";
		TaskResponse tr = new TaskResponse();
		tr.message = "OK Task retrieved";	// Overwritten if there is an error
		tr.status = "200";
		tr.version = 1;

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, userIdent);
		// End Authorisation

		int uId = GeneralUtilityMethods.getUserId(sd, userIdent);
		
		String host = request.getServerName();
		String protocol = (request.getLocalPort() == 443) ?  "https://"  : "http://";
		
		// Get the coordinates from which this request was made
		String latString = request.getHeader("lat");
		String lonString = request.getHeader("lon");
		Double lat = 0.0;
		Double lon = 0.0;
		
		if(latString != null) {
			try {
				lat = Double.parseDouble(latString);
			} catch (Exception e) {
				log.info("Invalid latitude: " + latString);
			}
		}
		if(lonString != null) {
			try {
				lon = Double.parseDouble(lonString);
			} catch (Exception e) {
				log.info("Invalid longitude: " + lonString);
			}
		}
		
		// Get the device time if set
		String deviceTimeString = request.getHeader("devicetime");
		long deviceTime = 0;
		if(deviceTimeString != null) {
			try {				
				deviceTime = Long.parseLong(deviceTimeString);
				tr.time_difference = deviceTime - (new java.util.Date()).getTime();
			} catch (Exception e) {
				log.info("Invalid device time: " + deviceTimeString);
			}
		}
		
		// Get the deviceid if set
		String deviceid = request.getHeader("deviceid");
		
		// Get the app version of set
		String appVersion = request.getHeader("appversion");
		log.info("Refresh assignments: " + deviceTimeString);
		
		PreparedStatement pstmtGetSettings = null;
		PreparedStatement pstmtGetProjects = null;
		PreparedStatement pstmtGeo = null;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtNumberTasks = null;
		PreparedStatement pstmtDeleteCancelled = null;

		Connection cResults = null;

		int oId = GeneralUtilityMethods.getOrganisationId(sd, userIdent);

		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";
			String basepath = GeneralUtilityMethods.getBasePath(request);
			
			String sqlDeleteCancelled = "update assignments set status = 'deleted', deleted_date = now() where id = ?";
			pstmtDeleteCancelled = sd.prepareStatement(sqlDeleteCancelled);
			String sqlNumberTasks = "select ft_number_tasks from organisation where id = ?";
			pstmtNumberTasks = sd.prepareStatement(sqlNumberTasks);
			pstmtNumberTasks.setInt(1, oId);
			int ft_number_tasks = 20;
			ResultSet rs = pstmtNumberTasks.executeQuery();
			if(rs.next()) {
				ft_number_tasks = rs.getInt(1);
			}
			
			boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());

			cResults = ResultsDataSource.getConnection(connectionString);
			sd.setAutoCommit(true);

			// Get the assignments
			StringBuilder sql1 = new StringBuilder("select "
					+ "t.id as task_id,"
					+ "t.title,"
					+ "t.url,"
					+ "s.ident as form_ident,"
					+ "s.version as form_version,"
					+ "s.p_id as pid,"
					+ "t.update_id,"
					+ "t.initial_data_source,"
					+ "t.schedule_at,"
					+ "t.schedule_finish,"
					+ "t.location_trigger,"
					+ "t.repeat,"
					+ "a.status as assignment_status,"
					+ "a.id as assignment_id, "
					+ "t.address as address, "
					+ "t.guidance as guidance,"
					+ "t.show_dist, "
					+ "ST_AsText(t.geo_point) as geo_point "
					+ "from tasks t, assignments a, users u, survey s, user_project up, project p, task_group tg "
					+ "where t.id = a.task_id "
					+ "and t.tg_id = tg.tg_id "
					+ "and t.survey_ident = s.ident "
					+ "and u.id = up.u_id "
					+ "and s.p_id = up.p_id "
					+ "and s.p_id = p.id "
					+ "and s.deleted = 'false' "
					+ "and s.blocked = 'false' "
					+ "and a.assignee = u.id "
					+ "and (a.status = 'cancelled' or a.status = 'accepted' or (a.status = 'submitted' and t.repeat)) "
					+ "and u.ident = ? "
					+ "and p.o_id = ? ");
			StringBuilder sqlOrder = new StringBuilder("order by t.schedule_at asc");
			
			StringBuilder distanceFilter = new StringBuilder("");
			if(lat != 0.0 || lon != 0.0) {
				distanceFilter.append(" and (tg.dl_dist = 0 or ST_AsText(t.geo_point) = 'POINT(0 0)' or ST_DWithin(t.geo_point, ST_Point(?, ?)::geography, tg.dl_dist)) ");
			}
			
			StringBuilder sql = new StringBuilder("");
			sql.append(sql1).append(distanceFilter).append(sqlOrder);
	
			pstmt = sd.prepareStatement(sql.toString());	
			int paramIndex = 1;
			pstmt.setString(paramIndex++, userIdent);
			pstmt.setInt(paramIndex++, oId);
			if(lat != 0.0 || lon != 0.0) {
				pstmt.setDouble(paramIndex++, lon);
				pstmt.setDouble(paramIndex++, lat);
			}

			log.info("Getting assignments: " + pstmt.toString());
			ResultSet resultSet = pstmt.executeQuery();

			int t_id = 0;
			ArrayList<Integer> cancelledAssignments = new ArrayList<Integer> ();
			while (resultSet.next()  && ft_number_tasks > 0) {

				// Create the list of task assignments if it has not already been created
				if(tr.taskAssignments == null) {
					tr.taskAssignments = new ArrayList<TaskResponseAssignment>();
				}

				// Create the new Task Assignment Objects
				TaskResponseAssignment ta = new TaskResponseAssignment();
				ta.task = new TrTask();
				ta.location = new TaskLocation();
				ta.assignment = new TrAssignment();

				// Populate the new Task Assignment
				t_id = resultSet.getInt("task_id");
				ta.task.id = t_id;
				ta.task.type = "xform";									// Kept for backward compatibility with old versions of fieldTask
				ta.task.title = resultSet.getString("title");
				ta.task.pid = resultSet.getString("pid");
				ta.task.url = resultSet.getString("url");
				ta.task.form_id = resultSet.getString("form_ident");		// Form id is survey ident
				
				String sVersion = resultSet.getString("form_version");
				int version = 0;
				try {
					version = Integer.valueOf(sVersion);
				} catch (Exception e) {
					
				}
				ta.task.form_version = version;
				
				ta.task.update_id = resultSet.getString("update_id");
				ta.task.initial_data_source = resultSet.getString("initial_data_source");
				ta.task.scheduled_at = resultSet.getTimestamp("schedule_at");
				ta.task.scheduled_finish = resultSet.getTimestamp("schedule_finish");
				ta.task.location_trigger = resultSet.getString("location_trigger");
				if(ta.task.location_trigger != null && ta.task.location_trigger.trim().length() == 0) {
					ta.task.location_trigger = null;
				}
				ta.task.repeat = resultSet.getBoolean("repeat");
				ta.task.address = resultSet.getString("address");
				ta.task.address = addKeyValuePair(ta.task.address, "guidance", resultSet.getString("guidance"));	// Address stored as json key value pairs
				ta.task.show_dist = resultSet.getInt("show_dist");
				ta.assignment.assignment_id = resultSet.getInt("assignment_id");
				ta.assignment.assignment_status = resultSet.getString("assignment_status");

				String geoString = resultSet.getString("geo_point");
				if(geoString != null) {
					int startIdx = geoString.lastIndexOf('(');
					int endIdx = geoString.indexOf(')');
					if(startIdx > 0 && endIdx > 0) {
						ta.location.geometry = new GeometryString();
						String geoString2 = geoString.substring(startIdx + 1, endIdx);
						ta.location.geometry.type = "POINT";
						ta.location.geometry.coordinates = geoString2.split(",");
					}
				}

				// Add the new task assignment to the list of task assignments
				tr.taskAssignments.add(ta);
				
				// Limit the number of non cancelled tasks that can be downloaded
				if(!ta.assignment.assignment_status.equals("cancelled")) {
					ft_number_tasks--;
				} else {
					cancelledAssignments.add(ta.assignment.assignment_id);
				}
			}
			
			/*
			 * If there is still room for tasks then add new tasks where the task group allows auto selection
			 */
			log.info("ft_number_tasks: " + ft_number_tasks);
			if(ft_number_tasks > 0) {
				TaskManager tm = new TaskManager(localisation, tz);
				TaskListGeoJson unassigned = tm.getUnassignedTasks(
						sd, 
						oId,			
						uId,
						ft_number_tasks,		// Maximum number of tasks to return
						userIdent
						);
				
				for(TaskFeature task : unassigned.features) {
					
					// Create the list of task assignments if it has not already been created
					if(tr.taskAssignments == null) {
						tr.taskAssignments = new ArrayList<TaskResponseAssignment>();
					}
					
					// Create the new Task Assignment Objects
					TaskResponseAssignment ta = new TaskResponseAssignment();
					ta.task = new TrTask();
					ta.location = new TaskLocation();
					ta.assignment = new TrAssignment();
					
					// Populate the new Task Assignment
					ta.task.id = task.properties.id;
					ta.task.type = "xform";	
					ta.task.title = task.properties.name;					
					ta.task.pid = String.valueOf(task.properties.p_id);				
					ta.task.form_id = task.properties.survey_ident;		// Form id is survey ident
					
					int version = 0;
					String sVersion = task.properties.form_version;
					try {
						version = Integer.valueOf(sVersion);
					} catch (Exception e) {
						
					}
					ta.task.form_version = version;
					
					ta.task.update_id = task.properties.update_id;				
					ta.task.initial_data_source = task.properties.initial_data_source;				
					ta.task.scheduled_at = task.properties.from;	
					ta.task.scheduled_finish = task.properties.to;	
					ta.task.location_trigger = task.properties.location_trigger;
					if(ta.task.location_trigger != null && ta.task.location_trigger.trim().length() == 0) {
						ta.task.location_trigger = null;
					}				
					ta.task.repeat = task.properties.repeat;				
					ta.task.address = task.properties.address;		
					ta.task.address = addKeyValuePair(ta.task.address, "guidance", task.properties.guidance);	// Address stored as json key value pairs				
					ta.task.show_dist = task.properties.show_dist;		
					ta.assignment.assignment_id = task.properties.a_id;
					ta.assignment.assignment_status = task.properties.status;

					if(task.properties.lat != 0.0 && task.properties.lon != 0.0) {				
						ta.location.geometry = new GeometryString();
						ta.location.geometry.type = "POINT";
						ta.location.geometry.coordinates = new String [2];
						ta.location.geometry.coordinates[0] = String.valueOf(task.properties.lon);
						ta.location.geometry.coordinates[1] = String.valueOf(task.properties.lat);
					}
					tr.taskAssignments.add(ta);
				}
			}
			
			/*
			 * Set all tasks to deleted that have been acknowledged as cancelled
			 */
			for(int assignmentId : cancelledAssignments) {
				pstmtDeleteCancelled.setInt(1, assignmentId);
				pstmtDeleteCancelled.executeUpdate();
			}

			/*
			 * Get the complete list of forms accessible by this user
			 */
			SurveyManager sm = new SurveyManager(localisation, "UTC");
			ArrayList<org.smap.sdal.model.Survey> surveys = sm.getSurveys(sd,
					userIdent,
					false, 
					false, 
					0,
					superUser,
					false,	// only group
					false,	// Group Details
					true,	// Only data survey
					false,	// get links
					null);	// UrL prefix
			
			SurveyTableManager stm = new SurveyTableManager(sd, localisation);
			TranslationManager translationMgr = new TranslationManager();
			ExternalFileManager efm = new ExternalFileManager(localisation);
			tr.forms = new ArrayList<FormLocator> ();
			if(getLinkedRefDefns) {
				tr.refSurveys = new ArrayList<> ();
			}
			
			
			for (Survey survey : surveys) {
				
				List<ManifestValue> manifestList = null;
				List<MediaFile> mediaFiles = null;
				
				boolean hasManifest = false;
				
				/*
				 * For each form that has a manifest that links to another form
				 *  generate the new CSV files if the linked data has changed
				 *  If we have been asked to return the manifest then return that too
				 */
				if(getManifests) {
					// Get all manifests
					manifestList = translationMgr.
							getManifestBySurvey(sd, request.getRemoteUser(), survey.id, basepath, survey.ident);
					hasManifest = manifestList.size() > 0;
				} else {
					// Get linked manifests only
					manifestList = translationMgr.
							getSurveyManifests(sd, survey.id, survey.ident, null, 0, true);	
					hasManifest = translationMgr.hasManifest(sd, request.getRemoteUser(), survey.id);
				}
				
				if(hasManifest && manifestList.size() > 0) {
					for( ManifestValue m : manifestList) {
	
						String dirPath;
						String logicalFilePath = null;
						if(m.type.equals("linked")) {
							
							/*
							 * Generate the CSV file if data has changed
							 */
							log.info("Linked file:" + m.fileName);
		
							/*
							 * The file is unique per survey unless there are roles on the survey or the reference data from the survey
							 *  is restricted to the user who submitted the record. 
							 */
							CustomUserReference cur = GeneralUtilityMethods.hasCustomUserReferenceData(sd, m.linkedSurveyIdent);
							dirPath = efm.getLinkedDirPath(basepath, survey.ident, userIdent, cur.needCustomFile());
							logicalFilePath = efm.getLinkedLogicalFilePath(dirPath, m.fileName);
							
							// Make sure the destination exists
							File dir = new File(dirPath);
							dir.mkdirs();
		
							efm.createLinkedFile(sd, cResults, oId, survey.id, m.fileName ,  
									logicalFilePath, 
									userIdent, 
									tz, 
									basepath,
									cur);
							
							/*
							 * Get pulldata definitions so that local data on the device can be searched
							 */
							if(getLinkedRefDefns) {	
								String refSurveyIdent = getReferenceSurveyIdent(m.fileName);
								if(refSurveyIdent != null) {
									ReferenceSurvey ref = new ReferenceSurvey();
									ref.survey = survey.ident;
									ref.referenceSurvey = refSurveyIdent;
									ref.tableName = m.fileName;
									ref.columns = stm.getQuestionNames(survey.id, m.fileName);
									tr.refSurveys.add(ref);
								}
							}		
						}
						
						/*
						 * If the manifests have been requested and the file exists then add it to the manifests to be returned
						 */
						if(getManifests) {
							String physicalFilePath = null;
							if(m.type.equals("linked")) {
								physicalFilePath = efm.getLinkedPhysicalFilePath(sd, logicalFilePath) + ".csv";
								m.fileName += ".csv";
								log.info("%%%%%: Referencing: " + physicalFilePath);
							} else {
								physicalFilePath = m.filePath;
							}
							
							if(physicalFilePath != null) {
								File f = new File(physicalFilePath);
								
								if(f.exists()) {
									if(mediaFiles == null) {
										mediaFiles = new ArrayList<>();
									}
									mediaFiles.add(new MediaFile(m.fileName, 
											GeneralUtilityMethods.getMd5(physicalFilePath), protocol + host + m.url));
								}
							}
						}
					}
				}

				FormLocator fl = new FormLocator();
				
				fl.ident = survey.ident;
				fl.version = survey.version;
				fl.name = survey.displayName;
				fl.project = survey.projectName;
				fl.pid = survey.p_id;
				fl.tasks_only = survey.getHideOnDevice();
				fl.read_only = survey.getReadOnlySurvey();
				fl.search_local_data = survey.getSearchLocalData();
				fl.hasManifest = hasManifest;
				fl.dirty = hasManifest;			// obsolete but used by FT

				if(getManifests) {
					fl.mediaFiles = mediaFiles;
				}
				tr.forms.add(fl);
				
				/*
				 * Add any cases assigned to this user
				 */
				CaseManager cm = new CaseManager(localisation);
				ArrayList<Case> cases = cm.getCases(sd, cResults, survey.ident, survey.displayName, survey.groupSurveyIdent, userIdent, survey.id);
				if(cases.size() > 0) {
					if(tr.taskAssignments == null) {
						tr.taskAssignments = new ArrayList<TaskResponseAssignment>();
					}
						
					for(Case c : cases) {

						// Convert to cases
						TaskResponseAssignment ta = new TaskResponseAssignment();
						ta.task = new TrTask();
						ta.location = new TaskLocation();
						ta.assignment = new TrAssignment();

						ta.task.form_id = survey.ident;
						ta.task.pid = String.valueOf(survey.p_id);
						ta.task.update_id = c.instanceid;
						ta.task.type = "case";
						ta.task.initial_data_source = "survey";
						ta.task.title = c.title;
						ta.assignment.assignment_status = TaskManager.STATUS_T_ACCEPTED;
						ta.assignment.assignment_id = 0;
						tr.taskAssignments.add(ta);
					}
				}
			}

			/*
			 * Get the settings for the phone
			 */
			tr.settings = new FieldTaskSettings();
			sql = new StringBuilder("select "
					+ "o.ft_delete,"
					+ "o.ft_send_location, "
					+ "o.ft_sync_incomplete, "
					+ "o.ft_odk_style_menus, "
					+ "o.ft_specify_instancename, "
					+ "o.ft_mark_finalized, "
					+ "o.ft_prevent_disable_track, "
					+ "o.ft_enable_geofence, "
					+ "o.ft_admin_menu, "
					+ "o.ft_server_menu, "
					+ "o.ft_meta_menu, "
					+ "o.ft_exit_track_menu, "
					+ "o.ft_bg_stop_menu, "
					+ "o.ft_review_final, "
					+ "o.ft_send,"
					+ "o.ft_image_size,"
					+ "o.ft_backward_navigation,"
					+ "o.ft_navigation,"
					+ "o.ft_pw_policy,"
					+ "o.ft_high_res_video,"
					+ "o.ft_guidance,"
					+ "o.ft_input_method,"
					+ "o.ft_im_ri,"
					+ "o.ft_im_acc "
					+ "from organisation o, users u "
					+ "where u.o_id = o.id "
					+ "and u.ident = ?");

			pstmtGetSettings = sd.prepareStatement(sql.toString());	
			pstmtGetSettings.setString(1, userIdent);
			log.info("Getting settings: " + pstmtGetSettings.toString());
			resultSet = pstmtGetSettings.executeQuery();

			if(resultSet.next()) {
				tr.settings.ft_delete = resultSet.getString("ft_delete");
				tr.settings.ft_delete_submitted = Organisation.get_ft_delete_submitted(tr.settings.ft_delete);		// deprecated
				tr.settings.ft_send_location = resultSet.getString("ft_send_location");
				if(tr.settings.ft_send_location == null) {
					tr.settings.ft_send_location = "off";
				}
				tr.settings.ft_sync_incomplete = resultSet.getBoolean("ft_sync_incomplete");
				tr.settings.ft_odk_style_menus = resultSet.getBoolean("ft_odk_style_menus");
				tr.settings.ft_specify_instancename = resultSet.getBoolean("ft_specify_instancename");
				tr.settings.ft_mark_finalized = resultSet.getBoolean("ft_mark_finalized");
				tr.settings.ft_prevent_disable_track = resultSet.getBoolean("ft_prevent_disable_track");
				tr.settings.setFtEnableGeofence(resultSet.getBoolean("ft_enable_geofence"));
				tr.settings.ft_admin_menu = resultSet.getBoolean("ft_admin_menu");
				tr.settings.ft_server_menu = resultSet.getBoolean("ft_server_menu");
				tr.settings.ft_meta_menu = resultSet.getBoolean("ft_meta_menu");
				tr.settings.ft_exit_track_menu = resultSet.getBoolean("ft_exit_track_menu");
				tr.settings.ft_bg_stop_menu = resultSet.getBoolean("ft_bg_stop_menu");
				tr.settings.ft_review_final = resultSet.getBoolean("ft_review_final");
				tr.settings.ft_send = resultSet.getString("ft_send");
				tr.settings.ft_send_wifi = Organisation.get_ft_send_wifi(tr.settings.ft_send);
				tr.settings.ft_send_wifi_cell = Organisation.get_ft_send_wifi_cell(tr.settings.ft_send);
				tr.settings.ft_image_size = resultSet.getString("ft_image_size");
				tr.settings.ft_backward_navigation = resultSet.getString("ft_backward_navigation");
				tr.settings.ft_navigation = resultSet.getString("ft_navigation");
				tr.settings.ft_pw_policy = resultSet.getInt("ft_pw_policy");
				tr.settings.ft_high_res_video = resultSet.getString("ft_high_res_video");
				tr.settings.ft_guidance = resultSet.getString("ft_guidance");
				tr.settings.ft_location_trigger = true;
				tr.settings.ft_input_method = resultSet.getString("ft_input_method");
				tr.settings.ft_im_ri = resultSet.getInt("ft_im_ri");
				tr.settings.ft_im_acc = resultSet.getInt("ft_im_acc");
				tr.settings.ft_location_trigger = true;
			}

			/*
			 * Get the projects
			 */
			if(!noProjects) {		// Double negative - however this preserves default behaviour which is to return projects
				tr.projects = new ArrayList<Project> ();
				sql = new StringBuilder("select p.id, p.name, p.description " +
						" from users u, user_project up, project p " + 
						"where u.id = up.u_id " +
						"and p.id = up.p_id " +
						"and u.ident = ? " +
						"and p.o_id = ? " +
						"order by name ASC;");	
	
				pstmtGetProjects = sd.prepareStatement(sql.toString());	
				pstmtGetProjects.setString(1, userIdent);
				pstmtGetProjects.setInt(2, oId);
	
				log.info("Getting projects: " + pstmtGetProjects.toString());
				resultSet = pstmtGetProjects.executeQuery();
	
				while(resultSet.next()) {
					Project p = new Project();
					p.id = resultSet.getInt(1);
					p.name = resultSet.getString(2);
					p.desc = resultSet.getString(3);
					tr.projects.add(p);
				}
			}
			
			/*
			 * Get the organisations
			 */
			if(getOrgs) {
				tr.current_org = GeneralUtilityMethods.getOrganisationName(sd, request.getRemoteUser());
				UserManager um = new UserManager(localisation);
				ArrayList<Organisation> orgs = new ArrayList<>();
				um.getUserOrganisations(sd, orgs, null, uId);
				tr.orgs = new HashSet<String> ();
				for(Organisation o : orgs) {
					tr.orgs.add(o.name);
				}
				tr.orgs.add(tr.current_org);
			}	

			/*
			 * Log the request
			 */
			UserLocationManager ulm = new UserLocationManager(localisation, tz);
			ulm.recordRefresh(sd, oId, userIdent, 
					lat, lon, deviceTime, request.getServerName(), deviceid, appVersion, true);
			
			/*
			 * Return the response
			 */
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm").create();
			String resp = gson.toJson(tr);
			response = Response.ok(resp).build();

		} catch (Exception e) {
			tr.message = "Error: Message=" + e.getMessage();
			tr.status = "400";
			log.log(Level.SEVERE,"", e);
			response = Response.serverError().build();
		} finally {
			try {if (pstmtGetSettings != null) {pstmtGetSettings.close();} } catch (Exception e) {}
			try {if (pstmtGetProjects != null) {pstmtGetProjects.close();} } catch (Exception e) {}
			try {if (pstmtGeo != null) {pstmtGeo.close();} } catch (Exception e) {}
			try {if (pstmt != null) {pstmt.close();} } catch (Exception e) {}
			try {if (pstmtNumberTasks != null) {pstmtNumberTasks.close();} } catch (Exception e) {}
			try {if (pstmtDeleteCancelled != null) {pstmtDeleteCancelled.close();} } catch (Exception e) {}

			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}

		return response;
	}

	/*
	 * Add a key value pair to an array of key value pairs stored as json
	 */
	String addKeyValuePair(String jIn, String name, String value) {

		
		String jOut = null;
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm").create();
		Type type = new TypeToken<ArrayList<KeyValueTask>>(){}.getType();

		ArrayList<KeyValueTask> kvArray = null;

		// 1. Get the current array
		if(jIn != null && jIn.trim().length() > 0 && !jIn.equals("[ ]")) {
			if(!jIn.trim().startsWith("[")) {
				jIn = "[" + jIn + "]";
			}
			try {
				kvArray = new Gson().fromJson(jIn, type);
			} catch (Exception e) {
				log.log(Level.SEVERE, jIn, e);
				kvArray = new ArrayList<KeyValueTask> ();
			}
		} else {
			kvArray = new ArrayList<KeyValueTask> ();
		}
		if(name != null && value != null && !value.equals("[ ]")) {
			// 2. Add the new kv pair
			if(value != null && value.trim().length() > 0 ) {
				KeyValueTask newKV = new KeyValueTask(name, value);
				kvArray.add(newKV);
			}
		}

		// 3. Return the updated list
		jOut = gson.toJson(kvArray);
		return jOut;
	}

	/*
	 * Update the task assignment
	 */
	public Response updateTasks(@Context HttpServletRequest request, 
			String assignInput,
			String userName) { 
		
		Response response = null;
		String connectionString = "surveyKPI-MyAssignments - updateTasks";

		Connection sd = SDDataSource.getConnection(connectionString);

		// Authorisation not required as a user can only update their own assignments

		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm").create();
		TaskResponse tr = gson.fromJson(assignInput, TaskResponse.class);

		// TODO that the status is valid (A different range of status values depending on the role of the user)
		PreparedStatement pstmtSetDeleted = null;
		PreparedStatement pstmtSetUpdatedRejected = null;	
		PreparedStatement pstmtSetUpdatedNotRejected = null;
		PreparedStatement pstmtTasks = null;		
		PreparedStatement pstmtEvents = null;
		PreparedStatement pstmtUpdateId = null;
		PreparedStatement pstmtUnassignedRejected = null;
		
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		try {	

			if(tr == null) {
				throw new ApplicationException("Task information was not set");
			}		
			log.info("Device:" + tr.deviceId + " for user " + userName);
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			CaseManager cm = new CaseManager(localisation);
			
			pstmtSetDeleted = getPreparedStatementSetDeleted(sd);
			pstmtSetUpdatedRejected = getPreparedStatementSetUpdatedRejected(sd);
			pstmtSetUpdatedNotRejected = getPreparedStatementSetUpdatedNotRejected(sd);
			pstmtEvents = getPreparedStatementEvents(sd);
			
			sd.setAutoCommit(false);
			for(TaskResponseAssignment ta : tr.taskAssignments) {
				
				if(ta.task != null && ta.task.type != null && ta.task.type.equals("case")) {
					if(ta.assignment.assignment_status != null && ta.assignment.assignment_status.equals("rejected")) {
						String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, ta.task.form_id);
						cm.assignRecord(sd, cResults, localisation, tableName, ta.task.update_id, request.getRemoteUser(), "release", null, ta.assignment.task_comment);
					}	
				} else if(ta.assignment.assignment_id > 0) {
					log.info("Task Assignment: " + ta.assignment.assignment_status);

					if(ta.task == null) {
						ta.task = new TrTask();
						ta.task.id = GeneralUtilityMethods.getTaskId(sd, ta.assignment.assignment_id);
					}
					updateAssignment(
							sd,
							cResults,
							localisation, 
							pstmtSetDeleted, 
							pstmtSetUpdatedRejected,
							pstmtSetUpdatedNotRejected,
							pstmtEvents,
							gson,
							userName,
							ta.task.id,
							ta.assignment.assignment_id,
							ta.assignment.assignment_status,
							ta.assignment.task_comment);
					
					/*
					 * Set the update id if it is not already set and this is a completed task
					 */
					if(ta.assignment.uuid != null
							&& ta.assignment.assignment_status != null 
							&& ta.assignment.assignment_status.equals("submitted")) {
						
						String sqlUpdateId = "update tasks set update_id = ? "
								+ "where id = ? "
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
			if(tr.taskCompletionInfo != null) {
				String sqlTasks = "insert into task_completion (" +
						"u_id, " +
						"device_id, " +
						"form_ident, " +
						"form_version, " +
						"uuid,"	+
						"the_geom," +		// keep this
						"completion_time" +
						") " +
						"values(?, ?, ?, ?, ?, ST_GeomFromText(?, 4326), ?)";
				pstmtTasks = sd.prepareStatement(sqlTasks);
				
				pstmtTasks.setInt(1, userId);
				pstmtTasks.setString(2, tr.deviceId);
				for(TaskCompletionInfo tci : tr.taskCompletionInfo) {

					pstmtTasks.setString(3, tci.ident);
					pstmtTasks.setInt(4, tci.version);
					pstmtTasks.setString(5, tci.uuid);
					pstmtTasks.setString(6, "POINT(" + tci.lon + " " + tci.lat + ")");
					pstmtTasks.setTimestamp(7, new Timestamp(tci.actFinish));

					log.info("Insert task: " + pstmtTasks.toString());
					pstmtTasks.executeUpdate();
									
				}

				/*
				 * Record user trail information
				 */
				UserLocationManager ulm = new UserLocationManager(localisation, "UTC");
				ulm.recordUserTrail(sd, userId, tr.deviceId, tr.userTrail);

			}

			if(!sd.getAutoCommit()) {
				sd.commit();
			}
			
			response = Response.ok().build();
			log.info("Assignments updated");	

		} catch (ApplicationException e) {		
			throw new SystemException(e.getMessage());
		} catch (Exception e) {		
			response = Response.serverError().build();
			log.log(Level.SEVERE,"Exception", e);
			try { sd.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}
		} finally {

			try {
				if(!sd.getAutoCommit()) {
					sd.setAutoCommit(true);
				}
			} catch(Exception e) {
				
			}
			
			try {if ( pstmtSetDeleted != null ) { pstmtSetDeleted.close(); }} catch (Exception e) {}
			try {if ( pstmtSetUpdatedRejected != null ) { pstmtSetUpdatedRejected.close(); }} catch (Exception e) {}
			try {if ( pstmtSetUpdatedNotRejected != null ) { pstmtSetUpdatedNotRejected.close(); }} catch (Exception e) {}
			try {if ( pstmtTasks != null ) { pstmtTasks.close(); }} catch (Exception e) {}
			try {if ( pstmtEvents != null ) { pstmtEvents.close(); }} catch (Exception e) {}
			try {if ( pstmtUpdateId != null ) { pstmtUpdateId.close(); }} catch (Exception e) {}
			try {if ( pstmtUnassignedRejected != null ) { pstmtUnassignedRejected.close(); }} catch (Exception e) {}

			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}

		return response;
	}
	
	// Get a prepared statement to set the status of an assignment to deleted
	private PreparedStatement getPreparedStatementSetDeleted(Connection sd) throws SQLException {

		String sql = "update assignments a set status = 'deleted', deleted_date = now() "
				+ "where a.id = ? "
				+ "and a.assignee in (select id from users u "
				+ "where u.ident = ?)";
		PreparedStatement pstmt = sd.prepareStatement(sql);
		return pstmt;
	}
	
	// Get a prepared statement to update the status of an assignment
	private PreparedStatement getPreparedStatementSetUpdatedRejected(Connection sd) throws SQLException {

		/*
		 * The assignment status can be set to rejected if the task is assigned to the user
		 * and the task is currently accepted
		 */
		String sql = "update assignments a "
				+ "set status = ?, "
				+ "comment = ?,"
				+ "assignee = ?,"
				+ "assignee_name = (select name from users where id = ?) "
				+ "where a.id = ? "
				+ "and a.assignee = ? "
				+ "and status = 'accepted' ";
		PreparedStatement pstmt = sd.prepareStatement(sql);
		return pstmt;
	}
	
	// Get a prepared statement to update the status of an assignment to anything oth than rejected
	private PreparedStatement getPreparedStatementSetUpdatedNotRejected(Connection sd) throws SQLException {

		/*
		 * The assignment status can be updated by a user if it is an auto allocate task and the current status is new
		 * or the task is assigned to the user
		 */
		String sql = "update assignments a "
				+ "set status = ?, "
				+ "comment = ?,"
				+ "assignee = ?,"
				+ "assignee_name = (select name from users where id = ?) "
				+ "where a.id = ? "
				+ "and (a.assignee < 0 and ((a.status = 'new' or a.status = 'submitted') and task_id in (select id from tasks where id = task_id and assign_auto)) "
				+ "or a.assignee = ?)";
		PreparedStatement pstmt = sd.prepareStatement(sql);
		return pstmt;
	}
	
	// Get a prepared statement to update the record events table
	private PreparedStatement getPreparedStatementEvents(Connection sd) throws SQLException {

		String sql = "select t.survey_ident, f.table_name, t.update_id, t.title, t.assign_auto from tasks t, form f, survey s "
				+ "where t.survey_ident = s.ident "
				+ "and f.s_id = s.s_id "
				+ "and t.id = ? ";
		PreparedStatement pstmt = sd.prepareStatement(sql);
		return pstmt;
	}
	
	/*
	 * Update the status of an assignment
	 */
	private void updateAssignment(
			Connection sd,
			Connection cResults,
			ResourceBundle localisation,
			PreparedStatement pstmtSetDeleted, 
			PreparedStatement pstmtSetUpdatedRejected,
			PreparedStatement pstmtSetUpdatedNotRejected,
			PreparedStatement pstmtEvents,
			Gson gson,
			String userName,
			int taskId,
			int assignmentId,
			String status,
			String comment) throws SQLException {
		
		RecordEventManager rem = new RecordEventManager();
		TaskManager tm = new TaskManager(localisation, "UTC");
		
		/*
		 * If the updated status = "cancelled" then this is an acknowledgment of the status set on the server
		 *   hence update the server status to "deleted"
		 */
		if(status.equals(TaskManager.STATUS_T_CANCELLED)) {
			pstmtSetDeleted.setInt(1, assignmentId);
			pstmtSetDeleted.setString(2, userName);
			log.info("Assignment:" + assignmentId + " acknowledge cancel - update assignments to deleted: " + pstmtSetDeleted.toString());
			pstmtSetDeleted.executeUpdate();
		} else {

			int uId = GeneralUtilityMethods.getUserId(sd, userName);
			
			// Apply update making sure the assignment was made to the updating user
			if(status.equals(TaskManager.STATUS_T_REJECTED)) {
				pstmtSetUpdatedRejected.setString(1, status);
				pstmtSetUpdatedRejected.setString(2, comment);
				pstmtSetUpdatedRejected.setInt(3, uId);
				pstmtSetUpdatedRejected.setInt(4, uId);
				pstmtSetUpdatedRejected.setInt(5, assignmentId);		// To get name
				pstmtSetUpdatedRejected.setInt(6, uId);
				log.info("update assignments rejected: " + pstmtSetUpdatedRejected.toString());
				pstmtSetUpdatedRejected.executeUpdate();
				
				// Potentially rejection of an unassigned  task 
				// Record rejection just for this user so the task is not re-downloaded
				updateTaskRejected(sd, assignmentId, userName);
				
			} else {
				pstmtSetUpdatedNotRejected.setString(1, status);
				pstmtSetUpdatedNotRejected.setString(2, comment);
				pstmtSetUpdatedNotRejected.setInt(3, uId);
				pstmtSetUpdatedNotRejected.setInt(4, uId);
				pstmtSetUpdatedNotRejected.setInt(5, assignmentId);		// To get name
				pstmtSetUpdatedNotRejected.setInt(6, uId);
				log.info("update assignments excluding rejected: " + pstmtSetUpdatedNotRejected.toString());
				pstmtSetUpdatedNotRejected.executeUpdate();
			}
			
			if(status.equals(TaskManager.STATUS_T_SUBMITTED)) {
				// Cancel other assignments if complete_all is not set for the task
				tm.cancelOtherAssignments(sd, cResults, assignmentId);
			}
		}
		if(status.equals(TaskManager.STATUS_T_REJECTED) && comment != null && comment.length() > 0) {
			// Get the oId here this should be a pretty rare event
			int oId = GeneralUtilityMethods.getOrganisationId(sd, userName);
			lm.writeLogOrganisation(sd, oId, userName, LogManager.TASK, 
					assignmentId + ": " + comment, 0);
		}
		
		/*
		 * Record the task status to the record event
		 */
		pstmtEvents.setInt(1, taskId);
		ResultSet rsEvents = pstmtEvents.executeQuery();
		if(rsEvents.next()) {
			String sIdent = rsEvents.getString(1);
			String tableName = rsEvents.getString(2);
			String updateId = rsEvents.getString(3);
			String taskName = rsEvents.getString(4);
			boolean assign_auto = rsEvents.getBoolean(5);
			if(updateId != null && sIdent != null && tableName != null) {
				rem.writeTaskStatusEvent(
						sd, 
						cResults,
						taskId,
						assignmentId,
						status,
						null,			// Assigned not changed
						taskName,
						assign_auto);
			}
		}
	}
	
	/*
	 * If a user has rejected a self allocating task then remember this so it is not sent to them again
	 */
	private void updateTaskRejected(Connection sd, int assignmentId, 
			String userName) throws SQLException {
		
		String sql = "select status from assignments where id = ?";
		PreparedStatement pstmt = null;
		
		String sqlUnassignedRejected = "insert into "
				+ "task_rejected(a_id, ident, rejected_at) "
				+ "values(?, ?, now()) ";
		PreparedStatement pstmtUnassignedRejected = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1,assignmentId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String status = rs.getString(1);
				if(status != null && status.equals("new")) {
					
					// Must have been a self allocate task
					pstmtUnassignedRejected = sd.prepareStatement(sqlUnassignedRejected);			
					pstmtUnassignedRejected.setInt(1, assignmentId);
					pstmtUnassignedRejected.setString(2, userName);
					try {
						log.info("Adding to unassigned rejected: " + pstmtUnassignedRejected.toString());
						pstmtUnassignedRejected.executeUpdate();
					} catch(Exception e) {
						// Ignore errors as if this has already been rejected we don't really care
						log.info("Error: Could not record rejection of unassigned task: " + e.getMessage());
					}
				}
			}		
			
		} finally {
			try {if ( pstmt != null ) { pstmt.close(); }} catch (Exception e) {}
			try {if ( pstmtUnassignedRejected != null ) { pstmtUnassignedRejected.close(); }} catch (Exception e) {}
		}
	}
	
	/*
	 * Return the ident of a reference survey if it is a standard linked survey
	 * Return null if it the reference is to chart data or the pd format
	 */
	String getReferenceSurveyIdent(String filename) {
		String ident = null;
		String linked = "linked_";
		String chart = "chart_";
		String pdLinked = "linked_s_pd";
		if(filename != null && !filename.startsWith(pdLinked)) {
			if(filename.startsWith(linked)) {
				ident = filename.substring(linked.length());
			} else if(filename.startsWith(chart)) {
				int idx1 = filename.indexOf('_');
				int idx2 = filename.indexOf('_', idx1 + 1);
				idx2 = filename.indexOf('_', idx2 + 1);
				if(idx2 > 0) {
					ident = filename.substring(idx1 + 1, idx2);
				} else {
					ident = filename.substring(idx1 + 1);
				}
			}
		}
		return ident;
	}
}



