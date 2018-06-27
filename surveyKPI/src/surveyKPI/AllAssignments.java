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
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.smap.model.SurveyTemplate;
import org.smap.model.TableManager;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.NotFoundException;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.model.AssignFromSurvey;
import org.smap.sdal.model.Assignment;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.TaskAddressSettings;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import model.FormDesc;
import taskModel.TaskAddress;
import utilities.ExchangeManager;
import utilities.QuestionInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/*
 * Used by an administrator or analyst to view task status and make updates
 */
@Path("/assignments")
public class AllAssignments extends Application {

	Authorise a = null;;

	private static Logger log =
			Logger.getLogger(Survey.class.getName());

	LogManager lm = new LogManager();		// Application log

	public AllAssignments() {

		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);		
	}

	/*
	 * Add a task for every survey
	 * Add a task for the array of locations passed in the input parameters
	 */
	@POST
	@Path("/addSurvey/{projectId}")
	public Response addSurvey(@Context HttpServletRequest request, 
			@PathParam("projectId") int projectId,
			@FormParam("settings") String settings) { 

		String urlprefix = request.getScheme() + "://" + request.getServerName() + "/";		

		Response response = null;
		ArrayList<TaskAddress> addressArray = null;
		String projectName = null;


		log.info("++++++++++++++++++++++++++++++++++++++ Assignment:" + settings);
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		AssignFromSurvey as = gson.fromJson(settings, AssignFromSurvey.class);

		System.out.println("Emails: " + as.emails + " Email question: " + as.assign_data);
		String userName = request.getRemoteUser();
		int sId = as.source_survey_id;								// Source survey id (optional)

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-AllAssignments");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidProject(sd, request.getRemoteUser(), projectId);
		if(sId > 0) {
			a.isValidSurvey(sd, userName, sId, false, superUser);	// Validate that the user can access this survey
		}
		// End Authorisation

		if(sId > 0) {
			lm.writeLog(sd, sId, request.getRemoteUser(), "create tasks", "Create tasks from survey data");
		}

		Connection cResults = null; 
		PreparedStatement pstmt = null;
		PreparedStatement pstmtCheckGeom = null;
		PreparedStatement pstmtTaskGroup = null;
		PreparedStatement pstmtGetSurveyIdent = null;
		PreparedStatement pstmtUniqueTg = null;

		int taskGroupId = -1;
		try {
			cResults = ResultsDataSource.getConnection("surveyKPI-AllAssignments");
			log.info("Set autocommit sd false");

			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			projectName = GeneralUtilityMethods.getProjectName(sd, projectId);
			SurveyManager sm = new SurveyManager(localisation);
			org.smap.sdal.model.Survey survey = null;
			String basePath = GeneralUtilityMethods.getBasePath(request);
			survey = sm.getById(sd, cResults, request.getRemoteUser(), sId, true, basePath, 
					null, false, false, false, false, false, "real", false, false, superUser, "geojson");	
			
			sd.setAutoCommit(false);
			
			/*
			 * Create the task group if an existing task group was not specified
			 */
			int oId = GeneralUtilityMethods.getOrganisationId(sd, userName, sId);
			ResultSet rsKeys = null;
			if(as.task_group_id <= 0) {

				/*
				 * Check that a task group of this name does not already exist
				 * This would be better implemented as a constraint on the database but existing customers probably have task
				 *  groups with duplicate names
				 */
				String checkUniqeTg = "select count(*) from task_group where name = ? and p_id = ?;";
				pstmtUniqueTg = sd.prepareStatement(checkUniqeTg);
				pstmtUniqueTg.setString(1, as.task_group_name);
				pstmtUniqueTg.setInt(2, projectId);
				log.info("Check uniqueness of task group name in project: " + pstmtUniqueTg.toString());
				ResultSet rs = pstmtUniqueTg.executeQuery();

				if(rs.next()) {
					if(rs.getInt(1) > 0) {
						throw new Exception("Task Group Name " + as.task_group_name + " already Exists");
					}
				}

				String addressParams = gson.toJson(as.address_columns); 	
				String tgSql = "insert into task_group ( "
						+ "name, "
						+ "p_id, "
						+ "address_params,"
						+ "rule,"
						+ "source_s_id,"
						+ "target_s_id) "
						+ "values (?, ?, ?, ?, ?, ?);";

				pstmtTaskGroup = sd.prepareStatement(tgSql, Statement.RETURN_GENERATED_KEYS);
				pstmtTaskGroup.setString(1, as.task_group_name);
				pstmtTaskGroup.setInt(2, projectId);
				pstmtTaskGroup.setString(3, addressParams);
				pstmtTaskGroup.setString(4, settings);
				pstmtTaskGroup.setInt(5, as.source_survey_id);
				pstmtTaskGroup.setInt(6, as.target_survey_id);
				log.info("Insert into task group: " + pstmtTaskGroup.toString());
				pstmtTaskGroup.execute();

				sd.commit();		// Success as TG is created, even if there are no existing tasks ready to go this is good

				rsKeys = pstmtTaskGroup.getGeneratedKeys();
				if(rsKeys.next()) {
					taskGroupId = rsKeys.getInt(1);
				}
			} else {
				taskGroupId = as.task_group_id;
			}

			/*
			 * Set the task email details
			 */
			TaskManager tm = new TaskManager(localisation);
			tm.updateEmailDetails(sd, projectId, taskGroupId, as.emailDetails);
			
			/*
			 * Create the tasks unless no tasks have been specified
			 */
			if(as.target_survey_id > 0 && as.add_current) {
				String sql = null;
				ResultSet resultSet = null;

				String checkGeomSQL = "select count(*) from information_schema.columns where table_name = ? and column_name = 'the_geom'";
				pstmtCheckGeom = cResults.prepareStatement(checkGeomSQL);

				String getSurveyIdentSQL = "select ident from survey where s_id = ?;";
				pstmtGetSurveyIdent = sd.prepareStatement(getSurveyIdentSQL);

				String hostname = request.getServerName();

				pstmtGetSurveyIdent.setInt(1, as.target_survey_id);
				resultSet = pstmtGetSurveyIdent.executeQuery();
				String instanceId = null;

				if(sId != -1) {
					
					/*
					 * Get Forms and row counts in this survey
					 */
					sql = "select distinct f.table_name, f.parentform from form f " +
							"where f.s_id = ? " + 
							"order by f.table_name;";		

					pstmt = sd.prepareStatement(sql);	 
					pstmt.setInt(1, sId);

					log.info("Get forms: " + pstmt.toString());
					resultSet = pstmt.executeQuery();

					/*
					 * Get all the source records
					 */
					while (resultSet.next()) {
						
						String tableName2 = null;
						String tableName = resultSet.getString(1);
						String p_id = resultSet.getString(2);
						if(p_id == null || p_id.equals("0")) {	// The top level form
							
							/*
							 * Check the filters
							 * Advanced filter takes precedence
							 * If that is not set then check simple filter
							 */
							QuestionInfo filterQuestion = null;
							String filterSql = null;
							if(as.filter != null && as.filter.advanced != null && as.filter.advanced.length() > 0) {
								log.info("+++++ Using advanced filter: " + as.filter.advanced);
								
								StringBuffer filterQuery = new StringBuffer(tableName);
								filterQuery.append(".instanceid in ");
								filterQuery.append(GeneralUtilityMethods.getFilterCheck(sd, 
										localisation, survey, as.filter.advanced));
								filterSql = filterQuery.toString();
								
								log.info("Query clause: " + filterSql);
								
							} else if(as.filter != null && as.filter.qId > 0) {
								String fValue = null;
								String fValue2 = null;
								filterQuestion = new QuestionInfo(localisation, sId, as.filter.qId, sd, 
										false, as.filter.lang, urlprefix, oId);
								log.info("Filter question type: " + as.filter.qType);
								if(as.filter.qType != null) {
									if(as.filter.qType.startsWith("select")) {
										fValue = as.filter.oValue;
									} else if(as.filter.qType.equals("int")) {
										fValue = String.valueOf(as.filter.qInteger);
									} else if(as.filter.qType.equals("date")  || as.filter.qType.equals("dateTime")) {
										Timestamp startDate = new Timestamp(as.filter.qStartDate);
										Timestamp endDate = new Timestamp(as.filter.qEndDate);

										fValue = startDate.toString();
										fValue2 = endDate.toString();
									} else {
										fValue = as.filter.qText;
									}
								}

								filterSql = filterQuestion.getFilterExpression(fValue, fValue2);		
								log.info("filter: " + filterSql);
							}
							
							// Check to see if we need to assign the task based on retrieved data
							String assignSql = null;
							if(as.assign_data != null && as.assign_data.trim().length() > 0) {
								SqlFrag frag = new SqlFrag();
								frag.addSqlFragment(as.assign_data, false, localisation);
								assignSql = frag.sql.toString();
							}
							
							// Check to see if this form has geometry columns
							boolean hasGeom = false;
							pstmtCheckGeom.setString(1, tableName);
							log.info("Check for geometry coulumn: " + pstmtCheckGeom.toString());
							ResultSet resultSetGeom = pstmtCheckGeom.executeQuery();
							if(resultSetGeom.next()) {
								if(resultSetGeom.getInt(1) > 0) {
									hasGeom = true;
								}
							}

							// Get the primary key, location and address columns from this top level table
							StringBuffer getTaskSql = new StringBuffer("");
							StringBuffer getTaskSqlWhere = new StringBuffer("");
							StringBuffer getTaskSqlEnd = new StringBuffer("");
							boolean hasInstanceName = GeneralUtilityMethods.hasColumn(cResults, tableName, "instancename");

							if(hasGeom) {
								log.info("Has geometry");
								getTaskSql.append("select ").append(tableName)
										.append(".prikey, ST_AsText(").append(tableName).append(".the_geom) as the_geom,")
										.append(tableName).append(".instanceid");
								
								if(hasInstanceName) {
									getTaskSql.append(", ").append(tableName).append(".instancename");
								}
								
								getTaskSqlWhere.append(" from ").append(tableName).append(" where ")
										.append(tableName).append("._bad = 'false'");	

							} else {
								log.info("No geom found");
								// Get a subform that has geometry

								PreparedStatement pstmt2 = sd.prepareStatement(sql);	 
								pstmt2.setInt(1, sId);

								log.info("Get subform with geometry: " + pstmt2.toString());
								ResultSet resultSet2 = pstmt2.executeQuery();

								while (resultSet2.next()) {
									String aTable = resultSet2.getString(1);
									pstmtCheckGeom.setString(1, aTable);
									log.info("Check geom: " + pstmtCheckGeom.toString());
									resultSetGeom = pstmtCheckGeom.executeQuery();
									if(resultSetGeom.next()) {
										if(resultSetGeom.getInt(1) > 0) {
											hasGeom = true;
											tableName2 = aTable;
										}
									}
								}
								pstmt2.close();
								resultSet2.close();
								getTaskSql.append("select ").append(tableName) 
										.append(".prikey, ST_AsText(ST_MakeLine(").append(tableName2)
										.append(".the_geom)) as the_geom, ").append(tableName).append(".instanceid");
								
								if(hasInstanceName) {
									getTaskSql.append(", ").append(tableName).append(".instancename");
								}

								getTaskSqlWhere.append(" from ").append(tableName).append(" left outer join ")
										.append(tableName2).append(" on ").append(tableName).append(".prikey = ")
										.append(tableName2).append(".parkey ")
										.append(" where ").append(tableName).append("._bad = 'false'");	
								
								getTaskSqlEnd.append("group by ").append(tableName).append(".prikey ");
							}

							// Finally if we still haven't found a geometry column then set all locations to 0, 0
							if(!hasGeom) {
								log.info("No geometry columns found");
								
								getTaskSql = new StringBuffer("");
								getTaskSqlWhere = new StringBuffer("");
								getTaskSqlEnd = new StringBuffer("");

								getTaskSql.append("select ").append(tableName).append(".prikey, 'POINT(0 0)' as the_geom, ")
											.append(tableName).append(".instanceid");
								
								if(hasInstanceName) {
									getTaskSql.append(", ").append(tableName).append(".instancename");
								}
								getTaskSqlWhere.append(" from ").append(tableName).append(" where ").append(tableName)
										.append("._bad = 'false'");	

							}

							if(assignSql != null) {
								getTaskSql.append(",").append(assignSql).append(" as _assign_key");
							}

							// Add address columns
							if(as.address_columns != null) {
								for(int i = 0; i < as.address_columns.size(); i++) {
									TaskAddressSettings add = as.address_columns.get(i);
									if(add.selected) {
										if(GeneralUtilityMethods.hasColumn(cResults, tableName, add.name)) {
											getTaskSql.append(",").append(tableName).append(".").append(add.name);
										} else {
											add.selected = false;
										}
										
									}
								}
							}
							
							// Add start date column
							if(as.taskStart != -1) {
								String name = null;
								if(as.taskStart > 0) {
									Question q = GeneralUtilityMethods.getQuestion(sd, as.taskStart);
									name = q.name;
								} else {
									MetaItem mi = GeneralUtilityMethods.getPreloadDetails(sd, sId, as.taskStart);
									name = mi.columnName;
								}
								getTaskSql.append(",").append(tableName).append(".").append(name).append(" as taskstart");
							}
	
							getTaskSql.append(getTaskSqlWhere);
							if(filterSql != null && filterSql.trim().length() > 0) {
								getTaskSql.append(" and ").append(filterSql);
							}
							getTaskSql.append(getTaskSqlEnd);

							if(pstmt != null) try {pstmt.close();} catch(Exception e) {};
							pstmt = cResults.prepareStatement(getTaskSql.toString());	
							
							
							log.info("SQL Get Tasks: ----------------------- " + pstmt.toString());
							if(resultSet != null) try {resultSet.close();} catch(Exception e) {};
							resultSet = pstmt.executeQuery();
							while (resultSet.next()) {
								
								// Get the task data from each survey record
								TaskManager.TaskInstanceData tid = tm.new TaskInstanceData();
								tid.prikey = resultSet.getInt("prikey");
								
								// Add location trigger
								tid.locationTrigger = null;		// Not currently set from existing data
								
								// Add dynamic assignment based on data
								if(assignSql != null) {
									tid.ident = resultSet.getString("_assign_key");
								}
								
								// instanceId (writeTask)
								if(as.update_results) {
									instanceId = resultSet.getString("instanceid");
								}

								// location (tid)
								if(hasGeom) {
									tid.location = resultSet.getString("the_geom");
								} 
								if(tid.location == null) {
									tid.location = "POINT(0 0)";
								} else if(tid.location.startsWith("LINESTRING")) {
									log.info("Starts with linestring: " + tid.location.split(" ").length);
									if(tid.location.split(" ").length < 3) {	// Convert to point if there is only one location in the line
										tid.location = tid.location.replaceFirst("LINESTRING", "POINT");
									}
								}	 

								// instanceName (tid)
								if(hasInstanceName) {
									tid.instanceName = resultSet.getString("instancename");
								}
								if(tid.instanceName == null || tid.instanceName.trim().length() == 0) {
									tid.instanceName = as.project_name + " : " + as.survey_name + " : " + resultSet.getString(1);
								} 
								
								// Address (tid)
								if(as.address_columns != null) {

									addressArray = new ArrayList<TaskAddress> ();
									for(int i = 0; i < as.address_columns.size(); i++) {
										TaskAddressSettings add = as.address_columns.get(i);
										if(add.selected) {
											TaskAddress ta = new TaskAddress();
											ta.name = add.name;
											if(add.isMedia) {
												ta.value = urlprefix + resultSet.getString(add.name);
											} else {
												ta.value = resultSet.getString(add.name);
											}
											addressArray.add(ta);
										}
									}
									gson = new GsonBuilder().disableHtmlEscaping().create();
									tid.address = gson.toJson(addressArray); 
								}
								
								// Start time (tid)
								Timestamp initial = null;
								if(as.taskStart != -1) {	
									initial = resultSet.getTimestamp("taskstart");
								}
								tid.taskStart = tm.getTaskStartTime(as, initial);	
								
								// Write the task to the database
								tm.writeTaskCreatedFromSurveyResults(
										sd, 
										cResults,
										as, 
										hostname, 
										taskGroupId, 
										as.task_group_name, 
										projectId, 
										projectName, 
										as.source_survey_id, 
										as.target_survey_id, 
										tid, 
										instanceId,
										false,
										request.getRemoteUser()); 
								
								/*
								// Insert the task
								int count = tm.insertTask(
										pstmtInsert,
										projectId,
										projectName,
										taskGroupId,
										as.task_group_name,
										title,
										as.target_survey_id,
										target_survey_url,
										initial_data_url,
										location,
										instanceId,
										addressString,
										taskStart,
										taskFinish,
										locationTrigger,
										false,
										null);
			
								if(count != 1) {
									log.info("Error: Failed to insert task");
								} else {
									int userId = as.user_id;
									int roleId = as.role_id;
									int fixedRoleId = as.fixed_role_id;
									if(assignSql != null) {
										String ident = resultSet.getString("_assign_key");
										if(as.user_id == -2) {
											userId = GeneralUtilityMethods.getUserIdOrgCheck(sd, ident, oId);   // Its a user ident
										} else {
											roleId = GeneralUtilityMethods.getRoleId(sd, ident, oId);   // Its a role name
										}
									}
									
									rsKeys = pstmtInsert.getGeneratedKeys();
									if(rsKeys.next()) {
										int taskId = rsKeys.getInt(1);
										tm.applyAllAssignments(sd, pstmtRoles, pstmtRoles2, pstmtAssign, taskId,userId, roleId, 
												fixedRoleId,
												as.emails);
								
									}
									if(rsKeys != null) try{ rsKeys.close(); } catch(SQLException e) {};
								}
								*/
							}

							break;
						} else {
							log.info("parent is:" + p_id + ":");
						}
					}
				}
				
				// Create a notification for the updated user
				if(as.user_id > 0) {
					String userIdent = GeneralUtilityMethods.getUserIdent(sd, as.user_id);
					MessagingManager mm = new MessagingManager();
					mm.userChange(sd, userIdent);
				}
			}

			log.info("Returning task group id:" + taskGroupId);
			response = Response.ok().entity("{\"tg_id\": " + taskGroupId + "}").build();

		} catch (Exception e) {
			log.info("Error: " + e.getMessage());
			if(e.getMessage() != null && e.getMessage().contains("\"the_geom\" does not exist")) {
				String msg = "The survey results do not have coordinates " + as.source_survey_name;
				response = Response.status(Status.NO_CONTENT).entity(msg).build();
			} else if(e.getMessage() != null && e.getMessage().contains("does not exist")) {
				response = Response.ok("{\"tg_id\": " + taskGroupId + "}").build();	// No problem
			} else {
				response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
				log.log(Level.SEVERE,"", e);
			}	

			try { sd.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}

		} finally {

			if(pstmt != null) try {	pstmt.close(); } catch(SQLException e) {};
			if(pstmtTaskGroup != null) try {	pstmtTaskGroup.close(); } catch(SQLException e) {};
			if(pstmtGetSurveyIdent != null) try {	pstmtGetSurveyIdent.close(); } catch(SQLException e) {};
			if(pstmtUniqueTg != null) try {	pstmtUniqueTg.close(); } catch(SQLException e) {};

			SDDataSource.closeConnection("surveyKPI-AllAssignments", sd);
			ResultsDataSource.closeConnection("surveyKPI-AllAssignments", cResults);

		}

		return response;
	}
	
	/*
	 * Update a task group
	 */
	@POST
	@Path("/updatetaskgroup/{projectId}/{tgId}")
	public Response updateTaskGroup(@Context HttpServletRequest request, 
			@PathParam("projectId") int projectId,
			@PathParam("tgId") int tgId,
			@FormParam("settings") String settings) { 

		Response response = null;

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		AssignFromSurvey as = gson.fromJson(settings, AssignFromSurvey.class);

		System.out.println("Emails: " + as.emails + " Email question: " + as.assign_data);
		String userName = request.getRemoteUser();
		int sId = as.source_survey_id;								// Source survey id (optional)

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-AllAssignments");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidProject(sd, request.getRemoteUser(), projectId);
		a.isValidTaskGroup(sd, request.getRemoteUser(), tgId, false);
		if(sId > 0) {
			a.isValidSurvey(sd, userName, sId, false, superUser);	// Validate that the user can access this survey
		}
		// End Authorisation
		
		PreparedStatement pstmtTaskGroup = null;

		try {
			log.info("Set autocommit sd false");

			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			/*
			 * Update the task group
			 */
			if(tgId > 0) {

				String addressParams = gson.toJson(as.address_columns); 	
				String tgSql = "update task_group set "
						+ "name = ?, "
						+ "p_id = ?, "
						+ "address_params = ?,"
						+ "rule = ?,"
						+ "source_s_id = ?,"
						+ "target_s_id = ? "
						+ "where tg_id = ?";

				pstmtTaskGroup = sd.prepareStatement(tgSql);
				pstmtTaskGroup.setString(1, as.task_group_name);
				pstmtTaskGroup.setInt(2, projectId);
				pstmtTaskGroup.setString(3, addressParams);
				pstmtTaskGroup.setString(4, settings);
				pstmtTaskGroup.setInt(5, as.source_survey_id);
				pstmtTaskGroup.setInt(6, as.target_survey_id);
				pstmtTaskGroup.setInt(7,  tgId);
				log.info("Update task group: " + pstmtTaskGroup.toString());
				pstmtTaskGroup.execute();

			} 

			TaskManager tm = new TaskManager(localisation);
			tm.updateEmailDetails(sd, projectId, tgId, as.emailDetails);
			
			response = Response.ok().entity("{\"tg_id\": " + tgId + "}").build();

		} catch (Exception e) {
			log.info("Error: " + e.getMessage());
			
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
			log.log(Level.SEVERE,"", e);

		} finally {

			if(pstmtTaskGroup != null) try {	pstmtTaskGroup.close(); } catch(SQLException e) {};
		
			SDDataSource.closeConnection("surveyKPI-AllAssignments", sd);
		}

		return response;
	}

	/*
	 * Update the task assignment
	 */
	@POST
	public Response updateAssignmentStatus(@Context HttpServletRequest request, 
			@FormParam("settings") String settings) { 

		Response response = null;

		log.info("Assignment:" + settings);
		Type type = new TypeToken<ArrayList<Assignment>>(){}.getType();		
		ArrayList<Assignment> aArray = new Gson().fromJson(settings, type);	

		// Authorisation - Access
		Connection connectionSD = SDDataSource.getConnection("surveyKPI-AllAssignments");
		a.isAuthorised(connectionSD, request.getRemoteUser());
		for(int i = 0; i < aArray.size(); i++) {

			Assignment ass = aArray.get(i);

			if(ass.assignment_id == 0) {	// New assignment
				a.isValidTask(connectionSD, request.getRemoteUser(), ass.task_id);
			} else {	// update existing assignment
				a.isValidAssignment(connectionSD, request.getRemoteUser(), ass.assignment_id);
			}

		}
		// End Authorisation

		PreparedStatement pstmtInsert = null;
		PreparedStatement pstmtUpdate = null;
		PreparedStatement pstmtDelete = null;
		String insertSQL = "insert into assignments (assignee, status, task_id) values (?, ?, ?);";
		String updateSQL = "update assignments set " +
				"assignee = ?," +
				"status = ? " +
				"where id = ?;";
		String deleteSQL = "delete from assignments where id = ?;";

		try {
			pstmtInsert = connectionSD.prepareStatement(insertSQL);
			pstmtUpdate = connectionSD.prepareStatement(updateSQL);
			pstmtDelete = connectionSD.prepareStatement(deleteSQL);
			log.info("Set autocommit sd false");
			connectionSD.setAutoCommit(false);

			for(int i = 0; i < aArray.size(); i++) {

				Assignment a = aArray.get(i);

				if(a.assignment_id == 0) {	// New assignment
					pstmtInsert.setInt(1,a.user.id);
					pstmtInsert.setString(2, a.assignment_status);
					pstmtInsert.setInt(3, a.task_id);
					log.info("Add new assignment: " + pstmtInsert.toString());
					pstmtInsert.executeUpdate();
				} else if(a.user.id >= 0) {	// update existing assignment
					pstmtUpdate.setInt(1,a.user.id);
					pstmtUpdate.setString(2, a.assignment_status);
					pstmtUpdate.setInt(3, a.assignment_id);
					log.info("Update existing assignment: " + pstmtUpdate.toString());
					pstmtUpdate.executeUpdate();
				} else {		// delete the assignment
					pstmtDelete.setInt(1, a.assignment_id);
					log.info("Delete existing assignment: " + pstmtDelete.toString());
					pstmtDelete.executeUpdate();
				}

			}
			connectionSD.commit();

		} catch (Exception e) {
			response = Response.serverError().build();
			log.log(Level.SEVERE,"", e);

			try { connectionSD.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}

		} finally {
			try {if (pstmtUpdate != null) {pstmtUpdate.close();}} catch (SQLException e) {}
			try {if (pstmtInsert != null) {pstmtInsert.close();}} catch (SQLException e) {}
			try {if (pstmtDelete != null) {pstmtDelete.close();}} catch (SQLException e) {}

			SDDataSource.closeConnection("surveyKPI-AllAssignments", connectionSD);
		}

		return response;
	}



	/*
	 * Load tasks, that is survey results, from:
	 *   1) a CSV file
	 *   2) an XLSX file
	 *   3) a ZIP file containing a CSV file and images
	 *   4) a ZIP file containing an XLSX file and images
	 */
	@POST
	@Path("/load")
	public Response loadResultsFromFile(@Context HttpServletRequest request) { 

		Response response = null;

		log.info("Load results from file");

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-AllAssignments-LoadTasks From File");
		a.isAuthorised(sd, request.getRemoteUser());
		// End role based authorisation - Check access to the requested survey once the survey id has been extracted

		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();	
		fileItemFactory.setSizeThreshold(20*1024*1024); // 20 MB TODO handle this with exception and redirect to an error page
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);

		// SQL to get a column name from the survey
		String sqlGetCol = "select q_id, qname, column_name, qtype "
				+ "from question "
				+ "where f_id = ? "
				+ "and lower(qname) = ? "
				+ "and source is not null "
				+ "and not soft_deleted";
		PreparedStatement pstmtGetCol = null;
		
		// Alternate SQL for data downloaded from google sheets - This will have all underscores stripped out
		String sqlGetColGS = "select q_id, qname, column_name, qtype "
				+ "from question "
				+ "where f_id = ? "
				+ "and replace(lower(qname), '_','') = ? "
				+ "and source is not null "
				+ "and not soft_deleted";
		PreparedStatement pstmtGetColGS = null;

		// SQL to get choices for a select question
		String sqlGetChoices = "select o.ovalue, o.column_name from option o, question q where q.q_id = ? and o.l_id = q.l_id";
		PreparedStatement pstmtGetChoices = null;

		PreparedStatement pstmtDeleteExisting = null;
		
		// SQL to clear entries in linked_forms that controls csv regeneration
		String sqlDelLinks = "delete from linked_forms where linked_s_id = ? ";
		PreparedStatement pstmtDelLinks = null;

		String uploadedFileName = null;
		String fileName = null;
		String filePath = null;
		File savedFile = null;									// The uploaded file
		ArrayList<File> dataFiles = new ArrayList<File> ();		// Uploaded data files - There may be multiple of these in a zip file
		HashMap<String, String> formFile = new HashMap<String, String> ();	// Mapping between form and the file that contains the data to populate it
		String contentType = null;
		int sId = 0;
		String sIdent = null;		// Survey Ident
		String sName = null;			// Survey Name
		ArrayList<MetaItem> preloads = null;
		boolean clear_existing = false;
		HashMap<String, File> mediaFiles = new HashMap<String, File> ();
		HashMap<String, File> formFileMap = null;
		ArrayList<String> responseMsg = new ArrayList<String> ();
		int recordsWritten = 0;

		Connection results = ResultsDataSource.getConnection("surveyKPI-AllAssignments-LoadTasks From File");
		boolean superUser = false;
		try {

			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			// Get the base path
			String basePath = GeneralUtilityMethods.getBasePath(request);

			// Get the items from the multi part mime
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();
			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();

				if(item.isFormField()) {
					log.info("Form field:" + item.getFieldName() + " - " + item.getString());

					if(item.getFieldName().equals("survey")) {
						sId = Integer.parseInt(item.getString());
						try {
							superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
						} catch (Exception e) {
						}
						a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
						a.canLoadTasks(sd, sId);

						sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
						sName = GeneralUtilityMethods.getSurveyName(sd, sId);
						preloads = GeneralUtilityMethods.getPreloads(sd, sId);
					} else if(item.getFieldName().equals("clear_existing")) {
						clear_existing = true;
					}


				} else if(!item.isFormField()) {
					// Handle Uploaded file
					log.info("Field Name = "+item.getFieldName()+
							", File Name = "+item.getName()+
							", Content type = "+item.getContentType()+
							", File Size = "+item.getSize());

					uploadedFileName = item.getName();

					if(item.getSize() > 0) {
						contentType = item.getContentType();

						String ext = "";
						if(contentType.contains("zip")) {
							ext = ".zip";
						} else if(contentType.contains("csv")) {
							ext = ".csv";
						} else {
							ext = ".xlsx";
						}
						fileName = String.valueOf(UUID.randomUUID()) + ext;

						filePath = basePath + "/temp/" + fileName;
						savedFile = new File(filePath);
						item.write(savedFile);
					}					
				}

			}
			log.info("Content Type: " + contentType);
			if(contentType == null) {
				throw new Exception("Missing file");
			}

			/*
			 * Get the forms for this survey 
			 */
			ExchangeManager xm = new ExchangeManager(localisation);
			ArrayList <FormDesc> formList = xm.getFormList(sd, sId);		

			pstmtGetCol = sd.prepareStatement(sqlGetCol);  			// Prepare the statement to get the column names in the survey that are to be updated
			pstmtGetColGS = sd.prepareStatement(sqlGetColGS); 
			pstmtGetChoices = sd.prepareStatement(sqlGetChoices);  // Prepare the statement to get select choices

			// If this is a zip file extract the contents and set the path to the expanded data file that should be inside
			// Refer to http://www.mkyong.com/java/how-to-decompress-files-from-a-zip-file/
			if(savedFile.getName().endsWith(".zip")) {
				String zipFolderPath = savedFile.getAbsolutePath() + ".dir";
				File zipFolder = new File(zipFolderPath);
				if(!zipFolder.exists()) {
					zipFolder.mkdir();
				}
				ZipInputStream zis = new ZipInputStream(new FileInputStream(savedFile));
				ZipEntry ze = null;
				byte[] buffer = new byte[1024];
				while((ze = zis.getNextEntry()) != null) {
					String zFileName = ze.getName();
					if(!zFileName.startsWith("__MAC")) {	// Files added by macintosh zip utility

						log.info("File in zip: " + ze.getName());
						File zFile = new File(zipFolderPath + File.separator + zFileName);

						new File(zFile.getParent()).mkdirs();	// Make sure path is complete 

						if(ze.isDirectory()) {
							zFile.mkdir();
						} else {
							if((zFileName.endsWith(".csv") || zFileName.endsWith(".xlsx")) && !zFileName.startsWith("~$")) {
								// Data file
								dataFiles.add(zFile);
							} else {
								// Media File. Save the filename and File for processing with each record of data
								// Remove the path from the filename - every file in the zip file must have a unique name
								int idx = zFileName.lastIndexOf('/');
								if(idx > 0) {
									zFileName = zFileName.substring(idx + 1);
								}
								mediaFiles.put(zFileName, zFile);
							}

							// Write the file
							FileOutputStream fos = new FileOutputStream(zFile);
							int len;
							while ((len = zis.read(buffer)) > 0) {
								fos.write(buffer, 0, len);
							}
							fos.close();
						}
					}
					zis.closeEntry();
				}
				zis.close();
			} else {
				dataFiles.add(savedFile);
			} 


			/*
			 * Get a mapping between form name and file name
			 * We need this as the data will need to be applied from parent form to child form in order rather than
			 *  in file order
			 */
			formFileMap = getFormFileMap(xm, dataFiles, formList);

			/*
			 * Create the results tables if they do not exist
			 */
			TableManager tm = new TableManager(localisation);
			FormDesc topForm = formList.get(0);
			
			SurveyTemplate template = new SurveyTemplate(localisation); 
			template.readDatabase(sd, sIdent, false);	
			tm.writeAllTableStructures(sd, results, sId, template,  0);
			
			boolean tableChanged = false;
			boolean tablePublished = false;

			// Apply any updates that have been made to the table structure since the last submission

			tableChanged = tm.applyTableChanges(sd, results, sId);

			// Add any previously unpublished columns not in a changeset (Occurs if this is a new survey sharing an existing table)
			tablePublished = tm.addUnpublishedColumns(sd, results, sId, topForm.table_name);			
			if(tableChanged || tablePublished) {
				for(FormDesc f : formList) {
					tm.markPublished(sd, f.f_id, sId);		// only mark published if there have been changes made
				}
			}

			/*
			 * Delete the existing data if requested
			 */
			results.setAutoCommit(false);
			if(clear_existing) {
				for(int i = 0; i < formList.size(); i++) {

					String sqlDeleteExisting = "truncate " + formList.get(i).table_name + ";";
					if(pstmtDeleteExisting != null) try {pstmtDeleteExisting.close();} catch(Exception e) {}
					pstmtDeleteExisting = results.prepareStatement(sqlDeleteExisting);

					log.info("Clearing results: " + pstmtDeleteExisting.toString());
					pstmtDeleteExisting.executeUpdate();
					
				}
				
				/*
				 * Delete any attachments
				 * TODO this will delete the attachments even if the new upload fails
				 */
				String fileFolder = basePath + "/attachments/" + sIdent;
				File folder = new File(fileFolder);
				try {
					log.info("Deleting attachments folder: " + fileFolder);
					FileUtils.deleteDirectory(folder);
				} catch (IOException e) {
					log.info("Error deleting attachments directory:" + fileFolder + " : " + e.getMessage());
				}
			}

			/*
			 * Process the data files
			 *   Identify forms
			 *   Identify columns in forms
			 */
			for(int formIdx = 0; formIdx < formList.size(); formIdx++) {

				FormDesc formDesc = formList.get(formIdx);

				File f = formFileMap.get(formDesc.name);
				
				if(f != null) {
					boolean isCSV = false;
					if(f.getName().endsWith(".csv")) {
						isCSV = true;
					}

					int count = xm.loadFormDataFromFile(results, 
							pstmtGetCol, 
							pstmtGetColGS,
							pstmtGetChoices, 
							f, 
							formDesc, 
							sIdent,
							mediaFiles,
							isCSV,
							responseMsg,
							basePath,
							localisation,
							preloads);

					if(formIdx == 0) {
						recordsWritten = count;
					}

				} else {
					responseMsg.add(localisation.getString("imp_no_file") + ": " + formDesc.name);
					log.info("No file of data for form: " + formDesc.name);
				}
			}		
			
			/*
			 * Clear any entries in linked_forms for this survey so that CSV files will be regenerated
			 */
			pstmtDelLinks = sd.prepareStatement(sqlDelLinks);
			pstmtDelLinks.setInt(1, sId);
			pstmtDelLinks.executeUpdate();

			results.commit();

			StringBuffer logMessage = new StringBuffer("");
			logMessage.append(recordsWritten);
			logMessage.append(" "); 
			logMessage.append(localisation.getString("imp_frm"));
			logMessage.append(" "); 
			logMessage.append(uploadedFileName);
			logMessage.append(" "); 
			logMessage.append(localisation.getString("imp_fs"));
			logMessage.append(" "); 
			logMessage.append(sName);
			logMessage.append(" "); 
			logMessage.append(localisation.getString("imp_pr"));
			logMessage.append(" "); 
			logMessage.append((clear_existing ? localisation.getString("imp_del") : localisation.getString("imp_pres")));

			lm.writeLog(sd, sId, request.getRemoteUser(), "import data", logMessage.toString());
			log.info("userevent: " + request.getRemoteUser() + " : loading file into survey: " + sId + " Previous contents are" + (clear_existing ? " deleted" : " preserved"));  // Write user event in english only

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();

			responseMsg.add(localisation.getString("imp_c"));
			response = Response.status(Status.OK).entity(gson.toJson(responseMsg)).build();

		} catch (AuthorisationException e) {
			log.log(Level.SEVERE,"", e);
			try { results.rollback();} catch (Exception ex){}
			response = Response.status(Status.FORBIDDEN).entity("Cannot load tasks from a file to this form. You need to enable loading tasks for this form in the form settings in the editor page.").build();

		} catch (NotFoundException e) {
			log.log(Level.SEVERE,"", e);
			try { results.rollback();} catch (Exception ex){}
			throw new NotFoundException();

		} catch (Exception e) {
			String msg = e.getMessage();
			if(msg != null && msg.startsWith("org.postgresql.util.PSQLException: Zero bytes")) {
				msg = "Invalid file format. Only zip and csv files accepted";
				log.info("Error: " + msg + " : " + e.getMessage());
			} else {
				log.log(Level.SEVERE,"", e);
			}
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(msg).build();
			try { results.rollback();} catch (Exception ex){}


		} finally {
			try {if (pstmtGetCol != null) {pstmtGetCol.close();}} catch (SQLException e) {}
			try {if (pstmtGetColGS != null) {pstmtGetColGS.close();}} catch (SQLException e) {}
			try {if (pstmtGetChoices != null) {pstmtGetChoices.close();}} catch (SQLException e) {}
			try {if (pstmtDeleteExisting != null) {pstmtDeleteExisting.close();}} catch (SQLException e) {}
			try {if (pstmtDelLinks != null) {pstmtDelLinks.close();}} catch (SQLException e) {}

			try {results.setAutoCommit(true);} catch (SQLException e) {}

			try {
				SDDataSource.closeConnection("surveyKPI-AllAssignments-LoadTasks From File", sd);
			} catch(Exception e) {};
			try {
				ResultsDataSource.closeConnection("surveyKPI-AllAssignments-LoadTasks From File", results);
			} catch(Exception e) {};
		}

		return response;
	}

	/*
	 * Update the task properties
	 * Keep in version 16.04+
	 */
	@POST
	@Path("/properties")
	public Response updateTaskProperties(@Context HttpServletRequest request) { 

		Response response = null;
		String dbConnectionTitle = "surveyKPI-AllAssignments- Update task properties";

		log.info("Updating task properties");	

		// Authorisation - Access
		Connection connectionSD = SDDataSource.getConnection(dbConnectionTitle);
		a.isAuthorised(connectionSD, request.getRemoteUser());
		// End role based authorisation - Check access to the requested survey once the survey id has been extracted

		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();	
		fileItemFactory.setSizeThreshold(20*1024*1024); // 20 MB TODO handle this with exception and redirect to an error page
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);

		PreparedStatement pstmtUpdate = null;


		int taskId = 0;
		String taskTitle = null;
		boolean repeat = false;
		Timestamp scheduleAt = null;
		String locationTrigger = null;
		Calendar cal = Calendar.getInstance(); 


		try {

			// Get the items from the multi part mime
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();
			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();

				if(item.isFormField()) {
					log.info("Form field:" + item.getFieldName() + " - " + item.getString());

					if(item.getFieldName().equals("taskid")) {
						taskId = Integer.parseInt(item.getString());	
					} if(item.getFieldName().equals("taskTitle")) {
						taskTitle = item.getString();	
					} else if(item.getFieldName().equals("repeat")) {
						repeat = true;	
					} else if(item.getFieldName().equals("scheduleAtUTC")) {
						scheduleAt = Timestamp.valueOf(item.getString());	
					} else if(item.getFieldName().equals("location_trigger")) {
						locationTrigger = item.getString();	
						if(locationTrigger != null && locationTrigger.equals("-1")) {
							locationTrigger = null;
						}
					}

				} else if(!item.isFormField()) {
					// Handle Uploaded file
					log.info("Field Name = "+item.getFieldName()+
							", File Name = "+item.getName()+
							", Content type = "+item.getContentType()+
							", File Size = "+item.getSize());		
				}

			}

			String sqlUpdate = "update tasks set repeat = ?, "
					+ "schedule_at = ?, "
					+ "location_trigger = ?,  "
					+ "title = ? where id = ?;";
			pstmtUpdate = connectionSD.prepareStatement(sqlUpdate);
			pstmtUpdate.setBoolean(1, repeat);
			pstmtUpdate.setTimestamp(2, scheduleAt);
			pstmtUpdate.setString(3, locationTrigger);
			pstmtUpdate.setString(4, taskTitle);
			pstmtUpdate.setInt(5, taskId);

			log.info("SQL Update properties: " + pstmtUpdate.toString());
			pstmtUpdate.executeUpdate();

		} catch (AuthorisationException e) {
			log.log(Level.SEVERE,"", e);
			response = Response.status(Status.FORBIDDEN).entity("Cannot update properties for this task").build();

		} catch (NotFoundException e) {
			log.log(Level.SEVERE,"", e);
			throw new NotFoundException();

		} catch (Exception e) {
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();

			log.log(Level.SEVERE,"", e);

		} finally {
			try {if (pstmtUpdate != null) {pstmtUpdate.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(dbConnectionTitle, connectionSD);

		}

		return response;
	}



	/*
	 * Delete tasks
	 *
	@DELETE
	public Response deleteTasks(@Context HttpServletRequest request,
			@FormParam("settings") String settings) { 

		Response response = null;

		log.info("Assignment:" + settings);
		Type type = new TypeToken<ArrayList<Assignment>>(){}.getType();		
		ArrayList<Assignment> aArray = new Gson().fromJson(settings, type);

		// Authorisation - Access
		Connection connectionSD = SDDataSource.getConnection("surveyKPI-AllAssignments");
		a.isAuthorised(connectionSD, request.getRemoteUser());
		for(int i = 0; i < aArray.size(); i++) {

			Assignment ass = aArray.get(i);		
			a.isValidTask(connectionSD, request.getRemoteUser(), ass.task_id);

		}
		// End Authorisation

		PreparedStatement pstmtCount = null;
		PreparedStatement pstmtUpdate = null;
		PreparedStatement pstmtDelete = null;
		PreparedStatement pstmtDeleteEmptyGroup = null;

		try {
			//connectionSD.setAutoCommit(false);
			//String countSQL = "select count(*) from tasks t, assignments a " +
			//		"where t.id = a.task_id " +
			//		"and t.id = ?";
			//pstmtCount = connectionSD.prepareStatement(countSQL);

			//String updateSQL = "update assignments set status = 'cancelled' where task_id = ? " +
			//		"and (status = 'new' " +
			//		"or status = 'accepted' " + 
			//		"or status = 'pending'); "; 
			//pstmtUpdate = connectionSD.prepareStatement(updateSQL);

			String deleteSQL = "delete from tasks where id = ?; "; 
			pstmtDelete = connectionSD.prepareStatement(deleteSQL);

			//String deleteEmptyGroupSQL = "delete from task_group tg where not exists (select 1 from tasks t where t.tg_id = tg.tg_id);";
			//pstmtDeleteEmptyGroup = connectionSD.prepareStatement(deleteEmptyGroupSQL);

			for(int i = 0; i < aArray.size(); i++) {

				Assignment a = aArray.get(i);

				// Check to see if the task has any assignments
				//pstmtCount.setInt(1, a.task_id);
				//ResultSet rs = pstmtCount.executeQuery();
				//int countAss = 0;
				//if(rs.next()) {
				//	countAss = rs.getInt(1);
				//}

				//if(countAss > 0) {
				//	log.info(updateSQL + " : " + a.task_id);
				//	pstmtUpdate.setInt(1, a.task_id);
				//	pstmtUpdate.execute();
				//} else {

				pstmtDelete.setInt(1, a.task_id);
				log.info("SQL: " + pstmtDelete.toString());
				pstmtDelete.execute();
				//}
			}

			// Delete any task groups that have no tasks
			//pstmtDeleteEmptyGroup.execute();
			//connectionSD.commit();

		} catch (Exception e) {
			response = Response.serverError().build();
			log.log(Level.SEVERE,"", e);

			try { connectionSD.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}

		} finally {
			if (pstmtCount != null) try {pstmtCount.close();} catch (SQLException e) {};
			if (pstmtUpdate != null) try {pstmtUpdate.close();} catch (SQLException e) {};
			if (pstmtDelete != null) try {pstmtDelete.close();} catch (SQLException e) {};
			if (pstmtDeleteEmptyGroup != null) try {pstmtDeleteEmptyGroup.close();} catch (SQLException e) {};

			SDDataSource.closeConnection("surveyKPI-AllAssignments", connectionSD);
		}

		return response;
	}
	*/
	
	/*
	 * Delete task group
	 * This web service takes no account of tasks that have already been assigned
	 */
	@DELETE
	@Path("/{taskGroupId}")
	public Response deleteTaskGroup(@Context HttpServletRequest request,
			@PathParam("taskGroupId") int tg_id) { 

		Response response = null;

		// Authorisation - Access
		Connection connectionSD = SDDataSource.getConnection("surveyKPI-AllAssignments");
		a.isAuthorised(connectionSD, request.getRemoteUser());
		a.isValidTaskGroup(connectionSD, request.getRemoteUser(), tg_id, false);
		// End Authorisation

		PreparedStatement pstmtDelete = null;

		try {
			
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(connectionSD, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			TaskManager tm = new TaskManager(localisation);
			tm.deleteTasksInTaskGroup(connectionSD, tg_id);		// Note can't rely on cascading delete as temporary users need to be deleted
			String deleteSQL = "delete from task_group where tg_id = ?; "; 
			pstmtDelete = connectionSD.prepareStatement(deleteSQL);

			pstmtDelete.setInt(1, tg_id);
			log.info("SQL: " + pstmtDelete.toString());
			pstmtDelete.execute();


		} catch (Exception e) {
			response = Response.serverError().build();
			log.log(Level.SEVERE,"", e);

			try { connectionSD.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}

		} finally {

			if (pstmtDelete != null) try {pstmtDelete.close();} catch (SQLException e) {};

			SDDataSource.closeConnection("surveyKPI-AllAssignments", connectionSD);
		}

		return response;
	}

	/*
	 * Mark cancelled tasks as deleted
	 * This may be required if tasks are allocated to a user who never updates them
	 *
	@Path("/cancelled/{projectId}")
	@DELETE
	public Response forceRemoveCancelledTasks(@Context HttpServletRequest request,
			@PathParam("projectId") int projectId
			) { 

		Response response = null;

		// Authorisation - Access
		Connection connectionSD = SDDataSource.getConnection("surveyKPI-AllAssignments");
		a.isAuthorised(connectionSD, request.getRemoteUser());
		a.isValidProject(connectionSD, request.getRemoteUser(), projectId);
		// End Authorisation

		PreparedStatement pstmtDelete = null;

		try {

			String deleteSQL = "delete from tasks t " +
					" where t.p_id = ? " +
					" and t.id in (select task_id from assignments a " +
					" where a.status = 'cancelled' or a.status = 'rejected'); "; 

			pstmtDelete = connectionSD.prepareStatement(deleteSQL);

			log.info(deleteSQL + " : " + projectId);
			pstmtDelete.setInt(1, projectId);
			pstmtDelete.execute();


		} catch (Exception e) {
			response = Response.serverError().build();
			log.log(Level.SEVERE,"", e);
			try {
				connectionSD.rollback();
			} catch (Exception ex) {

			}
		} finally {
			if (pstmtDelete != null) try {pstmtDelete.close();} catch (SQLException e) {};

			SDDataSource.closeConnection("surveyKPI-AllAssignments", connectionSD);
		}

		return response;
	}
	*/

	private HashMap<String, File> getFormFileMap(ExchangeManager xm, ArrayList<File> files, ArrayList<FormDesc> forms) throws Exception {
		HashMap<String, File> formFileMap = new HashMap<String, File> ();

		/*
		 * If there is only one csv file then associate it with the main form
		 * This is to ensure backward compatability for versions prior to 16.12 which only allowed a single data file of any name to load the main form
		 */
		boolean allDone = false;
		if(files.size() == 1) {
			File file = files.get(0);
			if(file.getName().endsWith(".csv")) {
				formFileMap.put("main", file);
				allDone = true;
			}
		}

		/*
		 * Otherwise associate forms with files
		 */
		if(!allDone) {
			for(int i = 0; i < files.size(); i++) {
				File file = files.get(i);
				String filename = file.getName();

				if(filename.endsWith(".csv")) {
					int idx = filename.lastIndexOf('.');
					String formName = filename.substring(0, idx);
					formFileMap.put(formName, file);
				} else {
					FileInputStream fis = new FileInputStream(file);
					ArrayList<String> formNames = xm.getFormsFromXLSX(fis);
					for(int j = 0; j < formNames.size(); j++) {
						formFileMap.put(formNames.get(j), file);
					}
					fis.close();
				}
			}
		}
		return formFileMap;
	}
}



