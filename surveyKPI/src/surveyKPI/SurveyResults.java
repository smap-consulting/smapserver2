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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ExternalFileManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.QuestionManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.SurveyTableManager;
import org.smap.sdal.model.GroupDetails;
import org.smap.sdal.model.Question;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;


@Path("/surveyResults/{sId}")
public class SurveyResults extends Application {
	
	Authorise a = new Authorise(null, Authorise.ANALYST);
	Authorise aManage = null;
	
	private static Logger log =
			 Logger.getLogger(Results.class.getName());

	LogManager lm = new LogManager();		// Application log

	public SurveyResults() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.MANAGE);
		authorisations.add(Authorise.ADMIN);
		aManage = new Authorise(authorisations, null);
	}
	
	/*
	 * Delete results for a survey
	 */
	@DELETE
	public Response deleteSurveyResults(@Context HttpServletRequest request,
			@PathParam("sId") int sId) { 
		
		Response response = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-SurveyResults");
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		// Escape any quotes
		if(sId > 0) {

			String sql = null;				
			Connection connectionRel = null; 
			PreparedStatement pstmt = null;
			PreparedStatement pstmtUnPublish = null;
			PreparedStatement pstmtUnPublishOption = null;
			PreparedStatement pstmtRemoveChangeset = null;
			PreparedStatement pstmtGetSoftDeletedQuestions = null;
			Statement stmtRel = null;
			try {
				// Get the users locale
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
				connectionRel = ResultsDataSource.getConnection("surveyKPI-SurveyResults");
				boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());

				// Delete tables associated with this survey
				
				String sqlUnPublish = "update question set published = 'false' where f_id in (select f_id from form where s_id = ?);";
				pstmtUnPublish = sd.prepareStatement(sqlUnPublish);
				
				String sqlUnPublishOption = "update option set published = 'false' where l_id in (select l_id from listname where s_id = ?);";
				pstmtUnPublishOption = sd.prepareStatement(sqlUnPublishOption);
				
				String sqlRemoveChangeset = "delete from changeset where s_id = ?;";
				pstmtRemoveChangeset = connectionRel.prepareStatement(sqlRemoveChangeset);
				
				String sqlGetSoftDeletedQuestions = "select q.f_id, q.qname from question q where soft_deleted = 'true' "
						+ "and q.f_id in (select f_id from form where s_id = ?);";
				pstmtGetSoftDeletedQuestions = sd.prepareStatement(sqlGetSoftDeletedQuestions);
				
				/*
				 * Get the surveys and tables that are part of the group that this survey belongs to
				 */
				SurveyManager sm = new SurveyManager(localisation, "UTC");
				String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
				ArrayList<GroupDetails> surveys = sm.getAccessibleGroupSurveys(sd, groupSurveyIdent, request.getRemoteUser(), superUser);
				ArrayList<String> tableList = sm.getGroupTables(sd, groupSurveyIdent, oId, request.getRemoteUser(), sId);
				
				/*
				 * Delete data from each form
				 */
				for(String tableName : tableList) {				

					sql = "drop TABLE " + tableName + ";";
					log.info("*********************************  Delete table contents and drop table: " + sql);
					
					
					try {if (stmtRel != null) {stmtRel.close();}} catch (SQLException e) {}
					stmtRel = connectionRel.createStatement();
					try {
						stmtRel.executeUpdate(sql);
					} catch (Exception e) {
						log.info("Error deleting table: " + e.getMessage());
					}
					log.info("userevent: " + request.getRemoteUser() + " : delete results : " + tableName + " in survey : "+ sId); 
					lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.DELETE, "Delete results: " + tableName  + " in survey : "+ sId, 0, request.getServerName());
				}
				
				/*
				 * Clean up the surveys
				 */
				ExternalFileManager efm = new ExternalFileManager(localisation);
				for(GroupDetails gd : surveys) {
					log.info("Clean survey: " + gd.surveyName);
					
					pstmtUnPublish.setInt(1, gd.sId);			// Mark questions as un-published
					log.info("Marking questions as unpublished: " + pstmtUnPublish.toString());
					pstmtUnPublish.executeUpdate();
					
					pstmtUnPublishOption.setInt(1, gd.sId);			// Mark options as un-published
					log.info("Marking options as unpublished: " + pstmtUnPublishOption.toString());
					pstmtUnPublishOption.executeUpdate();
					
					pstmtRemoveChangeset.setInt(1, gd.sId);
					log.info("Removing changeset: " + pstmtRemoveChangeset.toString());
					pstmtRemoveChangeset.executeUpdate();
					
					// Delete any soft deleted questions from the survey definitions
					QuestionManager qm = new QuestionManager(localisation);
					ArrayList<Question> questions = new ArrayList<Question> ();
					pstmtGetSoftDeletedQuestions.setInt(1, gd.sId);
					log.info("Get soft deleted questions: " + pstmtGetSoftDeletedQuestions.toString());
					ResultSet rs = pstmtGetSoftDeletedQuestions.executeQuery();
					while(rs.next()) {
						Question q = new Question();
						q.fId = rs.getInt(1);
						q.name = rs.getString(2);
						questions.add(q);
					}
					if(questions.size() > 0) {
						qm.delete(sd, connectionRel, gd.sId, questions, false, false);
					}
					
					// Force regeneration of any dynamic CSV files that this survey links to
					efm.linkerChanged(sd, gd.sId);	// deprecated
					try {
						SurveyTableManager stm = new SurveyTableManager(sd, localisation);
						stm.delete(sId);			// Delete references to this survey in the csv table so that they get regenerated
					} catch (Exception e) {
						log.log(Level.SEVERE, e.getMessage(), e);
					}
					
				}
				response = Response.ok("").build();
				
			} catch (Exception e) {
				String msg = e.getMessage();
				if(msg != null && msg.contains("does not exist")) {
					log.log(Level.SEVERE, msg);
					e.printStackTrace();
					response = Response.ok("").build();
				} else {
					log.log(Level.SEVERE, "Survey: Delete REsults");
					e.printStackTrace();
					response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
				}
			} finally {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				try {if (pstmtUnPublish != null) {pstmtUnPublish.close();}} catch (SQLException e) {}
				try {if (pstmtUnPublishOption != null) {pstmtUnPublishOption.close();}} catch (SQLException e) {}
				try {if (stmtRel != null) {stmtRel.close();}} catch (SQLException e) {}
				try {if (pstmtGetSoftDeletedQuestions != null) {pstmtGetSoftDeletedQuestions.close();}} catch (SQLException e) {}

				SDDataSource.closeConnection("surveyKPI-SurveyResults", sd);
				ResultsDataSource.closeConnection("surveyKPI-SurveyResults", connectionRel);
			}
		}

		return response; 
	}
	
	/*
	 * Restore results for a survey
	 * Deprecated.  Restoration is now done as a background report to allow AWS sync to complete
	 *
	@GET
	@Path("/restore")
	public Response restoreSurveyResults(@Context HttpServletRequest request,
			@PathParam("sId") int sId) { 
		
		Response response = null;
		String connectionString = "surveyKPI-SurveyResults-restore";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, connectionString, sId, false, superUser);
		// End Authorisation
		
		lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.RESTORE, "Restore results", 0, request.getServerName());
		
		// Escape any quotes
		if(sId > 0) {

			String sql = null;				
			Connection connectionRel = null; 
			PreparedStatement pstmt = null;
			PreparedStatement pstmtReset = null;
			PreparedStatement pstmtUnpublish = null;
			
			Statement stmtRel = null;
			try {
				// Get the users locale
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
				connectionRel = ResultsDataSource.getConnection(connectionString);
				
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
				 *
				SurveyManager sm = new SurveyManager(localisation, "UTC");
				String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
				ArrayList<GroupDetails> surveys = sm.getAccessibleGroupSurveys(sd, groupSurveyIdent, request.getRemoteUser(), superUser);
				ArrayList<String> tableList = sm.getGroupTables(sd, groupSurveyIdent, oId, request.getRemoteUser(), sId);
				
				/*
				 * Delete data from each form ready for reload
				 *
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
					log.info("userevent: " + request.getRemoteUser() + " : delete results : " + tableName + " in survey : "+ sId); 
				}
					
				/*
				 * Mark questions as unpublished
				 *
				connectionRel.setAutoCommit(false);
				for(GroupDetails gd : surveys) {
					pstmtUnpublish.setInt(1, gd.sId);
					log.info("set unpublished " + pstmtUnpublish.toString());
					pstmtUnpublish.executeUpdate();
				}
				
				/*
				 * Reload the surveys
				 *
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
				
				response = Response.ok("").build();
				
			} catch (Exception e) {
				String msg = e.getMessage();
				if(msg != null && msg.contains("does not exist")) {
					response = Response.ok("").build();
				} else {
					log.log(Level.SEVERE, "Survey: Restore Results");
					e.printStackTrace();
					response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
				}
			} finally {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				try {if (pstmtReset != null) {pstmtReset.close();}} catch (SQLException e) {}
				try {if (pstmtUnpublish != null) {pstmtUnpublish.close();}} catch (SQLException e) {}
				try {if (stmtRel != null) {stmtRel.close();}} catch (SQLException e) {}
			

				try {connectionRel.setAutoCommit(true);} catch (Exception e) {}
				
				SDDataSource.closeConnection(connectionString, sd);
				ResultsDataSource.closeConnection(connectionString, connectionRel);
			}
		}

		return response; 
	}
	*/
	
	/*
	 * Archive results for a survey
	 * All submissions received on or before the specified date will be moved into another survey
	 */
	
	private class ArchiveResponse {
		int count;
		ArrayList<String> archives = new ArrayList<>();
		ArrayList<String> surveys = new ArrayList<>();
		String msg;
	}
	
	private class SurveyMap {
		int oldSurveyId;
		int newSurveyId;
		public SurveyMap(int oldSurveyId, int newSurveyId) {
			this.oldSurveyId = oldSurveyId;
			this.newSurveyId = newSurveyId;
		}
	}
	
	@GET
	@Path("/archive")
	public Response archiveSurveyResults(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@QueryParam("beforeDate") Date beforeDate,
			@QueryParam("tz") String tz) {
		
		Response response = null;
		String connectionString = "surveyKPI-SurveyResults-archive";
		ArchiveResponse resp = new ArchiveResponse();
		resp.count = 0;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.ARCHIVE, "Archive results", 0, request.getServerName());
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		String sqlNewTable = "select table_name from form where name = ? and s_id = any (?)";
		
		if(sId > 0) {
			
			Connection cResults = null; 
			PreparedStatement pstmt = null;
			PreparedStatement pstmtCount = null;
			PreparedStatement pstmtCopy = null;
			PreparedStatement pstmtNewTable = null;
		
			try {
				// Get the users locale
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				cResults = ResultsDataSource.getConnection(connectionString);
				boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
				
				/*
				 * Get the surveys and tables that are part of the group that this survey belongs to
				 */
				SurveyManager sm = new SurveyManager(localisation, tz);
				String mainTableName = GeneralUtilityMethods.getMainResultsTable(sd, cResults, sId);
				String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
				ArrayList<GroupDetails> surveys = sm.getAccessibleGroupSurveys(sd, groupSurveyIdent, request.getRemoteUser(), superUser);
				
				/*
				 * Check to see that there is data to be archived
				 */
				if(GeneralUtilityMethods.tableExists(cResults, mainTableName)) {
				
					/*
					 * Create archive Surveys
					 */
					sd.setAutoCommit(false);
					cResults.setAutoCommit(false);
					
					DateFormat df = new SimpleDateFormat("yyyy-MM-dd");  
					String beforeString = df.format(beforeDate);
					ArrayList<SurveyMap> surveyMaps = new ArrayList<>();
					String newGroupSurveyIdent = null;
					
					for(GroupDetails gd : surveys) {
						
						String newName = gd.surveyName + "[" + beforeString + "]";

						if(sm.surveyExists(sd, newName, gd.pId)) {
							String msg = localisation.getString("tu_ae");
							msg = msg.replaceAll("%s1", newName);
							throw new ApplicationException(msg);
						}
						
						int newSurveyId = sm.createNewSurvey(sd, newName, gd.pId, true, gd.sId, false, 
								request.getRemoteUser(), superUser);	
						surveyMaps.add(new SurveyMap(gd.sId, newSurveyId));
						if(newGroupSurveyIdent == null) {
							newGroupSurveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, newSurveyId);
						}
						
						resp.surveys.add(gd.surveyName);	// Return list of surveys archived
					}
				
					/*
					 * Get the new and old survey ids into an integer array so they can be used with "any" in an sql statement
					 */
					Integer[] oldSurveys = new Integer[surveyMaps.size()];
					Integer[] newSurveys = new Integer[surveyMaps.size()];
					for(int i = 0; i < surveyMaps.size(); i++) {
						oldSurveys[i] = surveyMaps.get(i).oldSurveyId;
						newSurveys[i] = surveyMaps.get(i).newSurveyId;
					}
					
					/*
					 * Block all new Surveys
					 * Put all new surveys in the same group
					 */
					blockAndGroupNewSurveys(sd, newSurveys, newGroupSurveyIdent);
					
					/*
					 * Copy data to the archive tables
					 */
					HashMap<String, String> tablesDone = new HashMap<>();
					pstmtNewTable = sd.prepareStatement(sqlNewTable);
					pstmtNewTable.setArray(2, sd.createArrayOf("int", newSurveys));
					
					resp.count = copyAndPurgeData(sd, cResults, pstmtNewTable, oldSurveys, newSurveys, 0, null, tablesDone, beforeDate, tz);

					cResults.commit();
					sd.commit();
				}
				
				resp.msg = localisation.getString("arch_done");
				resp.msg = resp.msg.replace("%s1", String.valueOf(resp.count));
				StringBuilder surveyList = new StringBuilder("");
				for(String s : resp.surveys) {
					if(surveyList.length() > 0) {
						surveyList.append(", ");
					}
					surveyList.append(s);
				}
				resp.msg = resp.msg.replace("%s2", surveyList.toString());
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");  
				resp.msg = resp.msg.replace("%s3",df.format(beforeDate));
				lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.ARCHIVE, resp.msg, 0, request.getServerName());
				
				response = Response.ok(gson.toJson(resp)).build();
				
			} catch (Exception e) {
				
				try {cResults.rollback();} catch (Exception er) {}
				try {sd.rollback();} catch (Exception er) {}
				
				log.log(Level.SEVERE, "Survey: Restore Results");
				e.printStackTrace();
				response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();

				
			} finally {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				try {if (pstmtCount != null) {pstmtCount.close();}} catch (SQLException e) {}
				try {if (pstmtCopy != null) {pstmtCopy.close();}} catch (SQLException e) {}
				try {if (pstmtNewTable != null) {pstmtNewTable.close();}} catch (SQLException e) {}
			
				try {cResults.setAutoCommit(true);} catch (Exception e) {}
				try {sd.setAutoCommit(true);} catch (Exception e) {}
				
				SDDataSource.closeConnection(connectionString, sd);
				ResultsDataSource.closeConnection(connectionString, cResults);
			}
		}

		return response; 
	}
	
	/*
	 * Archive results for a survey
	 * All submissions received on or before the specified date will be moved into another survey
	 */
	@GET
	@Path("/archivecount")
	public Response archiveSurveyCount(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@QueryParam("beforeDate") Date beforeDate,
			@QueryParam("tz") String tz) {
		
		Response response = null;
		String connectionString = "surveyKPI-SurveyResults-archivecount";
		ArchiveResponse resp = new ArchiveResponse();
		resp.count = 0;
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		tz = (tz == null) ? "UTC" : tz;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		
		// Escape any quotes
		if(sId > 0) {
			
			Connection cResults = null; 
			PreparedStatement pstmt = null;
		
			try {
				// Get the users locale
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				cResults = ResultsDataSource.getConnection(connectionString);
				boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
				
				if(beforeDate == null) {
					throw new ApplicationException(localisation.getString("arch_nd"));
				}
				
				/*
				 * Get the surveys and tables that are part of the group that this survey belongs to
				 */
				SurveyManager sm = new SurveyManager(localisation, "UTC");
				String mainTableName = GeneralUtilityMethods.getMainResultsTable(sd, cResults, sId);
				String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
				ArrayList<GroupDetails> surveys = sm.getAccessibleGroupSurveys(sd, groupSurveyIdent, request.getRemoteUser(), superUser);
				
				/*
				 * Check to see that there is data to be archived
				 */
				if(GeneralUtilityMethods.tableExists(cResults, mainTableName)) {
					StringBuilder sql = new StringBuilder("select count(*) from ")
						.append(mainTableName)
						.append(" where _upload_time < ?");
	
					pstmt = cResults.prepareStatement(sql.toString());
					pstmt.setTimestamp(1, GeneralUtilityMethods.endOfDay(beforeDate, tz));
					log.info("Get archive count: " + pstmt.toString());
					ResultSet rs = pstmt.executeQuery();
					if(rs.next()) {
						resp.count = rs.getInt(1);
					}
				}
				
				for(GroupDetails gd : surveys) {
					resp.surveys.add(gd.surveyName);
					resp.archives.add(gd.surveyName + " : " + beforeDate);
				}
				
				response = Response.ok(gson.toJson(resp)).build();
				
			} catch (ApplicationException e) {
				response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
			} catch (Exception e) {
				String msg = e.getMessage();
				if(msg != null && msg.contains("does not exist")) {
					response = Response.ok("").build();
				} else {
					log.log(Level.SEVERE, "Survey: Restore Results");
					e.printStackTrace();
					response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
				}
			} finally {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				
				SDDataSource.closeConnection(connectionString, sd);
				ResultsDataSource.closeConnection(connectionString, cResults);
			}
		}

		return response; 
	}
	
	
	/*
	 * Get surveys belonging to a group
	 */
	@GET
	@Path("/groups")
	public Response getGroups(@Context HttpServletRequest request,
			@PathParam("sId") int sId) { 
		
		Response response = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-SurveyResults - getGroups");
		aManage.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		if(sId > 0) {
		
			PreparedStatement pstmt = null;
			PreparedStatement pstmtRestore = null;
			
			Statement stmtRel = null;
			try {
				// Get the users locale
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
				
				/*
				 * Get the surveys and tables that are part of the group that this survey belongs to
				 */
				SurveyManager sm = new SurveyManager(localisation, "UTC");
				String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
				ArrayList<GroupDetails> groups = sm.getAccessibleGroupSurveys(sd, groupSurveyIdent, 
						request.getRemoteUser(),superUser);
				
				Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
				response = Response.ok(gson.toJson(groups)).build();
				
			} catch (Exception e) {
				String msg = e.getMessage();
				if(msg != null && msg.contains("does not exist")) {
					response = Response.ok("").build();
				} else {
					log.log(Level.SEVERE, "Survey: Restore Results");
					e.printStackTrace();
					response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
				}
			} finally {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				try {if (pstmtRestore != null) {pstmtRestore.close();}} catch (SQLException e) {}
				
				try {if (stmtRel != null) {stmtRel.close();}} catch (SQLException e) {}
		
				
				SDDataSource.closeConnection("surveyKPI-SurveyResults - getGroups", sd);
			}
		}

		return response; 
	}
	
	
	/*
	 * Block the new surveys that have been created for archiving
	 */
	private void blockAndGroupNewSurveys(Connection sd, Integer[] surveys, String groupSurveyIdent) throws SQLException {
		
		String sql = "update survey set blocked = true, group_survey_ident = ? where s_id = any (?)";		
		String sqlForms = "select name, table_name from form where s_id = any (?)";
		String sqlTables = "update form set table_name = ? where name = ? and s_id = any (?)";
		
		PreparedStatement pstmt = null;	
		
		try {
			// Set group ident and block
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, groupSurveyIdent);
			pstmt.setArray(2, sd.createArrayOf("int", surveys));
			log.info("Block surveys: " + pstmt.toString());
			pstmt.executeUpdate();
			
			// Get a mapping between table names and form names
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
			
			HashMap<String, String> forms = new HashMap<>();
			pstmt = sd.prepareStatement(sqlForms);
			pstmt.setArray(1, sd.createArrayOf("int", surveys));
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String t = forms.get(rs.getString(1));
				if(t == null) {
					forms.put(rs.getString(1), rs.getString(2));
				}
			}
			
			// Rationalise the table names
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
			
			pstmt = sd.prepareStatement(sqlTables);
			pstmt.setArray(3, sd.createArrayOf("int", surveys));
			for(String f : forms.keySet()) {
				String t = forms.get(f);
				pstmt.setString(1,  t);
				pstmt.setString(2,  f);
				log.info("GroupTables: " + pstmt.toString());
				pstmt.executeUpdate();
			}
						
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
	}
	
	/*
	 * Copy data to the archive tables 
	 * Remove copied data from the original tables
	 */
	private int copyAndPurgeData(Connection sd, Connection cResults, 
			PreparedStatement pstmtNewTable,
			Integer[] oldSurveys, 
			Integer[] newSurveys, 
			int parent, 
			String newParentTable,
			HashMap<String, String> tablesDone,
			Date beforeDate,
			String tz
			) throws SQLException {
		
		int count = 0;
		
		String sqlForms = "select name, table_name, f_id from form where s_id = any (?) and parentform = ?";
		PreparedStatement pstmtForms = null;
		PreparedStatement pstmtCopy = null;
		PreparedStatement pstmtPurge = null;
		
		try {

			pstmtForms = sd.prepareStatement(sqlForms);
			pstmtForms.setArray(1, sd.createArrayOf("int", oldSurveys));
			
			pstmtForms.setInt(2,  parent);
			ResultSet rs = pstmtForms.executeQuery();
			while(rs.next()) {
				String oldTableName = rs.getString("table_name");
				String oldFormName = rs.getString("name");
				int oldFormId = rs.getInt("f_id");
				if(tablesDone.get(oldTableName) == null) {
					tablesDone.put(oldTableName, oldTableName);
					
					pstmtNewTable.setString(1, oldFormName);
					ResultSet rsTables = pstmtNewTable.executeQuery();
					if(rsTables.next()) {
						String newTableName = rsTables.getString("table_name");
						
						log.info("Copy from: " + oldTableName + " to " + newTableName);
						
						StringBuilder sqlCopy = new StringBuilder("create table ")
								.append(newTableName)
								.append(" as select * from ")
								.append(oldTableName);
						if(parent == 0) {
							sqlCopy.append(" where _upload_time < ?");
						} else {
							sqlCopy.append(" where parkey in (select prikey from ")
								.append(newParentTable)
								.append(")");		// Copy records whos parent has already been copied
							
						}
						
						try {if (pstmtCopy != null) {pstmtCopy.close();}} catch (SQLException e) {}
						pstmtCopy = cResults.prepareStatement(sqlCopy.toString());
						if(parent == 0) {
							pstmtCopy.setTimestamp(1, GeneralUtilityMethods.endOfDay(beforeDate, tz));
						}
						log.info("Copy for archive: " + pstmtCopy.toString());
						pstmtCopy.executeUpdate();
						
						/*
						 * Purge original data
						 */
						StringBuilder sqlPurge = new StringBuilder("delete from ")
								.append(oldTableName)
								.append(" where prikey in (select prikey from  ")
								.append(newTableName)
								.append(")");
						try {if (pstmtPurge != null) {pstmtPurge.close();}} catch (SQLException e) {}
						pstmtPurge = cResults.prepareStatement(sqlPurge.toString());
						log.info("Purge for archive: " + pstmtCopy.toString());
						count = pstmtPurge.executeUpdate();
						
						copyAndPurgeData(sd, cResults, pstmtNewTable, oldSurveys, newSurveys, oldFormId, newTableName, tablesDone, beforeDate, tz);	// Check out children
					}
					
				}
			}
		} finally {
			try {if (pstmtForms != null) {pstmtForms.close();}} catch (SQLException e) {}
			try {if (pstmtCopy != null) {pstmtCopy.close();}} catch (SQLException e) {}
			try {if (pstmtPurge != null) {pstmtPurge.close();}} catch (SQLException e) {}
		}
		
		return count;
		
	}
	
}
