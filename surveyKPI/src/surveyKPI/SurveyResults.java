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
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.QuestionManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.Question;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Delete results for a survey
 */
@Path("/surveyResults/{sId}")
public class SurveyResults extends Application {
	
	Authorise a = new Authorise(null, Authorise.ANALYST);
	
	private static Logger log =
			 Logger.getLogger(Results.class.getName());

	LogManager lm = new LogManager();		// Application log

		@DELETE
		public Response deleteSurveyResults(@Context HttpServletRequest request,
				@PathParam("sId") int sId) { 
			
			Response response = null;
			
			// Authorisation - Access
			Connection sd = SDDataSource.getConnection("surveyKPI-SurveyResults");
			a.isAuthorised(sd, request.getRemoteUser());
			// End Authorisation
			
			lm.writeLog(sd, sId, request.getRemoteUser(), "delete", "Delete results");
			
			ArrayList<String> tables = new ArrayList<String> ();
			// Escape any quotes
			if(sId > 0) {
	
				String sql = null;				
				Connection connectionRel = null; 
				PreparedStatement pstmt = null;
				PreparedStatement pstmtUnPublish = null;
				PreparedStatement pstmtUnPublishOption = null;
				PreparedStatement pstmtRemoveChangeHistory = null;
				PreparedStatement pstmtRemoveChangeset = null;
				PreparedStatement pstmtGetSoftDeletedQuestions = null;
				Statement stmtRel = null;
				try {
					// Get the users locale
					Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request.getRemoteUser()));
					ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
					
					connectionRel = ResultsDataSource.getConnection("surveyKPI-SurveyResults");

					// Delete tables associated with this survey
					
					String sqlUnPublish = "update question set published = 'false' where f_id in (select f_id from form where s_id = ?);";
					pstmtUnPublish = sd.prepareStatement(sqlUnPublish);
					
					String sqlUnPublishOption = "update option set published = 'false' where l_id in (select l_id from listname where s_id = ?);";
					pstmtUnPublishOption = sd.prepareStatement(sqlUnPublishOption);
					
					String sqlRemoveChangeHistory = "delete from change_history where s_id = ?;";
					pstmtRemoveChangeHistory = connectionRel.prepareStatement(sqlRemoveChangeHistory);
					
					String sqlRemoveChangeset = "delete from changeset where s_id = ?;";
					pstmtRemoveChangeset = connectionRel.prepareStatement(sqlRemoveChangeset);
					
					String sqlGetSoftDeletedQuestions = "select q.f_id, q.qname from question q where soft_deleted = 'true' "
							+ "and q.f_id in (select f_id from form where s_id = ?);";
					pstmtGetSoftDeletedQuestions = sd.prepareStatement(sqlGetSoftDeletedQuestions);
					
					/*
					sql = "select distinct f.table_name, f.f_id FROM form f " +
							"where f.s_id = ? " +
							"order by f.table_name;";						
					pstmt = connectionSD.prepareStatement(sql);	
					pstmt.setInt(1, sId);
					log.info("Get tables for delete: " + pstmt.toString());
					ResultSet resultSet = pstmt.executeQuery();
						
					while (resultSet.next()) {					
						tables.add(resultSet.getString(1));
					}				
					
					resultSet.close();
					*/
					
					/*
					 * Get the surveys and tables that are part of the group that this survey belongs to
					 */
					SurveyManager sm = new SurveyManager(localisation);
					ArrayList<Integer> surveys = sm.getGroupSurveys(sd, sId);
					HashMap<String, String> tableMap = sm.getGroupForms(sd, sId);
					
					/*
					 * Delete data from each form
					 */
					for(String formname : tableMap.keySet()) {
					//for (int i = 0; i < tables.size(); i++) {	
						
						String tableName = tableMap.get(formname);					

						sql = "drop TABLE " + tableName + ";";
						log.info("Delete table contents and drop table: " + sql);
						
						try {if (stmtRel != null) {stmtRel.close();}} catch (SQLException e) {}
						stmtRel = connectionRel.createStatement();
						try {
							stmtRel.executeUpdate(sql);
						} catch (Exception e) {
							// Ignore errors
						}
						log.info("userevent: " + request.getRemoteUser() + " : delete results : " + tableName + " in survey : "+ sId); 
					}
					
					/*
					 * Clean up the surveys
					 */
					for(int groupSurveyId : surveys) {
						pstmtUnPublish.setInt(1, groupSurveyId);			// Mark questions as un-published
						log.info("Marking questions as unpublished: " + pstmtUnPublish.toString());
						pstmtUnPublish.executeUpdate();
						
						pstmtUnPublishOption.setInt(1, groupSurveyId);			// Mark options as un-published
						log.info("Marking options as unpublished: " + pstmtUnPublishOption.toString());
						pstmtUnPublishOption.executeUpdate();
						
						pstmtRemoveChangeHistory.setInt(1, groupSurveyId);
						pstmtRemoveChangeHistory.executeUpdate();
						
						pstmtRemoveChangeset.setInt(1, groupSurveyId);
						pstmtRemoveChangeset.executeUpdate();
						
						// Delete any soft deleted questions from the survey definitions
						QuestionManager qm = new QuestionManager(localisation);
						ArrayList<Question> questions = new ArrayList<Question> ();
						pstmtGetSoftDeletedQuestions.setInt(1, groupSurveyId);
						ResultSet rs = pstmtGetSoftDeletedQuestions.executeQuery();
						while(rs.next()) {
							Question q = new Question();
							q.fId = rs.getInt(1);
							q.name = rs.getString(2);
							questions.add(q);
						}
						if(questions.size() > 0) {
							qm.delete(sd, connectionRel, groupSurveyId, questions, false, false);
						}
					}
					response = Response.ok("").build();
					
				} catch (Exception e) {
					String msg = e.getMessage();
					if(msg != null && msg.contains("does not exist")) {
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
					try {if (pstmtRemoveChangeHistory != null) {pstmtRemoveChangeHistory.close();}} catch (SQLException e) {}
					try {if (pstmtGetSoftDeletedQuestions != null) {pstmtGetSoftDeletedQuestions.close();}} catch (SQLException e) {}

					SDDataSource.closeConnection("surveyKPI-SurveyResults", sd);
					ResultsDataSource.closeConnection("surveyKPI-SurveyResults", connectionRel);
				}
			}

			return response; 
		}
}

