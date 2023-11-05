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
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.SDDataSource;

import model.GeomQuestions;
import model.Settings;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/dashboard")
public class Dashboard extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(Dashboard.class.getName());
	
	public Dashboard() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.VIEW_DATA);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get the dashboard settings
	 * The survey id is obtained from the survey table based on the survey name stored in the
	 *  dashboard_settings table
	 */
	@GET
	@Path("/{projectId}")
	@Produces("application/json")
	public Response getSettings(@Context HttpServletRequest request,
			@PathParam("projectId") int projectId 
			) {
		
		Response response = null;
		 		
		// Authorisation - Access
		String connectionString = "surveyKPI-Dashboard-get";
		Connection sd = SDDataSource.getConnection(connectionString);
		// No check for valid user as only panels owned by a user are returned
		a.isValidProject(sd, request.getRemoteUser(), projectId);
		// End Authorisation
		
		ArrayList<Settings> sArray = new ArrayList<Settings> ();

		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		try  {

			String sql = "select "
					+ "d.ds_id as id,"
					+ "d.ds_seq as seq,"
					+ "d.ds_state as state,"
					+ "d.ds_title as title,"
					+ "d.ds_s_id as sId,"
					+ "d.ds_u_id as uId,"
					+ "d.ds_s_name as sName,"
					+ "d.ds_type as type,"
					+ "d.ds_layer_id as layerId,"
					+ "d.ds_region as region,"
					+ "d.ds_lang as lang,"
					+ "d.ds_qname as qname,"
					+ "d.ds_date_question_name as dateQuestionName,"
					+ "d.ds_question as question,"
					+ "d.ds_fn as fn,"
					+ "d.ds_table as table, "
					+ "d.ds_key_words as key_words, "
					+ "d.ds_q1_function as q1_function, "
					+ "d.ds_group_question_name as groupQuestionName, "
					+ "d.ds_group_question_text as groupQuestionText, "
					+ "d.ds_group_type as groupType, "
					+ "d.ds_time_group as timeGroup, "
					+ "d.ds_from_date as fromDate, "
					+ "d.ds_to_date as toDate, "
					+ "d.ds_q_is_calc as qId_is_calc, "
					+ "d.ds_filter as filter, "
					+ "d.ds_advanced_filter as advanced_filter, "
					+ "d.ds_subject_type as subject_type, "
					+ "d.ds_inc_ro as inc_ro,"
					+ "d.ds_geom_questions as geom_questions, "
					+ "d.ds_selected_geom_question as selected_geom_question "
					+ "from dashboard_settings d, users u, user_project up, survey s "
					+ "where u.id = up.u_id "
					+ "and up.p_id = ? "
					+ "and s.p_id = up.p_id "
					+ "and s.s_id = d.ds_s_id "
					+ "and u.ident = d.ds_user_ident " 	// Restrict to owning user
					+ "and u.ident = ? "
					+ "and ds_subject_type = 'survey' "
					+ "order by ds_seq asc";
			
			String sqlUser = "select "
					+ "d.ds_id as id,"
					+ "d.ds_seq as seq,"
					+ "d.ds_state as state,"
					+ "d.ds_title as title,"
					+ "d.ds_s_id as sId,"
					+ "d.ds_u_id as uId,"
					+ "d.ds_s_name as sName,"
					+ "d.ds_type as type,"
					+ "d.ds_layer_id as layerId,"
					+ "d.ds_region as region,"
					+ "d.ds_lang as lang,"
					+ "d.ds_qname as qname,"
					+ "d.ds_date_question_name as dateQuestionName,"
					+ "d.ds_question as question,"
					+ "d.ds_fn as fn,"
					+ "d.ds_table as table, "
					+ "d.ds_key_words as key_words, "
					+ "d.ds_q1_function as q1_function, "
					+ "d.ds_group_question_name as groupQuestionName, "
					+ "d.ds_group_question_text as groupQuestionText, "
					+ "d.ds_group_type as groupType, "
					+ "d.ds_time_group as timeGroup, "
					+ "d.ds_from_date as fromDate, "
					+ "d.ds_to_date as toDate, "
					+ "d.ds_q_is_calc as qId_is_calc, "
					+ "d.ds_filter as filter, "
					+ "d.ds_advanced_filter as advanced_filter, "
					+ "d.ds_subject_type as subject_type,"
					+ "d.ds_inc_ro as inc_ro, "
					+ "d.ds_geom_questions as geom_questions, "
					+ "d.ds_selected_geom_question as selected_geom_question "
					+ "from dashboard_settings d "
					+ "where d.ds_user_ident = ? "
					+ "and d.ds_subject_type = 'user' "
					+ "and d.ds_u_id in (select id from users where o_id = ?) "
					+ "order by ds_seq asc";
			
			String sqlUserLocation = "select "
					+ "d.ds_id as id,"
					+ "d.ds_seq as seq,"
					+ "d.ds_state as state,"
					+ "d.ds_title as title,"
					+ "d.ds_s_id as sId,"
					+ "d.ds_u_id as uId,"
					+ "d.ds_s_name as sName,"
					+ "d.ds_type as type,"
					+ "d.ds_layer_id as layerId,"
					+ "d.ds_region as region,"
					+ "d.ds_lang as lang,"
					+ "d.ds_qname as qname,"
					+ "d.ds_date_question_name as dateQuestionName,"
					+ "d.ds_question as question,"
					+ "d.ds_fn as fn,"
					+ "d.ds_table as table, "
					+ "d.ds_key_words as key_words, "
					+ "d.ds_q1_function as q1_function, "
					+ "d.ds_group_question_name as groupQuestionName, "
					+ "d.ds_group_question_text as groupQuestionText, "
					+ "d.ds_group_type as groupType, "
					+ "d.ds_time_group as timeGroup, "
					+ "d.ds_from_date as fromDate, "
					+ "d.ds_to_date as toDate, "
					+ "d.ds_q_is_calc as qId_is_calc, "
					+ "d.ds_filter as filter, "
					+ "d.ds_advanced_filter as advanced_filter, "
					+ "d.ds_subject_type as subject_type,"
					+ "d.ds_inc_ro as inc_ro,"
					+ "d.ds_geom_questions as geom_questions,"
					+ "d.ds_selected_geom_question as selected_geom_question "
					+ "from dashboard_settings d "
					+ "where d.ds_user_ident = ? "
					+ "and d.ds_subject_type = 'user_locations' ";
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			
			// Add Survey panels
			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, projectId);
			pstmt.setString(2, request.getRemoteUser());

			int idx = 0;
			log.info("Get survey panels: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			while (resultSet.next()) {				
				Settings s = getSettings(sd, resultSet, idx++);
				sArray.add(s);	
			}
			resultSet.close();
			
			// Add  user panels
			try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
			pstmt = sd.prepareStatement(sqlUser);	
			pstmt.setString(1, request.getRemoteUser());
			pstmt.setInt(2, oId);

			log.info("Get user panels: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			while (resultSet.next()) {				
				Settings s = getSettings(sd,resultSet, idx++);
				sArray.add(s);	
			}
			resultSet.close();
			
			// Add  user location panel
			try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
			pstmt = sd.prepareStatement(sqlUserLocation);	
			pstmt.setString(1, request.getRemoteUser());

			log.info("Get user location panels: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			while (resultSet.next()) {				
				Settings s = getSettings(sd, resultSet, idx++);
				sArray.add(s);	
			}
			resultSet.close();

			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(sArray);
			response = Response.ok(resp).build();
			
		} catch (SQLException e) {
		    log.log(Level.SEVERE, "SQL Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;
	}
	
	/*
	 * Update the settings
	 */
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response updateSettings(
			@Context HttpServletRequest request, 
			@FormParam("settings") String settings
			) { 
		
		Response response = null;
		
		String user = request.getRemoteUser();
		log.info("Settings:" + settings);
		
		Type type = new TypeToken<ArrayList<Settings>>(){}.getType();
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		ArrayList<Settings> sArray = gson.fromJson(settings, type);
		
		PreparedStatement pstmtDel = null;
		PreparedStatement pstmtDelView = null;
		PreparedStatement pstmtAddView = null;
		PreparedStatement pstmtReplaceView = null;
		
		// Authorisation not required as dashboard settings are specific to each authenticated user
		String connectionString = "surveyKPI-Dashboard-updateSettings";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
	
			sd.setAutoCommit(false);
			
			String sqlDelView = "delete from dashboard_settings " +
					"where (ds_id = ? or ds_layer_id = ?) " +
					"and ds_user_ident = ?;";	
			pstmtDelView = sd.prepareStatement(sqlDelView);	
			
			String sqlAddView = "insert into dashboard_settings("
					+ "ds_state, ds_seq, ds_title, ds_s_id, ds_s_name, ds_type, ds_layer_id, ds_region,"
					+ "ds_lang, ds_qname, ds_date_question_name, ds_question, ds_fn, ds_table, ds_key_words, ds_q1_function, "
					+ "ds_group_question_name, ds_group_question_text, ds_group_type, ds_user_ident, ds_time_group,"
					+ "ds_from_date, ds_to_date, ds_q_is_calc, ds_filter, ds_advanced_filter, ds_subject_type, ds_u_id,"
					+ "ds_inc_ro, ds_geom_questions, ds_selected_geom_question) values ("
					+ "?, ?, ?, ?, ?, ?, ?, ?, ? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";		
			pstmtAddView = sd.prepareStatement(sqlAddView);	 			
			
			String sqlReplaceView = "update dashboard_settings set " +
					" ds_state = ?," +
					" ds_seq = ?," +
					" ds_title = ?," + 
					" ds_s_id = ?," +
					" ds_s_name = ?," +
					" ds_type = ?," +
					" ds_layer_id = ?," +
					" ds_region = ?," +
					" ds_lang = ?," +
					" ds_qname = ?," +
					" ds_date_question_name = ?," +
					" ds_question = ?," +
					" ds_fn = ?," +
					" ds_table = ?," +
					" ds_key_words = ?," +
					" ds_q1_function = ?," +
					" ds_group_question_name = ?," +
					" ds_group_question_text = ?," +
					" ds_group_type = ?," +  
					" ds_time_group = ?," +
					" ds_from_date = ?," +
					" ds_to_date = ?," +
					" ds_q_is_calc = ?," +
					" ds_filter = ?," +
					" ds_advanced_filter = ?," +
					" ds_subject_type = ?, " +
					" ds_u_id = ?, " +
					" ds_inc_ro = ?," +
					" ds_geom_questions = ?, " +
					" ds_selected_geom_question = ? " +
					" where ds_id = ? " +
					" and ds_user_ident = ?;";						
			pstmtReplaceView = sd.prepareStatement(sqlReplaceView);
			
			for(Settings s : sArray) {
				
				if(s.state != null) {
					
					//========================== Delete the old view
					
					if(s.id != -1 && s.state.equals("deleted")) {

						pstmtDelView.setInt(1, s.id);
						pstmtDelView.setInt(2, s.id);
						pstmtDelView.setString(3, user);
						log.info("Delete view: " + pstmtDelView.toString());
						pstmtDelView.executeUpdate();
					
					} else if(s.id == -1) {
						
						//==========================  Insert the new view 
						pstmtAddView.setString(1, s.state);
						pstmtAddView.setInt(2, s.seq);
						pstmtAddView.setString(3, HtmlSanitise.checkCleanName(s.title, localisation));
						pstmtAddView.setInt(4, s.sId);
						pstmtAddView.setString(5, s.sName);
						pstmtAddView.setString(6, s.type);
						pstmtAddView.setInt(7, s.layerId);
						pstmtAddView.setString(8, s.region);
						pstmtAddView.setString(9, s.lang);
						pstmtAddView.setString(10, GeneralUtilityMethods.getQuestionNameFromId(sd, s.sId, s.qId));
						pstmtAddView.setString(11, GeneralUtilityMethods.getQuestionNameFromId(sd, s.sId, s.dateQuestionId));
						pstmtAddView.setString(12, s.question);
						pstmtAddView.setString(13, s.fn);
						pstmtAddView.setString(14, s.table);
						pstmtAddView.setString(15, s.key_words);
						pstmtAddView.setString(16, s.q1_function);
						pstmtAddView.setString(17, GeneralUtilityMethods.getQuestionNameFromId(sd, s.sId, s.groupQuestionId));
						pstmtAddView.setString(18, s.groupQuestionText);
						pstmtAddView.setString(19, s.groupType);
						pstmtAddView.setString(20, user);
						pstmtAddView.setString(21, s.timeGroup);
						pstmtAddView.setDate(22, s.fromDate);
						pstmtAddView.setDate(23, s.toDate);
						pstmtAddView.setBoolean(24, s.qId_is_calc);
						pstmtAddView.setString(25, s.filter);
						pstmtAddView.setString(26, s.advanced_filter);
						pstmtAddView.setString(27, s.subject_type);
						pstmtAddView.setInt(28, s.uId);
						pstmtAddView.setBoolean(29, s.inc_ro);
						
						String gQuestions = null;
						if(s.geomFormQuestions != null) {
							gQuestions = gson.toJson(s.geomFormQuestions);
						}
						pstmtAddView.setString(30, gQuestions);
						pstmtAddView.setString(31, s.selectedGeomQuestion);
						
						log.info("Add view: " + pstmtAddView.toString());
						pstmtAddView.executeUpdate();		

					} else {
						
						//==========================  Update the existing view
						pstmtReplaceView.setString(1, s.state);
						pstmtReplaceView.setInt(2, s.seq);
						pstmtReplaceView.setString(3, HtmlSanitise.checkCleanName(s.title, localisation));
						pstmtReplaceView.setInt(4, s.sId);
						pstmtReplaceView.setString(5, s.sName);
						pstmtReplaceView.setString(6, s.type);
						pstmtReplaceView.setInt(7, s.layerId);
						pstmtReplaceView.setString(8, s.region);
						pstmtReplaceView.setString(9, s.lang);
						pstmtReplaceView.setString(10, GeneralUtilityMethods.getQuestionNameFromId(sd, s.sId, s.qId));
						pstmtReplaceView.setString(11, GeneralUtilityMethods.getQuestionNameFromId(sd, s.sId, s.dateQuestionId));
						pstmtReplaceView.setString(12, s.question);
						pstmtReplaceView.setString(13, s.fn);
						pstmtReplaceView.setString(14, s.table);
						pstmtReplaceView.setString(15, s.key_words);
						pstmtReplaceView.setString(16, s.q1_function);
						pstmtReplaceView.setString(17, GeneralUtilityMethods.getQuestionNameFromId(sd, s.sId, s.groupQuestionId));
						pstmtReplaceView.setString(18, s.groupQuestionText);
						pstmtReplaceView.setString(19, s.groupType);
						pstmtReplaceView.setString(20, s.timeGroup);
						pstmtReplaceView.setDate(21, s.fromDate);
						pstmtReplaceView.setDate(22, s.toDate);
						pstmtReplaceView.setBoolean(23, s.qId_is_calc);
						pstmtReplaceView.setString(24, s.filter);
						pstmtReplaceView.setString(25, s.advanced_filter);
						pstmtReplaceView.setString(26, s.subject_type);
						pstmtReplaceView.setInt(27, s.uId);
						pstmtReplaceView.setBoolean(28, s.inc_ro);
						
						String gQuestions = null;
						if(s.geomFormQuestions != null) {
							gQuestions = gson.toJson(s.geomFormQuestions);
						}
						pstmtReplaceView.setString(29, gQuestions);
						pstmtReplaceView.setString(30, s.selectedGeomQuestion);
						
						pstmtReplaceView.setInt(31, s.id);
						pstmtReplaceView.setString(32, user);
						
						log.info("Update view: " + pstmtReplaceView.toString());
						pstmtReplaceView.executeUpdate();
					}
					
				}
				
			}
			sd.commit();
			response = Response.ok().build();
				
		} catch (Exception e) {
			try { sd.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			
			try {if (pstmtDelView != null) {pstmtDelView.close();}} catch (SQLException e) {}
			try {if (pstmtAddView != null) {pstmtAddView.close();}} catch (SQLException e) {}
			try {if (pstmtReplaceView != null) {pstmtReplaceView.close();}} catch (SQLException e) {}
			try {if (pstmtDel != null) {pstmtDel.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}

	/*
	 * Update the state of the panel
	 */
	@POST
	@Path("/state")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response updateState(@Context HttpServletRequest request, 
			@FormParam("state") String stateString) { 
		
		Response response = null;
		
		// Authorisation - Access
		String connectionString = "surveyKPI-Dashboard-updateState";
		Connection sd = SDDataSource.getConnection(connectionString);
		// End Authorisation
		
		PreparedStatement pstmt = null;

		try {
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			Settings s = gson.fromJson(stateString, Settings.class);
			
			String sql;
			String user = request.getRemoteUser();

			if(s.state != null && !s.state.equals("deleted")) {
				
				sql = "update dashboard_settings set ds_state = ? " +
						"where ds_id = ? " +
						"and ds_user_ident = ?;";
				log.info(sql + " : " + s.state + " : " + s.id + " : " + user);
				
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, s.state);
				pstmt.setInt(2, s.id);
				pstmt.setString(3, user);
				pstmt.executeUpdate();

			} else if (s.state != null && s.state.equals("deleted")) {
				sql = "delete from dashboard_settings " +
						"where (ds_id = ? or (ds_layer_id != -1 and ds_layer_id = ?)) " +
						"and ds_user_ident = ?;";

				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, s.id);
				pstmt.setInt(2, s.id);
				pstmt.setString(3, user);
				log.info("delete panels: " + pstmt.toString());
				pstmt.executeUpdate();
				
			}
			
			response = Response.ok().build();
				
		} catch (Exception e) {
			response = Response.serverError().build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}

	private Settings getSettings(Connection sd, ResultSet resultSet, int idx) throws SQLException {
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		// Create the new Dashboard object
		Settings s = new Settings();

		// Populate the new Dashboard settings
		s.id = resultSet.getInt("id");
		s.seq = resultSet.getInt("seq");
		s.state = resultSet.getString("state");
		s.title = resultSet.getString("title");
		s.pId = idx;	// panel id
		s.sId = resultSet.getInt("sId");
		s.uId = resultSet.getInt("uId");
		s.sName = resultSet.getString("sName");
		s.type = resultSet.getString("type");
		s.layerId = resultSet.getInt("layerId");
		s.region = resultSet.getString("region");
		s.lang = resultSet.getString("lang");
		s.qId = GeneralUtilityMethods.getQuestionIdFromName(sd, s.sId, resultSet.getString("qname"));
		s.dateQuestionId = GeneralUtilityMethods.getQuestionIdFromName(sd, s.sId, resultSet.getString("dateQuestionName"));
		s.question = resultSet.getString("question");
		s.fn = resultSet.getString("fn");
		s.table = resultSet.getString("table");
		s.key_words = resultSet.getString("key_words");
		s.q1_function = resultSet.getString("q1_function");
		s.groupQuestionId = GeneralUtilityMethods.getQuestionIdFromName(sd, s.sId, resultSet.getString("groupQuestionName"));
		s.groupQuestionText = resultSet.getString("groupQuestionText");
		s.groupType = resultSet.getString("groupType");
		s.timeGroup = resultSet.getString("timeGroup");
		s.fromDate = resultSet.getDate("fromDate");
		s.toDate = resultSet.getDate("toDate");
		s.qId_is_calc = resultSet.getBoolean("qId_is_calc");
		s.filter = resultSet.getString("filter");
		s.advanced_filter = resultSet.getString("advanced_filter");
		s.subject_type = resultSet.getString("subject_type");
		s.inc_ro = resultSet.getBoolean("inc_ro");
		s.selectedGeomQuestion = resultSet.getString("selected_geom_question");
		
		String gQuestions = resultSet.getString("geom_questions");
		if(gQuestions != null) {
			try {
				s.geomFormQuestions = gson.fromJson(gQuestions, new TypeToken<GeomQuestions []>() {}.getType());
			} catch (Exception e) {}
		} 
	
		return s;
	}
}

