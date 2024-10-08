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

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.MessagingManager;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.model.Notification;
import org.smap.sdal.model.NotifyDetails;
import org.smap.sdal.model.SubmissionMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/*
 * Get the notifications set for this project
 */

@Path("/notifications")
public class NotificationList extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(NotificationList.class.getName());
	
	private Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

	public NotificationList() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);		// Enumerators with MANAGE access can process managed forms
		a = new Authorise(authorisations, null);		
	}
	
	@Path("/{projectId}")
	@GET
	public Response getNotifications(@Context HttpServletRequest request,
			@PathParam("projectId") int projectId,
			@QueryParam("tz") String tz) throws Exception { 
		
		Response response = null;
		String connectionString = "surveyKPI-Notifications";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidProject(sd, request.getRemoteUser(), projectId);
		// End Authorisation
		
		if(tz == null) {
			tz = "UTC";
		}
		
		PreparedStatement pstmt = null;
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			NotificationManager nm = new NotificationManager(localisation);
			ArrayList<Notification> nList = nm.getProjectNotifications(sd, pstmt, request.getRemoteUser(), projectId, tz);
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(nList);
			response = Response.ok(resp).build();
			
		} catch (SQLException e) {
			log.log(Level.SEVERE,"No data available", e);
		    response = Response.serverError().entity("No data available").build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
	
	@Path("/types")
	@GET
	public Response getTypes(@Context HttpServletRequest request,
			@QueryParam("page") String page) { 
		
		Response response = null;
		String connectionString = "surveyKPI-NotificationList-getTypes";
		
		// No Authorisation required
		
		Connection sd = SDDataSource.getConnection(connectionString);	
		
		if(page == null) {
			page = "notifications";
		}
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			NotificationManager fm = new NotificationManager(localisation);
			ArrayList<String> tList = fm.getNotificationTypes(sd, request.getRemoteUser(), page);
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(tList);
			response = Response.ok(resp).build();
			
		} catch (SQLException e) {
			log.log(Level.SEVERE,"No data available", e);
		    response = Response.serverError().entity("No data available").build();
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
	
	/*
	 * Add a notification
	 */
	@Path("/add")
	@POST
	public Response addNotification(@Context HttpServletRequest request,
			@FormParam("notification") String notificationString,
			@QueryParam("tz") String tz) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-Survey - add notification";
		
		Type type = new TypeToken<Notification>(){}.getType();
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		Notification n = gson.fromJson(notificationString, type);
		
		log.info("Add Notification:========== " + notificationString);
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		if(n.s_id > 0) {
			a.isValidSurvey(sd, request.getRemoteUser(), n.s_id, false, superUser);
		}
		if(n.p_id > 0) {
			a.isValidProject(sd, request.getRemoteUser(), n.p_id);
		}
		// End Authorisation
		
		PreparedStatement pstmt = null;
		
		if(tz == null) {
			tz = "UTC";
		}
		
		try {	
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			NotificationManager nm = new NotificationManager(localisation);
 
			nm.addNotification(sd, pstmt, request.getRemoteUser(), n, tz);
			
			response = Response.ok().build();
			
		} catch (SQLException e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"SQL Exception", e);
		} catch (AuthorisationException e) {
			log.info("Authorisation Exception");
		    response = Response.serverError().entity("Not authorised").build();
		} catch (ApplicationException e) {
		    response = Response.serverError().entity(e.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
	
	@Path("/update")
	@POST
	public Response updateNotification(@Context HttpServletRequest request,
			@FormParam("notification") String notificationString,
			@QueryParam("tz") String tz) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-Survey-update notification";
		
		Type type = new TypeToken<Notification>(){}.getType();
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		Notification n = gson.fromJson(notificationString, type);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		if(n.s_id > 0) {
			a.isValidSurvey(sd, request.getRemoteUser(), n.s_id, false, superUser);
		}
		if(n.p_id > 0) {
			a.isValidProject(sd, request.getRemoteUser(), n.p_id);
		}
		// End Authorisation
		
		log.info("Update notification for survey: " + request.getRemoteUser() + " : "+ n.s_id + " Remote s_id: " + 
				n.remote_s_ident + " Email Question: " + n.notifyDetails.emailQuestionName );
		
		PreparedStatement pstmt = null;
		
		if(tz == null) {
			tz = "UTC";
		}
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			NotificationManager nm = new NotificationManager(localisation);
			nm.updateNotification(sd, pstmt, request.getRemoteUser(), n, tz);	
			response = Response.ok().build();
			
		} catch (SQLException e) {
			log.log(Level.SEVERE,"SQL Exception", e);
		    response = Response.serverError().entity("SQL Error: " + e.getMessage()).build();
		} catch (AuthorisationException e) {
			log.info("Authorisation Exception");
		    response = Response.serverError().entity("Not authorised").build();
		} catch (ApplicationException e) {
		    response = Response.serverError().entity(e.getMessage()).build();
		} catch (Exception e) {	
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
	
	@Path("/{id}")
	@DELETE
	public Response deleteForward(@Context HttpServletRequest request,
			@PathParam("id") int id) { 
		
		Response response = null;
		String connectionString = "surveyKPI-Survey-delete notification";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		PreparedStatement pstmt = null;
		boolean superUser = false;
		
		// Data level authorisation, is the user authorised to delete this forward
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		
			
			ResultSet resultSet = null;
			String sql = "select s_id, p_id " +
					" from forward " +
					" where id = ?";
			
			pstmt = sd.prepareStatement(sql);	
			pstmt.setInt(1, id);	
			log.info("Delete forward, validate survey: " + pstmt.toString());

			resultSet = pstmt.executeQuery();
			if(resultSet.next()) {
				int sId = resultSet.getInt(1);
				int pId = resultSet.getInt(2);
				
				if(sId > 0) {
					superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
					a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
				} else {
					a.isValidProject(sd, request.getRemoteUser(), pId);
				}
				NotificationManager fm = new NotificationManager(localisation);
				fm.deleteNotification(sd, request.getRemoteUser(), id, sId);
			}
			
			response = Response.ok().build();
			
		} catch (SQLException e) {
			log.log(Level.SEVERE,"SQL Exception", e);
		    response = Response.serverError().entity("SQL Error").build();
		} catch (AuthorisationException e) {
			log.info("Authorisation Exception");
		    response = Response.serverError().entity("Not authorised").build();
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}
	
	/*
	 * send an immediate notification
	 */
	@Path("/immediate")
	@POST
	public Response immediateNotification(
			@Context HttpServletRequest request,
			@FormParam("notification") String notificationString) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-Survey-send immediate notification";
		
		Notification n = gson.fromJson(notificationString, Notification.class);
		
		log.info("Immediate Notification:========== " + notificationString);
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		if(n.s_id > 0) {
			a.isValidSurvey(sd, request.getRemoteUser(), n.s_id, false, superUser);
		} else {
			a.isValidSurveyIdent(sd, request.getRemoteUser(), n.sIdent, false, superUser);
		}
		// End Authorisation
		
		PreparedStatement pstmt = null;
		
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		try {	
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
					
			NotifyDetails nd = n.notifyDetails;
			int pId = GeneralUtilityMethods.getProjectIdFromSurveyIdent(sd, n.sIdent);
			String scheme = request.getScheme();
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			SubmissionMessage subMsg = new SubmissionMessage(
					"Immediate",		// Title
					0,					// Task Id - ignore, only relevant for a reminder
					pId,
					n.sIdent,			// Survey Ident
					null,				// Update Survey
					n.instanceId, 
					nd.from,
					nd.subject, 
					nd.content,
					nd.attach,
					nd.include_references,
					nd.launched_only,
					nd.emailQuestion,
					nd.emailQuestionName,
					nd.emailMeta,
					nd.emailAssigned,
					nd.emails,
					n.target,
					request.getRemoteUser(),
					scheme,
					nd.callback_url,
					n.remote_user,
					n.remote_password,
					0,			// Use default pdfTemplateId TODO make this selectable
					nd.survey_case,
					nd.assign_question,
					null,				// Report Period
					0,					// report id
					nd.ourNumber,
					nd.msgChannel,
					new Timestamp(new java.util.Date().getTime()));
			MessagingManager mm = new MessagingManager(localisation);
			mm.createMessage(sd, oId, NotificationManager.TOPIC_SUBMISSION, "", gson.toJson(subMsg));
			
			response = Response.ok().build();
			
		} catch (SQLException e) {
			response = Response.serverError().entity("SQL Error").build();
			log.log(Level.SEVERE,"SQL Exception", e);
		} catch (AuthorisationException e) {
			log.info("Authorisation Exception");
		    response = Response.serverError().entity("Not authorised").build();
		} catch (Exception e) {
			String msg = e.getMessage();
			log.log(Level.SEVERE,"Error", e);
		    response = Response.serverError().entity(msg).build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
			
		}

		return response;

	}

}

