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

package surveyMobileAPI;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.smap.managers.WebformManager;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.BlockedException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.JsonAuthorisationException;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.TempUserFinal;

/*
 * Return a survey as a webform
 */

@Path("/webForm")
public class WebForm extends Application {


	private static Logger log = Logger.getLogger(WebForm.class.getName());
	LogManager lm = new LogManager();		// Application log



	/*
	 * Get instance data Respond with JSON
	 */
	@GET
	@Path("/key/instance/{ident}/{updateid}/{key}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getInstanceJson(@Context HttpServletRequest request, @PathParam("ident") String formIdent,
			@PathParam("updateid") String updateid, // Unique id of instance data
			@PathParam("key") String authorisationKey) throws IOException {

		log.info("Requesting json instance");

		String userIdent = null;
		Response resp = null;
		String requester = "surveyMobileAPI-Webform";
		Connection sd = SDDataSource.getConnection(requester);

		try {
			userIdent = GeneralUtilityMethods.getDynamicUser(sd, authorisationKey);
			WebformManager wfm = new WebformManager("json", userIdent, false, "no", false, false);
			resp = wfm.getInstanceData(sd, request, formIdent, updateid, 0, false);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			SDDataSource.closeConnection(requester, sd);
		}

		if (userIdent == null) {
			log.info("User not found for key");
			throw new JsonAuthorisationException();
		}

		return resp;
	}

	/*
	 * Get instance data. Respond with JSON
	 * The data is identified by the form and the unique updateid for the record
	 * The JSON includes an instance XML as a string
	 */
	@GET
	@Path("/instance/{ident}/{updateid}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getInstanceJsonNoKey(
			@Context HttpServletRequest request, 
			@PathParam("ident") String formIdent,
			@PathParam("updateid") String updateid // Unique id of instance data
	) throws IOException {

		log.info("Requesting json instance no key");

		Response resp = null;
		String requester = "surveyMobileAPI-Webform";
		Connection sd = SDDataSource.getConnection(requester);

		try {
			String userIdent = request.getRemoteUser();
			log.info("Requesting instance as: " + userIdent);
			WebformManager wfm = new WebformManager("json", userIdent, false, "no", false, false);
			resp = wfm.getInstanceData(sd, request, formIdent, updateid, 0, true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			SDDataSource.closeConnection(requester, sd);
		}

		return resp;
	}
	
	/*
	 * Get task data
	 * The data is identified by the form and the unique updateid for the record
	 * The json includes an instance XML as a string
	 */
	@GET
	@Path("/instance/{ident}/task/{taskid}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getTaskDataJsonNoKey(
			@Context HttpServletRequest request, 
			@PathParam("ident") String formIdent,
			@PathParam("taskid") int taskId
	) throws IOException {

		log.info("Requesting json instance no key");

		Response resp = null;
		String requester = "surveyMobileAPI-Webform";
		Connection sd = SDDataSource.getConnection(requester);

		try {
			String userIdent = request.getRemoteUser();
			log.info("Requesting instance as: " + userIdent);
			WebformManager wfm = new WebformManager("json", userIdent, false, "no", false, false);
			resp = wfm.getInstanceData(sd, request, formIdent, null, taskId, true);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			SDDataSource.closeConnection(requester, sd);
		}

		return resp;
	}

	/*
	 * Get form data Respond with JSON
	 */
	@GET
	@Path("/key/{ident}/{key}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getFormJson(
			@Context HttpServletRequest request, 
			@PathParam("ident") String formIdent,
			@PathParam("key") String authorisationKey, 
			@QueryParam("datakey") String datakey, // Optional keys to instance data
			@QueryParam("datakeyvalue") String datakeyvalue, 
			@QueryParam("assignment_id") int assignmentId,
			@QueryParam("taskkey") int taskKey,	// Task id, if set initial data is from task
			@QueryParam("callback") String callback) throws IOException {

		Response response;
		
		log.info("Requesting json");

		String requester = "WebForm - getFormJson";
		Connection sd = SDDataSource.getConnection(requester);
		String userIdent = null;
		
		try {
			userIdent = GeneralUtilityMethods.getDynamicUser(sd, authorisationKey);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			SDDataSource.closeConnection(requester, sd);
		}

		if (userIdent == null) {
			log.info("User not found for key");
			throw new JsonAuthorisationException();
		}

		WebformManager wfm = new WebformManager("json", userIdent, false, "no", false, false);
		response = wfm.getWebform(request, "none", null, formIdent, datakey, datakeyvalue, 
				assignmentId, taskKey, callback, false, false, false, null);
		
		return response;
	}

	/*
	 * 
	 */
	@GET
	@Path("/{ident}")
	@Produces(MediaType.TEXT_HTML)
	public Response getFormHTML(@Context HttpServletRequest request, @PathParam("ident") String formIdent,
			@QueryParam("datakey") String datakey, // Optional keys to instance data
			@QueryParam("datakeyvalue") String datakeyvalue, 
			@QueryParam("assignment_id") int assignmentId,
			@QueryParam("taskkey") int taskKey,	// Task id, if set initial data is from task
			@QueryParam("viewOnly") boolean vo,
			@QueryParam("debug") String d,
			@QueryParam("app") boolean app,
			@QueryParam("callback") String callback) throws IOException {

		Response response = null;
		
		String mimeType = "html";
		if (callback != null) {
			// I guess they really want JSONP
			mimeType = "json";
		}

		String userIdent = request.getRemoteUser();
		
		/*
		 * Check to see if the assignment is already complete
		 */
		if(assignmentId > 0) {
			
			String requester = "WebForm - getFormHtml for assignment";
			Connection sd = SDDataSource.getConnection(requester);

			String status = null;
			try {
				status = GeneralUtilityMethods.getAssignmentCompletionStatus(sd, assignmentId);
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				SDDataSource.closeConnection(requester, sd);
			}

			if(status.equals(TaskManager.STATUS_T_SUBMITTED)) {
				response = getMessagePage(true, "mo_ss", null);
			} else if(status.equals(TaskManager.STATUS_T_DELETED)
					|| status.equals(TaskManager.STATUS_T_CANCELLED)) {
				response = getMessagePage(false, "mo_del_done", null);
			} else {
				log.info("Unknown status: " + status);
			}
			
		}
		
		if(response == null) {
			try {
				WebformManager wfm = new WebformManager(mimeType, userIdent, false, d, vo, app);
				response = wfm.getWebform(request, "none", null, 
						formIdent, datakey, datakeyvalue, assignmentId, 
						taskKey, callback,
						false, true, false, null);
			} catch (BlockedException e) {
				response = getMessagePage(false, "mo_blocked", null);
			}
		}
		
		return response;
	}

	/*
	 * Respond with HTML 
	 * Called by Temporary User
	 */
	//
	@GET
	@Path("/id/{temp_user}/{ident}")
	@Produces(MediaType.TEXT_HTML)
	public Response getFormHTMLTemporaryUser(
			@Context HttpServletRequest request, 
			@PathParam("ident") String formIdent,
			@PathParam("temp_user") String tempUser, 
			@QueryParam("datakey") String datakey, // Optional keys to instance data
			@QueryParam("datakeyvalue") String datakeyvalue, 
			@QueryParam("assignment_id") int assignmentId,
			@QueryParam("taskkey") int taskKey,	// Task id, if set initial data is from task
			@QueryParam("viewOnly") boolean vo,
			@QueryParam("debug") String d,
			@QueryParam("callback") String callback) throws IOException {

		String mimeType = "html";
		if (callback != null) {
			// I guess they really want JSONP
			mimeType = "json";
		}
		
		String userIdent = tempUser;
		WebformManager wfm = new WebformManager(mimeType, userIdent, true, d, vo, false);
		return wfm.getWebform(request, "none", null, formIdent, datakey, datakeyvalue, assignmentId, 
				taskKey, callback, false,
				true, false, null);
	}

	/*
	 * Respond with HTML 
	 * Called by Temporary User to complete a task
	 */
	//
	@GET
	@Path("/action/{ident}")
	@Produces(MediaType.TEXT_HTML)
	public Response getFormHTMLTemporaryUser(
			@Context HttpServletRequest request, 
			@QueryParam("debug") String d,
			@PathParam("ident") String ident) throws Exception {

		Response response = null;
		
		String userIdent = ident;
		String mimeType = "html";
		String requester = "surveyMobileAPI-webform task";
		
		Connection sd = SDDataSource.getConnection(requester);

		Action a = null;
		
		try {

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			// 1. Get details on the action to be performed using the user credentials
			ActionManager am = new ActionManager(localisation, "UTC");
			a = am.getAction(sd, userIdent);

			// 2. If temporary user does not exist then report the issue to the user
			if (a == null) {
				
				boolean success = false;
				String message = "mo_nf";
				
				TempUserFinal tuf = GeneralUtilityMethods.getTempUserFinal(sd,userIdent);
				
				if(tuf != null) {
					if(tuf.status.equals(UserManager.STATUS_COMPLETE)) {
						success = true;
						message = "mo_ss";
					} else if(tuf.status.equals(UserManager.STATUS_EXPIRED)) {
						success = false;
						message = "mo_exp";
					}
				} 
				response = getMessagePage(success, message, null);
			} else if(!a.action.equals("task") && !a.action.equals("mailout")) {
				response = getMessagePage(false, "mo_se", "Invalid action type: " + a.action);
			} else {

				// 3. Get webform
				userIdent = ident;
				try {
					WebformManager wfm = new WebformManager(mimeType, userIdent, true, d, false, false);
					response = wfm.getWebform(request, a.action, a.email, 
							a.surveyIdent, a.datakey, a.datakeyvalue, a.assignmentId, a.taskKey, 
							null, 
							false,
							true, 
							true,			// Close after saving
							a.initialData
							);
					
				} catch (BlockedException e) {
					response = getMessagePage(false, "mo_blocked", null);
				}
			}
		
		} catch (AuthorisationException e) {
			response = getMessagePage(false, "mo_na", null);
		} catch (Exception e) {
			response = getMessagePage(false, "mo_se", e.getMessage());
		} finally {
			SDDataSource.closeConnection(requester, sd);
		}
		
		return response;
		
	}
	





	private Response getMessagePage(boolean success, String msg, String systemError) {
		Response response = null;

		StringBuffer output = new StringBuffer();
		
		// Generate the page
		try {

			output.append("<!doctype html>");
			output.append("<html class='no-js' lang='en'>");
			output.append("<head>");
			output.append("<meta name='keywords' content='' />");
			output.append("<meta name='description' content='' />");
			output.append("<meta http-equiv='content-type' content='text/html; charset=utf-8' />");
			output.append("<meta name='viewport' content='width=device-width, initial-scale=1.0'>");
			output.append("<title class='lang' data-lang='c_msg'></title>");
			output.append("<link rel='shortcut icon' href='/favicon.ico' />");
			output.append("<link rel='stylesheet' href='/css/normalize.css' />");
			output.append("<link href='/css/bootstrap.v4.min.css' rel='stylesheet'>");
			output.append("<link href='/font-awesome/css/font-awesome.css' rel='stylesheet'>");

			output.append("<script src='/js/libs/modernizr.js'></script>");
			output.append("<script data-main='/js/msg' src='/js/libs/require.js'></script>");

			output.append("<style>");

			output.append(
				".success {"
					+ "color: green;"
					+ "text-align: center;"
					+ "margin-top: 100px;"
					+ "margin-bottom: 50px;"
					+ "font-size: 86px;"
				+ "}"
				+ ".failed {"
					+ "color: red;"
					+ "text-align: center;"
					+ "margin-top: 100px;"
					+ "margin-bottom: 50px;"
					+ "font-size: 86px;"
				+ "}"
				+ ".msg {"
					+ "text-align: center;"
					+ "margin-top: 50px;"
					+ "margin-bottom: 50px;"
				+ "}"
				+ ".system_msg {"
					+ "text-align: center;"
					+ "border-style: solid;"
					+ "border-width: 1px;"
					+ "margin-top: 50px;"
					+ "margin-bottom: 50px;"
					+ "margin-left: 100px;"
					+ "margin-right: 100px;"
				+ "}"
				);
			
			output.append("</style>");
			output.append("</head>");
			output.append("<body>");

			// Add icon
			output.append("<h1 class='");
			if(success) {
				output.append("success");
			} else {
				output.append("failed");
			}
			output.append("'><i class='fa ");
			if(success) {
				output.append("fa-check");
			} else {
				output.append("fa-times");
			}
			output.append("'></i></h1>");

			// Add msg
			output.append("<h1 class='msg lang' data-lang='");
			output.append(msg);
			output.append("'></h1>");
			
			if(systemError != null) {
				output.append("<p class='system_msg'>");
				output.append(systemError);
				output.append("</p>");
			}

			output.append("</body>");
			output.append("</html>");
			
			response = Response.status(Status.OK).entity(output.toString()).build();

		} catch (Exception e) {
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
			log.log(Level.SEVERE, e.getMessage(), e);
		}

		return response;
	}
}
