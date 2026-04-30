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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.servlet.http.HttpSession;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import javax.xml.transform.TransformerFactoryConfigurationError;

import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.BlockedException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.JsonAuthorisationException;
import org.smap.sdal.Utilities.NotFoundException;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.legacy.GetHtml;
import org.smap.sdal.legacy.SurveyTemplate;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.managers.OrganisationManager;
import org.smap.sdal.managers.PeopleManager;
import org.smap.sdal.managers.SMSManager;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.managers.TranslationManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.ManifestValue;
import org.smap.sdal.model.SMSNumber;
import org.smap.sdal.model.ServerData;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.SurveyData;
import org.smap.sdal.model.TempUserFinal;
import org.smap.sdal.model.WebformOptions;
import org.smap.server.utilities.GetXForm;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import surveyMobileAPI.managers.WebFormManager;

/*
 * Return a survey as a webform
 */

@Path("/webForm")
public class WebForm extends Application {

	Authorise a = null;
	Authorise aConsole = null;

	private static Logger log = Logger.getLogger(WebForm.class.getName());
	LogManager lm = new LogManager();		// Application log

	private HtmlSanitise sanitise = new HtmlSanitise();
	
	public WebForm() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ENUM);
		a = new Authorise(authorisations, null);
		
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ENUM);
		authorisations.add(Authorise.MANAGE);
		aConsole = new Authorise(authorisations, null);
		
	}
	
	class JsonResponse {
		List<ManifestValue> manifestList;
		SurveyData surveyData = new SurveyData();
		String main;
	}
	
	// Globals
	private ServerData serverData = null;
	private List<ManifestValue> manifestList = null;
	private String mimeType = null;
	private JsonResponse jr = null;
	SurveyTemplate template = null;
	ResourceBundle localisation = null;
	String tz = "UTC";
	Locale locale = null;
	boolean viewOnly = false;
	String userIdent = null;
	boolean isTemporaryUser = false;
	HashMap<String, Integer> gRecordCounts = null;
	private WebformOptions options;
	String debug = "no";
	boolean isApp = false;
	boolean myWork = false;			// When set use the myWork app to submit data rather than the form
	boolean single = false;
	boolean showDonePage = false;
	boolean showFormIndex = false;
	String gFormIdent = null;
	boolean requiresTurnstile = false;
	ArrayList<String> gNotificationTypes = new ArrayList<>();
	ArrayList<SMSNumber> gOurNumbers = new ArrayList<>();

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

		Response resp = null;
		String requester = "surveyMobileAPI-Webform";
		Connection sd = SDDataSource.getConnection(requester);

		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			userIdent = GeneralUtilityMethods.getDynamicUser(sd, authorisationKey);
			WebFormManager wfm = new WebFormManager(localisation, "UTC");
			resp = wfm.getInstanceData(sd, request, formIdent, updateid, 0, userIdent, false);
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
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			userIdent = request.getRemoteUser();
			log.info("Requesting instance as: " + userIdent);
			WebFormManager wfm = new WebFormManager(localisation, "UTC");
			resp = wfm.getInstanceData(sd, request, formIdent, updateid, 0, userIdent, true);
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
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			userIdent = request.getRemoteUser();
			log.info("Requesting instance as: " + userIdent);
			WebFormManager wfm = new WebFormManager(localisation, "UTC");
			resp = wfm.getInstanceData(sd, request, formIdent, null, taskId, userIdent, true);
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
			@QueryParam("readonly") boolean readOnly) throws IOException {

		Response response;
		
		log.info("Requesting json");

		String requester = "WebForm - getFormJson";
		Connection sd = SDDataSource.getConnection(requester);

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

		mimeType = "json";
		isTemporaryUser = false;
		response = getWebform(request, "none", null, formIdent, datakey, datakeyvalue, 
				assignmentId, taskKey, false, false, 
				false, 
				null,		// initial data
				false,		// show done page
				readOnly,
				null		// initial values
				);
		
		return response;
	}

	/*
	 * Get webform identified by logon credentials
	 */
	@GET
	@Path("/{ident}")
	@Produces(MediaType.TEXT_HTML)
	public Response getFormHTML(@Context HttpServletRequest request, @PathParam("ident") String formIdent,
			@QueryParam("datakey") String datakey, // Optional keys to instance data
			@QueryParam("datakeyvalue") String datakeyvalue, 
			@QueryParam("assignment_id") int assignmentId,
			@QueryParam("initial") String initialValues,
			@QueryParam("taskkey") int taskKey,	// Task id, if set initial data is from task
			@QueryParam("viewOnly") boolean vo,
			@QueryParam("debug") String d,
			@QueryParam("app") boolean app,
			@QueryParam("readonly") boolean readOnly) throws IOException {

		Response response = null;
		
		mimeType = "html";
		viewOnly = vo;
		debug = d;
		isApp = app;

		userIdent = request.getRemoteUser();
		isTemporaryUser = false;
		
		boolean single = false;
		boolean showDonePage = datakeyvalue == null ? false : true;
		
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
				response = getWebform(request, "none", null, 
						formIdent, datakey, datakeyvalue, assignmentId, 
						taskKey,
						false, true, 
						single, 		// Single Param
						null,			// Initial Data
						showDonePage,	// show done page
						readOnly,
						initialValues
						);
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
			@QueryParam("debug") String d) throws IOException {

		mimeType = "html";
		viewOnly = vo;
		debug = d;
		
		log.info("Get form");
		
		userIdent = tempUser;
		isTemporaryUser = true;
		return getWebform(request, "none", null, formIdent, datakey, datakeyvalue, assignmentId, 
				taskKey, false,
				true, false, 
				null, 	// initial data
				true, 
				false,
				null	// initial values
				);
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
			@QueryParam("proceed") String proceed,
			@PathParam("ident") String ident) throws Exception {

		Response response = null;
		
		userIdent = ident;
		mimeType = "html";
		String requester = "surveyMobileAPI-webform task";
		debug = d;
		
		Connection sd = SDDataSource.getConnection(requester);

		Action a = null;
		
		try {

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			// 1. Get details on the action to be performed using the user credentials
			ActionManager am = new ActionManager(localisation, tz);
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
			} else if(!a.single && a.submissionCount > 0 && proceed == null) {

				// For multiple-submission forms, show a count page after each submission
				// so the user knows they already submitted and can choose to submit again.
				String formUrl = request.getScheme() + "://" + request.getServerName() + "/webForm/action/" + ident;
				response = getSubmissionCountPage(a.submissionCount, formUrl);

			} else {

				// 3. Get webform
				userIdent = ident;
				isTemporaryUser = true;
				try {
					response = getWebform(request, a.action, a.email,
							a.surveyIdent, a.datakey, a.datakeyvalue, a.assignmentId, a.taskKey,
							false,
							true,
							true,			// Close after saving
							a.initialData,
							false,			// show done page
							false,
							null			// Initial Values
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

		// Prevent browser caching so revisiting the link always fetches fresh from server.
		// This ensures a completed one-shot form shows the message page immediately on re-visit.
		return Response.fromResponse(response)
				.header("Cache-Control", "no-cache")
				.build();

	}

	/*
	 * Get the response as either HTML or JSON
	 */
	private Response getWebform(HttpServletRequest request, 
			String action, 
			String email,
			String formIdent, 
			String datakey,
			String datakeyvalue, 
			int assignmentId, 
			int taskKey, 
			boolean simplifyMedia,
			boolean isWebForm,
			boolean singleParam,
			Instance initialData,
			boolean showDonePageParam,
			boolean readonly,
			String initialValues) {

		Response response = null;

		log.info("webForm:" + formIdent + " datakey:" + datakey + " datakeyvalue:" + datakeyvalue + "assignmentId:"
				+ assignmentId + " taskKey: " + taskKey);

		Survey survey = null;
		int orgId = 0;
		String accessKey = null;
		String requester = "surveyMobileAPI-getWebForm";
		boolean superUser = false;
		this.single = singleParam;
		this.showDonePage = showDonePageParam;
		
		gFormIdent = formIdent;
		
		/*
		 * Get the media manifest so we can set the url's of media files used by the form
		 * Also get the google api key
		 */
		String basePath = GeneralUtilityMethods.getBasePath(request);
		TranslationManager translationMgr = new TranslationManager();
		ServerManager sm = new ServerManager();
		StringBuffer outputString = new StringBuffer();

		if (userIdent != null) {
			
			// Authorisation
			Connection sd = SDDataSource.getConnection(requester);
			
			// Get the users locale
			try {
				locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, userIdent));
				localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			} catch (Exception e) {
				log.log(Level.SEVERE,"Error: ", e);
			}
			tz = "UTC";
			
			if (isTemporaryUser) {
				a.isValidTemporaryUser(sd, userIdent);
			} else {
				try {
					if(datakey != null && datakey.equals("instanceid")) {
						// Allow enumerator or Manage data if this webform is to edit a specific instance
						aConsole.isAuthorised(sd, userIdent);
					} else {
						// Require enumerator
						a.isAuthorised(sd, userIdent);
					}
				} catch(Exception e) {
					throw new AuthorisationException(localisation.getString("web_blocked"));
				}
			}
			
			SurveyManager surveyManager = new SurveyManager(localisation, "UTC");
			survey = surveyManager.getSurveyId(sd, formIdent); // Get the survey id from the templateName / key
			if (survey == null) {
				log.info("Error: Could not find survey id");
				SDDataSource.closeConnection(requester, sd);
				throw new NotFoundException();
			}
			try {
				// Assume that if a temporary user has been specifically assigned this form to complete then the
				// lack of a role should not stop them
				superUser = isTemporaryUser || GeneralUtilityMethods.isSuperUser(sd, userIdent);
			} catch (Exception e) {
			}
			if(!isTemporaryUser) {
				a.isValidSurvey(sd, userIdent, survey.surveyData.id, false, superUser); // Validate that the user has access	
			}
																					
			a.isBlocked(sd, survey.surveyData.id, false); // Validate that the survey is not blocked
			if(!isTemporaryUser && taskKey > 0) {
				a.isValidTask(sd, userIdent, taskKey);
			}
			
			// End Authorisation
			
			// Get the organisation id and an access key to upload the results of this form
			// (used from iPhones which do not do authentication on POSTs)
			try {
				orgId = GeneralUtilityMethods.getOrganisationId(sd, userIdent);
				accessKey = GeneralUtilityMethods.getNewAccessKey(sd, userIdent, isTemporaryUser);
				
				manifestList = translationMgr.getManifestBySurvey(sd, userIdent, survey.surveyData.id, basePath, 
						formIdent, false);
				serverData = sm.getServer(sd, localisation);

				// Turnstile required for public (temp user) forms that have it enabled
				requiresTurnstile = isTemporaryUser && survey.surveyData.turnstile
						&& serverData.turnstile_site_key != null && !serverData.turnstile_site_key.isEmpty()
						&& serverData.turnstile_secret_key != null && !serverData.turnstile_secret_key.isEmpty();
				showFormIndex = survey.surveyData.showFormIndex;
				if(requiresTurnstile) {
					request.getSession(true).setAttribute("survey_ident_" + accessKey, formIdent);
				}

				// Get the organisation specific options
				OrganisationManager om = new OrganisationManager(localisation);
				options = om.getWebform(sd, userIdent);

				// Get notification types and SMS numbers for embedding in surveyData
				try {
					NotificationManager nm = new NotificationManager(localisation);
					gNotificationTypes = nm.getNotificationTypes(sd, userIdent, "console");
				} catch (Exception e) {
					log.info("Could not get notification types: " + e.getMessage());
				}
				try {
					SMSManager smsm = new SMSManager(localisation, tz);
					gOurNumbers = smsm.getOurNumbers(sd, userIdent, true);
				} catch (Exception e) {
					log.info("Could not get SMS numbers: " + e.getMessage());
				}

				log.info("++++++ Action: " + action);
				// If this request is for a mailout then opt in
				if(action.equals("mailout")) {
					PeopleManager pm = new PeopleManager(localisation);
					pm.subscribeEmail(sd, email, orgId);
					lm.writeLog(sd, survey.surveyData.id, email, LogManager.MAILOUT, localisation.getString("mo_submitted_mailout"), 0, request.getServerName());
				} else 	if(action.equals("task")) {
					lm.writeLog(sd, survey.surveyData.id, email, LogManager.EMAIL_TASK, localisation.getString("mo_submitted_task"), 0, request.getServerName());
				}
				
			} catch (Exception e) {
				log.log(Level.SEVERE, "WebForm", e);
			} finally {
				SDDataSource.closeConnection(requester, sd);
			}
		} else {
			throw new AuthorisationException();
		}

		// Generate the web form
		try {

			// Get the XML of the Form
			template = new SurveyTemplate(localisation);
			template.setUser(userIdent);
			template.readDatabase(survey.surveyData.id, true);
			String surveyClass = template.getSurveyClass();

			// If required get the instance data
			String instanceXML = null;
			String instanceStrToEditId = null;
			
			if(initialValues != null) {
				if(initialData == null) {
					initialData = new Instance();
				}
				mergeValuesToData(initialData, initialValues);
			}
			if ((datakey != null && datakeyvalue != null) || taskKey > 0 || initialData != null) {
				
				log.info("Adding initial data");
				String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
				GetXForm xForm = new GetXForm(localisation, userIdent, tz);
				instanceXML = xForm.getInstanceXml(survey.surveyData.id, formIdent, template, datakey, datakeyvalue, 0, simplifyMedia,
						isWebForm, taskKey, urlprefix, initialData, false);
				instanceStrToEditId = xForm.getInstanceId();
				gRecordCounts = xForm.getRecordCounts();
			} 

			if (mimeType.equals("json")) {
				jr = new JsonResponse();
				jr.manifestList = new ArrayList<ManifestValue>();
			}

			// Convert to HTML / Json
			if (mimeType.equals("json")) {

				jr.surveyData.modelStr = getModelStr(request);
				if (instanceXML != null) {
					jr.surveyData.instanceStrToEdit = instanceXML.replace("\n", "").replace("\r", "");
				}
				jr.surveyData.instanceStrToEditId = instanceStrToEditId;

				// Add the assignment id if this was set
				if (assignmentId != 0) {
					jr.surveyData.assignmentId = assignmentId;
				}

				// Add access key for authentication
				if (accessKey != null) {
					jr.surveyData.accessKey = accessKey;
				}

				// Add survey class's - used for paging
				jr.surveyData.surveyClass = survey.surveyData.surveyClass;

				jr.main = addMain(request, instanceStrToEditId, orgId, true, surveyClass, superUser, survey.surveyData.readOnlySurvey || readonly).toString();

				Gson gsonResp = new GsonBuilder().disableHtmlEscaping().create();
				outputString.append(gsonResp.toJson(jr));

			} else {
				// MAIN ENTRY POINT - XXXX
				outputString.append(addDocument(request, instanceXML, instanceStrToEditId, assignmentId,
						survey.surveyData.surveyClass, orgId, accessKey, superUser, 
						survey.surveyData.readOnlySurvey || readonly,
						survey));
			}
			
			response = Response.status(Status.OK).entity(outputString.toString()).build();

			log.info("userevent: " + userIdent + " : webForm : " + formIdent);

		} catch (Exception e) {
			StringBuffer sb = new StringBuffer("");
			sb.append("<html>").append("<head></head>").append("<body>");
			sb.append("<b>").append(e.getMessage()).append("</b>");
			String msg = e.getMessage();
			if(msg != null && msg.contains("ERROR: column") && msg.contains("does not exist")) {
				sb.append("<p>").append(localisation.getString("msg_no_col")).append("</p>");
			}
			
			sb.append("</body>").append("</html>");
			response = Response.status(Status.OK).entity(sb.toString()).build();
			log.log(Level.SEVERE, e.getMessage(), e);
		}

		return response;
	}

	/*
	 * Add the HTML
	 */
	private String addDocument(HttpServletRequest request, String instanceXML,
			String dataToEditId, int assignmentId, String surveyClass, int orgId, String accessKey, 
			boolean superUser,
			boolean readOnly,
			Survey survey)
			throws TransformerFactoryConfigurationError, Exception {

		StringBuilder output = new StringBuilder();

		output.append("<!DOCTYPE html>\n");

		// Append locale
		output.append("<html lang='").append(locale.toString()).append("'  class='no-js'");

		output.append(">\n");

		output.append(
				addHead(request, instanceXML, dataToEditId, assignmentId, surveyClass, 
						accessKey, request.getRemoteUser(), survey));
		output.append(addBody(request, dataToEditId, orgId, surveyClass, superUser, readOnly));

		output.append("</html>\n");
		String html = postProcessHtml(output);		// Set URL links and replace escaped XML
		
		return html;
	}

	/*
	 * 
	 */
	private String postProcessHtml(StringBuilder output) {
		
		String html = output.toString();
		
		String dynamic = "";
		if(isTemporaryUser) {
			dynamic = "/id/" + userIdent;		
		}
		for (int i = 0; i < manifestList.size(); i++) {
			log.info(manifestList.get(i).fileName + " : " + manifestList.get(i).url + " : "
					+ manifestList.get(i).type);
			String type = manifestList.get(i).type;
			String name = manifestList.get(i).fileName;
			String url = manifestList.get(i).url;
			if (type.equals("image")) {
				type = "images";
			}
			
			if (url != null) {
				if (mimeType.equals("json")) {
					ManifestValue mv = new ManifestValue(); // Create a new version of the manifest to send to a
															// client that doesn't have unneeded data
					mv.type = type;
					mv.fileName = name;
					mv.url = url;
					jr.manifestList.add(mv);
				} else {
					url = url.replaceFirst("/surveyKPI/file", "/surveyKPI/file" + dynamic);
					html = html.replace("jr://" + type + "/" + name, url);
				}
			}
		}
		return html;
	}
	/*
	 * Add the head section
	 */
	private StringBuffer addHead(HttpServletRequest request, String instanceXML, String dataToEditId,
			int assignmentId, String surveyClass, String accessKey, 
			String user, Survey survey)
			throws TransformerFactoryConfigurationError, Exception {

		StringBuffer output = new StringBuffer();

		// head
		output.append("<head>\n");
		output.append("<title>" + sanitise.sanitiseHtml(survey.getDisplayName()) + "</title>");
		output.append("<link rel='preload' as='font' href='/fonts/OpenSans-Regular-webfont.woff' type='font/woff' crossorigin>");
		output.append("<link rel='preload'as='font' href='/fonts/OpenSans-Bold-webfont.woff' type='font/woff' crossorigin>");
		output.append("<link rel='preload' as='font' href='/fonts/fontawesome-webfont.woff' type='font/woff' crossorigin>");
		
		if (surveyClass != null && surveyClass.trim().contains("theme-grid")) {		
			output.append("<link type='text/css' href='/build/css/theme-grid.css' media='all' rel='stylesheet' />\n");
			output.append("<link type='text/css' href='/build/css/grid-print.css' media='print' rel='stylesheet'/>\n");
		} else {
			output.append("<link type='text/css' href='/build/css/theme-smap.css' media='all' rel='stylesheet' />\n");
			output.append("<link type='text/css' href='/build/css/theme-smap.print.css' media='print' rel='stylesheet'/>\n");
		}
		output.append("<link type='text/css' href='/build/css/webform.css' media='all' rel='stylesheet' />\n");
		output.append("<link type='text/css' href='/build/css/webform.print.css' media='print' rel='stylesheet' />\n");

		/*
		 * Add organisation specific css settings
		 */
		if(options != null) {
			output.append("<style type='text/css'>");
			if(options.page_background_color != null && options.page_background_color.trim().length() > 0) {
				output.append("body {background-color: " + options.page_background_color + "}");
			}
			if(options.paper_background_color != null && options.paper_background_color.trim().length() > 0) {
				output.append(".paper {background-color: " + options.paper_background_color + "}");
			}
			output.append(".form-footer .enketo-power {position:absolute; right: " + options.footer_horizontal_offset + "px}");

			// Set the background button colors
			if((options.button_background_color != null && options.button_background_color.trim().length() > 0) ||
					(options.button_text_color != null && options.button_text_color.trim().length() > 0)) {
				
				output.append(" .btn-primary.disabled, .btn-primary.disabled:hover, .btn-primary.disabled:focus, .btn-primary.disabled:active, .btn-primary.disabled.active, .btn-primary[disabled], .btn-primary[disabled]:hover, .btn-primary[disabled]:focus, .btn-primary[disabled]:active, .btn-primary[disabled].active,\n" + 
						"  fieldset[disabled] .btn-primary,\n" + 
						"  fieldset[disabled] .btn-primary:hover,\n" + 
						"  fieldset[disabled] .btn-primary:focus,\n" + 
						"  fieldset[disabled] .btn-primary:active,\n" + 
						"  fieldset[disabled] .btn-primary.active, .btn,.btn.disabled, .btn:focus {");
				if(options.button_background_color != null && options.button_background_color.trim().length() > 0) {
					output.append("background-color: " + options.button_background_color +";");
					output.append("border-color: " + GeneralUtilityMethods.adjustColor(options.button_background_color, 20) +";");
				}
				if(options.button_text_color != null && options.button_text_color.trim().length() > 0) {
					output.append("color: " + options.button_text_color + ";");
				}
				output.append("}");
				
				if(options.button_background_color != null && options.button_background_color.trim().length() > 0) {
					output.append("button.btn:hover, button.btn.disabled:hover, button.btn.disabled:focus {");
					output.append("background-color: " + options.button_background_color +";");
					output.append("filter: brightness(180%);");
					output.append("}");
				}
			
			}
			if(options.header_text_color != null && options.header_text_color.trim().length() > 0) {
				output.append(".or > h3, .or > h4, .or-group > h4 {color: " + options.header_text_color + "}");
			}
			output.append("</style>");
		}
		output.append("<link rel='shortcut icon' href='/favicon.ico'>\n");
		// <!-- For third-generation iPad with high-resolution Retina display: -->
		output.append(
				"<link rel='apple-touch-icon-precomposed' sizes='144x144' href='images/fieldTask_144_144_min.png'>\n");
		// <!-- For iPhone with high-resolution Retina display: -->
		output.append(
				"<link rel='apple-touch-icon-precomposed' sizes='114x114' href='images/fieldTask_114_114_min.png'>\n");
		// <!-- For first- and second-generation iPad: -->
		output.append(
				"<link rel='apple-touch-icon-precomposed' sizes='72x72' href='images/fieldTask_72_72_min.png'>\n");
		// <!-- For non-Retina iPhone, iPod Touch, and Android 2.1+ devices: -->
		output.append("<link rel='apple-touch-icon-precomposed' href='images/fieldTask_57_57_min.png'>\n");

		output.append("<meta charset='utf-8' />\n");
		output.append("<meta name='viewport' content='width=device-width, initial-scale=1.0' />\n");
		output.append("<meta name='apple-mobile-web-app-capable' content='yes' />\n");
		output.append("<script>");
		output.append("if ( navigator.userAgent.indexOf( 'Trident/' ) >= 0 ) {");
		output.append("window.location.href = '/browser-support.html'}");
		output.append("</script>");

		output.append("<script src='/js/libs/modernizr.js'></script>");
		output.append("<script src='/js/app/idbconfig.js'></script>");
		output.append(addData(request, instanceXML, dataToEditId, assignmentId, accessKey));
		// Add the google API key
		output.append("<script>");
		output.append("window.smapConfig = {};");
		if (serverData.google_key != null) {
			output.append("window.smapConfig.googleApiKey='");
			output.append(serverData.google_key);
			output.append("';");
		}
		// add user ID
		output.append("window.smapConfig.username='").append(user).append("';");
		
		output.append("window.smapConfig.myWork=" + (myWork ? "true" : "false") + ";");
		if(requiresTurnstile) {
			output.append("window.smapConfig.requiresTurnstile=true;");
			output.append("window.smapConfig.turnstileSiteKey='").append(serverData.turnstile_site_key).append("';");
			output.append("window.smapConfig.formIdent='").append(gFormIdent).append("';");
			output.append(getTurnstileJs());
		}
		output.append("</script>");
		if(requiresTurnstile) {
			output.append("<script src='https://challenges.cloudflare.com/turnstile/v0/api.js' async defer></script>\n");
		}
		output.append("</head>\n");

		return output;
	}
	
	/*
	 * Add the data
	 */
	private StringBuffer addData(HttpServletRequest request, String instanceXML, String dataToEditId,
			int assignmentId, String accessKey)
			throws TransformerFactoryConfigurationError, Exception {

		StringBuffer output = new StringBuffer();

		output.append("<script type='text/javascript'>\n");
		output.append("settings = {};\n");

		output.append("surveyData = {};\n");
		output.append("surveyData.surveyIdent='").append(gFormIdent).append("';\n");
		// Data model
		output.append("surveyData.modelStr=\"");
		output.append(getModelStr(request));
		output.append("\";\n");

		// Instance Data
		if (instanceXML != null) {
			output.append("surveyData.instanceStrToEdit='");
			output.append(instanceXML.replace("\n", "").replace("\r", ""));
			output.append("';\n");
		}

		// Add identifier of existing record, use this as a key on results submission to
		// update an existing record
		if (dataToEditId == null) {
			output.append("surveyData.instanceStrToEditId = undefined;\n");
		} else {
			output.append("surveyData.instanceStrToEditId='");
			output.append(dataToEditId);
			output.append("';\n");
		}

		// Add the assignment id if this was set
		if (assignmentId == 0) {
			output.append("surveyData.assignmentId = undefined;\n");
		} else {
			output.append("surveyData.assignmentId='");
			output.append(assignmentId);
			output.append("';\n");
		}
		
		if(single) {
			output.append("surveyData.closeAfterSending = true;\n");
		}
		
		if(showDonePage) {
			output.append("surveyData.showDonePage = true;\n");
		}

		// Add access key for authentication
		if (accessKey == null) {
			output.append("surveyData.key = undefined;\n");
		} else {
			output.append("surveyData.key='");
			output.append(accessKey);
			output.append("';\n");
		}
		
		// Add viewOnly flag if set
		if(viewOnly) {
			output.append("surveyData.viewOnly=true;\n");
		} else {
			output.append("surveyData.viewOnly=false;\n");
		}

		output.append("surveyData.showFormIndex=").append(showFormIndex).append(";\n");

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		output.append("surveyData.notificationTypes=").append(gson.toJson(gNotificationTypes)).append(";\n");
		output.append("surveyData.ourNumbers=").append(gson.toJson(gOurNumbers)).append(";\n");

		output.append("</script>\n");
		return output;
	}

	/*
	 * Get the model string
	 */
	private String getModelStr(HttpServletRequest request)
			throws TransformerFactoryConfigurationError, Exception {

		GetXForm xForm = new GetXForm(localisation, userIdent, tz);
		String model = xForm.get(template, true, true, true, userIdent, request);

		// We only want the model - remove any XML preanble
		int modelIdx = model.indexOf("<model>");
		
		if(modelIdx > 0) {
			model = model.substring(modelIdx);
			
			model = model.replace("\n", "");
			model = model.replace("\r", "");
			model = model.replaceAll("\"", "'");
		} 
		
		return model;

	}

	/*
	 * Verify a Cloudflare Turnstile token and store result in session
	 */
	@POST
	@Path("/captcha/verify")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response verifyCaptcha(
			@Context HttpServletRequest request,
			@FormParam("cfToken") String cfToken,
			@FormParam("formIdent") String formIdent) {

		String connectionString = "surveyMobileAPI-verifyCaptcha";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, null));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			// Check survey requires Turnstile
			boolean turnstileRequired = false;
			String sql = "select turnstile from survey where ident = ?";
			PreparedStatement pstmt = null;
			try {
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, formIdent);
				ResultSet rs = pstmt.executeQuery();
				if(rs.next()) {
					turnstileRequired = rs.getBoolean("turnstile");
				}
			} finally {
				if(pstmt != null) try { pstmt.close(); } catch(Exception e) {}
			}

			if(!turnstileRequired || cfToken == null || cfToken.isEmpty()) {
				return Response.status(Status.BAD_REQUEST).entity("{\"success\":false}").build();
			}

			// Get secret key from server settings
			ServerManager sm = new ServerManager();
			ServerData serverSettings = sm.getServer(sd, localisation);
			if(serverSettings.turnstile_secret_key == null || serverSettings.turnstile_secret_key.isEmpty()) {
				log.warning("Turnstile secret key not configured");
				return Response.status(Status.INTERNAL_SERVER_ERROR).entity("{\"success\":false}").build();
			}

			// Call Cloudflare verification API
			boolean verified = callTurnstileVerifyApi(cfToken, serverSettings.turnstile_secret_key, request.getRemoteAddr());

			if(verified) {
				HttpSession session = request.getSession(true);
				session.setAttribute("turnstile_verified_" + formIdent, Boolean.TRUE);
				return Response.ok("{\"success\":true}").build();
			} else {
				return Response.ok("{\"success\":false}").build();
			}

		} catch(Exception e) {
			log.log(Level.SEVERE, "verifyCaptcha", e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity("{\"success\":false}").build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	/*
	 * Call Cloudflare Turnstile siteverify API
	 */
	private boolean callTurnstileVerifyApi(String token, String secretKey, String remoteIp) {
		try {
			URL url = new URL("https://challenges.cloudflare.com/turnstile/v0/siteverify");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setConnectTimeout(5000);
			conn.setReadTimeout(5000);
			conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

			String body = "secret=" + secretKey + "&response=" + token
					+ (remoteIp != null ? "&remoteip=" + remoteIp : "");
			try(OutputStream os = conn.getOutputStream()) {
				os.write(body.getBytes(StandardCharsets.UTF_8));
			}

			StringBuilder sb = new StringBuilder();
			try(BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
				String line;
				while((line = br.readLine()) != null) { sb.append(line); }
			}
			String response = sb.toString();
			return response.contains("\"success\":true");
		} catch(Exception e) {
			log.log(Level.WARNING, "Turnstile API call failed", e);
			return false;
		}
	}

	/*
	 * Inline JS for Turnstile: disables submit until challenge completes and pre-verify succeeds
	 */
	private String getTurnstileJs() {
		return "(function(){"
				+ "window.smapTurnstileVerified=false;"
				+ "var _idx=window.location.pathname.indexOf('/webForm/');"
				+ "var _verifyUrl=(_idx>=0?window.location.pathname.substring(0,_idx):'')+'/webForm/captcha/verify';"
				+ "document.addEventListener('submit',function(e){"
				+ "if(!window.smapTurnstileVerified){e.stopImmediatePropagation();e.preventDefault();}"
				+ "},true);"
				+ "window.smapTurnstileCallback=function(token){"
				+ "fetch(_verifyUrl,{"
				+ "method:'POST',"
				+ "headers:{'Content-Type':'application/x-www-form-urlencoded','X-Requested-With':'XMLHttpRequest'},"
				+ "body:'cfToken='+encodeURIComponent(token)+'&formIdent='+encodeURIComponent(window.smapConfig.formIdent)"
				+ "}).then(function(r){return r.json();})"
				+ ".then(function(d){"
				+ "if(d.success){window.smapTurnstileVerified=true;}"
				+ "}).catch(function(){});"
				+ "};"
				+ "})();";
	}

	/*
	 * Add the body
	 */
	private String addBody(HttpServletRequest request, String dataToEditId, int orgId,
			String surveyClass, boolean superUser, boolean readOnly)
			throws TransformerFactoryConfigurationError, SQLException, Exception {
		StringBuilder output = new StringBuilder();

		output.append("<body class='clearfix edit'>");
		output.append(getAside());
		output.append("<div class='smap-layout'>\n");
		output.append(getSidePanel());
		output.append(addMain(request, dataToEditId, orgId, false, surveyClass, superUser, readOnly));
		output.append("</div>\n");
		output.append(getDialogs());

		// Webforms script
		if(debug != null && debug.equals("yes")) {
			output.append("<script src='/build/js/webform-bundle.js'></script>\n");
		} else {
			//output.append("<script src='/build/js/webform-bundle.js'></script>\n");	// TODO Fix compression
			output.append("<script src='/build/js/webform-bundle.min.js'></script>\n");
		}

		output.append("</body>");

		/*
		 * Replace escaping of html with real tags
		 */
		String html = output.toString();
		html = html.replaceAll("&gt;", ">");
		html = html.replaceAll("&lt;", "<");
		html = html.replaceAll("&amp;", "&");
		html = html.replaceAll("\\\\\\\\", "\\\\");
		
		return html;
	}

	/*
	 * Get the "Main" element of an web form
	 */
	private StringBuffer addMain(HttpServletRequest request, String dataToEditId, int orgId,
			boolean minimal, String surveyClass, boolean superUser, boolean readOnly)
			throws TransformerFactoryConfigurationError, SQLException, Exception {

		StringBuffer output = new StringBuffer();
		output.append(openMain(orgId, minimal));

		GetHtml getHtml = new GetHtml(localisation);
		String html = getHtml.get(request, template.getSurvey().getId(), superUser, userIdent, 
				gRecordCounts, isTemporaryUser, false);

		output.append(html);

		if (!minimal) {
			output.append(closeMain(dataToEditId, surveyClass, readOnly));
		}

		return output;
	}

	/*
	 * Add some dialogs
	 */
	private StringBuffer getDialogs() {
		StringBuffer output = new StringBuffer();

		output.append("<!-- Start Dialogs -->\n");
		output.append(
				"<div id='dialog-alert' class='modal fade' role='dialog' aria-label='Alert dialog' aria-hidden='true' data-keyboard='true'>\n");
		output.append("<div class='modal-dialog'>\n");
		output.append("<div class='modal-content'>\n");
		output.append("<div class='modal-header'>\n");
		output.append("<button type='button' class='close' data-dismiss='modal' aria-hidden='true'>&times;</button>\n");
		output.append("<div class='modal-title'></div>\n");
		output.append("</div>\n");
		output.append("<div class='modal-body'>\n");
		output.append("<p class=''></p>\n");
		output.append("</div>\n");
		output.append("<div class='modal-footer'>\n");
		output.append("<span class='self-destruct-timer'></span>\n");
		output.append("<button class='btn btn-primary lang close-dialog' data-lang='alert.default.button' data-dismiss='modal' aria-hidden='true'>Close</button>\n");
		output.append("</div>\n");
		output.append("</div>\n");
		output.append("</div>\n");
		output.append("</div>  <!-- end dialog-alert -->\n");

		output.append(
				"<div id='dialog-confirm' class='modal fade' role='dialog' aria-label='Confirmation dialog' aria-hidden='true' data-keyboard='true'>\n");
		output.append("<div class='modal-dialog'>\n");
		output.append("<div class='modal-content'>\n");
		output.append("<div class='modal-header'>\n");
		output.append("<button type='button' class='close' data-dismiss='modal' aria-hidden='true'>&times;</button>\n");
		output.append("</div>\n");
		output.append("<div class='modal-body'>\n");
		output.append("<div class='modal-title'></div>\n");
		output.append("<p class='alert alert-danger'></p>\n");
		output.append("<p class='msg'></p>\n");
		output.append("<span id=\"recname\"></span>");
		output.append("<input type=\"text\" name=\"record-name\" aria-labelledby=\"recname\" tabindex=\"-1\">\n");
		output.append("</div>\n");
		output.append("<div class='modal-footer'>\n");
		output.append("<span class='self-destruct-timer'></span>\n");
		output.append("<button class='negative btn lang' data-lang='alert.default.button'>Close</button>\n");
		output.append("<button class='positive btn btn-primary' data-lang='confirm.default.posButton'>Confirm</button>\n");
		output.append("</div>\n");
		output.append("</div>\n");
		output.append("</div>\n");
		output.append("</div>   <!-- end dialog-confirm -->\n");
		// TRANSLATION LIMIT

		output.append(
				"<div id='dialog-save' class='modal fade' role='dialog' aria-label='Save dialog' aria-hidden='true' data-keyboard='true'>\n");
		output.append("<div class='modal-dialog'>\n");
		output.append("<div class='modal-content'>\n");
		output.append("<div class='modal-header'>\n");
		output.append("<button type='button' class='close' data-dismiss='modal' aria-hidden='true'>&times;</button>\n");
		output.append("</div>\n");
		output.append("<div class='modal-body'>\n");
		output.append("<div class='modal-title'></div>\n");
		output.append("<form onsubmit='return false;'>\n");
		output.append("<div class='alert alert-danger'></div>\n");
		output.append("<label>\n");
		output.append("<span>Record Name</span>\n");
		output.append(
				"<span class='or-hint active'>This name allows you to easily find your draft record to finish it later. The record name will not be submitted to the server.</span>\n");
		output.append("<input name='record-name' type='text' required='required'/>\n");
		output.append("</label>\n");
		output.append("</form>\n");
		output.append("</div>\n");
		output.append("<div class='modal-footer'>\n");
		output.append("<button class='negative btn'>Close</button>\n");
		output.append("<button class='positive btn btn-primary'>Save &amp; Close</button>\n");
		output.append("</div>\n");
		output.append("</div>\n");
		output.append("</div>\n");
		output.append("</div>\n");  // end dialog-save 

		// used for Grid theme only
		output.append(
				"<div id='dialog-print' class='modal fade' role='dialog' aria-label='Print dialog' aria-hidden='true' data-keyboard='true'>\n");
		output.append("<div class='modal-dialog'>\n");
		output.append("<div class='modal-content'>\n");
		output.append("<div class='modal-header'>\n");
		output.append("<button type='button' class='close' data-dismiss='modal' aria-hidden='true'>&times;</button>\n");
		output.append("<h3 class='select-format'>Select Print Settings</h3>\n");
		output.append("<h3 class='progress'>Preparing...</h3>\n");
		output.append("</div>");
		output.append("<div class='modal-body'>\n");
		output.append("<section class='progress'>\n");
		output.append("<p>Working hard to prepare your optimized print view. Hold on!</p>\n");
		output.append("<progress></progress>\n");
		output.append("</section>\n");
		output.append("<section class='select-format'>\n");
		output.append("<p>To prepare an optimized print, please select the print settings below</p>\n");
		output.append("<form onsubmit='return false;'>\n");
		output.append("<fieldset>\n");
		output.append("<legend>Paper Size</legend>\n");
		output.append("<label>\n");
		output.append("<input name='format' type='radio' value='A4' required checked/>\n");
		output.append("<span>A4</span>\n");
		output.append("</label>\n");
		output.append("<label>\n");
		output.append("<input name='format' type='radio' value='letter' required/>\n");
		output.append("<span>Letter</span>\n");
		output.append("</label>\n");
		output.append("</fieldset>\n");
		output.append("<fieldset>\n");
		output.append("<legend>Paper Orientation</legend>\n");
		output.append("<label>\n");
		output.append("<input name='orientation' type='radio' value='portrait' required checked/>\n");
		output.append("<span>Portrait</span>\n");
		output.append("</label>\n");
		output.append("<label>\n");
		output.append("<input name='orientation' type='radio' value='landscape' required/>\n");
		output.append("<span>Landscape</span>\n");
		output.append("</label>\n");
		output.append("</fieldset>\n");
		output.append("</form>\n");
		output.append(
				"<p class='alert alert-info'>Remember to set these same print settings in the browser's print menu afterwards!</p>\n");
		output.append("</section>\n");
		output.append("</div>\n");
		output.append("<div class='modal-footer'>\n");
		output.append("<button class='negative btn'>Close</button>\n");
		output.append("<button class='positive btn btn-primary'>Prepare</button>\n");
		output.append("</div>\n");
		output.append("</div>\n");
		output.append("</div>\n");
		output.append("</div> <!-- end dialog-print for grid -->\n");

		return output;
	}

	private StringBuffer getAside() {

		StringBuffer output = new StringBuffer();

		output.append("<div id='feedback-bar' class='alert alert-warning'>\n");
		output.append("<span class='glyphicon glyphicon-info-sign'></span>\n");
		output.append("<button class='close' aria-label='Close Feedback Bar'><span class='glyphicon glyphicon-step-backward'></span></button>\n");
		output.append("</div>\n");

		output.append("<div id='smap-queue-panel' class='smap-full-panel' hidden>\n");
		output.append("<div class='smap-full-panel__header'>\n");
		output.append("<h3 class='lang' data-lang='record-list.title'>Queue</h3>\n");
		output.append("<button class='smap-full-panel__close' aria-label='Close'>&times;</button>\n");
		output.append("</div>\n");
		output.append("<div class='smap-full-panel__body'></div>\n");
		output.append("</div>\n");

		return output;
	}

	private StringBuffer getSidePanel() {
		StringBuffer output = new StringBuffer();

		output.append("<div id='smap-side-panel' class='smap-side-panel'>\n");

		output.append("<div id='smap-notification-panel' class='smap-notification-panel' hidden>\n");
		output.append("<div class='smap-panel-header'>\n");
		output.append("<h3 class='lang' data-lang='msg_send_notification'>Send Notification</h3>\n");
		output.append("<button class='smap-panel-close' aria-label='Close'>&times;</button>\n");
		output.append("</div>\n");
		output.append("<div class='smap-panel-body'>\n");
		output.append(getNotificationForm());
		output.append("</div>\n");
		output.append("</div>\n");

		output.append("<div id='smap-index-area'></div>\n");

		output.append("</div>\n");
		return output;
	}

	private StringBuffer getNotificationForm() {
		StringBuffer output = new StringBuffer();

		output.append("<div id='wf-notification-form'>\n");

		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='c_target' for='target'>Type</label>\n");
		output.append("<select class='form-select' id='target'></select>\n");
		output.append("</div>\n");

		output.append("<div class='email_options submission_options' style='display:none;'>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='c_email' for='notify_emails'>Emails</label>\n");
		output.append("<textarea id='notify_emails' class='form-control' rows='2'></textarea>\n");
		output.append("</div>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='c_subject' for='email_subject'>Subject</label>\n");
		output.append("<input type='text' id='email_subject' class='form-control'>\n");
		output.append("</div>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='c_content' for='email_content'>Content</label>\n");
		output.append("<textarea id='email_content' class='form-control' rows='3'></textarea>\n");
		output.append("</div>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='c_attach' for='email_attach'>Attachment</label>\n");
		output.append("<select class='form-select' id='email_attach'>\n");
		output.append("<option value='none' class='lang' data-lang='c_none'>None</option>\n");
		output.append("<option value='webform' class='lang' data-lang='n_wf'>Web Form</option>\n");
		output.append("<option value='pdf' class='lang' data-lang='n_pdfp'>PDF portrait</option>\n");
		output.append("<option value='pdf_landscape' class='lang' data-lang='n_pdfl'>PDF landscape</option>\n");
		output.append("</select>\n");
		output.append("</div>\n");
		output.append("</div>\n");

		output.append("<div class='sms_options' style='display:none;'>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='c_sms' for='notify_sms'>Phone</label>\n");
		output.append("<input type='tel' id='notify_sms' class='form-control'>\n");
		output.append("</div>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='n_si' for='sms_sender_id'>Sender ID</label>\n");
		output.append("<input type='text' id='sms_sender_id' class='form-control'>\n");
		output.append("</div>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='c_content' for='sms_content'>Content</label>\n");
		output.append("<textarea id='sms_content' class='form-control' rows='3'></textarea>\n");
		output.append("</div>\n");
		output.append("</div>\n");

		output.append("<div class='conv_options' style='display:none;'>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='n_their_nbr' for='msg_cur_nbr'>Their number</label>\n");
		output.append("<select id='msg_cur_nbr' class='form-select'><option value='other'>...</option></select>\n");
		output.append("</div>\n");
		output.append("<div class='other_msg' style='display:none;'>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='n_spec_nbr' for='msg_nbr_other'>Specify number</label>\n");
		output.append("<input type='number' id='msg_nbr_other' class='form-control'>\n");
		output.append("</div>\n");
		output.append("</div>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='c_channel' for='msg_channel'>Channel</label>\n");
		output.append("<select id='msg_channel' class='form-select'>\n");
		output.append("<option value='sms'>SMS</option>\n");
		output.append("<option value='whatsapp'>WhatsApp</option>\n");
		output.append("</select>\n");
		output.append("</div>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='n_our_nbr' for='msg_our_nbr'>Our number</label>\n");
		output.append("<select id='msg_our_nbr' class='form-select'></select>\n");
		output.append("</div>\n");
		output.append("<div class='form-group'>\n");
		output.append("<label class='lang' data-lang='rev_text' for='conversation_text'>Message</label>\n");
		output.append("<textarea id='conversation_text' class='form-control' rows='3'></textarea>\n");
		output.append("</div>\n");
		output.append("</div>\n");

		output.append("</div>\n");
		output.append("<div class='smap-notification-footer'>\n");
		output.append("<div id='wf-notification-status' class='alert' style='display:none;'></div>\n");
		output.append("<ul id='wf-pending-notifications' class='wf-pending-list' style='display:none;'></ul>\n");
		output.append("<button id='wf-send-notification' class='btn btn-primary lang' data-lang='c_queue'>Queue</button>\n");
		output.append("</div>\n");

		return output;
	}

	private StringBuffer openMain(int orgId, boolean minimal) {
		StringBuffer output = new StringBuffer();

		output.append("<main class='main' aria-labelledby='form-title'>\n");

		// Add the google api key
		if (serverData != null) {
			output.append("<div id='googleApiKey' style='display:none;'>");
			output.append(serverData.google_key);
			output.append("</div>");
		}

		output.append("<div class='loader'></div>");
		output.append("<article class='paper' style='display:none;'>\n");
		if (!minimal) {
			output.append("<header class='form-header clearfix'>\n");
			output.append("<div class='offline-enabled'>\n");
			output.append("<div title='Records Queued' class='queue-length'>0</div>\n");
			if(isApp) {		// include back button
				output.append("<div style=' font-size: large;'><button onclick='window.history.back();' aria-label='Go back'><i class='fa fa-arrow-left' aria-hidden='true'></i></button></div>\n");
			}
			output.append("<div><img id=\"hour_glass\" src=\"/images/ajax-loader.gif\" style=\"display:none;\" alt=\"hourglass\" height=\"34\" width=\"34\"></div>\n");
			output.append("</div>\n");
			//output.append("<button onclick='window.print();' class='print' title='Print this Form'> </button>\n");
			output.append("<button class=\"print form-header__button--print btn-bg-icon-only\" onclick=\"return false;\" aria-label=\"Print form\"></button>");
			output.append("<span class='form-language-selector'><span class='lang' data-lang='form.chooseLanguage'>language</span></span>\n");
			output.append("<div class='form-progress'></div>\n");

			/*
			 * Add the webform banner
			 */
			output.append("<span class='logo-wrapper'>\n");
			output.append(addNoScriptWarning());

			output.append("<img class='banner_logo' src='/custom/banner/")
				.append(orgId)
				.append("' alt=''>\n");
			output.append("</span>\n");

			output.append("<div class='smap-menu-wrapper'>\n");
			output.append("<button class='smap-menu-btn' aria-label='Menu'>&#9776;</button>\n");
			output.append("<ul class='smap-dropdown-menu' hidden>\n");
			output.append("<li><button class='smap-menu-item lang' data-action='open-queue' data-lang='record-list.title'>Queue</button></li>\n");
			if(showFormIndex) {
				output.append("<li><button class='smap-menu-item lang' data-action='toggle-index' data-lang='form.index'>Index</button></li>\n");
			}
			output.append("<li id='smap-notif-menu-item' hidden><button class='smap-menu-item lang' data-action='toggle-notification' data-lang='msg_send_notification'>Send Notification</button></li>\n");
			output.append("<li><a class='smap-menu-item lang' href='/app/myWork/history.html' target='_blank' data-lang='record-list.history'>History</a></li>\n");
			output.append("</ul>\n");
			output.append("</div>\n");

			output.append("</header>\n");
		}
		return output;
	}

	private StringBuffer closeMain(String dataToEditId, String surveyClass, boolean readOnly) {
		StringBuffer output = new StringBuffer();

		output.append("<section class='form-footer'>\n");
		output.append("<div class='content'>\n");
		output.append("<div class='main-controls'>\n");
		if (readOnly) {
			output.append("<button id='exit-form' class='btn btn-primary btn-large lang' data-lang='alert.default.button'>Close</button>\n");
		} else if (dataToEditId == null) {
			output.append("<button id='submit-form' class='btn btn-primary btn-large lang' data-lang='formfooter.submit.btn'>Submit</button>\n");
		} else {
			output.append("<button id='submit-form-single' class='btn btn-primary btn-large lang' data-lang='formfooter.submit.btn'>Submit</button>\n");
		}
		if(!options.wf_hide_draft) {
			output.append(
				"<fieldset class='draft question'><div class='option-wrapper'><label class='select'><input class='ignore' type='checkbox' name='draft'/><span class='option-label lang' data-lang='formfooter.savedraft.label'>Save as Draft</span></label></div></fieldset>\n");
		}
		if(requiresTurnstile && !readOnly) {
			output.append("<div class='cf-turnstile' style='margin-top:12px;' data-sitekey='").append(serverData.turnstile_site_key)
					.append("' data-callback='smapTurnstileCallback'></div>\n");
		}
		if (surveyClass != null && surveyClass.contains("pages")) {
			output.append("<button class='btn previous-page disabled' href='#'><<<</button>\n");
			output.append("<button class='btn next-page' href='#'>>>></span></button>\n");
		}
		output.append(
				"<div class=\"enketo-power\" style=\"margin-bottom: 30px;\"><span class='lang' data-lang='enketo.power'>Powered by</span> <a href=\"http://enketo.org\" title=\"enketo.org website\" tabindex=\"-1\"><img src=\"/images/enketo_bare_150x56.png\" alt=\"enketo logo\" /></a> </div>");
		// output.append("<img src=/images/enketo.png style=\"position: absolute; right:
		// 0px; bottom: 0px; height:40px;\">");
		output.append("</div>\n"); // main controls

		if (surveyClass != null && surveyClass.contains("pages")) {

			output.append("<div class='jump-nav'>\n");
			output.append("<a class='lang btn btn-default disabled first-page' data-lang='forms.pages.return' href='#'>Return to Beginning</a>\n");
			output.append("<a class='lang btn btn-default disabled last-page' data-lang='form.pages.end' href='#'>Go to End</a>\n");
			output.append("</div>");
		}

		output.append("</div>\n"); // content
		output.append("</section> <!-- end form-footer -->\n");

		output.append("</article>\n");
		output.append("</main> <!-- end main -->\n");

		return output;
	}



	private String addNoScriptWarning() {
		StringBuffer output = new StringBuffer();
		output.append("<noscript>");
		output.append("<div>");
		output.append("<span style=\"color:red\">");
		output.append(localisation.getString("wf_njs"));
		output.append("</span>");
		output.append("</div>");
		output.append("</noscript>");

		return output.toString();
	}

	private Response getSubmissionCountPage(int count, String formUrl) {
		Response response = null;
		StringBuffer output = new StringBuffer();

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
			output.append("<link href='/css/bootstrap.v4.5.min.css' rel='stylesheet'>");
			output.append("<link href='/css/fa.v5.15.1.all.min.css' rel='stylesheet'>");
			output.append("<script src='/js/libs/modernizr.js'></script>");
			output.append("<script src='/js/libs/jquery-3.5.1.min.js'></script>");
			output.append("<script src='/js/libs/bootstrap.bundle.v4.5.min.js'></script>");
			output.append("<style>");
			output.append(
				".success {"
					+ "color: green;"
					+ "text-align: center;"
					+ "margin-top: 100px;"
					+ "margin-bottom: 50px;"
					+ "font-size: 86px;"
				+ "}"
				+ ".msg {"
					+ "text-align: center;"
					+ "margin-top: 20px;"
					+ "margin-bottom: 10px;"
					+ "font-size: 1.2em;"
				+ "}"
				+ ".count {"
					+ "text-align: center;"
					+ "font-size: 3em;"
					+ "font-weight: bold;"
					+ "margin-bottom: 10px;"
				+ "}"
				+ ".proceed {"
					+ "text-align: center;"
					+ "margin-top: 40px;"
				+ "}"
			);
			output.append("</style>");
			output.append("<script type='module' src='/js/msg.js'></script>");
			output.append("</head>");
			output.append("<body>");
			output.append("<h1 class='success'><i class='fa fa-check'></i></h1>");
			output.append("<p class='msg'>");
			output.append("<span class='lang' data-lang='mo_ms_count_pre'></span>");
			output.append(" <strong>" + count + "</strong> ");
			output.append("<span class='lang' data-lang='mo_ms_count_post'></span>");
			output.append("</p>");
			output.append("<div class='proceed'>");
			output.append("<a href='" + formUrl + "?proceed=true' class='btn btn-primary lang' data-lang='mo_ms_again'></a>");
			output.append("</div>");
			output.append("</body>");
			output.append("</html>");

			response = Response.status(Status.OK).entity(output.toString()).build();

		} catch (Exception e) {
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
			log.log(Level.SEVERE, e.getMessage(), e);
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
			output.append("<link href='/css/bootstrap.v4.5.min.css' rel='stylesheet'>");
			output.append("<link href='/css/fa.v5.15.1.all.min.css' rel='stylesheet'>");

			output.append("<script src='/js/libs/modernizr.js'></script>");
			output.append("<script src='/js/libs/jquery-3.5.1.min.js'></script>");
			output.append("<script src='/js/libs/bootstrap.bundle.v4.5.min.js'></script>");

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
			output.append("<script type='module' src='/js/msg.js'></script>");
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
	
	/*
	 * Tasks can pass an instance to be the initial data for a webform
	 * Random initial values can also be passed in the URL
	 * Combine these here
	 */
	private void mergeValuesToData(Instance data, String values) {
		
		if(values != null) {
			String [] a = values.split(",");
			if(a.length > 0) {
				for(int i = 0; i < a.length; i++) {
					String [] a2 = a[i].split(":");
					if(a2.length == 2) {
						data.values.put(a2[0].trim(), a2[1].trim());
					}
				}
			}
		}
			
	}
}
