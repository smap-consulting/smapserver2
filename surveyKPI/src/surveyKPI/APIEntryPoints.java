package surveyKPI;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;

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

import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
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
import org.smap.sdal.Utilities.SystemException;
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.managers.ContactManager;
import org.smap.sdal.managers.DataManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.MailoutManager;
import org.smap.sdal.managers.RecordEventManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.model.CaseCount;
import org.smap.sdal.model.DataItemChangeEvent;
import org.smap.sdal.model.LogsDt;
import org.smap.sdal.model.Mailout;
import org.smap.sdal.model.MailoutPerson;
import org.smap.sdal.model.MailoutPersonDt;
import org.smap.sdal.model.MailoutPersonTotals;
import org.smap.sdal.model.SubItemDt;
import org.smap.sdal.model.SubsDt;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskProperties;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Allow the GUI to get data from API functions while having a different entry point that can 
 * be authenticated differently from the API users
 */
@Path("/api")
public class APIEntryPoints extends Application {

	private static Logger log =
			 Logger.getLogger(APIEntryPoints.class.getName());
	
	Authorise aAdminAnalyst = null;
	Authorise aContacts = null;
	
	boolean forDevice = false;	// Attachment URL prefixes should be in the client format
	
	public APIEntryPoints() {
		ArrayList<String> authMailout = new ArrayList<String> ();	
		authMailout.add(Authorise.ADMIN);
		authMailout.add(Authorise.ANALYST);
		aAdminAnalyst = new Authorise(authMailout, null);
		
		ArrayList<String> authContacts = new ArrayList<String> ();	
		authContacts.add(Authorise.ADMIN);
		aContacts = new Authorise(authContacts, null);		
	}
	
	/*
	 * Get records for an individual survey in JSON format
	 */
	@GET
	@Produces("application/json")
	@Path("/data/{sIdent}")
	public Response getDataRecordsServiceSmap(@Context HttpServletRequest request,
			@Context HttpServletResponse response,
			@PathParam("sIdent") String sIdent,
			@QueryParam("start") int start,				// Primary key to start from
			@QueryParam("limit") int limit,				// Number of records to return
			@QueryParam("mgmt") boolean mgmt,
			@QueryParam("oversightSurvey") String oversightSurvey,	// Console
			@PathParam("view") int viewId,					// Console
			@QueryParam("schema") boolean schema,			// Console return schema with the data
			@QueryParam("group") boolean group,			// If set include a dummy group value in the response, used by duplicate query
			@QueryParam("sort") String sort,				// Column Human Name to sort on
			@QueryParam("dirn") String dirn,				// Sort direction, asc || desc
			@QueryParam("form") String formName,			// Form name (optional only specify for a child form)
			@QueryParam("start_parkey") int start_parkey,// Parent key to start from
			@QueryParam("parkey") int parkey,			// Parent key (optional, use to get records that correspond to a single parent record)
			@QueryParam("hrk") String hrk,				// Unique key (optional, use to restrict records to a specific hrk)
			@QueryParam("key") String key,				// Unique key (optional, use to restrict records to a specific key - same as hrk)
			@QueryParam("format") String format,			// dt for datatables otherwise assume kobo
			@QueryParam("bad") String include_bad,		// yes | only | none Include records marked as bad
			@QueryParam("completed") String include_completed,		// If yes return unassigned records that have the final status
			@QueryParam("audit") String audit_set,		// if yes return audit data
			@QueryParam("merge_select_multiple") String merge, 	// If set to yes then do not put choices from select multiple questions in separate objects
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("geojson") String geojson,		// if set to yes then format as geoJson
			@QueryParam("geom_question") String geomQuestion,
			@QueryParam("links") String links,
			@QueryParam("meta") String meta,
			@QueryParam("filter") String filter,
			@QueryParam("dd_filter") String dd_filter,		// Drill Down Filter when driling down to a child survey
			@QueryParam("prikey") int prikey,				// Return data for a specific primary key (Distinct from using start with limit 1 as this is for drill down and settings should not be stored)
			@QueryParam("dd_hrk") String dd_hrk,				// Return data matching key when drilling down to parent
			@QueryParam("dateName") String dateName,			// Name of question containing the date to filter by
			@QueryParam("startDate") Date startDate,
			@QueryParam("endDate") Date endDate,
			@QueryParam("instanceid") String instanceId,
			@QueryParam("getSettings") boolean getSettings			// if set true get the settings from the database
			) throws ApplicationException, Exception { 
			
		boolean incLinks = false;
		if(links != null && (links.equals("true") || links.equals("yes"))) {
			incLinks = true;
		}
		if(formName != null) {
			incLinks = false;		// Links can only be specified for the main form
		}
		
		if(key != null) {
			hrk = key;
		}
		
		boolean includeMeta = true;		// Default to true for get all records (Historical consistency reason)
		if(meta != null && (meta.equals("false") || meta.equals("no"))) {
			includeMeta = false;
		}
		
		// Authorisation, localisation and timezone are determined in getDataRecords
		DataManager dm = new DataManager(null, "UTC");	
		String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
		String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, forDevice);
		
		dm.getDataRecords(request, response, sIdent, start, limit, mgmt, oversightSurvey, viewId, 
				schema, group, sort, dirn, formName, start_parkey,
				parkey, hrk, format, include_bad, include_completed, audit_set, merge, geojson, geomQuestion,
				tz, incLinks, 
				filter, 
				dd_filter, prikey, dd_hrk, dateName, startDate, endDate, getSettings, 
				instanceId, includeMeta, 
				urlprefix,
				attachmentPrefix);
		
		return Response.status(Status.OK).build();
	}
	
	/*
	 * Get a list of emails in a mailout
	 */
	@GET
	@Produces("application/json")
	@Path("/mailout/{mailoutId}/emails")
	public Response getSubscriptions(@Context HttpServletRequest request,
			@PathParam("mailoutId") int mailoutId,
			@QueryParam("dt") boolean dt
			) { 
		
		String connectionString = "API - get emails in mailout";
		Response response = null;
		ArrayList<MailoutPerson> data = new ArrayList<> ();
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aAdminAnalyst.isAuthorised(sd, request.getRemoteUser());
		aAdminAnalyst.isValidMailout(sd, request.getRemoteUser(), mailoutId);
		// End authorisation
		
		try {
	
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			MailoutManager mm = new MailoutManager(localisation);
			data = mm.getMailoutPeople(sd, mailoutId, oId, dt);				
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			if(dt) {
				MailoutPersonDt mpDt = new MailoutPersonDt();
				mpDt.data = data;
				response = Response.ok(gson.toJson(mpDt)).build();
			} else {
				response = Response.ok(gson.toJson(data)).build();
			}
			
	
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error: ", e);
			String msg = e.getMessage();
			if(msg == null) {
				msg = "System Error";
			}
		    throw new SystemException(msg);
		} finally {
					
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
		
	}
	
	/*
	 * Get a list of mailouts
	 */
	@GET
	@Path("/mailout/{survey}")
	@Produces("application/json")
	public Response getMailouts(@Context HttpServletRequest request,
			@PathParam("survey") String surveyIdent,
			@QueryParam("links") boolean links
			) { 

		Response response = null;
		String connectionString = "surveyKPI-Mailout List";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aAdminAnalyst.isAuthorised(sd, request.getRemoteUser());
		aAdminAnalyst.isValidSurveyIdent(sd, request.getRemoteUser(), surveyIdent, false, true);
		// End Authorisation
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
						
			MailoutManager mm = new MailoutManager(localisation);
				
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
			ArrayList<Mailout> mailouts = mm.getMailouts(sd, surveyIdent, links, urlprefix); 
				
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(mailouts);
			response = Response.ok(resp).build();
				
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
			String msg = e.getMessage();
			if(msg == null) {
				msg = "System Error";
			}
		    throw new SystemException(msg);
		    
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * Add or update a mailout campaign
	 */
	@POST
	@Path("/mailout")
	public Response addUpdateMailout(@Context HttpServletRequest request,
			@FormParam("mailout") String mailoutString) { 
		
		Response response = null;
		String connectionString = "mailout - add mailout";
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		Mailout mailout = null;
		try {
			mailout = gson.fromJson(mailoutString, Mailout.class);
		} catch (Exception e) {
			throw new SystemException("JSON Error: " + e.getMessage());
		}
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aAdminAnalyst.isAuthorised(sd, request.getRemoteUser());
		if(mailout.id > 0) {
			aAdminAnalyst.isValidMailout(sd, request.getRemoteUser(), mailout.id);
		}
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aAdminAnalyst.isValidSurveyIdent(sd, request.getRemoteUser(), mailout.survey_ident, false, superUser);
		// End Authorisation
		
		try {	
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			MailoutManager mm = new MailoutManager(localisation);
 
			if(mailout.id <= 0) {
				mailout.id = mm.addMailout(sd, mailout);
			} else {
				mm.updateMailout(sd, mailout);
			}
			
			response = Response.ok(gson.toJson(mailout)).build();
			
		} catch(ApplicationException e) {
			throw new SystemException(e.getMessage());
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error: ", e);
			String msg = e.getMessage();
			if(msg == null) {
				msg = "System Error";
			}
		    throw new SystemException(msg);
		    
		} finally {			
			SDDataSource.closeConnection(connectionString, sd);			
		}

		return response;
	}
	
	/*
	 * Get subscription totals
	 */
	@GET
	@Produces("application/json")
	@Path("/mailout/{mailoutId}/emails/totals")
	public Response getSubscriptionTotals(@Context HttpServletRequest request,
			@PathParam("mailoutId") int mailoutId
			) { 
		
		String connectionString = "API - get emails in mailout";
		Response response = null;
		MailoutPersonTotals totals = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aAdminAnalyst.isAuthorised(sd, request.getRemoteUser());
		aAdminAnalyst.isValidMailout(sd, request.getRemoteUser(), mailoutId);
		// End authorisation
		
		try {
	
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		
			
			MailoutManager mm = new MailoutManager(localisation);
			totals = mm.getMailoutPeopleTotals(sd,mailoutId);		
			
			Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			response = Response.ok(gson.toJson(totals)).build();			
	
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error: ", e);
			String msg = e.getMessage();
			if(msg == null) {
				msg = "System Error";
			}
		    throw new SystemException(msg);
		} finally {
					
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
		
	}
	
	/*
	 * Get subscription entries
	 */
	@GET
	@Path("/subscriptions")
	@Produces("application/json")
	public Response getSubscriptions(@Context HttpServletRequest request,
			@QueryParam("dt") boolean dt,
			@QueryParam("tz") String tz					// Timezone
			) { 
		
		String connectionString = "API - get subscriptions";
		Response response = null;
		ArrayList<SubItemDt> data = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aContacts.isAuthorised(sd, request.getRemoteUser());
		
		tz = (tz == null) ? "UTC" : tz;
		
		try {
	
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			ContactManager cm = new ContactManager(localisation);
			data = cm.getSubscriptions(sd, request.getRemoteUser(), tz, dt);
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			if(dt) {
				SubsDt subs = new SubsDt();
				subs.data = data;
				response = Response.ok(gson.toJson(subs)).build();
			} else {
				response = Response.ok(gson.toJson(data)).build();
			}
			
	
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
					
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
		
	}

	/*
	 * DataTables API version 1 /log
	 * Get log entries
	 */
	@GET
	@Path("/log")
	@Produces("application/json")
	public Response getDataTableLogs(@Context HttpServletRequest request,
			@QueryParam("draw") int draw,
			@QueryParam("start") int start,
			@QueryParam("limit") int limit,
			@QueryParam("length") int length,
			@QueryParam("sort") String sort,			// Column Name to sort on
			@QueryParam("dirn") String dirn,			// Sort direction, asc || desc
			@QueryParam("month") int month,
			@QueryParam("year") int year,
			@QueryParam("tz") String tz
			) { 
		
		String connectionString = "Get logs";
		Response response = null;
		String user = request.getRemoteUser();
		LogsDt logs = new LogsDt();
		logs.draw = draw;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aAdminAnalyst.isAuthorised(sd, request.getRemoteUser());
		
		String sqlTotal = "select count(*) from log where o_id = ?";
		PreparedStatement pstmtTotal = null;
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		if(dirn == null) {
			dirn = "desc";
		} else {
			dirn = dirn.replace("'", "''");
		}
		if(sort == null) {
			sort = "id";
		}
		if(dirn.equals("desc") && start == 0) {
			start = Integer.MAX_VALUE;
		}
		
		// Limit overrides length which is retained for backwards compatability
		if(limit > 0) {
			length = limit;
		}
		// Set a default limit for the client app of 10,000 records
		if(length == 0 || length > 10000) {
			length = 10000;
		}
		
		if(tz == null || tz.equals("undefined")) {
			tz = "UTC";
		}
		
		try {
	
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			
			/*
			 * Get total log entries
			 */
			pstmtTotal = sd.prepareStatement(sqlTotal);
			pstmtTotal.setInt(1, oId);
			rs = pstmtTotal.executeQuery();
			if(rs.next()) {
				logs.recordsTotal = rs.getInt(1);
				logs.recordsFiltered = rs.getInt(1);
			}
			rs.close();
				
			LogManager lm = new LogManager();
			if(year > 0 && month > 0) {
				logs.data = lm.getMonthLogEntries(sd, localisation, oId, year, month, tz, true);
			} else {
				logs.data = lm.getLogEntries(sd, localisation, oId, dirn, start, sort, length, true);
			}
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			response = Response.ok(gson.toJson(logs)).build();
			
	
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {
			
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			try {if (pstmtTotal != null) {pstmtTotal.close();	}} catch (SQLException e) {	}
					
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
		
	}
	
	/*
	 * Creates a new task
	 */
	@POST
	@Path("/tasks")
	@Produces("application/json")
	public Response createTask(@Context HttpServletRequest request,
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("preserveInitialData") boolean preserveInitialData,		// Set true when the initial data for a task should not be updated
			@FormParam("task") String task
			) throws ApplicationException, Exception { 
		
		/*
		 * Localisation and timezone will be determined in the createTask function
		 */
		TaskManager tm = new TaskManager(null, null);
		return tm.createTask(request, task, preserveInitialData);
	}

	/*
	 * Get similar records for an individual survey in JSON format
	 */
	@GET
	@Produces("application/json")
	@Path("/data/similar/{sId}/{select}")
	public Response getSimilarDataRecords(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("select") String select,			// comma separated list of qname::function
														//  where function is none || lower
			@QueryParam("start") int start,
			@QueryParam("limit") int limit,
			@QueryParam("mgmt") boolean mgmt,
			@QueryParam("form") int fId,				// Form id (optional only specify for a child form)
			@QueryParam("format") String format			// dt for datatables otherwise assume an API call
			) { 

		DataManager dm = new DataManager(null, null);
		
		String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
		String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, forDevice);
		
		return dm.getSimilarDataRecords(request, select, format, 
				sId, fId, mgmt, start, limit, 
				urlprefix, 
				attachmentPrefix);
	}
	
	/*
	 * Get changes to a record
	 */
	@GET
	@Produces("application/json")
	@Path("/data/changes/{sId}/{key}")
	public Response getRecordChanges(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("key") String key,		
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("geojson") String geojson		// if set to yes then format as geoJson
			) throws ApplicationException, Exception { 
		
		Response response = null;
		String connectionString = "koboToolboxApi - get data changes";
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.VIEW_OWN_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		Authorise a = new Authorise(authorisations, null);

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
			
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		try {			
			String tableName = GeneralUtilityMethods.getMainResultsTable(sd, cResults, sId);
			String thread = GeneralUtilityMethods.getThread(cResults, tableName, key);
			RecordEventManager rem = new RecordEventManager();
			ArrayList<DataItemChangeEvent> changeEvents = rem.getChangeEvents(sd, tz, tableName, thread);
			
			response = Response.ok(gson.toJson(changeEvents)).build();
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, "Exception", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}
		
		return response;
		
	}
	
	/*
	 * Returns counts of created and closed cases over time
	 */
	@GET
	@Path("/cases/progress/{sId}")
	@Produces("application/json")
	public Response getOpenClosed(@Context HttpServletRequest request,
			@PathParam("sId") int sId,						// Any survey in the survey bundle
			@QueryParam("interval") String interval,		// hour, day, week
			@QueryParam("intervalCount") int intervalCount,
			@QueryParam("aggregationInterval") String aggregationInterval,	// hour, day, week
			@QueryParam("tz") String tz) { 

		Response response = null;
		String connectionString = "SurveyKPI - getOpenClosedCases";

		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.VIEW_DATA);
		Authorise a = new Authorise(authorisations, null);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(),e);
		}
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		
		tz = (tz == null) ? "UTC" : tz;
		
		Connection cResults = null;
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			cResults = ResultsDataSource.getConnection(connectionString);
			
			// Validate parameters
			interval = "day";
			if(intervalCount <= 0) {
				intervalCount = 7;		// By default last week of data
			}
			aggregationInterval = "day";

			CaseManager cm = new CaseManager(localisation);
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			ArrayList<CaseCount> cc = cm.getOpenClosed(sd, cResults, sIdent, interval, intervalCount, aggregationInterval);
			
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			response = Response.ok(gson.toJson(cc)).build();
			
		} catch (SQLException e) {
			e.printStackTrace();
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}

		return response;
	}
	
	/*
	 * Returns a single task assignment
	 */
	@GET
	@Path("/tasks/assignment/{id}")
	@Produces("application/json")
	public Response getTaskAssignment(@Context HttpServletRequest request,
			@PathParam("id") int aId,
			@QueryParam("taskid") int taskId,			// Optional task if if unassigned task
			@QueryParam("tz") String tz					// Timezone
			) throws ApplicationException, Exception { 
		
		String connectionString = "SurveyKPI - Tasks - get Task Assignment";
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		authorisations.add(Authorise.MANAGE_TASKS);
		Authorise a = new Authorise(authorisations, null);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		if(aId > 0) {
			a.isValidAssignment(sd, request.getRemoteUser(), aId);
		} else {
			a.isValidTask(sd, request.getRemoteUser(), taskId);
		}
		// End authorisation

		Response response = null;
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String urlprefix = request.getScheme() + "://" + request.getServerName();
			
			// Get assignments
			TaskManager tm = new TaskManager(localisation, tz);
			TaskListGeoJson t = tm.getTasks(
					sd, 
					urlprefix,
					0,		// Organisation id 
					0, 		// task group id
					aId == 0 ? taskId : 0,		// task id
					aId,		// Assignment Id
					true, 
					0,		// userId 
					null, 
					null,	// period 
					0,		// start 
					0,		// limit
					null,	// sort
					null,
					true);	// sort direction	
			
			if(t != null && t.features.size() > 0) {
				TaskProperties tp = t.features.get(0).properties;
				
				// Return groups to calling program
				Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
				String resp = gson.toJson(tp);	
				response = Response.ok(resp).build();	
			} else {
				response = Response.serverError().entity(localisation.getString("mf_nf")).build();
			}
			
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
}

