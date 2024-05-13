package koboToolboxApi;
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
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import managers.DataEntryPoints;

import java.sql.Connection;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.GET;
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
import org.smap.sdal.managers.DataManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.RecordEventManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.DataItemChangeEvent;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.RecordUpdateEvent;
import org.smap.sdal.model.Survey;

/*
 * Provides access to collected data
 */
@Path("/v2/data")
public class Data2 extends Application {

	boolean forDevice = true;	// Attachment URL prefixes for API should have the device/API format
	
	Authorise a = null;			// TODO remove
	Authorise aSuper = null;	// TODO remove
	
	private static String VERSION = "v2";
	
	private static Logger log =
			Logger.getLogger(Data2.class.getName());

	LogManager lm = new LogManager();		// Application log

	/*
	 * KoboToolBox API version 1 /data
	 * Returns a list of data end points
	 */
	@GET
	@Produces("application/json")
	public Response getData(@Context HttpServletRequest request) { 

		String connectionString = "koboToolBoxAPI-getData2";
		DataEntryPoints dep = new DataEntryPoints();
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		String remoteUser = GeneralUtilityMethods.getApiKeyUser(sd, request);
		
		return dep.getData(VERSION,sd, connectionString, request, remoteUser);
		
	}

	/*
	 * Get records for an individual survey in JSON format
	 * Survey and form identifiers are strings
	 */
	@GET
	@Produces("application/json")
	@Path("/{sIdent}")
	public Response getDataRecordsService(@Context HttpServletRequest request,
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
		
		String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
		String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, forDevice);
		
		String connectionString = "koboToolboxApi - get data records 2";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		String remoteUser = GeneralUtilityMethods.getApiKeyUser(sd, request);
		if(remoteUser == null) {
			return Response.status(Status.UNAUTHORIZED).build();
		}
		
		// Authorisation, localisation and timezone are determined in getDataRecords
		DataManager dm = new DataManager(null, "UTC");		
		dm.getDataRecords(sd,
				connectionString,
				request, 
				remoteUser,
				response, sIdent, start, limit, mgmt, oversightSurvey, viewId, 
				schema, group, sort, dirn, formName, start_parkey,
				parkey, hrk, format, include_bad, include_completed, audit_set, merge, geojson, geomQuestion,
				tz, incLinks, 
				filter, 
				dd_filter, 
				prikey, dd_hrk, dateName, startDate, endDate, getSettings, 
				instanceId, 
				includeMeta,
				urlprefix,
				attachmentPrefix);
		
		return Response.status(Status.OK).build();
	}
	
	/*
	 * KoboToolBox API version 1 /data
	 * Get a single data record in json format
	 */
	@GET
	@Produces("application/json")
	@Path("/{sIdent}/{uuid}")
	public Response getSingleDataRecord(@Context HttpServletRequest request,
			@PathParam("sIdent") String sIdent,
			@PathParam("uuid") String uuid,		
			@QueryParam("merge_select_multiple") String merge, 	// If set to yes then do not put choices from select multiple questions in separate objects
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("geojson") String geojson,		// if set to yes then format as geoJson
			@QueryParam("meta") String meta,				// If set true then include meta
			@QueryParam("hierarchy") String hierarchy
			) throws ApplicationException, Exception { 
		
		if(tz == null) {
			tz = "UTC";
		}
		
		String connectionString = "koboToolboxApi - get single data record-2";
		DataEntryPoints dep = new DataEntryPoints();
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		String remoteUser = GeneralUtilityMethods.getApiKeyUser(sd, request);
		
		return dep.getSingleDataRecord(sd, connectionString, request, remoteUser,
				sIdent, uuid, meta, hierarchy, merge, tz);
		
	}
	
	/*
	 * KoboToolBox API version 1 /data
	 * Get multiple data records in hierarchy format
	 */
	@GET
	@Produces("application/json")
	@Path("/poll")
	public Response getMultipleHierarchyDataRecords(@Context HttpServletRequest request,
			@QueryParam("survey") String sIdent,	
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("filter") String filter
			) throws ApplicationException, Exception { 
		
		Response response;
	
		if(tz == null) {
			tz = "UTC";
		}
		
		String connectionString = "koboToolboxApi - get record - hierarchy";
		
		Connection cResults = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		
		int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);	
		
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		try {
			cResults = ResultsDataSource.getConnection(connectionString);
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
			String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, forDevice);
			
			if(!GeneralUtilityMethods.isApiEnabled(sd, request.getRemoteUser())) {
				throw new ApplicationException(localisation.getString("susp_api"));
			}
			
			DataManager dm = new DataManager(localisation, tz);

			response = dm.getRecordHierarchy(sd, cResults, request,
					sIdent,
					sId,
					null,
					"yes", 			// If set to yes then do not put choices from select multiple questions in separate objects
					localisation,
					tz,				// Timezone
					true,
					urlprefix,
					attachmentPrefix
					);	
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			String resp = "{error: " + e.getMessage() + "}";
			response = Response.serverError().entity(resp).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}
		return response;
	}
	
	/*
	 * Get changes to a record
	 */
	@GET
	@Produces("application/json")
	@Path("/changes/{sId}/{key}")
	public Response getRecordChanges(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("key") String key,		
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("geojson") String geojson		// if set to yes then format as geoJson
			) throws ApplicationException, Exception { 
		
		Response response = null;
		String connectionString = "koboToolboxApi - get data changes";
		
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
	 * Get changes applied to a survey's data
	 */
	@GET
	@Produces("application/json")
	@Path("/changes/{creatingSurveyId}/{changingSurveyId}/survey")
	public Response getSurveyDataChanges(@Context HttpServletRequest request,
			@PathParam("creatingSurveyId") int creatingSurveyId,
			@PathParam("changingSurveyId") int changingSurveyId,
			@QueryParam("from") Date startDate,
			@QueryParam("to") Date endDate,		
			@QueryParam("tz") String tz					// Timezone
			) throws ApplicationException, Exception { 
		
		Response response = null;
		String connectionString = "koboToolboxApi - get changes applied to a survey";
		
		if(tz == null) {
			tz = "UTC";
		}
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), creatingSurveyId, false, superUser);
		a.isValidSurvey(sd, request.getRemoteUser(), changingSurveyId, false, superUser);
		// End Authorisation
			
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		try {
			//Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			//ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String creatingSurveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, creatingSurveyId);
			String changingSurveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, changingSurveyId);
			String tableName = GeneralUtilityMethods.getMainResultsTable(sd, cResults, creatingSurveyId);
			
			RecordEventManager rem = new RecordEventManager();
			ArrayList<ArrayList<RecordUpdateEvent>> changeEvents = rem.getDataChanges(sd, tz, tableName, 
					creatingSurveyIdent, changingSurveyIdent, startDate, endDate);
			
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
	 * KoboToolBox API version 1 /data
	 * Get a single record in JSON format
	 */
	private Response getSingleRecord(
			Connection sd,
			Connection cResults,
			HttpServletRequest request,
			String sIdent,
			int sId,
			String uuid,
			String merge, 			// If set to yes then do not put choices from select multiple questions in separate objects
			ResourceBundle localisation,
			String tz,				// Timezone
			boolean includeMeta,
			String urlprefix,
			String attachmentPrefix
			) throws ApplicationException, Exception { 

		Response response;
			
			lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.API_SINGLE_VIEW, "Managed Forms or the API. ", 0, request.getServerName());
			
			
			if(!GeneralUtilityMethods.isApiEnabled(sd, request.getRemoteUser())) {
				throw new ApplicationException(localisation.getString("susp_api"));
			}

			SurveyManager sm = new SurveyManager(localisation, tz);
			
			Survey s = sm.getById(
					sd, 
					cResults, 
					request.getRemoteUser(),
					false,
					sId, 
					true, 		// full
					null, 		// basepath
					null, 		// instance id
					false, 		// get results
					false, 		// generate dummy values
					true, 		// get property questions
					false, 		// get soft deleted
					true, 		// get HRK
					"external", 	// get external options
					false, 		// get change history
					false, 		// get roles
					true,		// superuser 
					null, 		// geomformat
					false, 		// reference surveys
					false,		// only get launched
					false		// Don't merge set value into default values
					);
			
			ArrayList<Instance> instances = sm.getInstances(
					sd,
					cResults,
					s,
					s.getFirstForm(),
					0,
					null,
					uuid,
					sm,
					includeMeta,
					urlprefix,
					attachmentPrefix
					);
		
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			
			// Just return the first instance - at the top level there should only ever be one
			if(instances.size() > 0) {
				response = Response.ok(gson.toJson(instances.get(0))).build();
			} else {
				log.log(Level.SEVERE, "Instance not found for " + s.surveyData.displayName + " : " + uuid);
				response = Response.serverError().status(Status.NOT_FOUND).build();
			}

		return response;

	}
	
	/*
	 * Get similar records for an individual survey in JSON format
	 */
	@GET
	@Produces("application/json")
	@Path("/similar/{sId}/{select}")
	public Response getSimilarDataRecords(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("select") String select,			// comma separated list of qname::function
			//  where function is none || lower
			@QueryParam("start") int start,
			@QueryParam("limit") int limit,
			@QueryParam("mgmt") boolean mgmt,
			@QueryParam("form") int fId,				// Form id (optional only specify for a child form)
			@QueryParam("format") String format			// dt for datatables otherwise assume kobo
			) { 

		DataManager dm = new DataManager(null, null);
		String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
		String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, forDevice);
		return dm.getSimilarDataRecords(request, select, format, sId, fId, mgmt, start, limit, 
				urlprefix,
				attachmentPrefix);
	}
}

