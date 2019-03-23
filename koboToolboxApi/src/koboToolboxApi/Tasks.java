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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskServerDefn;

/*
 * Provides access to collected data
 */
@Path("/v1/tasks")
public class Tasks extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(Tasks.class.getName());

	LogManager lm = new LogManager();		// Application log

	// Tell class loader about the root classes.  (needed as tomcat6 does not support servlet 3)
	public Set<Class<?>> getClasses() {
		Set<Class<?>> s = new HashSet<Class<?>>();
		s.add(Tasks.class);
		return s;
	}

	public Tasks() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		a = new Authorise(authorisations, null);
	}

	/*
	 * Returns a list of tasks
	 */
	@GET
	@Produces("application/json")
	public Response getTasks(@Context HttpServletRequest request,
			@QueryParam("user") String userIdent,		// User to get tasks for
			@QueryParam("period") String period,			// Period to get tasks for all || week || month
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("tg_id") int tg_id,				// Task group			
			@QueryParam("start") int start,				// Task id to start from			
			@QueryParam("limit") int limit,				// Number of records to return
			@QueryParam("sort") String sort,				// Column to sort on
			@QueryParam("dirn") String dirn				// Sort direction, asc || desc
			
			) throws ApplicationException, Exception { 
		
		String connectionString = "surveyKPI - Tasks - getTasks";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		if(tg_id > 0) {
			a.isValidTaskGroup(sd, request.getRemoteUser(), tg_id);
		}
		// End authorisation

		Response response = null;
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = 0;
			if(tg_id == 0) {
				oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser(), 0);
			}
			
			// Parameters
			int userId = 0;								// All Users
			if(userIdent != null) {
				if(userIdent.equals("_unassigned")) {
					userId = -1;							// Only unassigned
				} else {
					userId = GeneralUtilityMethods.getUserId(sd, userIdent);
				}
			}
			
			if(period == null) {
				period = "week";		// As per default in UI
			}
			
			String urlprefix = request.getScheme() + "://" + request.getServerName();
			
			// Get assignments
			TaskManager tm = new TaskManager(localisation, tz);
			TaskListGeoJson t = tm.getTasks(sd, 
					urlprefix,
					oId, 
					tg_id, 
					0,			// task id
					true, 
					userId, 
					null, 
					period, 
					start, 
					limit,
					sort,
					dirn);		
			
			// Return groups to calling program
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			String resp = gson.toJson(t);	
			response = Response.ok(resp).build();	
			
		} catch(Exception ex) {
			log.log(Level.SEVERE,ex.getMessage(), ex);
			response = Response.serverError().entity(ex.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	/*
	 * Returns a single task
	 */
	@GET
	@Path("/{id}")
	@Produces("application/json")
	public Response getTask(@Context HttpServletRequest request,
			@PathParam("id") int taskId,
			@QueryParam("tz") String tz					// Timezone
			) throws ApplicationException, Exception { 
		
		String connectionString = "surveyKPI - Tasks - get Task";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidTask(sd, request.getRemoteUser(), taskId);
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
					taskId,
					true, 
					0,		// userId 
					null, 
					null,	// period 
					0,		// start 
					0,		// limit
					null,	// sort
					null);	// sort direction	
			
			if(t != null && t.features.size() > 0) {
				TaskFeature tf = t.features.get(0);
				
				// Return groups to calling program
				Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
				String resp = gson.toJson(tf);	
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
	
	/*
	 * Creates a new task
	 */
	@POST
	@Path("/new")
	@Produces("application/json")
	public Response getTask(@Context HttpServletRequest request,
			@QueryParam("tz") String tz,					// Timezone
			@FormParam("task") String task
			) throws ApplicationException, Exception { 
		
		Response response = null;
		String connectionString = "surveyKPI - Tasks - add new task";
		
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		TaskFeature tf = gson.fromJson(task, TaskFeature.class);	
		
		// Authorisation - Access
		Connection cResults = null;
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidTaskGroup(sd, request.getRemoteUser(), tf.properties.tg_id);
		// End Authorisation
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			if(tz == null) {
				tz = "UTC";	// Set default for timezone
			}
			
			cResults = ResultsDataSource.getConnection(connectionString);
			
			TaskManager tm = new TaskManager(localisation, tz);
			TaskServerDefn tsd = tm.convertTaskFeature(tf);
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser(), 0);
			tm.writeTask(sd, cResults, tf.properties.tg_id, tsd, request.getServerName(), false, oId, true, request.getRemoteUser());
			response = Response.ok().build();
		
		} catch (Exception e) {
			log.log(Level.SEVERE,e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
	
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
			
		}
		
		return response;
	}


}

