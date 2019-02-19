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

import managers.DataManager;
import model.DataEndPoint;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.Set;
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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.CustomReportsManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.TableDataManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.model.ReportConfig;
import org.smap.sdal.model.TableColumn;
import org.smap.sdal.model.TaskListGeoJson;

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
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI - Tasks - getTasks");
		a.isAuthorised(sd, request.getRemoteUser());
		// End authorisation

		Response response = null;
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = 0;
			int tgId = 0;
			if(tg_id != 0) {
				tgId = tg_id;
			} else {
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
			
			// Get assignments
			TaskManager tm = new TaskManager(localisation, tz);
			TaskListGeoJson t = tm.getTasks(sd, 
					oId, 
					tgId, 
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
			SDDataSource.closeConnection("surveyKPI - Tasks - getTasks", sd);
		}
		
		return response;
	}
	



}

