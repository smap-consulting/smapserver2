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
import java.sql.Connection;
import java.sql.SQLException;
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

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.UserLocationManager;
import org.smap.sdal.model.CaseCount;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Provides access to statistics on cases
 */
@Path("/v1/cases")
public class Cases extends Application {
	
	Authorise a = null;
	private static Logger log =
			Logger.getLogger(UserLocationManager.class.getName());

	LogManager lm = new LogManager();		// Application log

	public Cases() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.VIEW_DATA);
		a = new Authorise(authorisations, null);
	}

	/*
	 * Returns counts of created and closed cases over time
	 */
	@GET
	@Path("/progress/{sId}")
	@Produces("application/json")
	public Response getOpenClosed(@Context HttpServletRequest request,
			@PathParam("sId") int sId,						// Any survey in the survey bundle
			@QueryParam("interval") String interval,		// hour, day, week
			@QueryParam("intervalCount") int intervalCount,
			@QueryParam("aggregationInterval") String aggregationInterval,	// hour, day, week
			@QueryParam("tz") String tz) { 

		Response response = null;
		String connectionString = "API - getOpenClosedCases";

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
	 * TODO 
	 * Stick to days for all intervals 
	 */
	private String validateInterval(String interval, String defInterval) {
		String valInterval = defInterval;	
		
		if(interval != null) {
			interval = interval.trim();
			if(interval.equals("day") || interval.equals("week")) {
				valInterval = interval;
			} else {
				log.info("Error: Invalid Interval: " + interval);
			}
		}
		
		return valInterval;
	}
}

