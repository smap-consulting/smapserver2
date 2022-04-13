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
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.WebformChainingManager;
import org.smap.sdal.model.WebformChainRule;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/chainrules")
public class ChainRules extends Application {

	Authorise aGet = null;
	Authorise aUpdate = null;
	
	private static Logger log =
			 Logger.getLogger(ChainRules.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	public ChainRules() {
		
		ArrayList<String> authorisations1 = new ArrayList<String> ();
		ArrayList<String> authorisations2 = new ArrayList<String> ();
		
		authorisations1.add(Authorise.ANALYST);
		authorisations1.add(Authorise.VIEW_DATA);
		authorisations1.add(Authorise.ADMIN);
		authorisations1.add(Authorise.ENUM);
		authorisations1.add(Authorise.MANAGE_TASKS);
		
		authorisations2.add(Authorise.ANALYST);
		authorisations2.add(Authorise.ADMIN);
		
		aGet = new Authorise(authorisations1, null);
		aUpdate = new Authorise(authorisations2, null);
		
	}

	/*
	 * Get the webform chain rules
	 */
	@Path("/{sId}")
	@GET
	@Produces("application/json")
	public Response getTemplates(@Context HttpServletRequest request,
			@PathParam("sId") int sId) { 
 
		Response response = null;
		String connectionString = "surveyKPI - Get Webform Chain Rules";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		aGet.isAuthorised(sd, request.getRemoteUser());
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);			

			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			WebformChainingManager cm = new WebformChainingManager(localisation, "UTC");
			ArrayList<WebformChainRule> rules = cm.getRules(sd, sIdent);
			
			response = Response.ok(gson.toJson(rules)).build();
			
		} catch (Exception e) {
			log.log(Level.SEVERE,e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	@POST
	@Produces("application/json")
	@Path("/add/{sId}")
	public Response getLink(
			@Context HttpServletRequest request, 
			@PathParam("sId") int sId,
			@FormParam("item") String item,
			@QueryParam("tz") String tz			// Keep this one to set up action manager
			) {
		
		Response response = null;
		String connectionString = "surveyKPI - Add webform chain rule";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		
		aUpdate.isAuthorised(sd, request.getRemoteUser());
		aUpdate.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);	
			WebformChainRule chainrule = gson.fromJson(item, WebformChainRule.class);
			chainrule.sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			chainrule.newSurveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, chainrule.newSurveyId);
			
			WebformChainingManager cm = new WebformChainingManager(localisation, "UTC");
			cm.writeRule(sd, request.getRemoteUser(), request.getServerName(), sId, chainrule);
			System.out.println(chainrule.newSurveyId);
			
		} catch (Exception e) {
			log.log(Level.SEVERE,e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}
	
}

