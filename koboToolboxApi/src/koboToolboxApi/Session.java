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

import model.LogItemDt;
import model.LogsDt;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.http.auth.AuthenticationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.HourlyLogSummaryItem;
import org.smap.sdal.model.OrgLogSummaryItem;
import org.smap.sdal.model.SurveyIdent;

/*
 * Provides API access managed using a session key
 * 
 */
@Path("/v1/session")
public class Session extends Application {
	
	Authorise a = null;
	Authorise aOrg = null;
	
	private static Logger log =
			 Logger.getLogger(Session.class.getName());
	
	private class SessionResponse {
		String sessionKey;
		public SessionResponse(String key) {
			sessionKey = key;
		}
	}
	
	public Session() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);	 
	}
	
	/*
	 * Get a token
	 */
	@POST
	@Produces("application/json")
	@Path("/login")
	public Response login(@Context HttpServletRequest request,
			@FormParam("username") String username,
			@FormParam("password") String password) {
		
		String connectionString = "Session - login";
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		Response response;
		
		log.info("xxxxxxx: " + username + " : " + password);
		String sessionKey = null;
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			if(GeneralUtilityMethods.isPasswordValid(sd, username, password)) {
				sessionKey = GeneralUtilityMethods.getNewAccessKey(sd, username, false);
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			 SDDataSource.closeConnection(connectionString, sd);
		}
		
		if(sessionKey == null) {
			response = Response.status(Status.UNAUTHORIZED).header(HttpHeaders.WWW_AUTHENTICATE, "Smap").entity("Authorisation Error").build();
		} else {
			 SessionResponse sr = new SessionResponse(sessionKey);
			 response = Response.ok(gson.toJson(sr)).build();
		}

		return response; 
	}

	/*
	 * Get available surveys
	 */
	@GET
	@Produces("application/json")
	@Path("/surveys")
	public Response getSurveys(@Context HttpServletRequest request,
			@QueryParam("sessionKey") String sessionKey) throws AuthenticationException {
		
		log.info("xxxxx: " + sessionKey + " : " + request.getHeader("X-Token"));
		
		String connectionString = "session - Get Survey Idents";
		Response response = null;
		Connection sd = null;
	
		try {
			sd = SDDataSource.getConnection(connectionString);
			String user = null;
			try {
				user = authoriseSessionKey(sd, sessionKey);
			} catch (Exception e) {
				
			}
			if(user == null) {
				response = Response.status(Status.UNAUTHORIZED).header(HttpHeaders.WWW_AUTHENTICATE, "Smap").entity("Authorisation Error").build();
			} else {
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, user));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				SurveyManager sm = new SurveyManager(localisation, "UTC");
				ArrayList<SurveyIdent> surveyIdents = sm.getSurveyIdentList(sd, user, false);
				Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
				
				response = Response.ok(gson.toJson(surveyIdents)).build();	
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			response = Response.serverError().build();
		} finally {			
			SDDataSource.closeConnection(connectionString , sd);				
		}

		return response;
	}
	
	private String authoriseSessionKey(Connection sd, String sessionKey) throws AuthenticationException {
		
		String user = null;
		try {
			user = GeneralUtilityMethods.getDynamicUser(sd, sessionKey);
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}
		if(user == null) {
			throw new AuthenticationException();
		}
		a.isAuthorised(sd, user);
		return user;
	}

}

