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
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.EmailManager;
import org.smap.sdal.managers.MailoutManager;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.managers.PeopleManager;
import org.smap.sdal.managers.ProjectManager;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.Mailout;
import org.smap.sdal.model.Notification;
import org.smap.sdal.model.People;
import org.smap.sdal.model.Project;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Manage subscribers
 * This is closely related to the subscribers service
 */
@Path("/mailout")
public class MailoutSvc extends Application {

	private static Logger log =
			 Logger.getLogger(MailoutSvc.class.getName());
	
	Authorise a = null;	
	
	public MailoutSvc() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);		
	}
	
	/*
	 * Get a list of mailouts
	 */
	@GET
	@Path("/{survey}")
	@Produces("application/json")
	public Response getMailouts(@Context HttpServletRequest request,
			@PathParam("survey") String surveyIdent
			) { 

		Response response = null;
		String connectionString = "surveyKPI-Mailout List";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurveyIdent(sd, request.getRemoteUser(), surveyIdent, false, true);
		// End Authorisation
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
						
			MailoutManager mm = new MailoutManager(localisation);
						
			ArrayList<Mailout> mailouts = mm.getMailouts(sd, surveyIdent); 
				
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			String resp = gson.toJson(mailouts);
			response = Response.ok(resp).build();
				
		} catch (Exception e) {
			
			log.log(Level.SEVERE,"Error: ", e);
		    response = Response.serverError().build();
		    
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	/*
	 * Add a person
	 */
	@POST
	public Response addMailout(@Context HttpServletRequest request,
			@FormParam("mailout") String mailoutString) { 
		
		Response response = null;
		String connectionString = "surveyKPI-Survey - add mailout";
		
		Type type = new TypeToken<Mailout>(){}.getType();
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		Mailout mailout = gson.fromJson(mailoutString, type);
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		if(mailout.id > 0) {
			// a.isValidMailout(sd, request.getRemoteUser(), mailout.id);
			// Validate survey
		}
		// End Authorisation
		
		try {	
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			MailoutManager mm = new MailoutManager(localisation);
 
			if(mailout.id <= 0) {
				mm.addMailout(sd, mailout);
			} else {
				//mm.updateMailout(sd, mailout);
			}
			
			response = Response.ok().build();
			
		} catch (Exception e) {
			String msg = e.getMessage();
			log.info(msg);
			if(msg == null) {
				msg = "System Error";
			}
		    response = Response.serverError().entity(msg).build();
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
			
		}

		return response;

	}

}

