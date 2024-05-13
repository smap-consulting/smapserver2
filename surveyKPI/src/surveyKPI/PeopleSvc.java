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
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.PeopleManager;
import org.smap.sdal.model.People;

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
@Path("/people")
public class PeopleSvc extends Application {

	private static Logger log =
			 Logger.getLogger(PeopleSvc.class.getName());
	
	Authorise a = null;	
	
	public PeopleSvc() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);		
	}

	/*
	 * Add a person
	 */
	@POST
	public Response addPerson(@Context HttpServletRequest request,
			@FormParam("person") String personString) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String connectionString = "surveyKPI-Survey - add person";
		
		Type type = new TypeToken<People>(){}.getType();
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		People person = gson.fromJson(personString, type);
		
		log.info("Add Person:========== " + personString);
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		if(person.id > 0) {
			a.isValidOptin(sd, request.getRemoteUser(), person.id);
		}
		// End Authorisation
		
		ResourceBundle localisation = null;
		try {	
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			PeopleManager pm = new PeopleManager(localisation);
 
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			if(person.id <= 0) {
				pm.addPerson(sd, oId, person);
			} else {
				pm.updatePerson(sd, person);
			}
			
			response = Response.ok().build();
			
		} catch (SQLException e) {
			String msg = e.getMessage();
			if(msg.contains("forwarddest")) {	// Unique key
				response = Response.serverError().entity("Duplicate forwarding address").build();
			} else {
				response = Response.serverError().entity("SQL Error").build();
				log.log(Level.SEVERE,"SQL Exception", e);
			}
		} catch (AuthorisationException e) {
			log.info("Authorisation Exception");
			String msg = null;
			if(localisation != null) {
				msg = localisation.getString("ae");
			} else {
				msg = "Authoisation Error";
			}
		    response = Response.serverError().entity(msg).build();
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

	/*
	 * Delete the contact
	 */
	@DELETE
	@Path("/{id}")
	public Response deletePerson(
			@Context HttpServletRequest request,
			@PathParam("id") int id) { 
		
		Response response = null;
		String connectionString = "surveyKPI-Survey - delete contact";
			
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		if(id > 0) {
			a.isValidOptin(sd, request.getRemoteUser(), id);
		}
		// End Authorisation
		
		ResourceBundle localisation = null;
		try {	
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			PeopleManager pm = new PeopleManager(localisation);
			pm.deletePerson(sd, id);
			
			response = Response.ok().build();
			
		} catch (AuthorisationException e) {
			log.info("Authorisation Exception");
			String msg = null;
			if(localisation != null) {
				msg = localisation.getString("ae");
			} else {
				msg = "Authoisation Error";
			}
		    response = Response.serverError().entity(msg).build();
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

