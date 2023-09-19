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
import java.sql.PreparedStatement;
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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.SystemException;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.managers.MailoutManager;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.Mailout;
import org.smap.sdal.model.MailoutPerson;
import org.smap.sdal.model.MailoutPersonDt;
import org.smap.sdal.model.MailoutPersonTotals;

/*
 * Provides access to mailouts
 */
@Path("/v1/mailout")
public class MailoutApi extends Application {
	
	Authorise a = null;
	
	private static Logger log =
			 Logger.getLogger(MailoutApi.class.getName());
	
	public MailoutApi() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ANALYST);
		a = new Authorise(authorisations, null);
	}	
	
	/*
	 * Add or update a mailout campaign
	 */
	@POST
	public Response addUpdateMailout(@Context HttpServletRequest request,
			@FormParam("mailout") String mailoutString) { 
		
		Response response = null;
		String connectionString = "api/v1/mailout - add mailout";
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		Mailout mailout = null;
		try {
			mailout = gson.fromJson(mailoutString, Mailout.class);
		} catch (Exception e) {
			throw new SystemException("JSON Error: " + e.getMessage());
		}
	
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		if(mailout.id > 0) {
			a.isValidMailout(sd, request.getRemoteUser(), mailout.id);
		}
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isValidSurveyIdent(sd, request.getRemoteUser(), mailout.survey_ident, false, superUser);
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
	 * Get a list of mailouts
	 */
	@GET
	@Path("/{survey}")
	@Produces("application/json")
	public Response getMailouts(@Context HttpServletRequest request,
			@PathParam("survey") String surveyIdent,
			@QueryParam("links") boolean links
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
	 * Get a list of emails in a mailout
	 */
	@GET
	@Produces("application/json")
	@Path("/{mailoutId}/emails")
	public Response getSubscriptions(@Context HttpServletRequest request,
			@PathParam("mailoutId") int mailoutId,
			@QueryParam("dt") boolean dt
			) { 
		
		String connectionString = "API - get emails in mailout";
		Response response = null;
		ArrayList<MailoutPerson> data = new ArrayList<> ();
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidMailout(sd, request.getRemoteUser(), mailoutId);
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
	 * Get subscription totals
	 */
	@GET
	@Produces("application/json")
	@Path("/{mailoutId}/emails/totals")
	public Response getSubscriptionTotals(@Context HttpServletRequest request,
			@PathParam("mailoutId") int mailoutId
			) { 
		
		String connectionString = "API - get emails in mailout";
		Response response = null;
		MailoutPersonTotals totals = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidMailout(sd, request.getRemoteUser(), mailoutId);
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
	 * Add a mailout email
	 */
	@POST
	@Produces("application/json")
	@Path("/{mailoutId}/email")
	public Response createLinks(
			@Context HttpServletRequest request,
			@PathParam("mailoutId") int mailoutId,
			@FormParam("email") String emailString,
			@FormParam("action") String action  // send || manual || none
			) { 
		
		Response response = null;
		String connectionString = "api/v1/mailout - add email";

		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
	
		MailoutPerson mailoutPerson = null;
		try {
			mailoutPerson = gson.fromJson(emailString, MailoutPerson.class);
		} catch (Exception e) {
			String msg = "JSON Error: " + e.getMessage() + " : " + emailString;
			log.info("Error: " + msg);
			throw new SystemException(msg);
		}
		
		String sqlUrl = "update mailout_people set link = ?,"
				+ "user_ident = ? where id = ?";
		PreparedStatement pstmtSent = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidMailout(sd, request.getRemoteUser(), mailoutId);
		// End Authorisation
		
		try {		
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
	
			// Convert action into an initial status
			String initialStatus = MailoutManager.STATUS_NEW;
			if(action != null) {
				if(action.equals("manual")) {
					initialStatus = MailoutManager.STATUS_MANUAL;
				} else if(action.equals("email")) {
					initialStatus = MailoutManager.STATUS_PENDING;
				}
			}
			
			// Validation
			if(mailoutPerson.email == null) {
				throw new ApplicationException(localisation.getString("email_nf"));
			}
			
			// Save mailout emails to the database
			MailoutManager mm = new MailoutManager(localisation);
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());				
			ArrayList<MailoutPerson> mop = new ArrayList<MailoutPerson> ();
			mop.add(mailoutPerson);
			int mailoutPersonId = mm.writeEmails(sd, oId, mop, mailoutId, initialStatus);
			
			if(action != null && action.equals("manual")) {
				
				Mailout mo = mm.getMailoutDetails(sd, mailoutId);
				
				// Create an action to complete the form
				ActionManager am = new ActionManager(localisation, "UTC");
				Action a = new Action("mailout");
				a.surveyIdent = mo.survey_ident;
				a.pId = GeneralUtilityMethods.getProjectIdFromSurveyIdent(sd, mo.survey_ident);
				a.single = !mo.multiple_submit;
				a.mailoutPersonId = mailoutPersonId;
				a.email = mailoutPerson.email;
				a.name = mo.name;
				a.anonymousCampaign = mo.anonymous;
				
				if(mailoutPerson.initialData != null) {
					a.initialData = mailoutPerson.initialData;
				}
				
				mailoutPerson.id = mailoutPersonId;
				String link = am.getLink(sd, a, oId, a.single);
				mailoutPerson.url = "https://" + request.getServerName() 
						+ "/app/myWork/webForm" + link;
				
				// Write the URL to the mailout
				String userIdent = null;
				int idx = link.lastIndexOf("/");
				if(idx >= 0) {
					userIdent = link.substring(idx + 1);
				}
				pstmtSent = sd.prepareStatement(sqlUrl);
				pstmtSent.setString(1,mailoutPerson.url);
				pstmtSent.setString(2,userIdent);
				pstmtSent.setInt(3, mailoutPersonId);
				log.info(pstmtSent.toString());
				pstmtSent.executeUpdate();
				
				response = Response.status(Status.OK).entity(gson.toJson(mailoutPerson)).build();
					
			} else {
				response = Response.status(Status.OK).entity("{}").build();
			}
			
			
		} catch(Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			String msg = e.getMessage();
			if(msg == null) {
				msg = "System Error";
			}
		    throw new SystemException(msg);
		} finally {
	
			if(pstmtSent != null) try {pstmtSent.close();}catch(Exception e) {}
			SDDataSource.closeConnection(connectionString, sd);
			
		}
		
		return response;
		
	}

}

