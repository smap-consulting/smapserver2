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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.RequestLimiter;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.ServerSettings;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.EmailManager;
import org.smap.sdal.managers.PeopleManager;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.Organisation;

import java.sql.*;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Manage subscribers
 * This is closely related to the People service
 */
@Path("/subscriptions")
public class Subscriptions extends Application {

	private static Logger log =
			Logger.getLogger(Subscriptions.class.getName());

	// One re-subscribe request every 10 minutes for each address entered
	private static final RequestLimiter limiter = new RequestLimiter(10 * 60 * 1000L);

	/*
	 * Unsubscribe
	 */
	@GET
	@Path("/unsubscribe/{token}")
	public Response unsubscribe(
			@Context HttpServletRequest request,
			@PathParam("token") String token) { 

		Response response = null;

		log.info("Unsubscribing user: " + token);

		Connection sd = SDDataSource.getConnection("surveyKPI-Register");

		try {

			// Localisation
			String hostname = request.getServerName();
			String loc_code = "en";
			if(hostname.contains("kontrolid")) {
				loc_code = "es";
			} 
			Locale locale = new Locale(loc_code);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		

			PeopleManager pm = new PeopleManager(localisation);
			pm.unsubscribe(sd, token);

			response = Response.ok().build();	

		} catch(ApplicationException e) {
			response = Response.serverError().entity(e.getMessage()).build();
		} catch(Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			SDDataSource.closeConnection("surveyKPI-Register", sd);
		}

		return response;
	}


	/*
	 * Subscribe Step 1
	 * Email the address a confirm link for each organisation it has unsubscribed from
	 *
	 * The answer is the same whatever the address, and only the mailbox learns which
	 * organisations it belongs to.  This used to take an organisation id, answer differently
	 * when the address was not one of its contacts, and send an email to any contact, even one
	 * who had not unsubscribed, each time giving them a new key, which broke the unsubscribe
	 * links in every email already sent to them.
	 */
	@POST
	@Path("/subscribe")
	public Response subscribe(
			@Context HttpServletRequest request,
			@FormParam("email") String email) {

		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		if(email == null || email.trim().isEmpty()) {
			return Response.status(Status.BAD_REQUEST).build();
		}
		
		Response response = null;
		String connectionString = "SurveyKPI - Post Subscribe";

		Connection sd = SDDataSource.getConnection(connectionString);

		try {

			ServerSettings.setBasePath(request);
			
			// Localisation
			String hostname = request.getServerName();
			String loc_code = "en";
			if(hostname.contains("kontrolid")) {
				loc_code = "es";
			} 
			Locale locale = new Locale(loc_code);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		

			int minutesToWait = limiter.minutesToWait(email);
			if(minutesToWait > 0) {
				log.info("warning: email: " + email + " multiple subscribe requests");
				String msg = localisation.getString("email_pas");
				msg = msg.replace("%s1", String.valueOf(minutesToWait));
				throw new ApplicationException(msg);
			}
			
			PeopleManager pm = new PeopleManager(localisation);
			EmailManager em = new EmailManager(localisation);
			for(PeopleManager.Unsubscribed u : pm.getUnsubscribed(sd, email)) {
				
				// Each organisation's own email server, as for the emails they unsubscribed from
				EmailServer emailServer = UtilityMethodsEmail.getEmailServer(sd, localisation, null, null, u.oId);
				if(emailServer == null) {
					log.info("Re-subscribe email not sent to " + email + " for organisation " + u.oId + ": " 
							+ localisation.getString("email_ne"));
					continue;
				}
				Organisation o = GeneralUtilityMethods.getOrganisation(sd, u.oId);
				String subscribeToken = pm.createSubscribeToken(sd, u.oId, email);
				
				String msg = localisation.getString("c_opt_in_content");
				msg = msg.replace("%s1", GeneralUtilityMethods.htmlText(u.orgName));
				// A link styled as a button, a button element inside a link is not valid HTML
				StringBuilder content = new StringBuilder();
				content.append("<p>").append(localisation.getString("email_subs_s")).append("</p>")
						.append("<p>").append(msg).append("</p>")
						.append("<p style=\"text-align:center;\">")
						.append("<a href=\"")
						.append(request.getScheme()).append("://").append(request.getServerName())
						.append("/app/subscriptions.html?subscribe=yes&token=")
						.append(subscribeToken)
						.append("\" style=\"").append(EmailManager.LINK_BUTTON_STYLE).append("\">")
						.append(localisation.getString("c_s"))
						.append("</a></p>");
				
				em.sendEmailHtml(
						null,
						u.orgName,
						null,
						null,
						email.trim(),
						"bcc",
						localisation.getString("c_s"),
						content.toString(),
						null,
						null,
						emailServer,
						request.getServerName(),
						u.key,
						localisation,
						null,
						o == null ? null : o.getAdminEmail(),
						null,
						GeneralUtilityMethods.getNextEmailId(sd, null),
						null);
			}
			
			response = Response.ok().build();

		} catch(ApplicationException e) {
			response = Response.serverError().entity(e.getMessage()).build();
		} catch(Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}


	/*
	 * Subscribe step 2
	 * Confirm the subscription by submitting a token
	 */
	@GET
	@Path("/subscribe/{token}")
	public Response subscribeStep2(
			@Context HttpServletRequest request,
			@PathParam("token") String token) { 

		Response response = null;

		log.info("Subscribing user: " + token);
		String connectionString = "surveyKPI-Subscribe Step 2";

		Connection sd = SDDataSource.getConnection(connectionString);

		try {

			// Localisation
			String hostname = request.getServerName();
			String loc_code = "en";
			if(hostname.contains("kontrolid")) {
				loc_code = "es";
			} 
			Locale locale = new Locale(loc_code);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		

			PeopleManager pm = new PeopleManager(localisation);
			pm.subscribeStep2(sd, token);

			response = Response.ok().build();	

		} catch(ApplicationException e) {
			response = Response.serverError().entity(e.getMessage()).build();
		} catch(Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

}
