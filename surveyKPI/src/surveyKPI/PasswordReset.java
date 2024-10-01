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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.ServerSettings;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.EmailManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.PasswordManager;
import org.smap.sdal.managers.PeopleManager;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.SubscriptionStatus;

import com.google.gson.Gson;

import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Forgotton password
 */

@Path("/onetimelogon")
public class PasswordReset extends Application {

	private static Logger log =
			Logger.getLogger(PasswordReset.class.getName());

	LogManager lm = new LogManager();		// Application log

	/*
	 * Send an email with a link for a one time logon
	 */
	@POST
	public Response oneTimeLogon(@Context HttpServletRequest request, String email) throws ApplicationException { 

		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
			throw new AuthorisationException();   
		} 

		Response response = null;

		Connection sd = SDDataSource.getConnection("surveyKPI-onetimelogon");
		PreparedStatement pstmt = null;

		try {
			ServerSettings.setBasePath(request);
			
			if(email != null && email.trim().length() > 0) {	

				// Localisation
				String hostname = request.getServerName();
				String loc_code = "en";
				if(hostname.contains("kontrolid")) {
					loc_code = "es";
				} 
				Locale locale = new Locale(loc_code);
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

				/*
				 * Check to see if there is a one time password has been sent within the last 10 mins
				 */
				int minutesSinceSent = UtilityMethodsEmail.hasOnetimePasswordBeenSent(sd, pstmt, email, "600 seconds");
				if(minutesSinceSent > 0) {
					// Potential spam
					log.info("warning: email: " + email + " multiple password reset requests");
					String msg = localisation.getString("email_pas");
					msg = msg.replaceAll("%s1", String.valueOf(10 - minutesSinceSent));
					throw new ApplicationException(msg);
				}

				String interval = "1 hour";
				String uuid = UtilityMethodsEmail.setOnetimePassword(sd, pstmt, email, interval);

				if(uuid != null) {
					// Update succeeded
					log.info("Sending email");

					EmailServer emailServer = UtilityMethodsEmail.getEmailServer(sd, localisation, email, request.getRemoteUser(), 0);

					PeopleManager pm = new PeopleManager(localisation);
					SubscriptionStatus subStatus = pm.getEmailKey(sd, 0, email);
					if(subStatus.unsubscribed) {
						// Person has unsubscribed
						String msg = localisation.getString("email_us");
						msg = msg.replaceFirst("%s1", email);
						throw new ApplicationException(msg);
					}

					if(emailServer != null) {

						String adminEmail = null;
						int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
						Organisation o = GeneralUtilityMethods.getOrganisation(sd, oId);
						if(o != null) {
							adminEmail = o.getAdminEmail();
						}

						ArrayList<String> idents = UtilityMethodsEmail.getIdentsFromEmail(sd, pstmt, email);

						String subject = localisation.getString("c_r_p");
						EmailManager em = new EmailManager(localisation);

						StringBuilder content = new StringBuilder(); 
						String info = localisation.getString("c_rp_info");
						info = info.replace("%s1", request.getServerName());
						content.append("<p>").append(info).append("</p><p>").append(localisation.getString("c_goto")).append(" ")
						.append("<a href=\"").append("https").append("://").append(request.getServerName())
						.append("/app/resetForgottonPassword.html?token=")
						.append(uuid)
						.append("\">")
						.append(localisation.getString("email_link"))
						.append("</a> ")				
						.append(localisation.getString("email_rp"))
						.append("</p>");

						// User ident
						StringBuffer identString = new StringBuffer();
						int count = 0;
						if(idents != null) {
							for(String ident : idents) {
								if(count++ > 0) {
									identString.append(" or ");
								} 
								identString.append(ident);
							}
						}			
						content.append("<p>")
						.append(localisation.getString("email_un"))
						.append(": ")
						.append(identString.toString())
						.append("</p>");

						// Email validity
						content.append("<p>")
						.append(localisation.getString("email_vf"))
						.append(" ")
						.append(interval)
						.append("</p>");

						content.append("<br/><br/><p>")
						.append(localisation.getString("email_dnr"))
						.append("</p>");

						em.sendEmailHtml(
								email, 
								"bcc", 
								subject, 
								content.toString(), 
								null, 
								null, 
								emailServer,
								request.getServerName(),
								subStatus.emailKey,
								localisation,
								null,
								null,
								null,
								GeneralUtilityMethods.getNextEmailId(sd, null));

						UtilityMethodsEmail.reportOnetimePasswordSent(sd, email);

						response = Response.ok().build();
					} else {
						String msg = localisation.getString("email_ne");
						log.info(msg);
						response = Response.status(Status.NOT_FOUND).entity(msg).build();
					}
				} else {
					// email was not found 
					log.info("Email was not found.  Respond with OK for security reasons.");
					response = Response.ok().build();
				}
			} else {
				response = Response.status(Status.NOT_FOUND).entity("Email not specified").build();
			}


		} catch (SQLException e) {

			String msg = e.getMessage();
			String respMsg = "Database Error";
			if(msg.contains("does not exist")) {
				log.info("No data: " + msg);
				respMsg = "Database Error: No data";
			} else {
				log.log(Level.SEVERE,"Exception", e);
			}	
			response = Response.status(Status.NOT_FOUND).entity(respMsg).build();

		} catch (ApplicationException e) {
			response = Response.status(Status.NOT_FOUND).entity(e.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE,"Exception", e);
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}

			SDDataSource.closeConnection("surveyKPI-onetimelogon", sd);
		} 

		return response;
	}


	/*
	 * Update the users password
	 */
	class PasswordDetails {
		String onetime;
		String password;
	}

	@POST
	@Path("/set")
	public Response setPassword(@Context HttpServletRequest request, 
			@QueryParam("lang") String lang,
			@FormParam("passwordDetails") String passwordDetails) {

		Response response = null;

		Connection sd = SDDataSource.getConnection("surveyKPI-setPassword");

		PasswordDetails pd = new Gson().fromJson(passwordDetails, PasswordDetails.class);

		PreparedStatement pstmt = null;
		PreparedStatement pstmtDel = null;
		PreparedStatement pstmtUpdate = null;
		try {

			sd.setAutoCommit(false);

			Locale locale = new Locale(lang);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			// Get the user ident just for logging, also check that there is a valid onetime token
			String sql = "select ident, name from users where one_time_password = ? and one_time_password_expiry > timestamp 'now'"; 
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, pd.onetime);
			log.info("SQL set password: " + pstmt.toString());

			ResultSet rs = pstmt.executeQuery();
			int count = 0;
			while(rs.next()) {
				String ident = rs.getString(1);
				String name = rs.getString(2);

				log.info("Updating password for user " + name + " with ident " + ident);

				/*
				 * Verify that the password is strong enough
				 */
				PasswordManager pwm = null;
				if(pd.password != null) {
					pwm = new PasswordManager(sd, locale, localisation,ident, request.getServerName());
					pwm.checkStrength(pd.password);
				}

				sql = "update users "
						+ "set password = md5(?), "
						+ "basic_password = '{SHA}'|| encode(digest(?,'sha1'),'base64'), "
						+ "password_reset = 'true', "
						+ "password_set = now() "
						+ "where one_time_password = ? and ident = ?";
				pstmtUpdate = sd.prepareStatement(sql);
				String pwdString = ident + ":smap:" + pd.password;
				pstmtUpdate.setString(1, pwdString);
				pstmtUpdate.setString(2, pd.password);
				pstmtUpdate.setString(3, pd.onetime);
				pstmtUpdate.setString(4, ident);

				pstmtUpdate.executeUpdate();
				response = Response.ok().build();
				count++;


				if(pwm != null) {
					pwm.logReset();		// Record the sucessful password reset
				}
				log.info("userevent: " + ident + "reset password / forgot password");
			} 

			if(count == 0) {
				// Clean up an expired token
				sql = "update users set one_time_password = null, one_time_password_expiry = null where one_time_password = ?";
				pstmtDel = sd.prepareStatement(sql);
				pstmtDel.setString(1, pd.onetime);
				int nbrUpdated = pstmtDel.executeUpdate();
				if(nbrUpdated > 0) {
					response = Response.status(Status.NOT_FOUND).entity("Token has expired").build();
					log.info("Error: Token has expired");
				} else {
					response = Response.status(Status.NOT_FOUND).entity("Token not found").build();
					log.info("Error: Token not found");
				}

			} else {
				response = Response.status(Status.OK).entity("").build();
			}

			sd.commit();

			

		} catch (ApplicationException e) {	
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
			try { sd.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}
		} catch (Exception e) {	
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
			e.printStackTrace();
			try { sd.rollback();} catch (Exception ex){log.log(Level.SEVERE,"", ex);}
		} finally {

			try {if ( pstmt != null ) { pstmt.close(); }} catch (Exception e) {}
			try {if ( pstmtDel != null ) { pstmtDel.close(); }} catch (Exception e) {}
			try {if ( pstmtUpdate != null ) { pstmtUpdate.close(); }} catch (Exception e) {}
			try {
				if (sd != null) {
					sd.setAutoCommit(true);
					sd.close();
				}
			} catch (SQLException e) {
				log.info("Failed to close connection");
				e.printStackTrace();
			}
		}

		return response;
	}
}


