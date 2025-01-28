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
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.notifications.interfaces.EmitNotifications;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.OrganisationManager;
import org.smap.sdal.managers.ProjectManager;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.User;
import org.smap.sdal.model.UserGroup;

import com.google.gson.Gson;
import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Returns a list of all projects that are in the same organisation as the user making the request
 */
@Path("/register")
public class Register extends Application {

	private static Logger log =
			 Logger.getLogger(Register.class.getName());
	
	class RegistrationDetails {
		String email;
		String org_name;
		String admin_name;
		String website;
	}
	
	/*
	 * Register a new organisation
	 */
	@POST
	public Response register(
			@Context HttpServletRequest request,
			@FormParam("registrationDetails") String registrationDetails) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		
		GeneralUtilityMethods.assertSelfRegistrationServer(request.getServerName());
		
		RegistrationDetails rd = new Gson().fromJson(registrationDetails, RegistrationDetails.class);
		
		log.info("Registering a new user: " + rd.email + " : " + rd.admin_name);
		
		Connection sd = SDDataSource.getConnection("surveyKPI-Register");
		
		PreparedStatement pstmt = null;
		String newUserIdent = null;
		try {
			
			sd.setAutoCommit(false);  // Transaction
			
			// Localisation
			String hostname = request.getServerName();
			String loc_code = "en";
			if(hostname.contains("kontrolid")) {
				loc_code = "es";
			} 
			Locale locale = new Locale(loc_code);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String requestUrl = request.getRequestURL().toString();
			String userIdent = request.getRemoteUser();
			String basePath = GeneralUtilityMethods.getBasePath(request);
			
			/*
			 * 1. Create organisation
			 */
			OrganisationManager om = new OrganisationManager(localisation);
			Organisation o = new Organisation();
			o.admin_email = rd.email;
			o.name = rd.org_name;
			o.company_name = rd.org_name;
			o.website = rd.website;
			o.can_edit = true;
			o.can_notify = true;
			o.can_use_api = true;
			o.can_submit = true;
			o.appearance.set_as_theme = false;
			o.appearance.navbar_color = Organisation.DEFAULT_NAVBAR_COLOR;
			o.appearance.navbar_text_color = Organisation.DEFAULT_NAVBAR_TEXT_COLOR;
			o.email_task = false;
			o.can_sms = false;
			o.send_optin = true;
			o.e_id = 1;				// Default organisation!
			
			int o_id = om.createOrganisation(
					sd, 
					o, 
					userIdent, 
					null,
					null,
					requestUrl,
					basePath,
					null,
					null,
					rd.email);
			
			/*
			 * 2. Create the user
			 */
			UserManager um = new UserManager(localisation);
			
			User u = new User();
			u.name = rd.admin_name;
			u.company_name = rd.org_name;
			u.email = rd.email;
			u.ident = rd.email;
			u.sendEmail = true;
			u.projects = new ArrayList<Project> ();	// Empty list initially as no projects exist yet
			
			newUserIdent = u.ident;	// For error reporting
			
			// Add first three groups as default for an administrator
			u.groups = new ArrayList<UserGroup> ();
			for(int i = 1; i <= 3; i++) {
				u.groups.add(new UserGroup(i, "group"));
			}
			// Add security manager group
			u.groups.add(new UserGroup(Authorise.SECURITY_ID, Authorise.SECURITY));
			
			int u_id = um.createUser(sd, u, o_id,
					false,		// Organisational Administrator
					true,		// Security Manager
					false,		// Enterprise Manager
					false,		// Server owner
					request.getRemoteUser(),
					request.getScheme(),
					request.getServerName(),
					rd.admin_name,
					null,
					localisation);			 

			 // 3. Create a default project
			ProjectManager pm = new ProjectManager(localisation);
			Project p = new Project();
			p.name = "default";
			p.desc = "Default Project - Created on registration";
			pm.createProject(sd, "system", p, o_id, u_id, request.getRemoteUser());
			
			 // 4. Create a notification recording this event
			try {
				EmitNotifications en = new EmitNotifications();
				en.publish(EmitNotifications.AWS_REGISTER_ORGANISATION,
						getRegistrationMsg(rd, request.getServerName()),
						"Register new organisation");
			} catch (Exception e) {
				// Don't fail on this step
				log.log(Level.SEVERE, e.getMessage(), e);
			} catch(NoClassDefFoundError e) {		// This should be fixed!
				log.log(Level.SEVERE, e.getMessage(), e);
			}
			
			sd.commit();
			
			response = Response.ok().build();
		
				
		} catch (SQLException e) {
			
			try {sd.rollback();}catch(Exception ex) {}
			
			String state = e.getSQLState();
			log.info("Register: sql state:" + state);
			if(state.startsWith("23")) {
				String msg = e.getMessage();
				if(msg != null) {
					if(msg.contains("duplicate key") && msg.contains("idx_users_ident")) {
						msg = "This user identifier (" + newUserIdent + ") already exists on the server";
					}
				}
				response = Response.status(Status.CONFLICT).entity(msg).build();
			} else {
				response = Response.serverError().entity(e.getMessage()).build();
				log.log(Level.SEVERE,"Error", e);
			}
		} catch(ApplicationException e) {
			
			try {sd.rollback();}catch(Exception ex) {}
			response = Response.serverError().entity(e.getMessage()).build();
			
		} catch(Exception e) {
			try {sd.rollback();}catch(Exception ex) {}
			String msg = e.getMessage();
			response = Response.serverError().entity(msg).build();		
			if(msg == null || !msg.equals("xxxx")) {
				log.log(Level.SEVERE,"Error", e);
			}
		} finally {
			
			try{sd.setAutoCommit(true);}catch(Exception ex) {}
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-Register", sd);
		}
		
		return response;
	}
	
	/*
	 * Convert registration details into a text string suitable for email
	 */
	private String getRegistrationMsg(RegistrationDetails rd, String server) {
		
		StringBuffer msg = new StringBuffer();
		
		msg.append("Name: " + rd.admin_name + "\n");
		msg.append("Email: " + rd.email + "\n");
		msg.append("Organisation: " + rd.org_name + "\n");
		if(rd.website != null) {
			msg.append("Website: " + rd.website + "\n");
		}
		msg.append("Server: " + server);

		return msg.toString();
	}
	


}

