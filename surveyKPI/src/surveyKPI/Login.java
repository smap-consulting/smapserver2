package surveyKPI;


import java.lang.reflect.Type;

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.PasswordDetails;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/*
 * Login functions
 */
@Path("/login")
public class Login extends Application {
	
	private static Logger log =
			 Logger.getLogger(Login.class.getName());

	private class Key {
		public String key;
	};
	
	private class Basic {
		public boolean hasBasicPassword = true;
	};
	
	/*
	 * Login and get a key for future authentication
	 */
	@GET
	@Path("/key")
	@Produces("application/json")
	public Response getKey(@Context HttpServletRequest request) {
		
		Response response = null;
		String connectionString = "surveyKPI-login-key";
		 		
		// No authorisation is required - the key is returned to the authenticated user
		
		Connection sd = SDDataSource.getConnection(connectionString);	
		
		String user = request.getRemoteUser();
		
		Key accessToken = new Key();
		try {
			accessToken.key = GeneralUtilityMethods.getNewAccessKey(sd, user, false);
			log.info("userevent: " + user + " : requested access key : " + accessToken.key);
		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to get access key", e);
			response = Response.serverError().build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		String resp = gson.toJson(accessToken);
		response = Response.ok(resp).build();
		
		return response;

	}

	/*
	 * Return true if the passed in user ident has a basic password
	 */
	@GET
	@Path("/basic/{ident}")
	@Produces("application/json")
	public Response hasBasicPassword(@Context HttpServletRequest request,
			@PathParam("ident") String ident) {
		
		Response response = null;
		String connectionString = "surveyKPI-login-has-basic";
		 		
		// No authorisation is required - the key is returned to the authenticated user
		
		Connection sd = SDDataSource.getConnection(connectionString);	
		
		Basic basic = new Basic();
		
		String sql = "select count(*) from users where ident = ? and basic_password is not null";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ident);
			log.info("Test for basic password: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();		
			if(rs.next() && rs.getInt(1) > 0) {
				basic.hasBasicPassword = true;
			} else {
				basic.hasBasicPassword = false;
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to get basic password status", e);
			response = Response.serverError().build();
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		response = Response.ok(gson.toJson(basic)).build();
		
		return response;

	}
	
	/*
	 * Update the user password
	 */
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/basic/password")
	public Response updateUserPassword(@Context HttpServletRequest request,
			@FormParam("passwordDetails") String passwordDetails) { 
		
		Response response = null;
		String authString = "surveyKPI - change password";

		// Authorisation - Not Required - the user is updating their own settings
		Connection sd = SDDataSource.getConnection(authString);
		
		Type type = new TypeToken<PasswordDetails>(){}.getType();		
		PasswordDetails pwd = new Gson().fromJson(passwordDetails, type);		// The user settings
		
		try {	
			
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		

			UserManager um = new UserManager(localisation);
			response = um.setPassword(sd, locale, localisation, request.getRemoteUser(), request.getServerName(), pwd);
					
			response = Response.ok().build();
			
			
		} catch (Exception e) {

			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
			
		} finally {
			
			SDDataSource.closeConnection(authString, sd);
		}
		
		return response;
	}

}

