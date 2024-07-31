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
import java.util.ArrayList;
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
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.PasswordDetails;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/*
 * Request authorisation to access services
 * This should be used with web pages that need to only allow authorised users to access them rather than allowing
 * any logged in user to have access
 */
@Path("/authorise")
public class AuthoriseUser extends Application {
	
	private static Logger log =
			 Logger.getLogger(Authorise.class.getName());

	/*
	 * Login and get a key for future authentication
	 */
	@GET
	@Path("/{key}")
	public Response getKey(@Context HttpServletRequest request,
			@PathParam("key") String key) {
		
		String connectionString = "surveyKPI-authorise";
			
		ArrayList<String> authorisations = new ArrayList<String> ();
		if(key.equals("api")) {
			authorisations.add(Authorise.ANALYST);
			authorisations.add(Authorise.ADMIN);
		}		
		Authorise a = new Authorise(authorisations, null);
		
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		SDDataSource.closeConnection(connectionString, sd);
		
		return Response.ok().build();

	}

}

