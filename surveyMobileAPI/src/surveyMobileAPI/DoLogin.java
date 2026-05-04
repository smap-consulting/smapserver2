package surveyMobileAPI;


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
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.smap.sdal.Utilities.SDDataSource;

/*
 * Login to support session based authentication using mod_auth_form
 */
@Path("/dologin")
public class DoLogin extends Application {
	
	/*
	 * Basic login request for enumerator access
	 */
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response login(@Context HttpServletRequest request,
			@FormParam("username") String username,
			@FormParam("password") String password) {
		String connectionString = "surveyMobileAPI-dologin";
		Connection sd = SDDataSource.getConnection(connectionString);
	    
	    SDDataSource.closeConnection(connectionString, sd);
		return Response.ok("{}").build();
	}
	

}

