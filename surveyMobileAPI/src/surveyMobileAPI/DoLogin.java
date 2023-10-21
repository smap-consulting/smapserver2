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
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.SDDataSource;

/*
 * Login to support session based authentication using mod_auth_form
 */
@Path("/dologin")
public class DoLogin extends Application {

	Authorise a = new Authorise(null, Authorise.ENUM);
	
	/*
	 * Basic login request for enumerator access
	 */
	@POST
	public Response login(@Context HttpServletRequest request,
			@FormParam("username") String username,
			@FormParam("password") String password) {
		String connectionString = "surveyMobileAPI-dologin";
		Connection sd = SDDataSource.getConnection(connectionString);
	    System.out.println("Login request");
	    
	    SDDataSource.closeConnection(connectionString, sd);
		return Response.ok("{}").build();
	}
	

}

