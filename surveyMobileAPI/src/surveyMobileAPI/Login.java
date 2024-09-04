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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.SDDataSource;

/*
 * Login functions
 */
@Path("/login")
public class Login extends Application {

	Authorise a = new Authorise(null, Authorise.ENUM);
	
	/*
	 * Basic login request for enumerator access
	 */
	@GET
	public Response login(@Context HttpServletRequest request) {
		String connectionString = "surveyMobileAPI-login";
		Connection sd = SDDataSource.getConnection(connectionString);
	    a.isAuthorised(sd, request.getRemoteUser());	//Authorisation - Access 
	    SDDataSource.closeConnection(connectionString, sd);
		return Response.ok("{}").build();
	}
	
}

