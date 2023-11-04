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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.AuthorisationException;

/*
 * Login functions
 */
@Path("/logout")
public class Logout extends Application {
	
	@GET
	@Produces("application/json")
	public Response logout(@Context HttpServletRequest request) {
		
		/*
		 * Delete any session keys for this user
		 *
		 * Not sure what the thinking was here.  Session keys should still work when the user has logged out.
		String connectionString = "surveyKPI-Logout";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			GeneralUtilityMethods.deleteAccessKeys(sd, request.getRemoteUser());
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		*/
		
		// Throw an authorisation exception to close browser session (chrome works with this at least)
		if(request != null) {	// Hack to allow us to always throw an exception while satisfying jersey and eclipse validation
			throw new AuthorisationException();
		}
		
		return Response.ok().build();
	}

	



}

