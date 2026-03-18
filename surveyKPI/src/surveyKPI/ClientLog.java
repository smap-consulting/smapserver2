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
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.AuthorisationException;

import java.util.logging.Logger;

/*
 * Receives client-side error reports (e.g. from the global JS error handler)
 * and logs them server-side so they appear in the application logs.
 */
@Path("/clientlog")
public class ClientLog {

	private static Logger log = Logger.getLogger(ClientLog.class.getName());

	@POST
	@Consumes("application/json")
	public Response logClientError(String body, @Context HttpServletRequest request) {
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		String user = request.getRemoteUser();
		log.warning("[client-error] user=" + (user != null ? user : "anonymous")
				+ " ip=" + request.getRemoteAddr()
				+ " body=" + body);
		return Response.ok().build();
	}
}
