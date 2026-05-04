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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.FileManager;

/*
 * Downloads a file for a user using an anonymous link
 */

@Path("/file/id/{ident}/{filename}")
public class GetFileAnonymous extends Application {
	
	Authorise a = null;
	Authorise aOrg = new Authorise(null, Authorise.ORG);
	
	private static Logger log =
			 Logger.getLogger(GetFileAnonymous.class.getName());

	public GetFileAnonymous() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ENUM);
		a = new Authorise(authorisations, null);	
	}
	
	/*
	 * Get file for anonymous user
	 */
	@GET
	@Produces("application/x-download")
	@Path("/organisation")
	public Response getOrganisationFileAnon(
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("filename") String filename,
			@QueryParam("thumbs") boolean thumbs,
			@PathParam("ident") String user) throws SQLException {
			
		int oId = 0;
		Response r = null;
		String connectionString = "Get File for anonymous user";
		
		// Authorisation - Access
		Connection connectionSD = SDDataSource.getConnection(connectionString);	
		a.isValidTemporaryUser(connectionSD, user);
		a.isAuthorised(connectionSD, user);		
		try {		
			oId = GeneralUtilityMethods.getOrganisationId(connectionSD, user);
		} catch(Exception e) {
			// ignore error
		}
		// End Authorisation 
		
		try {
			
			FileManager fm = new FileManager();
			r = fm.getOrganisationFile(request, response, user, oId, 
					filename, false, thumbs);
			
		}  catch (Exception e) {
			log.info("Error getting file:" + e.getMessage());
			r = Response.status(Status.NOT_FOUND).build();
		} finally {	
			SDDataSource.closeConnection(connectionString, connectionSD);	
		}
		
		return r;
		
	}
	
	
}
