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
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.FileManager;

/*
 * Provide user customisation
 */
@Path("/custom")
public class Custom extends Application {
	
	/*
	 * Get the webform banner
	 */
	@GET
	@Produces("application/x-download")
	@Path("/banner/{oId}")
	public Response banner(@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("oId") int oId) {
		
		Response r = null;
		
		/*
		 * No Authorisation
		 */
		String connectionString = "surveyMobileAPI-banner";
		Connection sd = SDDataSource.getConnection(connectionString);
	    
	    try {
	    	FileManager fm = new FileManager();
	    	String basePath = GeneralUtilityMethods.getBasePath(request);

	    	/*
	    	 * First try to get the custom organisation logo
	    	 */
	    	try {
		 		fm.getFile(response, basePath + "/media/organisation/" + oId + "/settings/bannerLogo", "bannerLogo");
	    	} catch (Exception e) {
	    		/*
	    		 * No hard feelings
	    		 * return the default banner
	    		 */
	    		fm.getFile(response, basePath + "/misc/smap_logo.png", "smap_logo.png");
	    	}
	    } catch (Exception e) {
			r = Response.status(Status.NOT_FOUND).entity(e.getMessage()).build();
	    } finally {
	    	SDDataSource.closeConnection(connectionString, sd);
	    }
	   		
		r = Response.ok("").build();
		
		return r;
	}
	

}

