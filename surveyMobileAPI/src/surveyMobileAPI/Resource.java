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

import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.SharedResourceManager;


/*
 * Login functions
 */
@Path("/resource/{resourcename}")
public class Resource extends Application {

	Authorise a = new Authorise(null, Authorise.ENUM);
	
	private static Logger log =
			 Logger.getLogger(Resource.class.getName());
	
	/*
	 * Get survey level resource file
	 */
	@GET
	@Path("/survey/{sId}")
	@Produces("application/x-download")
	public Response getSurveyFile (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("filename") String filename,
			@PathParam("sId") int sId,
			@QueryParam("thumbs") boolean thumbs,
			@QueryParam("linked") boolean linked) throws Exception {
		
		log.info("Get Resource: " + filename + " for survey: " + sId);
		SharedResourceManager srm = new SharedResourceManager(null, null);
		return srm.getSurveyFile(request, response,filename, sId, thumbs, linked);	
	}

}

