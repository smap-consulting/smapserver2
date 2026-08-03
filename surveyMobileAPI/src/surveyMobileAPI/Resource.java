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
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.FileManager;
import org.smap.sdal.managers.OfflineLayerManager;
import org.smap.sdal.managers.SharedResourceManager;


/*
 * Login functions
 */
@Path("/resource/{filename}")
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
	
	/*
	 * Get organisation level resource file
	 */
	@GET
	@Path("/organisation")
	@Produces("application/x-download")
	public Response getOrganisationFileUser (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@PathParam("filename") String filename,
			@QueryParam("settings") boolean settings,
			@QueryParam("thumbs") boolean thumbs,
			@QueryParam("org") int requestedOrgId) throws Exception {
		
		SharedResourceManager srm = new SharedResourceManager(null, null);
		return srm.getOrganisationFile(request, response, null, requestedOrgId, filename, settings, false, thumbs);
	}

	/*
	 * Get an offline map layer.  These files are large so the response honours a byte
	 * range request, allowing a device on a poor connection to resume a download.
	 */
	@GET
	@Path("/layer/{id}")
	@Produces("application/x-download")
	public Response getOfflineLayer (
			@Context HttpServletRequest request,
			@PathParam("filename") String filename,
			@PathParam("id") int id) throws Exception {

		Response r = null;
		String connectionString = "surveyMobileAPI-getOfflineLayer";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {
			String user = request.getRemoteUser();
			if(user == null) {
				user = GeneralUtilityMethods.getUserFromRequestKey(sd, request, "app");
			}

			// Authorisation - Access
			a.isAuthorised(sd, request, user);
			// End Authorisation

			OfflineLayerManager olm = new OfflineLayerManager();

			// Only a user the layer has been assigned to can download it
			String filepath = olm.getLayerPathForUser(sd, id, user);
			if(filepath == null) {
				log.info("Offline layer " + id + " not available to user " + user);
				return Response.status(Status.NOT_FOUND).build();
			}

			FileManager fm = new FileManager();
			r = fm.getRangeFileResponse(request, filepath, filename, olm.getMd5(sd, id));

		} catch (ApplicationException e) {
			log.info("Error: Failed to get layer: " + e.getMessage());
			r = Response.status(Status.NOT_FOUND).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return r;
	}

}

