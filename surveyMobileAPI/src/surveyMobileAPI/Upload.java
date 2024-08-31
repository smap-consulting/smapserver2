/*****************************************************************************

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

 ******************************************************************************/

package surveyMobileAPI;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.Authorise;
import surveyMobileAPI.managers.UploadManager;

/*
 * Accept submitted surveys
 */
@Path("/submission")
public class Upload extends Application {
	
	Authorise a = new Authorise(null, Authorise.ENUM);
	
	private static Logger log = Logger.getLogger(Upload.class.getName());
	
	/*
	 * New Submission
	 * No Key - login required
	 */
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response postInstance(
			@QueryParam("deviceID") String deviceId,
			@Context HttpServletRequest request) throws IOException {
		
		// Do not check for Ajax as android device request is not ajax
		
		log.info("New submssion from device: " + deviceId);
		UploadManager ulm = new UploadManager();
		return ulm.submission(request, null, null, deviceId);
	}
	
	/*
	 * Update
	 * No Key provided login required
	 */
	@POST
	@Path("/{instanceId}")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response postUpdateInstance(
			@Context HttpServletRequest request,
			@QueryParam("deviceID") String deviceId,
	        @PathParam("instanceId") String instanceId) throws IOException {
		
		log.info("Update submssion: " + instanceId);
		UploadManager ulm = new UploadManager();
		return ulm.submission(request, instanceId, null, deviceId);
	}
	
	/*
	 * New Submission
	 * Authentication key included
	 */
	@POST
	@Path("/key/{key}")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response postInstanceWithKey(
			@Context HttpServletRequest request,
			@QueryParam("deviceID") String deviceId,
			@PathParam("key") String key) throws IOException {
		
		log.info("New submssion with key from device: " + deviceId);
		UploadManager ulm = new UploadManager();
		return ulm.submission(request, null, key, deviceId);
	}
	
	/*
	 * Update
	 * Key Provided
	 */
	@POST
	@Path("/key/{key}/{instanceId}")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response postUpdateInstanceWithKey(
			@Context HttpServletRequest request,
			@QueryParam("deviceID") String deviceId,
			@PathParam("key") String key,
	        @PathParam("instanceId") String instanceId) throws IOException {
		
		log.info("Update submssion with key: " + instanceId);
		UploadManager ulm = new UploadManager();
		return ulm.submission(request, instanceId, key, deviceId);
	}
	
	/*
	 * Head request to return the actual URL to submit data to
	 * This is required by the Java Rosa protocol
	 */
	@HEAD
	@Produces(MediaType.TEXT_XML)
	public void getHead(@Context HttpServletRequest request,  @Context HttpServletResponse resp) {

		UploadManager ulm = new UploadManager();
		ulm.setHeaderResponse(request, resp);
	
	}
}

