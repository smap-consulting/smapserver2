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
import java.sql.Connection;
import java.util.Locale;
import java.util.ResourceBundle;
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
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;

import surveyMobileAPI.managers.UploadManager;

/*
 * An alternative JSON submission end point
 */
@Path("/upload")
public class UploadJson extends Application {
	
	Authorise a = new Authorise(null, Authorise.ENUM);
	
	private static Logger log = Logger.getLogger(UploadJson.class.getName());
	
	/*
	 * New Submission
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postJsonInstance(
			@Context HttpServletRequest request) throws IOException {

		// Do not check for Ajax as device request is not ajax

		String user = request.getRemoteUser();
		log.info("New JSON upload request from: " + user);

		String connectionString = "surveyMobileAPI-UploadJson";
		Response response = null;
		ResourceBundle localisation = null;
		Connection sd = null;

		try {
			sd = SDDataSource.getConnection(connectionString);
			a.isAuthorised(sd, user);

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			JsonFormData jsonForm = new JsonFormData();
			jsonForm.loadJson(sd, localisation, request, user);

			response = Response.status(Status.CREATED).build();
		} catch (Exception e) {
			response = Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
}

