package surveyKPI;

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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.managers.SharePointManager;
import org.smap.sdal.model.ServerData;

import com.google.gson.Gson;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * GET /sharepoint/lists — return the titles of all non-hidden lists on the
 * configured SharePoint server for the caller's organisation.
 */
@Path("/sharepoint/lists")
public class SharePointLists extends Application {

	private static Logger log = Logger.getLogger(SharePointLists.class.getName());

	Authorise adminAuth;

	public SharePointLists() {
		ArrayList<String> auth = new ArrayList<>();
		auth.add(Authorise.ADMIN);
		adminAuth = new Authorise(auth, null);
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getLists(@Context HttpServletRequest request) {

		String conn = "surveyKPI-SharePointLists-get";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request.getRemoteUser());

		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			ServerData serverData = new ServerManager().getServer(sd, localisation);
			List<String> titles = SharePointManager.getAvailableLists(serverData);
			return Response.ok(new Gson().toJson(titles)).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}
}
