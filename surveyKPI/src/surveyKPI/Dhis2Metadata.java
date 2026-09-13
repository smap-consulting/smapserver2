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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.Dhis2MetadataManager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * DHIS2 metadata cached for the mapping screens
 *
 * Data sets and programs, refreshed when asked for rather than on a schedule, because this is
 * configuration time data that nobody is looking at most of the time
 */
@Path("/dhis2/metadata")
public class Dhis2Metadata extends Application {

	private static Logger log = Logger.getLogger(Dhis2Metadata.class.getName());

	Authorise adminAuth;

	public Dhis2Metadata() {
		ArrayList<String> auth = new ArrayList<>();
		auth.add(Authorise.ADMIN);
		adminAuth = new Authorise(auth, null);
	}

	// -------------------------------------------------------------------------
	// GET, the cached list of one object type
	// -------------------------------------------------------------------------

	@GET
	@Path("/{type}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getList(@Context HttpServletRequest request, @PathParam("type") String type) {

		String conn = "surveyKPI-Dhis2Metadata-list";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			return Response.ok(gson.toJson(new Dhis2MetadataManager().getList(sd, oId, type))).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// POST refresh, read the list again from DHIS2
	// -------------------------------------------------------------------------

	@POST
	@Path("/{type}/refresh")
	@Produces(MediaType.APPLICATION_JSON)
	public Response refreshList(@Context HttpServletRequest request, @PathParam("type") String type) {

		String conn = "surveyKPI-Dhis2Metadata-refresh";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			int count = new Dhis2MetadataManager().refreshList(sd, oId, type);

			HashMap<String, Object> result = new HashMap<>();
			result.put("count", count);
			return Response.ok(new Gson().toJson(result)).build();

		} catch (Exception e) {
			// Reaching DHIS2 can fail for reasons the user can act on, so pass the reason back
			log.info("DHIS2 metadata refresh failed: " + e.getMessage());
			return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// GET detail, one object as DHIS2 describes it
	// -------------------------------------------------------------------------

	/*
	 * Fetched from DHIS2 the first time it is asked for and cached after that, because a data
	 * set with every data element and category option combo is a large response and only the
	 * one being mapped is worth holding
	 */
	@GET
	@Path("/{type}/{uid}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDetail(@Context HttpServletRequest request,
			@PathParam("type") String type,
			@PathParam("uid") String uid,
			@QueryParam("refresh") boolean refresh) {

		String conn = "surveyKPI-Dhis2Metadata-detail";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			String payload = new Dhis2MetadataManager().getDetail(sd, oId, type, uid, refresh);
			return Response.ok(payload).build();

		} catch (Exception e) {
			log.info("DHIS2 metadata detail failed for " + type + " " + uid + ": " + e.getMessage());
			return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}
}
