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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.Dhis2Manager;
import org.smap.sdal.managers.Dhis2ServerManager;
import org.smap.sdal.model.Dhis2ConnectionTest;
import org.smap.sdal.model.Dhis2Server;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * The DHIS2 connection belonging to an organisation
 * One per organisation, so there is no id in any of these paths
 */
@Path("/dhis2/server")
public class Dhis2ServerConfig extends Application {

	private static Logger log = Logger.getLogger(Dhis2ServerConfig.class.getName());

	Authorise adminAuth;

	public Dhis2ServerConfig() {
		ArrayList<String> auth = new ArrayList<>();
		auth.add(Authorise.ADMIN);
		adminAuth = new Authorise(auth, null);
	}

	// -------------------------------------------------------------------------
	// GET, the organisation's connection.  Empty body if none is set up
	// -------------------------------------------------------------------------

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getServer(@Context HttpServletRequest request) {

		String conn = "surveyKPI-Dhis2ServerConfig-get";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			Dhis2Server server = new Dhis2ServerManager().get(sd, oId);
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			return Response.ok(gson.toJson(server == null ? new Dhis2Server() : server)).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// PUT, create or update.  An empty token leaves the stored one alone
	// -------------------------------------------------------------------------

	@PUT
	public Response saveServer(@Context HttpServletRequest request, String body) {

		String conn = "surveyKPI-Dhis2ServerConfig-put";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			Dhis2Server server = new Gson().fromJson(body, Dhis2Server.class);

			String invalid = validate(server);
			if(invalid != null) {
				return Response.status(Response.Status.BAD_REQUEST).entity(invalid).build();
			}

			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			new Dhis2ServerManager().save(sd, oId, server);
			return Response.ok().build();

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// DELETE
	// -------------------------------------------------------------------------

	@DELETE
	public Response deleteServer(@Context HttpServletRequest request) {

		String conn = "surveyKPI-Dhis2ServerConfig-delete";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			new Dhis2ServerManager().delete(sd, oId);
			return Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// POST test, check the connection and report what it can do
	// -------------------------------------------------------------------------

	@POST
	@Path("/test")
	@Produces(MediaType.APPLICATION_JSON)
	public Response testServer(@Context HttpServletRequest request) {

		String conn = "surveyKPI-Dhis2ServerConfig-test";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			Dhis2Server server = new Dhis2ServerManager().getWithToken(sd, oId);
			if(server == null) {
				return Response.status(Response.Status.NOT_FOUND)
						.entity("No DHIS2 connection has been set up").build();
			}

			Dhis2Manager dhis2 = new Dhis2Manager();
			Dhis2ConnectionTest result = dhis2.test(server);
			new Dhis2ServerManager().recordTest(sd, oId, dhis2.summarise(result));

			return Response.ok(new Gson().toJson(result)).build();

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// GET optionsets, what the connected instance offers
	// -------------------------------------------------------------------------

	/*
	 * Asked of the DHIS2 instance rather than of Smap, so that someone setting up a resource
	 * chooses from a list rather than having to know a uid
	 */
	@GET
	@Path("/optionsets")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getOptionSets(@Context HttpServletRequest request) {

		String conn = "surveyKPI-Dhis2ServerConfig-optionsets";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			Dhis2Server server = new Dhis2ServerManager().getWithToken(sd, oId);
			if(server == null) {
				return Response.status(Response.Status.BAD_REQUEST)
						.entity("No DHIS2 connection has been set up").build();
			}

			return Response.ok(new Gson().toJson(new Dhis2Manager().listOptionSets(server))).build();

		} catch (Exception e) {
			// Reaching DHIS2 can fail for reasons the user can act on, so pass the reason back
			log.info("DHIS2 option sets could not be read: " + e.getMessage());
			return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	private String validate(Dhis2Server server) {
		if(server.label == null || server.label.trim().length() == 0) {
			return "A name is required";
		}
		if(server.base_url == null || server.base_url.trim().length() == 0) {
			return "A URL is required";
		}
		String url = server.base_url.trim().toLowerCase();
		if(!url.startsWith("http://") && !url.startsWith("https://")) {
			return "The URL must start with http:// or https://";
		}
		return null;
	}
}
