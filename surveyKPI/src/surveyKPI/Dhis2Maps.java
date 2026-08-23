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
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.Dhis2MapManager;
import org.smap.sdal.model.Dhis2Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * DHIS2 reference data cached as organisation level CSV files
 * Each mapping is referenced in a form as "dhis2_{smap_name}"
 */
@Path("/dhis2/maps")
public class Dhis2Maps extends Application {

	private static Logger log = Logger.getLogger(Dhis2Maps.class.getName());

	Authorise adminAuth;

	public Dhis2Maps() {
		ArrayList<String> auth = new ArrayList<>();
		auth.add(Authorise.ADMIN);
		adminAuth = new Authorise(auth, null);
	}

	// -------------------------------------------------------------------------
	// GET, the mappings for the caller's organisation
	// -------------------------------------------------------------------------

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMappings(@Context HttpServletRequest request) {

		String conn = "surveyKPI-Dhis2Maps-get";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			ArrayList<Dhis2Map> maps = new Dhis2MapManager().getMappings(sd, oId);
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
			return Response.ok(gson.toJson(maps)).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// POST, add a mapping
	// -------------------------------------------------------------------------

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public Response addMapping(@Context HttpServletRequest request, String body) {

		String conn = "surveyKPI-Dhis2Maps-post";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			Dhis2Map m = new Gson().fromJson(body, Dhis2Map.class);

			String invalid = validate(m);
			if(invalid != null) {
				return Response.status(Response.Status.BAD_REQUEST).entity(invalid).build();
			}

			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			int newId = new Dhis2MapManager().addMapping(sd, oId, m);
			if(newId < 0) {
				return Response.serverError().entity("Insert failed").build();
			}
			m.id = newId;
			m.o_id = oId;
			return Response.ok(new Gson().toJson(m)).build();

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// PUT, update a mapping
	// -------------------------------------------------------------------------

	@PUT
	@Path("/{id}")
	public Response updateMapping(@Context HttpServletRequest request,
			@PathParam("id") int id, String body) {

		String conn = "surveyKPI-Dhis2Maps-put";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			Dhis2Map m = new Gson().fromJson(body, Dhis2Map.class);
			m.id = id;

			String invalid = validate(m);
			if(invalid != null) {
				return Response.status(Response.Status.BAD_REQUEST).entity(invalid).build();
			}

			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			new Dhis2MapManager().updateMapping(sd, oId, m);
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
	@Path("/{id}")
	public Response deleteMapping(@Context HttpServletRequest request, @PathParam("id") int id) {

		String conn = "surveyKPI-Dhis2Maps-delete";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			new Dhis2MapManager().deleteMapping(sd, oId, id);
			return Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// POST sync, refresh one mapping now
	// -------------------------------------------------------------------------

	@POST
	@Path("/{id}/sync")
	@Produces(MediaType.APPLICATION_JSON)
	public Response syncMapping(@Context HttpServletRequest request, @PathParam("id") int id) {

		String conn = "surveyKPI-Dhis2Maps-sync";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request, request.getRemoteUser());
			Dhis2MapManager mm = new Dhis2MapManager();
			Dhis2Map m = mm.getMapping(sd, oId, id);
			if(m == null) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}

			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources",
					new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser())));

			try {
				int count = mm.syncOne(sd, m, localisation);
				HashMap<String, Object> result = new HashMap<>();
				result.put("count", count);
				return Response.ok(new Gson().toJson(result)).build();
			} catch (Exception e) {
				/*
				 * A sync that fails is an answer, not a server fault.  Record why against the
				 * mapping so it is visible in the list, and tell the caller plainly
				 */
				log.info("DHIS2 sync failed for '" + m.smap_name + "': " + e.getMessage());
				mm.updateSyncStatus(sd, m.id, 0, m.row_count, e.getMessage());
				return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	private String validate(Dhis2Map m) {
		if(m.smap_name == null || m.smap_name.trim().length() == 0) {
			return "A resource name is required";
		}
		if(m.smap_name.contains(" ")) {
			return "The resource name must not contain spaces";
		}
		if(m.resource_type == null || m.resource_type.trim().length() == 0) {
			return "A resource type is required";
		}
		if(!Dhis2Map.TYPE_ORGUNITS.equals(m.resource_type)
				&& !Dhis2Map.TYPE_OPTIONSET.equals(m.resource_type)
				&& !Dhis2Map.TYPE_PROGRAMS.equals(m.resource_type)) {
			return "Unknown resource type: " + m.resource_type;
		}
		return null;
	}
}
