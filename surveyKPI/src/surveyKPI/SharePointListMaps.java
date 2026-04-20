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
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.managers.SharePointListMapManager;
import org.smap.sdal.model.ServerData;
import org.smap.sdal.model.SharePointListMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * CRUD + sync-now for SharePoint list mappings.
 * All endpoints are org-scoped (admin only).
 */
@Path("/sharepoint/listmaps")
public class SharePointListMaps extends Application {

	private static Logger log = Logger.getLogger(SharePointListMaps.class.getName());

	Authorise adminAuth;

	public SharePointListMaps() {
		ArrayList<String> auth = new ArrayList<>();
		auth.add(Authorise.ADMIN);
		adminAuth = new Authorise(auth, null);
	}

	// -------------------------------------------------------------------------
	// GET — list all mappings for the caller's org
	// -------------------------------------------------------------------------

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMappings(@Context HttpServletRequest request) {

		String conn = "surveyKPI-SharePointListMaps-get";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request.getRemoteUser());

		try {
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			ArrayList<SharePointListMap> maps = new SharePointListMapManager().getMappings(sd, oId);
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
	// POST — add a new mapping
	// -------------------------------------------------------------------------

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public Response addMapping(@Context HttpServletRequest request, String body) {

		String conn = "surveyKPI-SharePointListMaps-post";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request.getRemoteUser());

		try {
			SharePointListMap m = new Gson().fromJson(body, SharePointListMap.class);
			if(m.smap_name != null && m.smap_name.contains(" ")) {
				return Response.status(Response.Status.BAD_REQUEST)
						.entity("Smap name must not contain spaces").build();
			}
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			int newId = new SharePointListMapManager().addMapping(sd, oId, m);
			if (newId < 0) {
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
	// PUT — update an existing mapping
	// -------------------------------------------------------------------------

	@PUT
	@Path("/{id}")
	public Response updateMapping(@Context HttpServletRequest request,
			@PathParam("id") int id, String body) {

		String conn = "surveyKPI-SharePointListMaps-put";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request.getRemoteUser());

		try {
			SharePointListMap m = new Gson().fromJson(body, SharePointListMap.class);
			m.id = id;
			new SharePointListMapManager().updateMapping(sd, m);
			return Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// DELETE — remove a mapping
	// -------------------------------------------------------------------------

	@DELETE
	@Path("/{id}")
	public Response deleteMapping(@Context HttpServletRequest request,
			@PathParam("id") int id) {

		String conn = "surveyKPI-SharePointListMaps-delete";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request.getRemoteUser());

		try {
			new SharePointListMapManager().deleteMapping(sd, id);
			return Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// POST /{id}/sync — trigger an immediate sync for one mapping
	// -------------------------------------------------------------------------

	@POST
	@Path("/{id}/sync")
	public Response syncNow(@Context HttpServletRequest request,
			@PathParam("id") int id) {

		String conn = "surveyKPI-SharePointListMaps-syncNow";
		Connection sd = SDDataSource.getConnection(conn);
		adminAuth.isAuthorised(sd, request.getRemoteUser());

		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			ArrayList<SharePointListMap> maps = new SharePointListMapManager().getMappings(sd, oId);
			SharePointListMap target = null;
			for (SharePointListMap m : maps) {
				if (m.id == id) { target = m; break; }
			}
			if (target == null) {
				return Response.status(Status.NOT_FOUND).entity("Mapping not found").build();
			}

			ServerData serverData = new ServerManager().getServer(sd, localisation);
			int count = new SharePointListMapManager().syncOne(sd, target, serverData, localisation);
			return Response.ok("{\"count\":" + count + "}").type(MediaType.APPLICATION_JSON).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(conn, sd);
		}
	}
}
