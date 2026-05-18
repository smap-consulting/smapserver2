package koboToolboxApi;

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

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.model.Notification;
import org.smap.sdal.model.NotifyDetails;
import org.smap.sdal.model.SpNotification;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * CRUD API for SharePoint list notifications.
 * Intended for server-to-server calls from the SP Add-In — no X-Requested-With header required.
 *
 * GET    /api/v1/sharepoint/notifications          list SP notifications for caller
 * POST   /api/v1/sharepoint/notifications          create SP notification
 * PUT    /api/v1/sharepoint/notifications/{id}     update SP notification
 * DELETE /api/v1/sharepoint/notifications/{id}     delete SP notification
 */
@Path("/v1/sharepoint/notifications")
public class SpNotifications extends Application {

	private static Logger log = Logger.getLogger(SpNotifications.class.getName());

	private Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	Authorise auth;

	public SpNotifications() {
		ArrayList<String> authorisations = new ArrayList<>();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		auth = new Authorise(authorisations, null);
	}

	/*
	 * List all SP notifications accessible to the caller across all their projects.
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getSpNotifications(
			@Context HttpServletRequest request,
			@QueryParam("tz") String tz) {

		String conn = "koboToolboxApi-SpNotifications-get";
		Connection sd = SDDataSource.getConnection(conn);
		auth.isAuthorised(sd, request.getRemoteUser());

		if (tz == null) tz = "UTC";

		PreparedStatement pstmt = null;
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			NotificationManager nm = new NotificationManager(localisation);
			ArrayList<Notification> all = nm.getUserNotifications(sd, pstmt, request.getRemoteUser(), tz);

			ArrayList<SpNotification> result = new ArrayList<>();
			for (Notification n : all) {
				if ("sharepoint_list".equals(n.target)) {
					result.add(toSpNotification(sd, n));
				}
			}
			return Response.ok(gson.toJson(result)).build();

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ignored) {}
			SDDataSource.closeConnection(conn, sd);
		}
	}

	/*
	 * Create a new SP notification. Returns the created notification including its id.
	 *
	 * Required: survey_ident, sp_list_title
	 * Optional: name, enabled (default true), sp_operation (default "insert"),
	 *           sp_match_column, sp_match_field (required when sp_operation = "update"),
	 *           column_map
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createSpNotification(
			@Context HttpServletRequest request,
			String body,
			@QueryParam("tz") String tz) {

		String conn = "koboToolboxApi-SpNotifications-post";
		Connection sd = SDDataSource.getConnection(conn);
		auth.isAuthorised(sd, request.getRemoteUser());

		if (tz == null) tz = "UTC";

		PreparedStatement pstmt = null;
		try {
			SpNotification spn = gson.fromJson(body, SpNotification.class);

			if (spn == null || spn.survey_ident == null || spn.sp_list_title == null) {
				return Response.status(Response.Status.BAD_REQUEST)
						.entity("survey_ident and sp_list_title are required").build();
			}

			boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			auth.isValidSurveyIdent(sd, request.getRemoteUser(), spn.survey_ident, false, superUser);

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			Notification n = toNotification(sd, spn);
			NotificationManager nm = new NotificationManager(localisation);
			int newId = nm.addNotification(sd, pstmt, request.getRemoteUser(), n, tz);

			spn.id = newId;
			return Response.status(Response.Status.CREATED)
					.entity(gson.toJson(spn)).build();

		} catch (AuthorisationException e) {
			return Response.status(Response.Status.FORBIDDEN).entity("Not authorised").build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ignored) {}
			SDDataSource.closeConnection(conn, sd);
		}
	}

	/*
	 * Update an existing SP notification.
	 */
	@PUT
	@Path("/{id}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateSpNotification(
			@Context HttpServletRequest request,
			@PathParam("id") int id,
			String body,
			@QueryParam("tz") String tz) {

		String conn = "koboToolboxApi-SpNotifications-put";
		Connection sd = SDDataSource.getConnection(conn);
		auth.isAuthorised(sd, request.getRemoteUser());

		if (tz == null) tz = "UTC";

		PreparedStatement pstmt = null;
		try {
			SpNotification spn = gson.fromJson(body, SpNotification.class);
			if (spn == null || spn.survey_ident == null || spn.sp_list_title == null) {
				return Response.status(Response.Status.BAD_REQUEST)
						.entity("survey_ident and sp_list_title are required").build();
			}
			spn.id = id;

			boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			auth.isValidSurveyIdent(sd, request.getRemoteUser(), spn.survey_ident, false, superUser);

			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			Notification n = toNotification(sd, spn);
			NotificationManager nm = new NotificationManager(localisation);
			nm.updateNotification(sd, pstmt, request.getRemoteUser(), n, tz);

			return Response.ok().build();

		} catch (AuthorisationException e) {
			return Response.status(Response.Status.FORBIDDEN).entity("Not authorised").build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ignored) {}
			SDDataSource.closeConnection(conn, sd);
		}
	}

	/*
	 * Delete an SP notification.
	 */
	@DELETE
	@Path("/{id}")
	public Response deleteSpNotification(
			@Context HttpServletRequest request,
			@PathParam("id") int id) {

		String conn = "koboToolboxApi-SpNotifications-delete";
		Connection sd = SDDataSource.getConnection(conn);
		auth.isAuthorised(sd, request.getRemoteUser());

		PreparedStatement pstmt = null;
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			int sId = 0;
			String sql = "select s_id from forward where id = ? and target = 'sharepoint_list'";
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, id);
			ResultSet rs = pstmt.executeQuery();
			if (!rs.next()) {
				return Response.status(Response.Status.NOT_FOUND).entity("Notification not found").build();
			}
			sId = rs.getInt("s_id");

			NotificationManager nm = new NotificationManager(localisation);
			nm.deleteNotification(sd, request.getRemoteUser(), id, sId);

			return Response.ok().build();

		} catch (AuthorisationException e) {
			return Response.status(Response.Status.FORBIDDEN).entity("Not authorised").build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException ignored) {}
			SDDataSource.closeConnection(conn, sd);
		}
	}

	// -------------------------------------------------------------------------
	// Conversion helpers
	// -------------------------------------------------------------------------

	private SpNotification toSpNotification(Connection sd, Notification n) throws SQLException {
		SpNotification spn = new SpNotification();
		spn.id = n.id;
		spn.name = n.name;
		spn.enabled = n.enabled;
		spn.survey_name = n.s_name;
		spn.survey_ident = GeneralUtilityMethods.getSurveyIdent(sd, n.s_id);
		if (n.notifyDetails != null) {
			spn.sp_list_title = n.notifyDetails.sp_list_title;
			spn.sp_operation = n.notifyDetails.sp_operation;
			spn.sp_match_column = n.notifyDetails.sp_match_column;
			spn.sp_match_field = n.notifyDetails.sp_match_field;
			spn.column_map = n.notifyDetails.sp_column_map;
		}
		return spn;
	}

	private Notification toNotification(Connection sd, SpNotification spn) throws SQLException {
		Notification n = new Notification();
		n.id = spn.id;
		n.name = spn.name;
		n.enabled = spn.enabled;
		n.trigger = "submission";
		n.target = "sharepoint_list";
		n.s_id = GeneralUtilityMethods.getSurveyId(sd, spn.survey_ident);
		n.p_id = GeneralUtilityMethods.getProjectIdFromSurveyIdent(sd, spn.survey_ident);

		NotifyDetails nd = new NotifyDetails();
		nd.sp_list_title = spn.sp_list_title;
		nd.sp_operation = (spn.sp_operation != null) ? spn.sp_operation : "insert";
		nd.sp_match_column = spn.sp_match_column;
		nd.sp_match_field = spn.sp_match_field;
		nd.sp_column_map = spn.column_map;
		n.notifyDetails = nd;

		return n;
	}
}
