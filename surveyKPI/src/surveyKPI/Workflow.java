package surveyKPI;

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

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.WorkflowManager;
import org.smap.sdal.model.NotifyDetails;
import org.smap.sdal.model.WorkflowData;
import org.smap.sdal.model.WorkflowItem;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

@Path("/workflow")
public class Workflow extends Application {

	Authorise a = null;

	private static Logger log = Logger.getLogger(Workflow.class.getName());

	public Workflow() {
		ArrayList<String> authorisations = new ArrayList<String>();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}

	// ============================================================
	// Inner model classes for edit operations
	// ============================================================

	static class WorkflowEditNotif {
		public int    id;
		public String name;
		public String trigger;
		public int    srcSurveyId;
		public String srcSurveyName;
		public String target;
		public String remoteUser;
		public String filter;
		public boolean enabled;
		public int    projectId;
		public boolean bundle;
		// email
		public String emailTo;
		public String emailSubject;
		public String emailContent;
		// sms
		public String smsTo;
		public String smsMessage;
		// case
		public String caseSurveyIdent;
		public String caseSurveyName;
	}

	static class WorkflowEditTG {
		public int    tgId;
		public String name;
		public int    sourceSurveyId;
		public String sourceSurveyName;
		public int    targetSurveyId;
		public String targetSurveyName;
		public String filter;
		public String remoteUser;   // derived assignee display string (read-only in simple editor)
		public int    projectId;
	}

	static class WorkflowSurvey {
		public int    sId;
		public String name;
		public String ident;
		public int    projectId;
		public String projectName;
	}

	// ============================================================
	// GET /workflow/items
	// Returns WorkflowData (items + links) with server-layout merged with saved positions.
	// ============================================================
	@Path("/items")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getWorkflowItems(@Context HttpServletRequest request) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-items";

		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		try {
			WorkflowManager wm = new WorkflowManager();
			WorkflowData data = wm.getWorkflowItems(sd, request.getRemoteUser());

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			response = Response.ok(gson.toJson(data)).build();

		} catch (SQLException e) {
			log.log(Level.SEVERE, "No data available", e);
			response = Response.serverError().entity("No data available").build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error getting workflow items", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	// ============================================================
	// PUT /workflow/positions  — save full node positions map
	// ============================================================
	@Path("/positions")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response savePositions(@Context HttpServletRequest request, String body) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-savePositions";

		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		try {
			Map<String, WorkflowItem> positions = new Gson().fromJson(body,
					new TypeToken<Map<String, WorkflowItem>>(){}.getType());
			WorkflowManager wm = new WorkflowManager();
			wm.savePositions(sd, request.getRemoteUser(), positions);
			response = Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error saving workflow positions", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	// ============================================================
	// DELETE /workflow/positions  — revert to server-computed defaults
	// ============================================================
	@Path("/positions")
	@DELETE
	public Response resetPositions(@Context HttpServletRequest request) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-resetPositions";

		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		try {
			WorkflowManager wm = new WorkflowManager();
			wm.resetPositions(sd, request.getRemoteUser());
			response = Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error resetting workflow positions", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}

	// ============================================================
	// GET /workflow/edit/surveys
	// Returns all surveys accessible to the user (for dropdowns).
	// ============================================================
	@Path("/edit/surveys")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getEditSurveys(@Context HttpServletRequest request) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-editSurveys";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		String sql = "select s.s_id, s.display_name, s.ident, s.p_id, p.name as project_name "
				+ "from survey s "
				+ "join project p on p.id = s.p_id "
				+ "join user_project up on p.id = up.p_id "
				+ "join users u on up.u_id = u.id "
				+ "where u.ident = ? and not s.deleted "
				+ "order by p.name, s.display_name";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, request.getRemoteUser());
			ResultSet rs = pstmt.executeQuery();
			List<WorkflowSurvey> surveys = new ArrayList<>();
			while (rs.next()) {
				WorkflowSurvey ws = new WorkflowSurvey();
				ws.sId         = rs.getInt("s_id");
				ws.name        = rs.getString("display_name");
				ws.ident       = rs.getString("ident");
				ws.projectId   = rs.getInt("p_id");
				ws.projectName = rs.getString("project_name");
				surveys.add(ws);
			}
			response = Response.ok(new GsonBuilder().disableHtmlEscaping().create().toJson(surveys)).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error getting surveys for workflow edit", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}

	// ============================================================
	// GET /workflow/edit/notifications?ids=1,2,3
	// Returns notification details for the edit drawer.
	// ============================================================
	@Path("/edit/notifications")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getEditNotifications(@Context HttpServletRequest request,
			@QueryParam("ids") String idsParam) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-editNotifications-get";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		String sql = "select f.id, f.name, f.trigger, f.s_id, "
				+ "s.display_name as src_survey_name, "
				+ "f.target, f.remote_user, f.filter, f.enabled, f.p_id, "
				+ "f.notify_details, "
				+ "s_case.display_name as case_survey_name "
				+ "from forward f "
				+ "left join survey s on s.s_id = f.s_id "
				+ "left join survey s_case on f.target = 'escalate' "
				+ "  and f.notify_details is not null "
				+ "  and s_case.ident = (f.notify_details::json->>'survey_case') "
				+ "where f.id = any(?) "
				+ "and (f.p_id in (select p.id from project p, user_project up, users u "
				+ "    where p.id = up.p_id and up.u_id = u.id and u.ident = ?) "
				+ "  or f.s_id in (select s2.s_id from survey s2, project p, user_project up, users u "
				+ "    where s2.p_id = p.id and p.id = up.p_id and up.u_id = u.id and u.ident = ? and not s2.deleted)) "
				+ "order by f.id";

		PreparedStatement pstmt = null;
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		try {
			Integer[] ids = parseIds(idsParam);
			if (ids.length == 0) {
				return Response.ok("[]").build();
			}
			Array arr = sd.createArrayOf("integer", ids);
			pstmt = sd.prepareStatement(sql);
			pstmt.setArray(1, arr);
			pstmt.setString(2, request.getRemoteUser());
			pstmt.setString(3, request.getRemoteUser());
			ResultSet rs = pstmt.executeQuery();
			List<WorkflowEditNotif> list = new ArrayList<>();
			while (rs.next()) {
				WorkflowEditNotif n = new WorkflowEditNotif();
				n.id            = rs.getInt("id");
				n.name          = rs.getString("name");
				n.trigger       = rs.getString("trigger");
				n.srcSurveyId   = rs.getInt("s_id");
				n.srcSurveyName = rs.getString("src_survey_name");
				n.target        = rs.getString("target");
				n.remoteUser    = rs.getString("remote_user");
				n.filter        = rs.getString("filter");
				n.enabled       = rs.getBoolean("enabled");
				n.projectId     = rs.getInt("p_id");
				// Extract relevant notify_details fields
				String ndJson = rs.getString("notify_details");
				if (ndJson != null) {
					NotifyDetails nd = gson.fromJson(ndJson, NotifyDetails.class);
					if (nd != null) {
						if ("email".equals(n.target)) {
							if (nd.emails != null && !nd.emails.isEmpty()) {
								n.emailTo = String.join(", ", nd.emails);
							}
							n.emailSubject = nd.subject;
							n.emailContent = nd.content;
						} else if ("sms".equals(n.target)) {
							n.smsTo      = nd.ourNumber;
							n.smsMessage = nd.content;
						} else if ("escalate".equals(n.target)) {
							n.caseSurveyIdent = nd.survey_case;
							n.caseSurveyName  = rs.getString("case_survey_name");
						}
					}
				}
				list.add(n);
			}
			response = Response.ok(gson.toJson(list)).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error getting notifications for edit", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}

	// ============================================================
	// PUT /workflow/edit/notifications
	// Saves a batch of updated notifications (shared fields + per-connection filters).
	// ============================================================
	@Path("/edit/notifications")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateNotifications(@Context HttpServletRequest request, String body) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-editNotifications-put";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		List<WorkflowEditNotif> notifs = gson.fromJson(body,
				new TypeToken<List<WorkflowEditNotif>>(){}.getType());

		PreparedStatement pstmtGet = null;
		PreparedStatement pstmtUpd = null;
		try {
			// For each notification: fetch existing notify_details, merge changes, save.
			String sqlGet = "select notify_details, target from forward where id = ? "
					+ "and (p_id in (select p.id from project p, user_project up, users u "
					+ "    where p.id = up.p_id and up.u_id = u.id and u.ident = ?) "
					+ "  or s_id in (select s2.s_id from survey s2, project p, user_project up, users u "
					+ "    where s2.p_id = p.id and p.id = up.p_id and up.u_id = u.id and u.ident = ? and not s2.deleted))";

			String sqlUpd = "update forward set name = ?, remote_user = ?, filter = ?, enabled = ?, "
					+ "notify_details = ?, updated = true "
					+ "where id = ? "
					+ "and (p_id in (select p.id from project p, user_project up, users u "
					+ "    where p.id = up.p_id and up.u_id = u.id and u.ident = ?) "
					+ "  or s_id in (select s2.s_id from survey s2, project p, user_project up, users u "
					+ "    where s2.p_id = p.id and p.id = up.p_id and up.u_id = u.id and u.ident = ? and not s2.deleted))";

			pstmtGet = sd.prepareStatement(sqlGet);
			pstmtUpd = sd.prepareStatement(sqlUpd);

			for (WorkflowEditNotif n : notifs) {
				// Fetch existing notify_details to preserve unedited fields
				pstmtGet.setInt(1, n.id);
				pstmtGet.setString(2, request.getRemoteUser());
				pstmtGet.setString(3, request.getRemoteUser());
				ResultSet rs = pstmtGet.executeQuery();
				NotifyDetails nd = new NotifyDetails();
				if (rs.next()) {
					String existing = rs.getString("notify_details");
					if (existing != null) {
						NotifyDetails parsed = gson.fromJson(existing, NotifyDetails.class);
						if (parsed != null) nd = parsed;
					}
				}
				rs.close();

				// Merge edited fields into notify_details
				String target = n.target != null ? n.target : "";
				if ("email".equals(target)) {
					if (n.emailTo != null) {
						nd.emails = new ArrayList<>();
						for (String e : n.emailTo.split(",")) {
							String trimmed = e.trim();
							if (!trimmed.isEmpty()) nd.emails.add(trimmed);
						}
					}
					if (n.emailSubject != null) nd.subject = n.emailSubject;
					if (n.emailContent != null) nd.content  = n.emailContent;
				} else if ("sms".equals(target)) {
					if (n.smsTo      != null) nd.ourNumber = n.smsTo;
					if (n.smsMessage != null) nd.content   = n.smsMessage;
				} else if ("escalate".equals(target)) {
					if (n.caseSurveyIdent != null) nd.survey_case = n.caseSurveyIdent;
				}

				pstmtUpd.setString(1, n.name);
				pstmtUpd.setString(2, n.remoteUser);
				pstmtUpd.setString(3, n.filter);
				pstmtUpd.setBoolean(4, n.enabled);
				pstmtUpd.setString(5, gson.toJson(nd));
				pstmtUpd.setInt(6, n.id);
				pstmtUpd.setString(7, request.getRemoteUser());
				pstmtUpd.setString(8, request.getRemoteUser());
				pstmtUpd.executeUpdate();
			}
			response = Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error updating notifications", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmtGet != null) pstmtGet.close(); } catch (SQLException e) {}
			try { if (pstmtUpd != null) pstmtUpd.close(); } catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}

	// ============================================================
	// DELETE /workflow/edit/notification/{id}
	// Deletes a single forward record.
	// ============================================================
	@Path("/edit/notification/{id}")
	@DELETE
	public Response deleteNotification(@Context HttpServletRequest request,
			@PathParam("id") int id) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-editNotification-delete";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		String sql = "delete from forward where id = ? "
				+ "and (p_id in (select p.id from project p, user_project up, users u "
				+ "    where p.id = up.p_id and up.u_id = u.id and u.ident = ?) "
				+ "  or s_id in (select s2.s_id from survey s2, project p, user_project up, users u "
				+ "    where s2.p_id = p.id and p.id = up.p_id and up.u_id = u.id and u.ident = ? and not s2.deleted))";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, id);
			pstmt.setString(2, request.getRemoteUser());
			pstmt.setString(3, request.getRemoteUser());
			pstmt.executeUpdate();
			response = Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error deleting notification " + id, e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}

	// ============================================================
	// POST /workflow/edit/notification
	// Creates a new forward record from the simplified edit model.
	// ============================================================
	@Path("/edit/notification")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createNotification(@Context HttpServletRequest request, String body) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-editNotification-post";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		WorkflowEditNotif n = gson.fromJson(body, WorkflowEditNotif.class);

		// Derive p_id and bundle_ident from source survey if needed
		PreparedStatement pstmtPid = null;
		PreparedStatement pstmtIns = null;
		String bundleIdent = null;
		try {
			if (n.srcSurveyId > 0) {
				pstmtPid = sd.prepareStatement(
						"select p_id, group_survey_ident from survey where s_id = ?");
				pstmtPid.setInt(1, n.srcSurveyId);
				ResultSet rs = pstmtPid.executeQuery();
				if (rs.next()) {
					if (n.projectId <= 0) n.projectId = rs.getInt("p_id");
					if (n.bundle) bundleIdent = rs.getString("group_survey_ident");
				}
				rs.close();
			}

			// Build notify_details
			NotifyDetails nd = new NotifyDetails();
			String target = n.target != null ? n.target : "task";
			if ("email".equals(target)) {
				nd.emails = new ArrayList<>();
				if (n.emailTo != null) {
					for (String e : n.emailTo.split(",")) {
						String trimmed = e.trim();
						if (!trimmed.isEmpty()) nd.emails.add(trimmed);
					}
				}
				nd.subject = n.emailSubject;
				nd.content  = n.emailContent;
			} else if ("sms".equals(target)) {
				nd.ourNumber = n.smsTo;
				nd.content   = n.smsMessage;
			} else if ("escalate".equals(target)) {
				nd.survey_case = n.caseSurveyIdent;
			}

			String sql = "insert into forward(s_id, enabled, trigger, target, filter, name, p_id, "
					+ "remote_user, notify_details, bundle, bundle_ident, updated) "
					+ "values(?, ?, 'submission', ?, ?, ?, ?, ?, ?::jsonb, ?, ?, true) "
					+ "returning id";
			pstmtIns = sd.prepareStatement(sql);
			// For bundle notifications the survey is identified via bundle_ident, not s_id
			if (n.bundle) {
				pstmtIns.setNull(1, java.sql.Types.INTEGER);
			} else {
				pstmtIns.setInt(1, n.srcSurveyId);
			}
			pstmtIns.setBoolean(2, true);
			pstmtIns.setString(3, target);
			pstmtIns.setString(4, n.filter);
			pstmtIns.setString(5, n.name != null ? n.name : "");
			pstmtIns.setInt(6, n.projectId);
			pstmtIns.setString(7, n.remoteUser);
			pstmtIns.setString(8, gson.toJson(nd));
			pstmtIns.setBoolean(9, n.bundle);
			pstmtIns.setString(10, bundleIdent);
			ResultSet rs = pstmtIns.executeQuery();
			int newId = rs.next() ? rs.getInt(1) : 0;
			response = Response.ok("{\"id\":" + newId + "}").build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error creating notification", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmtPid != null) pstmtPid.close(); } catch (SQLException e) {}
			try { if (pstmtIns != null) pstmtIns.close(); } catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}

	// ============================================================
	// GET /workflow/edit/taskgroups?ids=1,2
	// Returns task group details for the edit drawer.
	// ============================================================
	@Path("/edit/taskgroups")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getEditTaskGroups(@Context HttpServletRequest request,
			@QueryParam("ids") String idsParam) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-editTaskGroups-get";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		String sql = "select tg.tg_id, tg.name, tg.source_s_id, tg.target_s_id, tg.rule, tg.p_id, "
				+ "s_src.display_name as src_survey_name, "
				+ "s_tgt.display_name as tgt_survey_name "
				+ "from task_group tg "
				+ "left join survey s_src on s_src.s_id = tg.source_s_id "
				+ "left join survey s_tgt on s_tgt.s_id = tg.target_s_id "
				+ "where tg.tg_id = any(?) "
				+ "and tg.p_id in (select p.id from project p, user_project up, users u "
				+ "    where p.id = up.p_id and up.u_id = u.id and u.ident = ?) "
				+ "order by tg.tg_id";

		PreparedStatement pstmt = null;
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		try {
			Integer[] ids = parseIds(idsParam);
			if (ids.length == 0) {
				return Response.ok("[]").build();
			}
			Array arr = sd.createArrayOf("integer", ids);
			pstmt = sd.prepareStatement(sql);
			pstmt.setArray(1, arr);
			pstmt.setString(2, request.getRemoteUser());
			ResultSet rs = pstmt.executeQuery();
			List<WorkflowEditTG> list = new ArrayList<>();
			while (rs.next()) {
				WorkflowEditTG tg = new WorkflowEditTG();
				tg.tgId            = rs.getInt("tg_id");
				tg.name            = rs.getString("name");
				tg.sourceSurveyId  = rs.getInt("source_s_id");
				tg.sourceSurveyName = rs.getString("src_survey_name");
				tg.targetSurveyId  = rs.getInt("target_s_id");
				tg.targetSurveyName = rs.getString("tgt_survey_name");
				tg.projectId       = rs.getInt("p_id");
				// Derive filter and assignee from rule JSON
				String ruleJson = rs.getString("rule");
				if (ruleJson != null) {
					org.smap.sdal.model.AssignFromSurvey afs =
						gson.fromJson(ruleJson, org.smap.sdal.model.AssignFromSurvey.class);
					if (afs != null) {
						if (afs.filter != null) {
							tg.filter = afs.filter.advanced != null
									? afs.filter.advanced : afs.filter.qText;
						}
						// Derive human-readable assignee
						if (afs.assign_data != null && !afs.assign_data.trim().isEmpty()) {
							tg.remoteUser = "_data";
						} else if (afs.emails != null && !afs.emails.trim().isEmpty()) {
							tg.remoteUser = afs.emails.trim();
						} else if (afs.role_id > 0) {
							tg.remoteUser = lookupRole(sd, afs.role_id);
						} else if (afs.user_id > 0) {
							tg.remoteUser = lookupUser(sd, afs.user_id);
						}
					}
				}
				list.add(tg);
			}
			response = Response.ok(gson.toJson(list)).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error getting task groups for edit", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}

	// ============================================================
	// PUT /workflow/edit/taskgroups
	// Saves a batch of updated task groups (filter only; use Advanced for assignee).
	// ============================================================
	@Path("/edit/taskgroups")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public Response updateTaskGroups(@Context HttpServletRequest request, String body) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-editTaskGroups-put";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		List<WorkflowEditTG> tgs = gson.fromJson(body,
				new TypeToken<List<WorkflowEditTG>>(){}.getType());

		PreparedStatement pstmtGet = null;
		PreparedStatement pstmtUpd = null;
		try {
			String sqlGet = "select rule from task_group where tg_id = ? "
					+ "and p_id in (select p.id from project p, user_project up, users u "
					+ "    where p.id = up.p_id and up.u_id = u.id and u.ident = ?)";
			String sqlUpd = "update task_group set name = ?, rule = ? where tg_id = ? "
					+ "and p_id in (select p.id from project p, user_project up, users u "
					+ "    where p.id = up.p_id and up.u_id = u.id and u.ident = ?)";

			pstmtGet = sd.prepareStatement(sqlGet);
			pstmtUpd = sd.prepareStatement(sqlUpd);

			for (WorkflowEditTG tg : tgs) {
				// Fetch and update the rule JSON (preserving fields we don't edit)
				pstmtGet.setInt(1, tg.tgId);
				pstmtGet.setString(2, request.getRemoteUser());
				ResultSet rs = pstmtGet.executeQuery();
				org.smap.sdal.model.AssignFromSurvey afs = new org.smap.sdal.model.AssignFromSurvey();
				if (rs.next()) {
					String existing = rs.getString("rule");
					if (existing != null) {
						org.smap.sdal.model.AssignFromSurvey parsed =
							gson.fromJson(existing, org.smap.sdal.model.AssignFromSurvey.class);
						if (parsed != null) afs = parsed;
					}
				}
				rs.close();

				// Update filter in the rule
				if (tg.filter != null && !tg.filter.trim().isEmpty()) {
					if (afs.filter == null) {
						afs.filter = new org.smap.sdal.model.SqlWhereClause();
					}
					afs.filter.advanced = tg.filter;
				} else {
					afs.filter = null;
				}

				pstmtUpd.setString(1, tg.name);
				pstmtUpd.setString(2, gson.toJson(afs));
				pstmtUpd.setInt(3, tg.tgId);
				pstmtUpd.setString(4, request.getRemoteUser());
				pstmtUpd.executeUpdate();
			}
			response = Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error updating task groups", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmtGet != null) pstmtGet.close(); } catch (SQLException e) {}
			try { if (pstmtUpd != null) pstmtUpd.close(); } catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}

	// ============================================================
	// DELETE /workflow/edit/taskgroup/{id}
	// Deletes a task group and its associated forward record(s).
	// ============================================================
	@Path("/edit/taskgroup/{id}")
	@DELETE
	public Response deleteTaskGroup(@Context HttpServletRequest request,
			@PathParam("id") int id) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-editTaskGroup-delete";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		PreparedStatement pstmtFwd = null;
		PreparedStatement pstmtTG  = null;
		try {
			// Delete any forward records that reference this task group
			pstmtFwd = sd.prepareStatement("delete from forward where tg_id = ?");
			pstmtFwd.setInt(1, id);
			pstmtFwd.executeUpdate();

			// Delete the task group itself (checking project access)
			pstmtTG = sd.prepareStatement(
					"delete from task_group where tg_id = ? "
					+ "and p_id in (select p.id from project p, user_project up, users u "
					+ "    where p.id = up.p_id and up.u_id = u.id and u.ident = ?)");
			pstmtTG.setInt(1, id);
			pstmtTG.setString(2, request.getRemoteUser());
			pstmtTG.executeUpdate();
			response = Response.ok().build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error deleting task group " + id, e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmtFwd != null) pstmtFwd.close(); } catch (SQLException e) {}
			try { if (pstmtTG  != null) pstmtTG.close();  } catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}

	// ============================================================
	// POST /workflow/edit/taskgroup
	// Creates a new task group from the simplified edit model.
	// ============================================================
	@Path("/edit/taskgroup")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createTaskGroup(@Context HttpServletRequest request, String body) {

		Response response = null;
		String connectionString = "surveyKPI-Workflow-editTaskGroup-post";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());

		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		WorkflowEditTG tg = gson.fromJson(body, WorkflowEditTG.class);

		PreparedStatement pstmtPid = null;
		PreparedStatement pstmtIns = null;
		try {
			if (tg.projectId <= 0 && tg.sourceSurveyId > 0) {
				pstmtPid = sd.prepareStatement("select p_id from survey where s_id = ?");
				pstmtPid.setInt(1, tg.sourceSurveyId);
				ResultSet rs = pstmtPid.executeQuery();
				if (rs.next()) tg.projectId = rs.getInt(1);
				rs.close();
			}

			org.smap.sdal.model.AssignFromSurvey afs = new org.smap.sdal.model.AssignFromSurvey();
			afs.task_group_name  = tg.name;
			afs.source_survey_id = tg.sourceSurveyId;
			afs.target_survey_id = tg.targetSurveyId;
			if (tg.remoteUser != null && !tg.remoteUser.trim().isEmpty()) {
				if ("_data".equals(tg.remoteUser)) {
					afs.assign_data = "_data";
				} else {
					afs.emails = tg.remoteUser;
				}
			}
			if (tg.filter != null && !tg.filter.trim().isEmpty()) {
				afs.filter = new org.smap.sdal.model.SqlWhereClause();
				afs.filter.advanced = tg.filter;
			}

			String sql = "insert into task_group(name, p_id, source_s_id, target_s_id, rule, "
					+ "address_params, dl_dist, complete_all, assign_auto) "
					+ "values(?, ?, ?, ?, ?::jsonb, '[]', 0, false, false) returning tg_id";
			pstmtIns = sd.prepareStatement(sql);
			pstmtIns.setString(1, tg.name != null ? tg.name : "");
			pstmtIns.setInt(2, tg.projectId);
			pstmtIns.setInt(3, tg.sourceSurveyId);
			pstmtIns.setInt(4, tg.targetSurveyId);
			pstmtIns.setString(5, gson.toJson(afs));
			ResultSet rs = pstmtIns.executeQuery();
			int newId = rs.next() ? rs.getInt(1) : 0;
			response = Response.ok("{\"tgId\":" + newId + "}").build();
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error creating task group", e);
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try { if (pstmtPid != null) pstmtPid.close(); } catch (SQLException e) {}
			try { if (pstmtIns != null) pstmtIns.close(); } catch (SQLException e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}
		return response;
	}

	// ============================================================
	// Helpers
	// ============================================================

	private static Integer[] parseIds(String idsParam) {
		if (idsParam == null || idsParam.trim().isEmpty()) return new Integer[0];
		String[] parts = idsParam.split(",");
		Integer[] ids = new Integer[parts.length];
		for (int i = 0; i < parts.length; i++) {
			try { ids[i] = Integer.parseInt(parts[i].trim()); }
			catch (NumberFormatException e) { ids[i] = 0; }
		}
		return ids;
	}

	private String lookupRole(Connection sd, int roleId) {
		try (PreparedStatement ps = sd.prepareStatement("select name from roles where id = ?")) {
			ps.setInt(1, roleId);
			ResultSet rs = ps.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		} catch (SQLException e) { return null; }
	}

	private String lookupUser(Connection sd, int userId) {
		try (PreparedStatement ps = sd.prepareStatement("select ident from users where id = ?")) {
			ps.setInt(1, userId);
			ResultSet rs = ps.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		} catch (SQLException e) { return null; }
	}
}
