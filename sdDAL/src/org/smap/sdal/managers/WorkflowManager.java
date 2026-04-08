package org.smap.sdal.managers;

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.AssignFromSurvey;
import org.smap.sdal.model.WorkflowData;
import org.smap.sdal.model.WorkflowItem;
import org.smap.sdal.model.WorkflowLink;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class WorkflowManager {

	private static Logger log = Logger.getLogger(WorkflowManager.class.getName());

	// WorkItem type constants
	private static final String TYPE_FORM      = "form";
	private static final String TYPE_TASK      = "task";
	private static final String TYPE_CASE      = "case";
	private static final String TYPE_PERIODIC  = "periodic";
	private static final String TYPE_REMINDER  = "reminder";
	private static final String TYPE_EMAIL     = "email";
	private static final String TYPE_SMS       = "sms";
	private static final String TYPE_DECISION  = "decision";

	// WorkItem role constants (determines visual shape)
	private static final String ROLE_FORM         = "form";
	private static final String ROLE_TRIGGER      = "trigger";
	private static final String ROLE_NOTIFICATION = "notification";
	private static final String ROLE_DECISION     = "decision";

	/*
	 * Holds raw data for a bundle-level forward notification, deferred until
	 * all survey-specific nodes have been built.
	 */
	private static class BundleNotif {
		int     fId;
		String  bundleIdent;
		String  bundleName;   // display name of the group survey (used for highlight)
		String  target;
		boolean enabled;
		String  filter;
		String  fName;
		String  caseSurvey;
		String  caseSurveyIdent;
		String  projectName;
		String  remoteUser;
	}

	/*
	 * Build the full set of workflow nodes and links accessible to the user.
	 *
	 * Processing is two-pass:
	 *   Pass 1 — non-bundle forward records and task_group records.
	 *             Builds all survey-specific nodes and tracks which node keys
	 *             belong to each survey (surveyItemKeys).
	 *   Pass 2 — bundle forward notifications.
	 *             Sources are all existing nodes whose survey is a bundle member,
	 *             plus a fresh form node for any member not yet on the canvas.
	 */
	public WorkflowData getWorkflowItems(Connection sd, String user) throws Exception {

		WorkflowData data = new WorkflowData();
		LinkedHashMap<String, WorkflowItem> itemMap = new LinkedHashMap<>();

		// sId → all node keys created for that survey (both src and dst sides)
		Map<Integer, List<String>> surveyItemKeys = new LinkedHashMap<>();

		Map<String, List<String[]>> bundleMembers = getBundleMembers(sd, user);
		// Reverse map: sId → bundle display name, for post-process stamping
		Map<Integer, String> surveyBundleNames = new LinkedHashMap<>();
		for (List<String[]> members : bundleMembers.values()) {
			for (String[] m : members) {
				if (m[3] != null) surveyBundleNames.put(Integer.parseInt(m[0]), m[3]);
			}
		}
		List<BundleNotif> pendingBundles = new ArrayList<>();

		// -------------------------------------------------------------------
		// Pass 1a: forward table — non-bundle records
		// -------------------------------------------------------------------
		String sqlForward =
				"select f.id, f.name, f.trigger, f.target, f.enabled, f.bundle, "
				+ "f.s_id, f.bundle_ident, f.p_id, f.filter, f.remote_user, "
				+ "s_src.display_name as trigger_survey, "
				+ "s_bun.display_name as bundle_name, "
				+ "s_case.display_name as case_survey, "
				+ "s_case.ident as case_survey_ident, "
				+ "proj.name as project_name "
				+ "from forward f "
				+ "left outer join survey s_src on s_src.s_id = f.s_id "
				+ "left outer join survey s_bun on s_bun.ident = f.bundle_ident "
				+ "left outer join survey s_case on f.target = 'escalate' "
				+ "  and f.notify_details is not null "
				+ "  and s_case.ident = (f.notify_details::json->>'survey_case') "
				+ "left outer join project proj on proj.id = f.p_id "
				+ "where (f.p_id in (select p.id from project p, user_project up, users u "
				+ "  where p.id = up.p_id and up.u_id = u.id and u.ident = ?) "
				+ "or f.s_id in (select s2.s_id from survey s2, project p, user_project up, users u "
				+ "  where s2.p_id = p.id and p.id = up.p_id and up.u_id = u.id and u.ident = ? and not s2.deleted) "
				+ "or f.bundle_ident in (select s2.group_survey_ident from survey s2, project p, user_project up, users u "
				+ "  where s2.p_id = p.id and p.id = up.p_id and up.u_id = u.id and u.ident = ? and not s2.deleted)) "
				+ "order by f.name asc";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sqlForward);
			pstmt.setString(1, user);
			pstmt.setString(2, user);
			pstmt.setString(3, user);
			log.fine("Workflow items from forward: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				int     fId              = rs.getInt("id");
				String  fName            = rs.getString("name");
				String  trigger          = rs.getString("trigger");
				String  target           = rs.getString("target");
				boolean enabled          = rs.getBoolean("enabled");
				boolean isBundle         = rs.getBoolean("bundle");
				int     sId              = rs.getInt("s_id");
				String  bundleIdent      = rs.getString("bundle_ident");
				String  triggerSurvey    = rs.getString("trigger_survey");
				String  bundleName       = rs.getString("bundle_name");
				String  caseSurvey       = rs.getString("case_survey");
				String  caseSurveyIdent  = rs.getString("case_survey_ident");
				String  filter           = rs.getString("filter");
				String  projectName      = rs.getString("project_name");
				String  remoteUser       = rs.getString("remote_user");

				// Defer bundle notifications until all survey nodes are built
				if (isBundle) {
					BundleNotif bn = new BundleNotif();
					bn.fId             = fId;
					bn.bundleIdent     = bundleIdent;
					bn.bundleName      = bundleName;
					bn.target          = target;
					bn.enabled         = enabled;
					bn.filter          = filter;
					bn.fName           = fName;
					bn.caseSurvey      = caseSurvey;
					bn.caseSurveyIdent = caseSurveyIdent;
					bn.projectName     = projectName;
					bn.remoteUser      = remoteUser;
					pendingBundles.add(bn);
					continue;
				}

				// -- Source node --
				String srcKey;
				if ("submission".equals(trigger) && sId > 0) {
					srcKey = "form:s:" + sId;
					WorkflowItem src = new WorkflowItem();
					src.id      = srcKey;
					src.type    = TYPE_FORM;
					src.role    = ROLE_FORM;
					src.name    = triggerSurvey != null ? triggerSurvey : fName;
					src.enabled = enabled;
					src.project = projectName;
					itemMap.putIfAbsent(srcKey, src);
					recordSurveyKey(surveyItemKeys, sId, srcKey);
				} else if ("periodic".equals(trigger)) {
					srcKey = "periodic:f:" + fId;
					WorkflowItem src = new WorkflowItem();
					src.id      = srcKey;
					src.type    = TYPE_PERIODIC;
					src.role    = ROLE_TRIGGER;
					src.name    = fName;
					src.enabled = enabled;
					src.project = projectName;
					itemMap.putIfAbsent(srcKey, src);
				} else if ("reminder".equals(trigger)) {
					srcKey = "reminder:f:" + fId;
					WorkflowItem src = new WorkflowItem();
					src.id      = srcKey;
					src.type    = TYPE_REMINDER;
					src.role    = ROLE_TRIGGER;
					src.name    = fName;
					src.enabled = enabled;
					src.project = projectName;
					itemMap.putIfAbsent(srcKey, src);
				} else {
					srcKey = "trigger:" + trigger + ":f:" + fId;
					WorkflowItem src = new WorkflowItem();
					src.id      = srcKey;
					src.type    = trigger;
					src.role    = ROLE_TRIGGER;
					src.name    = fName;
					src.enabled = enabled;
					src.project = projectName;
					itemMap.putIfAbsent(srcKey, src);
				}

				// -- Destination node --
				String dstKey;
				WorkflowItem dst = new WorkflowItem();
				dst.enabled = enabled;
				dst.project = projectName;

				if ("task".equals(target)) {
					String assignee   = mapAssignee(remoteUser);
					String assigneeK  = assigneeKey(remoteUser);
					dstKey = "task:f:" + fId + ":a:" + assigneeK;
					dst.name     = fName;
					dst.type     = TYPE_TASK;
					dst.role     = ROLE_FORM;
					dst.assignee = assignee;
				} else if ("escalate".equals(target)) {
					String assignee   = mapAssignee(remoteUser);
					String assigneeK  = assigneeKey(remoteUser);
					dstKey = caseSurveyIdent != null
							? "case:s:" + caseSurveyIdent + ":a:" + assigneeK
							: "case:f:" + fId + ":a:" + assigneeK;
					dst.name     = caseSurvey != null ? caseSurvey : fName;
					dst.type     = TYPE_CASE;
					dst.role     = ROLE_FORM;
					dst.assignee = assignee;
				} else if ("email".equals(target)) {
					dstKey   = "email:f:" + fId;
					dst.type = TYPE_EMAIL;
					dst.role = ROLE_NOTIFICATION;
					dst.name = fName;
				} else if ("sms".equals(target)) {
					dstKey   = "sms:f:" + fId;
					dst.type = TYPE_SMS;
					dst.role = ROLE_NOTIFICATION;
					dst.name = fName;
				} else {
					dstKey   = "action:" + target + ":f:" + fId;
					dst.type = target;
					dst.role = ROLE_NOTIFICATION;
					dst.name = fName;
				}
				dst.id = dstKey;
				itemMap.putIfAbsent(dstKey, dst);
				if (sId > 0 && ROLE_FORM.equals(dst.role)) recordSurveyKey(surveyItemKeys, sId, dstKey);

				linkWithOptionalDecision(data, itemMap, filter, fId, "f", srcKey, dstKey, enabled, projectName, null);
			}
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}

		// -------------------------------------------------------------------
		// Pass 1b: task_group table — two-sub-pass approach
		//   Sub-pass i:  Create all destination task nodes; record in surveyItemKeys.
		//   Sub-pass ii: Resolve source nodes using surveyItemKeys (so a task node
		//                created in sub-pass i is found as the source when a
		//                subsequent task_group chains off that same survey), then
		//                create links.
		// -------------------------------------------------------------------
		String sqlTg =
				"select tg.tg_id, tg.name, tg.source_s_id, tg.target_s_id, tg.rule, "
				+ "src.display_name as trigger_survey, "
				+ "src.data_survey as src_data_survey, "
				+ "src.hide_on_device as src_hide_on_device, "
				+ "tgt.display_name as target_survey, "
				+ "proj.name as project_name "
				+ "from task_group tg "
				+ "left outer join survey src on src.s_id = tg.source_s_id "
				+ "left outer join survey tgt on tgt.s_id = tg.target_s_id "
				+ "left outer join project proj on proj.id = tg.p_id "
				+ "where tg.p_id in (select p.id from project p, user_project up, users u "
				+ "  where p.id = up.p_id and up.u_id = u.id and u.ident = ?) "
				+ "order by tg.name asc";

		// Stored for sub-pass ii: {tgId, sourceSId, triggerSurvey, dstKey, tgFilterName, tgProjectName}
		List<Object[]> pendingTgLinks = new ArrayList<>();

		try {
			pstmt = sd.prepareStatement(sqlTg);
			pstmt.setString(1, user);
			log.fine("Workflow items from task_group: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				int    tgId           = rs.getInt("tg_id");
				String tgName         = rs.getString("name");
				int    sourceSId      = rs.getInt("source_s_id");
				int    targetSId      = rs.getInt("target_s_id");
				String triggerSurvey  = rs.getString("trigger_survey");
				boolean srcDataSurvey = rs.getBoolean("src_data_survey");
				boolean srcHideOnDev  = rs.getBoolean("src_hide_on_device");
				String targetSurvey   = rs.getString("target_survey");
				String rule           = rs.getString("rule");
				String tgProjectName  = rs.getString("project_name");

				String tgAssignee   = null;
				String tgAssigneeK  = "";
				String tgFilterName = null;
				if (rule != null && !rule.trim().isEmpty()) {
					AssignFromSurvey afs = new Gson().fromJson(rule, AssignFromSurvey.class);
					if (afs != null) {
						tgAssignee  = deriveAssignee(sd, afs);
						tgAssigneeK = assigneeKey(tgAssignee);
						if (afs.filter != null) {
							tgFilterName = afs.filter.advanced != null ? afs.filter.advanced : afs.filter.qText;
						}
					}
				}
				String dstKey = targetSId > 0
						? "task:s:" + targetSId + ":a:" + tgAssigneeK
						: "task:tg:" + tgId + ":a:" + tgAssigneeK;
				if (!itemMap.containsKey(dstKey)) {
					WorkflowItem dst = new WorkflowItem();
					dst.id       = dstKey;
					dst.type     = TYPE_TASK;
					dst.role     = ROLE_FORM;
					dst.name     = targetSurvey != null ? targetSurvey : tgName;
					dst.enabled  = true;
					dst.project  = tgProjectName;
					dst.assignee = tgAssignee;
					itemMap.put(dstKey, dst);
				}
				// Record the task node under its own (target) survey so that
				// a downstream task_group that sources this survey will find it.
				if (targetSId > 0) recordSurveyKey(surveyItemKeys, targetSId, dstKey);

				pendingTgLinks.add(new Object[]{tgId, sourceSId, triggerSurvey, dstKey, tgFilterName, tgProjectName, srcDataSurvey, srcHideOnDev});
			}
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}

		// Sub-pass ii: resolve source nodes and create links.
		// We look for nodes whose key directly encodes the source survey ID:
		//   "form:s:{sourceSId}"          — plain form submission trigger
		//   "task:s:{sourceSId}:a:*"      — task workitem for that survey (from sub-pass i)
		// This avoids accidentally picking up task nodes that were *triggered by*
		// the source survey (recorded under the same sId by the forward pass).
		for (Object[] tgr : pendingTgLinks) {
			int     tgId          = (int)     tgr[0];
			int     sourceSId     = (int)     tgr[1];
			String  triggerSurvey = (String)  tgr[2];
			String  dstKey        = (String)  tgr[3];
			String  tgFilterName  = (String)  tgr[4];
			String  tgProjectName = (String)  tgr[5];
			boolean srcDataSurvey = (boolean) tgr[6];
			boolean srcHideOnDev  = (boolean) tgr[7];

			List<String> srcKeyList = new ArrayList<>();
			String formKey = "form:s:" + sourceSId;

			// Task/case nodes whose key directly encodes this survey's sId
			// (created in sub-pass i for chained task_groups).
			String taskPrefix = "task:s:" + sourceSId + ":a:";
			for (String k : itemMap.keySet()) {
				if (k.startsWith(taskPrefix)) srcKeyList.add(k);
			}

			// If the source survey is a visible data-collection form
			// (data_survey=true AND hide_on_device=false) it should also
			// appear as an explicit Form workitem and trigger the new step.
			boolean isVisibleForm = srcDataSurvey && !srcHideOnDev;
			if (isVisibleForm) {
				if (!itemMap.containsKey(formKey)) {
					WorkflowItem src = new WorkflowItem();
					src.id      = formKey;
					src.type    = TYPE_FORM;
					src.role    = ROLE_FORM;
					src.name    = triggerSurvey;
					src.enabled = true;
					src.project = tgProjectName;
					itemMap.put(formKey, src);
					recordSurveyKey(surveyItemKeys, sourceSId, formKey);
				}
				if (!srcKeyList.contains(formKey)) srcKeyList.add(formKey);
			}

			if (srcKeyList.isEmpty()) {
				// Non-visible survey with no existing workitem — fallback form node.
				if (!itemMap.containsKey(formKey)) {
					WorkflowItem src = new WorkflowItem();
					src.id      = formKey;
					src.type    = TYPE_FORM;
					src.role    = ROLE_FORM;
					src.name    = triggerSurvey;
					src.enabled = true;
					src.project = tgProjectName;
					itemMap.put(formKey, src);
					recordSurveyKey(surveyItemKeys, sourceSId, formKey);
				}
				srcKeyList.add(formKey);
			}

			// Create the decision node (if filtered) without a src, then wire
			// each source key to it — mirrors the bundle-processing pattern.
			linkWithOptionalDecision(data, itemMap, tgFilterName, tgId, "tg", null, dstKey, true, tgProjectName, null);
			for (String srcKey : srcKeyList) {
				String linkTarget = (tgFilterName != null && !tgFilterName.trim().isEmpty())
						? "decision:tg:" + tgId : dstKey;
				addLinkIfAbsent(data, srcKey, linkTarget);
			}
		}

		// -------------------------------------------------------------------
		// Pass 2: bundle notifications
		// Sources = all existing nodes for each bundle member survey +
		//           a fresh form node for any member not yet on the canvas.
		// -------------------------------------------------------------------
		for (BundleNotif bn : pendingBundles) {
			List<String[]> members = bundleMembers.getOrDefault(bn.bundleIdent, new ArrayList<>());

			List<String> srcKeys = new ArrayList<>();
			for (String[] member : members) {
				int     memberSId    = Integer.parseInt(member[0]);
				String  memberName   = member[1];
				boolean isDataSurvey = "true".equals(member[2]);

				List<String> existing = surveyItemKeys.get(memberSId);
				if (existing != null && !existing.isEmpty()) {
					for (String key : existing) {
						if (!srcKeys.contains(key)) srcKeys.add(key);
					}
				} else if (isDataSurvey) {
					// Data survey with no existing nodes — add a bare form node
					String key = "form:s:" + memberSId;
					WorkflowItem src = new WorkflowItem();
					src.id      = key;
					src.type    = TYPE_FORM;
					src.role    = ROLE_FORM;
					src.name    = memberName;
					src.enabled = bn.enabled;
					itemMap.putIfAbsent(key, src);
					recordSurveyKey(surveyItemKeys, memberSId, key);
					srcKeys.add(key);
				}
				// Oversight-only surveys have no form node — they appear only as task/case
				// nodes created by other notifications, already captured via surveyItemKeys.
			}

			// Fallback: bundle with no accessible members
			if (srcKeys.isEmpty()) {
				String key = "form:bundle:" + bn.bundleIdent;
				WorkflowItem src = new WorkflowItem();
				src.id      = key;
				src.type    = TYPE_FORM;
				src.role    = ROLE_FORM;
				src.name    = bn.bundleIdent;
				src.enabled = bn.enabled;
				itemMap.putIfAbsent(key, src);
				srcKeys.add(key);
			}

			// Destination node
			String dstKey;
			WorkflowItem dst = new WorkflowItem();
			dst.enabled = bn.enabled;
			dst.project = bn.projectName;
			dst.bundle  = bn.bundleName;

			if ("task".equals(bn.target)) {
				String assignee  = mapAssignee(bn.remoteUser);
				String assigneeK = assigneeKey(bn.remoteUser);
				dstKey       = "task:f:" + bn.fId + ":a:" + assigneeK;
				dst.name     = bn.fName;
				dst.type     = TYPE_TASK;
				dst.role     = ROLE_FORM;
				dst.assignee = assignee;
			} else if ("escalate".equals(bn.target)) {
				String assignee  = mapAssignee(bn.remoteUser);
				String assigneeK = assigneeKey(bn.remoteUser);
				dstKey       = bn.caseSurveyIdent != null
						? "case:s:" + bn.caseSurveyIdent + ":a:" + assigneeK
						: "case:f:" + bn.fId + ":a:" + assigneeK;
				dst.name     = bn.caseSurvey != null ? bn.caseSurvey : bn.fName;
				dst.type     = TYPE_CASE;
				dst.role     = ROLE_FORM;
				dst.assignee = assignee;
			} else if ("email".equals(bn.target)) {
				dstKey   = "email:f:" + bn.fId;
				dst.type = TYPE_EMAIL;
				dst.role = ROLE_NOTIFICATION;
				dst.name = bn.fName;
			} else if ("sms".equals(bn.target)) {
				dstKey   = "sms:f:" + bn.fId;
				dst.type = TYPE_SMS;
				dst.role = ROLE_NOTIFICATION;
				dst.name = bn.fName;
			} else {
				dstKey   = "action:" + bn.target + ":f:" + bn.fId;
				dst.type = bn.target;
				dst.role = ROLE_NOTIFICATION;
				dst.name = bn.fName;
			}
			dst.id = dstKey;
			itemMap.putIfAbsent(dstKey, dst);

			linkWithOptionalDecision(data, itemMap, bn.filter, bn.fId, "f", null, dstKey, bn.enabled, bn.projectName, bn.bundleName);
			for (String srcKey : srcKeys) {
				String linkTarget = (bn.filter != null && !bn.filter.trim().isEmpty())
						? "decision:f:" + bn.fId : dstKey;
				addLinkIfAbsent(data, srcKey, linkTarget);
			}
		}

		// Post-process: stamp bundle name onto form nodes for bundle member surveys
		for (Map.Entry<String, WorkflowItem> entry : itemMap.entrySet()) {
			if (entry.getKey().startsWith("form:s:") && entry.getValue().bundle == null) {
				int sId = Integer.parseInt(entry.getKey().substring(7));
				String bName = surveyBundleNames.get(sId);
				if (bName != null) entry.getValue().bundle = bName;
			}
		}

		data.items.addAll(itemMap.values());
		applyLayout(data);
		mergeUserPositions(sd, user, data);
		return data;
	}

	/*
	 * Save the full positions map for the user's current organisation.
	 * Replaces any existing saved positions, which implicitly removes orphaned nodes.
	 * positions: map of node_id -> WorkflowItem (only x and y are used)
	 */
	public void savePositions(Connection sd, String user, Map<String, WorkflowItem> positions) throws Exception {
		int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
		String sql = "insert into workflow_node_positions(user_ident, o_id, positions) values(?, ?, ?::jsonb) "
				+ "on conflict(user_ident, o_id) do update set positions = excluded.positions";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, user);
			pstmt.setInt(2, oId);
			pstmt.setString(3, new Gson().toJson(positions));
			pstmt.executeUpdate();
		}
	}

	/*
	 * Delete saved positions for the user's current organisation, reverting to defaults.
	 */
	public void resetPositions(Connection sd, String user) throws Exception {
		int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
		String sql = "delete from workflow_node_positions where user_ident = ? and o_id = ?";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, user);
			pstmt.setInt(2, oId);
			pstmt.executeUpdate();
		}
	}

	/*
	 * Overwrite x/y on items that have a saved position for this user+org.
	 */
	private void mergeUserPositions(Connection sd, String user, WorkflowData data) throws Exception {
		int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
		String sql = "select positions from workflow_node_positions where user_ident = ? and o_id = ?";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, user);
			pstmt.setInt(2, oId);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				String json = rs.getString("positions");
				if (json != null) {
					Map<String, WorkflowItem> saved = new Gson().fromJson(json,
							new TypeToken<Map<String, WorkflowItem>>(){}.getType());
					for (WorkflowItem item : data.items) {
						WorkflowItem pos = saved.get(item.id);
						if (pos != null) {
							item.x = pos.x;
							item.y = pos.y;
						}
					}
				}
			}
		}
	}

	private static final int CARD_W    = 240;
	private static final int X_SPACING = CARD_W + 80;   // 320px — card width plus gap
	private static final int Y_SPACING = 150;
	private static final int Y_OFFSET  = 40;            // top margin below the menu bar

	/*
	 * Assign x/y pixel positions to each node.
	 *
	 * x column: propagated left-to-right through links until stable —
	 *   each node's column = max(predecessor columns) + 1.
	 * y row: within each column, nodes are stacked in their insertion order.
	 */
	private void applyLayout(WorkflowData data) {
		LinkedHashMap<String, WorkflowItem> byId = new LinkedHashMap<>();
		for (WorkflowItem item : data.items) {
			byId.put(item.id, item);
		}

		// A DAG with n nodes converges in at most n-1 passes.
		// If the graph contains a cycle the loop would never terminate, so cap
		// iterations at n to detect and break out of cyclic graphs safely.
		int maxPasses = data.items.size();
		boolean changed = true;
		for (int pass = 0; changed && pass < maxPasses; pass++) {
			changed = false;
			for (WorkflowLink link : data.links) {
				WorkflowItem from = byId.get(link.from);
				WorkflowItem to   = byId.get(link.to);
				if (from != null && to != null) {
					int candidate = from.x + 1;
					if (candidate > to.x) {
						to.x  = candidate;
						changed = true;
					}
				}
			}
		}
		if (changed) {
			log.warning("applyLayout: cycle detected in workflow graph — layout may be incorrect");
		}

		LinkedHashMap<Integer, List<WorkflowItem>> columns = new LinkedHashMap<>();
		for (WorkflowItem item : data.items) {
			columns.computeIfAbsent(item.x, k -> new ArrayList<>()).add(item);
		}

		for (List<WorkflowItem> col : columns.values()) {
			for (int row = 0; row < col.size(); row++) {
				WorkflowItem item = col.get(row);
				item.x = item.x * X_SPACING;
				item.y = Y_OFFSET + row * Y_SPACING;
			}
		}
	}

	/*
	 * Returns all bundle members accessible to the user, keyed by bundle ident.
	 * Each entry is [sId-as-string, display_name].
	 */
	// member[0] = s_id, member[1] = display_name, member[2] = "true" if data_survey,
	// member[3] = bundle display name (group survey's display_name)
	private Map<String, List<String[]>> getBundleMembers(Connection sd, String user) throws SQLException {
		Map<String, List<String[]>> result = new LinkedHashMap<>();
		String sql = "select s.group_survey_ident, s.s_id, s.display_name, s.data_survey, "
				+ "gs.display_name as bundle_display_name "
				+ "from survey s "
				+ "left join survey gs on gs.ident = s.group_survey_ident "
				+ "join project p on s.p_id = p.id "
				+ "join user_project up on p.id = up.p_id "
				+ "join users u on up.u_id = u.id "
				+ "where u.ident = ? "
				+ "  and s.group_survey_ident is not null "
				+ "  and not s.deleted "
				+ "order by s.group_survey_ident, s.display_name";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, user);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				String bundleId    = rs.getString("group_survey_ident");
				String sId         = String.valueOf(rs.getInt("s_id"));
				String name        = rs.getString("display_name");
				String dataSurvey  = String.valueOf(rs.getBoolean("data_survey"));
				String bundleName  = rs.getString("bundle_display_name");
				result.computeIfAbsent(bundleId, k -> new ArrayList<>()).add(new String[]{sId, name, dataSurvey, bundleName});
			}
		}
		return result;
	}

	/*
	 * Creates a decision node (if filter is non-empty) and links src → [dec →] dst.
	 * decSuffix distinguishes decision keys: "f" for forward, "tg" for task_group.
	 */
	/*
	 * Creates a decision node (if filter non-empty) and links src → [dec →] dst.
	 * When srcKey is null (bundle pass, multiple srcs handled by caller), only the
	 * decision node itself is created; the caller links each source to it.
	 */
	private void linkWithOptionalDecision(WorkflowData data, LinkedHashMap<String, WorkflowItem> itemMap,
			String filter, int id, String decSuffix, String srcKey, String dstKey,
			boolean enabled, String project, String bundle) {
		if (filter != null && !filter.trim().isEmpty()) {
			String decKey = "decision:" + decSuffix + ":" + id;
			WorkflowItem dec = new WorkflowItem();
			dec.id      = decKey;
			dec.type    = TYPE_DECISION;
			dec.role    = ROLE_DECISION;
			dec.name    = filter;
			dec.enabled = enabled;
			dec.project = project;
			dec.bundle  = bundle;
			itemMap.putIfAbsent(decKey, dec);
			if (srcKey != null) addLinkIfAbsent(data, srcKey, decKey);
			addLinkIfAbsent(data, decKey, dstKey);
		} else if (srcKey != null) {
			addLinkIfAbsent(data, srcKey, dstKey);
		}
	}

	private void recordSurveyKey(Map<Integer, List<String>> surveyItemKeys, int sId, String key) {
		surveyItemKeys.computeIfAbsent(sId, k -> new ArrayList<>()).add(key);
	}

	private void addLinkIfAbsent(WorkflowData data, String from, String to) {
		for (WorkflowLink l : data.links) {
			if (l.from.equals(from) && l.to.equals(to)) return;
		}
		WorkflowLink link = new WorkflowLink();
		link.from = from;
		link.to   = to;
		data.links.add(link);
	}

	/*
	 * Maps the raw remote_user value to a display string.
	 * "_submitter" → "Submitter", "_data" → "From Data", else the raw value.
	 */
	private String mapAssignee(String remoteUser) {
		if (remoteUser == null || remoteUser.trim().isEmpty()) return null;
		if ("_submitter".equals(remoteUser)) return "Submitter";
		if ("_data".equals(remoteUser))      return "From Data";
		return remoteUser;
	}

	/*
	 * Returns a normalised string safe for use as part of a node key.
	 * Null or blank input → empty string.
	 */
	private String assigneeKey(String assignee) {
		if (assignee == null || assignee.trim().isEmpty()) return "";
		return assignee.trim().toLowerCase().replaceAll("[^a-z0-9@._-]", "_");
	}

	/*
	 * Derives a display assignee string from an AssignFromSurvey rule.
	 * Priority: assign_data → emails → role_id (lookup) → user_id (lookup).
	 */
	private String deriveAssignee(Connection sd, AssignFromSurvey afs) {
		if (afs == null) return null;
		if (afs.assign_data != null && !afs.assign_data.trim().isEmpty()) return "From Data";
		if (afs.emails != null && !afs.emails.trim().isEmpty()) return afs.emails.trim();
		if (afs.role_id > 0) {
			String name = lookupRoleName(sd, afs.role_id);
			if (name != null) return name;
		}
		if (afs.user_id > 0) {
			String name = lookupUserName(sd, afs.user_id);
			if (name != null) return name;
		}
		return null;
	}

	private String lookupUserName(Connection sd, int userId) {
		String sql = "select ident from users where id = ?";
		try (PreparedStatement ps = sd.prepareStatement(sql)) {
			ps.setInt(1, userId);
			ResultSet rs = ps.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		} catch (SQLException e) {
			log.warning("lookupUserName failed for id " + userId + ": " + e.getMessage());
			return null;
		}
	}

	private String lookupRoleName(Connection sd, int roleId) {
		String sql = "select name from roles where id = ?";
		try (PreparedStatement ps = sd.prepareStatement(sql)) {
			ps.setInt(1, roleId);
			ResultSet rs = ps.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		} catch (SQLException e) {
			log.warning("lookupRoleName failed for id " + roleId + ": " + e.getMessage());
			return null;
		}
	}
}
