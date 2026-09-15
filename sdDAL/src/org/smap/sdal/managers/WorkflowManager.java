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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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
	private static final String TYPE_EMAILTASK = "emailtask";
	private static final String TYPE_CASE      = "case";
	private static final String TYPE_REFERENCE = "reference";
	private static final String TYPE_PERIODIC  = "periodic";
	private static final String TYPE_REMINDER  = "reminder";
	private static final String TYPE_EMAIL           = "email";
	private static final String TYPE_SMS             = "sms";
	private static final String TYPE_SHAREPOINT_LIST = "sharepoint_list";
	private static final String TYPE_DECISION        = "decision";

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
		String  wfPrevNodeId;
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
		PreparedStatement pstmt = null;

		// -------------------------------------------------------------------
		// Pass 0: explicit form start nodes from workflow_start table
		// -------------------------------------------------------------------
		String sqlStart =
				"select ws.id, ws.s_ident, s.s_id, s.display_name, proj.name as project_name "
				+ "from workflow_start ws "
				+ "join survey s on s.ident = ws.s_ident "
				+ "join project proj on proj.id = ws.p_id "
				+ "join user_project up on up.p_id = ws.p_id "
				+ "join users u on u.id = up.u_id "
				+ "where u.ident = ? and not s.deleted and proj.o_id = u.o_id "
				+ "order by s.display_name";
		try {
			pstmt = sd.prepareStatement(sqlStart);
			pstmt.setString(1, user);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				int    startId     = rs.getInt("id");
				int    sId         = rs.getInt("s_id");
				String displayName = rs.getString("display_name");
				String projectName = rs.getString("project_name");
				String key = "form:s:" + sId;
				if (!itemMap.containsKey(key)) {
					WorkflowItem src = new WorkflowItem();
					src.id      = key;
					src.type    = TYPE_FORM;
					src.role    = ROLE_FORM;
					src.name    = displayName;
					src.enabled = true;
					src.project = projectName;
					itemMap.put(key, src);
				}
				itemMap.get(key).startIds.add(startId);
				recordSurveyKey(surveyItemKeys, sId, key);
			}
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}

		// -------------------------------------------------------------------
		// Pass 1a: forward table — non-bundle records
		// -------------------------------------------------------------------
		String sqlForward =
				"select f.id, f.name, f.trigger, f.target, f.enabled, f.bundle, "
				+ "f.s_id, f.bundle_ident, f.p_id, f.filter, f.remote_user, f.wf_prev_node_id, "
				+ "s_src.display_name as trigger_survey, "
				+ "s_bun.display_name as bundle_name, "
				+ "s_case.display_name as case_survey, "
				+ "s_case.ident as case_survey_ident, "
				+ "s_case.s_id as case_survey_id, "
				+ "rep.name as report_name, "
				+ "(f.notify_details::json->>'report_type') as report_type, "
				+ "proj.name as project_name "
				+ "from forward f "
				+ "left outer join survey s_src on s_src.s_id = f.s_id "
				+ "left outer join survey s_bun on s_bun.ident = f.bundle_ident "
				+ "left outer join report rep on rep.id = f.r_id "
				+ "left outer join survey s_case on (f.target = 'escalate' or f.target = 'reference') "
				+ "  and f.notify_details is not null "
				+ "  and s_case.ident = (f.notify_details::json->>'survey_case') "
				+ "left outer join project proj on proj.id = f.p_id "
				+ "where (f.p_id in (select p.id from project p, user_project up, users u "
				+ "  where p.id = up.p_id and up.u_id = u.id and u.ident = ? and p.o_id = u.o_id) "
				+ "or f.s_id in (select s2.s_id from survey s2, project p, user_project up, users u "
				+ "  where s2.p_id = p.id and p.id = up.p_id and up.u_id = u.id and u.ident = ? and not s2.deleted and p.o_id = u.o_id) "
				+ "or f.bundle_ident in (select s2.group_survey_ident from survey s2, project p, user_project up, users u "
				+ "  where s2.p_id = p.id and p.id = up.p_id and up.u_id = u.id and u.ident = ? and not s2.deleted and p.o_id = u.o_id)) "
				+ "order by f.name asc";

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
				int     caseSurveyId     = rs.getInt("case_survey_id");
				String  filter           = rs.getString("filter");
				String  projectName      = rs.getString("project_name");
				String  remoteUser       = rs.getString("remote_user");
				String  wfPrevNodeId     = rs.getString("wf_prev_node_id");
				String  reportName       = rs.getString("report_name");
				String  reportType       = rs.getString("report_type");

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
					bn.wfPrevNodeId    = wfPrevNodeId;
					pendingBundles.add(bn);
					continue;
				}

				// -- Source node --
				String srcKey;
				if (wfPrevNodeId != null && !wfPrevNodeId.trim().isEmpty()) {
					// Explicit predecessor set by workflow canvas — use it directly.
					// The predecessor node will already be (or will become) present in
					// itemMap from its own record; no need to create it here.
					srcKey = wfPrevNodeId;
				} else if ("submission".equals(trigger) && sId > 0) {
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
					// Body shows the report; header shows the user-entered label (forward.name)
					src.name    = "ops_summary".equals(reportType) ? "Operations Summary"
							: (reportName != null ? reportName : fName);
					src.label   = fName;
					src.enabled = enabled;
					src.project = projectName;
					// Periodic trigger is editable (schedule/label/report) so it carries
					// the backing forward id; the email node it points to owns recipients.
					src.fwdIds.add(fId);
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
					String assignee   = mapAssignee(sd, remoteUser);
					String assigneeK  = assigneeKey(remoteUser);
					boolean wfCreated = wfPrevNodeId != null && !wfPrevNodeId.trim().isEmpty();
					if (wfCreated) {
						// Created from workflow page — unique per forward record
						dstKey = "case:f:" + fId + ":a:" + assigneeK;
					} else {
						dstKey = caseSurveyIdent != null
								? "case:s:" + caseSurveyIdent + ":a:" + assigneeK
								: "case:f:" + fId + ":a:" + assigneeK;
					}
					dst.name         = caseSurvey != null ? caseSurvey : fName;
					dst.type         = TYPE_CASE;
					dst.role         = ROLE_FORM;
					dst.assignee     = assignee;
					dst.caseSurveyId = caseSurveyId;
				} else if ("reference".equals(target)) {
					String assignee   = mapAssignee(sd, remoteUser);
					String assigneeK  = assigneeKey(remoteUser);
					boolean wfCreated = wfPrevNodeId != null && !wfPrevNodeId.trim().isEmpty();
					if (wfCreated) {
						// Created from workflow page — unique per forward record
						dstKey = "reference:f:" + fId + ":a:" + assigneeK;
					} else {
						dstKey = caseSurveyIdent != null
								? "reference:s:" + caseSurveyIdent + ":a:" + assigneeK
								: "reference:f:" + fId + ":a:" + assigneeK;
					}
					dst.name         = caseSurvey != null ? caseSurvey : fName;
					dst.type         = TYPE_REFERENCE;
					dst.role         = ROLE_FORM;
					dst.assignee     = assignee;
					dst.caseSurveyId = caseSurveyId;
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
				} else if ("sharepoint_list".equals(target)) {
					dstKey   = "sharepoint_list:f:" + fId;
					dst.type = TYPE_SHAREPOINT_LIST;
					dst.role = ROLE_NOTIFICATION;
					dst.name = fName;
				} else {
					dstKey   = "action:" + target + ":f:" + fId;
					dst.type = target;
					dst.role = ROLE_NOTIFICATION;
					dst.name = fName;
				}
				dst.id    = dstKey;
				dst.label = fName;
				itemMap.putIfAbsent(dstKey, dst);
				itemMap.get(dstKey).fwdIds.add(fId);
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
				"select tg.tg_id, tg.name, tg.source_s_id, tg.target_s_id, tg.rule, tg.wf_prev_node_id, "
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
				+ "  where p.id = up.p_id and up.u_id = u.id and u.ident = ? and p.o_id = u.o_id) "
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
				String tgWfPrevNodeId = rs.getString("wf_prev_node_id");

				String tgAssignee   = null;
				String tgAssigneeK  = "";
				String tgFilterName = null;
				boolean tgIsEmail   = false;
				/*
				 * A task group with no rule at all is ad-hoc: nothing was ever configured to
				 * generate tasks into it, so every task it holds was made by hand.  It is skipped
				 * for the same reason as the rule-with-neither-flag case below - the page shows the
				 * rules that create tasks, not the tasks themselves.
				 *
				 * Without this it fell through the guard beneath, which only runs when there IS a
				 * rule to inspect, and appeared on the page dragging a "form:s:0" node with it: a
				 * box standing for survey zero, which is to say for nothing.
				 */
				if (rule == null || rule.trim().isEmpty()) {
					continue;
				}
				if (rule != null && !rule.trim().isEmpty()) {
					AssignFromSurvey afs = new Gson().fromJson(rule, AssignFromSurvey.class);
					if (afs != null) {
						// Skip ad-hoc task groups. These are created with "add from survey"
						// unchecked so tasks are only created manually - never generated
						// from a submission. The client guarantees add_current or add_future
						// is set whenever "add from survey" is checked, so a rule with
						// neither flag is ad-hoc and must not appear on the workflow page.
						// (This mirrors the add_future gate in TaskManager.updateTasksForSubmission
						//  that decides whether a submission generates new tasks.)
						if (!afs.add_current && !afs.add_future) {
							continue;
						}
						tgAssignee  = deriveAssignee(sd, afs);
						tgAssigneeK = assigneeKey(tgAssignee);
						tgIsEmail   = afs.emails != null && !afs.emails.trim().isEmpty();
						if (afs.filter != null) {
							tgFilterName = afs.filter.advanced != null ? afs.filter.advanced : afs.filter.qText;
						}
					}
				}
				boolean tgWfCreated = tgWfPrevNodeId != null && !tgWfPrevNodeId.trim().isEmpty();
				String dstKey = tgWfCreated
						? "task:tg:" + tgId + ":a:" + tgAssigneeK       // workflow-page record — always unique
						: (targetSId > 0
								? "task:s:" + targetSId + ":a:" + tgAssigneeK  // legacy — deduplicate by survey+assignee
								: "task:tg:" + tgId + ":a:" + tgAssigneeK);
				if (!itemMap.containsKey(dstKey)) {
					WorkflowItem dst = new WorkflowItem();
					dst.id       = dstKey;
					dst.type     = tgIsEmail ? TYPE_EMAILTASK : TYPE_TASK;
					dst.role     = ROLE_FORM;
					dst.name     = targetSurvey != null ? targetSurvey : tgName;
					dst.label    = tgName;
					dst.enabled  = true;
					dst.project  = tgProjectName;
					dst.assignee       = tgAssignee;
					dst.targetSurveyId = targetSId;
					itemMap.put(dstKey, dst);
				}
				// Record the task node under its own (target) survey so that
				// a downstream task_group that sources this survey will find it.
				itemMap.get(dstKey).tgIds.add(tgId);
				if (targetSId > 0) recordSurveyKey(surveyItemKeys, targetSId, dstKey);

				pendingTgLinks.add(new Object[]{tgId, sourceSId, triggerSurvey, dstKey, tgFilterName, tgProjectName, srcDataSurvey, srcHideOnDev, tgWfPrevNodeId});
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
			String  wfPrevNodeId  = tgr.length > 8 ? (String) tgr[8] : null;

			List<String> srcKeyList = new ArrayList<>();
			String formKey = "form:s:" + sourceSId;

			// Explicit predecessor set by workflow canvas — skip inference.
			if (wfPrevNodeId != null && !wfPrevNodeId.trim().isEmpty()) {
				srcKeyList.add(wfPrevNodeId);
				linkWithOptionalDecision(data, itemMap, tgFilterName, tgId, "tg", null, dstKey, true, tgProjectName, null);
				for (String srcKey : srcKeyList) {
					String linkTarget = (tgFilterName != null && !tgFilterName.trim().isEmpty())
							? "decision:tg:" + tgId : dstKey;
					addLinkIfAbsent(data, srcKey, linkTarget);
				}
				continue;
			}

			// Task/case nodes whose key directly encodes this survey's sId
			// (created in sub-pass i for chained task_groups).
			// Skip when target == source survey: the single-fire rule prevents
			// re-triggering for the same record, so including these nodes would
			// create a misleading visual cycle.
			String taskPrefix = "task:s:" + sourceSId + ":a:";
			if (!dstKey.startsWith(taskPrefix)) {
				for (String k : itemMap.keySet()) {
					if (k.startsWith(taskPrefix)) srcKeyList.add(k);
				}
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
				String assignee  = mapAssignee(sd, bn.remoteUser);
				String assigneeK = assigneeKey(bn.remoteUser);
				boolean bnWfCreated = bn.wfPrevNodeId != null && !bn.wfPrevNodeId.trim().isEmpty();
				dstKey = bnWfCreated
						? "case:f:" + bn.fId + ":a:" + assigneeK
						: (bn.caseSurveyIdent != null
								? "case:s:" + bn.caseSurveyIdent + ":a:" + assigneeK
								: "case:f:" + bn.fId + ":a:" + assigneeK);
				dst.name     = bn.caseSurvey != null ? bn.caseSurvey : bn.fName;
				dst.type     = TYPE_CASE;
				dst.role     = ROLE_FORM;
				dst.assignee = assignee;
			} else if ("reference".equals(bn.target)) {
				String assignee  = mapAssignee(sd, bn.remoteUser);
				String assigneeK = assigneeKey(bn.remoteUser);
				boolean bnWfCreated = bn.wfPrevNodeId != null && !bn.wfPrevNodeId.trim().isEmpty();
				dstKey = bnWfCreated
						? "reference:f:" + bn.fId + ":a:" + assigneeK
						: (bn.caseSurveyIdent != null
								? "reference:s:" + bn.caseSurveyIdent + ":a:" + assigneeK
								: "reference:f:" + bn.fId + ":a:" + assigneeK);
				dst.name     = bn.caseSurvey != null ? bn.caseSurvey : bn.fName;
				dst.type     = TYPE_REFERENCE;
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
			} else if ("sharepoint_list".equals(bn.target)) {
				dstKey   = "sharepoint_list:f:" + bn.fId;
				dst.type = TYPE_SHAREPOINT_LIST;
				dst.role = ROLE_NOTIFICATION;
				dst.name = bn.fName;
			} else {
				dstKey   = "action:" + bn.target + ":f:" + bn.fId;
				dst.type = bn.target;
				dst.role = ROLE_NOTIFICATION;
				dst.name = bn.fName;
			}
			dst.id    = dstKey;
			dst.label = bn.fName;
			itemMap.putIfAbsent(dstKey, dst);
			itemMap.get(dstKey).fwdIds.add(bn.fId);

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

		linkCasesToTheirForms(sd, data, itemMap, surveyBundleNames);

		data.items.addAll(itemMap.values());
		applyLayout(data);
		mergeUserPositions(sd, user, data);
		return data;
	}

	/*
	 * Save the positions the user has chosen, merged into the ones already saved.
	 *
	 * Merged rather than replaced because the page now sends only the nodes somebody actually
	 * placed.  It used to send every node it was holding, defaults included, so one drag wrote the
	 * whole computed layout back as though all of it had been chosen - and a node that had lost its
	 * saved position, which is what happens when the rule behind it is replaced, was pinned to
	 * wherever the default layout had just dropped it.
	 *
	 * Nothing is pruned here.  A saved position whose node is gone is what mergeUserPositions reads
	 * to find the node again after its id changes, and it can only ever match the record it came
	 * from, because forward and task group ids are not reused.  Reset clears the lot.
	 *
	 * positions: map of node_id -> WorkflowItem (x, y and the backing record ids are used)
	 */
	public void savePositions(Connection sd, String user, Map<String, WorkflowItem> positions) throws Exception {
		int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
		if(positions == null) {
			positions = new HashMap<>();
		}

		Map<String, WorkflowItem> merged = new HashMap<>();
		String sqlGet = "select positions from workflow_node_positions where user_ident = ? and o_id = ?";
		try (PreparedStatement pstmt = sd.prepareStatement(sqlGet)) {
			pstmt.setString(1, user);
			pstmt.setInt(2, oId);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				String json = rs.getString("positions");
				if (json != null) {
					Map<String, WorkflowItem> existing = new Gson().fromJson(json,
							new TypeToken<Map<String, WorkflowItem>>(){}.getType());
					if (existing != null) {
						merged.putAll(existing);
					}
				}
			}
		}
		merged.putAll(positions);

		String sql = "insert into workflow_node_positions(user_ident, o_id, positions) values(?, ?, ?::jsonb) "
				+ "on conflict(user_ident, o_id) do update set positions = excluded.positions";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, user);
			pstmt.setInt(2, oId);
			pstmt.setString(3, new Gson().toJson(merged));
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
	 *
	 * A node id contains data values, such as the assignee or the case survey, so editing a step
	 * changes its id.  The saved position is then matched on the forward / task group records
	 * backing the node, which do not change, so that editing a step does not move it.
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

					/*
					 * Index on the backing records those saved positions whose node no longer
					 * exists.  A saved position that still has its node is left out, otherwise a
					 * node could be matched to the position of another node that shares a record,
					 * such as a periodic trigger and the email step it points to
					 */
					Set<String> liveIds = new HashSet<>();
					for (WorkflowItem item : data.items) {
						liveIds.add(item.id);
					}
					Map<String, WorkflowItem> byRecord = new HashMap<>();
					for (Map.Entry<String, WorkflowItem> entry : saved.entrySet()) {
						if (!liveIds.contains(entry.getKey())) {
							addRecordKeys(byRecord, entry.getValue());
						}
					}

					for (WorkflowItem item : data.items) {
						WorkflowItem pos = saved.get(item.id);
						if (pos == null) {
							pos = getPositionForRecord(byRecord, item);
						}
						if (pos != null) {
							item.x = pos.x;
							item.y = pos.y;
							item.pinned = true;
						}
					}
				}
			}
		}
	}

	/*
	 * Index a saved position on each record that backs it.  The first position saved for a record
	 * is kept, a record can only be in one place
	 */
	private void addRecordKeys(Map<String, WorkflowItem> byRecord, WorkflowItem pos) {
		if (pos == null) {
			return;
		}
		if (pos.fwdIds != null) {
			for (Integer fwdId : pos.fwdIds) {
				byRecord.putIfAbsent("f:" + fwdId, pos);
			}
		}
		if (pos.tgIds != null) {
			for (Integer tgId : pos.tgIds) {
				byRecord.putIfAbsent("t:" + tgId, pos);
			}
		}
	}

	/*
	 * Get the saved position of a node that shares a backing record with this item
	 */
	private WorkflowItem getPositionForRecord(Map<String, WorkflowItem> byRecord, WorkflowItem item) {
		if (item.fwdIds != null) {
			for (Integer fwdId : item.fwdIds) {
				WorkflowItem pos = byRecord.get("f:" + fwdId);
				if (pos != null) {
					return pos;
				}
			}
		}
		if (item.tgIds != null) {
			for (Integer tgId : item.tgIds) {
				WorkflowItem pos = byRecord.get("t:" + tgId);
				if (pos != null) {
					return pos;
				}
			}
		}
		return null;
	}

	private static final int CARD_W    = 240;
	private static final int X_SPACING = CARD_W + 80;   // 320px — card width plus gap
	private static final int Y_SPACING = 150;
	private static final int Y_OFFSET  = 40;            // top margin below the menu bar
	private static final int BAND_GAP  = 1;             // blank rows between one bundle and the next
	/*
	 * Joins a band name to a column number to make one map key.  A character no bundle name can
	 * contain, so that two different band-and-column pairs cannot produce the same key.
	 */
	private static final String KEY_SEP = "\u0000";

	private static String slot(String band, int column) {
		return band + KEY_SEP + column;
	}

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

		/*
		 * Which bundle each node belongs to, so that one process reads as one horizontal band.
		 *
		 * Only form nodes are given a bundle when they are built - a case, a decision or an email
		 * has no survey of its own to take one from - so stacking on the field alone would put a
		 * form in its band and every step of its process in with the unbundled ones.  The band is
		 * therefore taken from the links: a step belongs to the process that leads to it.
		 */
		Map<String, String> band = bands(data, byId);

		/*
		 * Bands in name order, the ones belonging to no bundle last.  Alphabetical rather than
		 * insertion order because this runs when somebody asks to reset the layout, and a reset
		 * that returned a different arrangement each time would be no easier to read than what it
		 * replaced.
		 */
		TreeSet<String> named = new TreeSet<>();
		boolean anyUnbundled = false;
		for (WorkflowItem item : data.items) {
			String b = band.get(item.id);
			if (b == null || b.isEmpty()) {
				anyUnbundled = true;
			} else {
				named.add(b);
			}
		}
		List<String> order = new ArrayList<>(named);
		if (anyUnbundled) {
			order.add("");
		}

		/*
		 * How many rows each band needs: the most nodes it ever has in a single column.  Counted
		 * before anything is placed so that the bands below start clear of the one above however
		 * wide this one gets.
		 */
		Map<String, Integer> heights = new LinkedHashMap<>();
		for (WorkflowItem item : data.items) {
			String b = band.get(item.id);
			b = b == null ? "" : b;
			heights.merge(slot(b, item.x), 1, Integer::sum);
		}
		Map<String, Integer> bandRows = new LinkedHashMap<>();
		for (Map.Entry<String, Integer> e : heights.entrySet()) {
			String b = e.getKey().substring(0, e.getKey().indexOf(KEY_SEP));
			bandRows.merge(b, e.getValue(), Math::max);
		}

		Map<String, Integer> bandStart = new LinkedHashMap<>();
		int row = 0;
		for (String b : order) {
			bandStart.put(b, row);
			row += bandRows.getOrDefault(b, 1) + BAND_GAP;
		}

		/*
		 * Place each node: its column decides x, and its position within its band's rows decides y.
		 * Within a band and column the nodes keep the order they were built in, which follows the
		 * order the rules were made.
		 */
		Map<String, Integer> nextRow = new LinkedHashMap<>();
		for (WorkflowItem item : data.items) {
			String b = band.get(item.id);
			b = b == null ? "" : b;
			int within = nextRow.merge(slot(b, item.x), 1, Integer::sum) - 1;
			int r = bandStart.getOrDefault(b, 0) + within;
			item.x = item.x * X_SPACING;
			item.y = Y_OFFSET + r * Y_SPACING;
			/* Reported so the page can colour a whole process, not only the forms in it */
			item.band = b;
		}
	}

	/*
	 * The bundle each node belongs to.
	 *
	 * Seeded from the nodes that carry one and spread along the links, forwards first - a form's
	 * bundle reaching the decisions and cases that follow from submitting it - and then backwards,
	 * so a trigger with no bundle of its own joins the process it feeds.  Repeated until nothing
	 * more changes, capped at the number of nodes because a cyclic graph would otherwise not stop.
	 *
	 * A node reachable from two bundles keeps the first one to reach it.  The passes run over the
	 * links in the order they were built, so the same graph always bands the same way.
	 */
	private Map<String, String> bands(WorkflowData data, LinkedHashMap<String, WorkflowItem> byId) {

		Map<String, String> band = new LinkedHashMap<>();
		for (WorkflowItem item : data.items) {
			if (item.bundle != null && !item.bundle.trim().isEmpty()) {
				band.put(item.id, item.bundle.trim());
			}
		}

		int maxPasses = data.items.size();
		boolean changed = true;
		for (int pass = 0; changed && pass < maxPasses; pass++) {
			changed = false;
			for (WorkflowLink link : data.links) {
				String from = band.get(link.from);
				if (from != null && byId.containsKey(link.to) && !band.containsKey(link.to)) {
					band.put(link.to, from);
					changed = true;
				}
			}
		}
		changed = true;
		for (int pass = 0; changed && pass < maxPasses; pass++) {
			changed = false;
			for (WorkflowLink link : data.links) {
				String to = band.get(link.to);
				if (to != null && byId.containsKey(link.from) && !band.containsKey(link.from)) {
					band.put(link.from, to);
					changed = true;
				}
			}
		}
		return band;
	}


	/*
	 * Join each case step to the form it sends somebody to.
	 *
	 * A case step and the form its case points at were built as unconnected nodes: the step came
	 * from the rule that creates it, and the form came from being the trigger of some other rule.
	 * Nothing linked them, so every form in a bundle after the first appeared to start a workflow
	 * of its own - the Bail process drew as three separate beginnings rather than one.
	 *
	 * The two are a single step of a process.  A case says go and fill this form in, and submitting
	 * it is what fires whatever comes next, so the edge is real and this draws it.
	 *
	 * A form node is created for a case survey that has none.  Without it the last stage of a
	 * process - the one whose submission triggers nothing further, because it is the end - would be
	 * the one stage missing from the picture.
	 *
	 * Only case steps.  A reference gives somebody read only sight of a record, not a form to fill
	 * in, so linking it would draw a step nobody is being asked to perform.
	 */
	private void linkCasesToTheirForms(Connection sd, WorkflowData data,
			LinkedHashMap<String, WorkflowItem> itemMap, Map<Integer, String> surveyBundleNames)
					throws SQLException {

		/* The case surveys that have no form node yet, looked up in one query rather than per node */
		Set<Integer> wanted = new HashSet<>();
		for (WorkflowItem item : itemMap.values()) {
			if (TYPE_CASE.equals(item.type) && item.caseSurveyId > 0
					&& !itemMap.containsKey("form:s:" + item.caseSurveyId)) {
				wanted.add(item.caseSurveyId);
			}
		}
		Map<Integer, String[]> missing = new HashMap<>();	// s_id -> [display_name, project_name]
		if (!wanted.isEmpty()) {
			StringBuilder in = new StringBuilder();
			for (int i = 0; i < wanted.size(); i++) {
				in.append(i == 0 ? "?" : ",?");
			}
			String sql = "select s.s_id, s.display_name, p.name as project_name "
					+ "from survey s join project p on p.id = s.p_id "
					+ "where s.s_id in (" + in + ") and not s.deleted";
			try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
				int idx = 1;
				for (Integer id : wanted) {
					pstmt.setInt(idx++, id);
				}
				ResultSet rs = pstmt.executeQuery();
				while (rs.next()) {
					missing.put(rs.getInt("s_id"), new String[] {
							rs.getString("display_name"), rs.getString("project_name") });
				}
			}
		}

		for (WorkflowItem item : new ArrayList<>(itemMap.values())) {
			if (!TYPE_CASE.equals(item.type) || item.caseSurveyId <= 0) {
				continue;
			}
			String formKey = "form:s:" + item.caseSurveyId;
			if (!itemMap.containsKey(formKey)) {
				String[] survey = missing.get(item.caseSurveyId);
				if (survey == null) {
					/*
					 * Deleted, or outside what this user can reach.  Left undrawn rather than drawn
					 * as a step, because a case pointing at a survey that is not there is a fault to
					 * be found, not a stage of the process.
					 */
					continue;
				}
				WorkflowItem form = new WorkflowItem();
				form.id      = formKey;
				form.type    = TYPE_FORM;
				form.role    = ROLE_FORM;
				form.name    = survey[0];
				form.project = survey[1];
				form.enabled = true;
				form.bundle  = surveyBundleNames.get(item.caseSurveyId);
				itemMap.put(formKey, form);
			}
			/*
			 * A process that keeps somebody on the same form - a status question moving a record
			 * through its stages - would otherwise be drawn as a loop back into its own trigger, and
			 * the column layout walks forward through the links until it stops changing.  So the
			 * edge is added only where it does not close a circle.
			 */
			if (!reaches(data, formKey, item.id)) {
				addLinkIfAbsent(data, item.id, formKey);
			}
		}
	}

	/* Whether to is reachable from from by following links */
	private boolean reaches(WorkflowData data, String from, String to) {
		if (from.equals(to)) {
			return true;
		}
		Set<String> seen = new HashSet<>();
		Deque<String> queue = new ArrayDeque<>();
		queue.add(from);
		seen.add(from);
		while (!queue.isEmpty()) {
			String at = queue.poll();
			for (WorkflowLink link : data.links) {
				if (link.from.equals(at) && seen.add(link.to)) {
					if (link.to.equals(to)) {
						return true;
					}
					queue.add(link.to);
				}
			}
		}
		return false;
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
				+ "  and p.o_id = u.o_id "
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

	private String mapAssignee(Connection sd, String remoteUser) {
		if (remoteUser != null && remoteUser.startsWith("_role:")) {
			try {
				int roleId = Integer.parseInt(remoteUser.substring(6));
				String name = lookupRoleName(sd, roleId);
				if (name != null) return name;
			} catch (NumberFormatException ignored) {}
		}
		return mapAssignee(remoteUser);
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
		String sql = "select name from role where id = ?";
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
