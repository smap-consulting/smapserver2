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
		String  target;
		boolean enabled;
		String  filter;
		String  fName;
		String  caseSurvey;
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
		List<BundleNotif> pendingBundles = new ArrayList<>();

		// -------------------------------------------------------------------
		// Pass 1a: forward table — non-bundle records
		// -------------------------------------------------------------------
		String sqlForward =
				"select f.id, f.name, f.trigger, f.target, f.enabled, f.bundle, "
				+ "f.s_id, f.bundle_ident, f.p_id, f.filter, "
				+ "s_src.display_name as trigger_survey, "
				+ "s_bun.display_name as bundle_name, "
				+ "s_case.display_name as case_survey "
				+ "from forward f "
				+ "left outer join survey s_src on s_src.s_id = f.s_id "
				+ "left outer join survey s_bun on s_bun.ident = f.bundle_ident "
				+ "left outer join survey s_case on f.target = 'escalate' "
				+ "  and f.notify_details is not null "
				+ "  and s_case.ident = (f.notify_details::json->>'survey_case') "
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
				int     fId           = rs.getInt("id");
				String  fName         = rs.getString("name");
				String  trigger       = rs.getString("trigger");
				String  target        = rs.getString("target");
				boolean enabled       = rs.getBoolean("enabled");
				boolean isBundle      = rs.getBoolean("bundle");
				int     sId           = rs.getInt("s_id");
				String  bundleIdent   = rs.getString("bundle_ident");
				String  triggerSurvey = rs.getString("trigger_survey");
				String  caseSurvey    = rs.getString("case_survey");
				String  filter        = rs.getString("filter");

				// Defer bundle notifications until all survey nodes are built
				if (isBundle) {
					BundleNotif bn = new BundleNotif();
					bn.fId         = fId;
					bn.bundleIdent = bundleIdent;
					bn.target      = target;
					bn.enabled     = enabled;
					bn.filter      = filter;
					bn.fName       = fName;
					bn.caseSurvey  = caseSurvey;
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
					itemMap.putIfAbsent(srcKey, src);
				} else if ("reminder".equals(trigger)) {
					srcKey = "reminder:f:" + fId;
					WorkflowItem src = new WorkflowItem();
					src.id      = srcKey;
					src.type    = TYPE_REMINDER;
					src.role    = ROLE_TRIGGER;
					src.name    = fName;
					src.enabled = enabled;
					itemMap.putIfAbsent(srcKey, src);
				} else {
					srcKey = "trigger:" + trigger + ":f:" + fId;
					WorkflowItem src = new WorkflowItem();
					src.id      = srcKey;
					src.type    = trigger;
					src.role    = ROLE_TRIGGER;
					src.name    = fName;
					src.enabled = enabled;
					itemMap.putIfAbsent(srcKey, src);
				}

				// -- Destination node --
				String dstKey;
				WorkflowItem dst = new WorkflowItem();
				dst.enabled = enabled;

				if ("task".equals(target)) {
					dstKey   = "task:f:" + fId;
					dst.name = fName;
					dst.type = TYPE_TASK;
					dst.role = ROLE_FORM;
				} else if ("escalate".equals(target)) {
					dstKey   = "case:f:" + fId;
					dst.name = caseSurvey != null ? caseSurvey : fName;
					dst.type = TYPE_CASE;
					dst.role = ROLE_FORM;
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

				linkWithOptionalDecision(data, itemMap, filter, fId, "f", srcKey, dstKey, enabled);
			}
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}

		// -------------------------------------------------------------------
		// Pass 1b: task_group table — source_s_id (form) → tg_id (task)
		// -------------------------------------------------------------------
		String sqlTg =
				"select tg.tg_id, tg.name, tg.source_s_id, tg.target_s_id, tg.rule, "
				+ "src.display_name as trigger_survey, "
				+ "tgt.display_name as target_survey "
				+ "from task_group tg "
				+ "left outer join survey src on src.s_id = tg.source_s_id "
				+ "left outer join survey tgt on tgt.s_id = tg.target_s_id "
				+ "where tg.p_id in (select p.id from project p, user_project up, users u "
				+ "  where p.id = up.p_id and up.u_id = u.id and u.ident = ?) "
				+ "order by tg.name asc";

		try {
			pstmt = sd.prepareStatement(sqlTg);
			pstmt.setString(1, user);
			log.fine("Workflow items from task_group: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				int    tgId          = rs.getInt("tg_id");
				String tgName        = rs.getString("name");
				int    sourceSId     = rs.getInt("source_s_id");
				int    targetSId     = rs.getInt("target_s_id");
				String triggerSurvey = rs.getString("trigger_survey");
				String targetSurvey  = rs.getString("target_survey");
				String rule          = rs.getString("rule");

				String srcKey = "form:s:" + sourceSId;
				if (!itemMap.containsKey(srcKey)) {
					WorkflowItem src = new WorkflowItem();
					src.id      = srcKey;
					src.type    = TYPE_FORM;
					src.role    = ROLE_FORM;
					src.name    = triggerSurvey != null ? triggerSurvey : tgName;
					src.enabled = true;
					itemMap.put(srcKey, src);
				}
				recordSurveyKey(surveyItemKeys, sourceSId, srcKey);

				String dstKey = "task:tg:" + tgId;
				if (!itemMap.containsKey(dstKey)) {
					WorkflowItem dst = new WorkflowItem();
					dst.id      = dstKey;
					dst.type    = TYPE_TASK;
					dst.role    = ROLE_FORM;
					dst.name    = targetSurvey != null ? targetSurvey : tgName;
					dst.enabled = true;
					itemMap.put(dstKey, dst);
				}
				// Record the task node under its own survey (target), not the triggering survey
				if (targetSId > 0) recordSurveyKey(surveyItemKeys, targetSId, dstKey);

				String tgFilterName = null;
				if (rule != null && !rule.trim().isEmpty()) {
					AssignFromSurvey afs = new Gson().fromJson(rule, AssignFromSurvey.class);
					if (afs != null && afs.filter != null) {
						tgFilterName = afs.filter.advanced != null ? afs.filter.advanced : afs.filter.qText;
					}
				}
				linkWithOptionalDecision(data, itemMap, tgFilterName, tgId, "tg", srcKey, dstKey, true);
			}
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
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

			if ("task".equals(bn.target)) {
				dstKey   = "task:f:" + bn.fId;
				dst.name = bn.fName;
				dst.type = TYPE_TASK;
				dst.role = ROLE_FORM;
			} else if ("escalate".equals(bn.target)) {
				dstKey   = "case:f:" + bn.fId;
				dst.name = bn.caseSurvey != null ? bn.caseSurvey : bn.fName;
				dst.type = TYPE_CASE;
				dst.role = ROLE_FORM;
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

			// Link each source → decision (if filter) → dst
			if (bn.filter != null && !bn.filter.trim().isEmpty()) {
				String decKey = "decision:f:" + bn.fId;
				WorkflowItem dec = new WorkflowItem();
				dec.id      = decKey;
				dec.type    = TYPE_DECISION;
				dec.role    = ROLE_DECISION;
				dec.name    = bn.filter;
				dec.enabled = bn.enabled;
				itemMap.putIfAbsent(decKey, dec);
				for (String srcKey : srcKeys) {
					addLinkIfAbsent(data, srcKey, decKey);
				}
				addLinkIfAbsent(data, decKey, dstKey);
			} else {
				for (String srcKey : srcKeys) {
					addLinkIfAbsent(data, srcKey, dstKey);
				}
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

		boolean changed = true;
		while (changed) {
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
	// member[0] = s_id, member[1] = display_name, member[2] = "true" if data_survey
	private Map<String, List<String[]>> getBundleMembers(Connection sd, String user) throws SQLException {
		Map<String, List<String[]>> result = new LinkedHashMap<>();
		String sql = "select s.group_survey_ident, s.s_id, s.display_name, s.data_survey "
				+ "from survey s "
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
				String bundleId   = rs.getString("group_survey_ident");
				String sId        = String.valueOf(rs.getInt("s_id"));
				String name       = rs.getString("display_name");
				String dataSurvey = String.valueOf(rs.getBoolean("data_survey"));
				result.computeIfAbsent(bundleId, k -> new ArrayList<>()).add(new String[]{sId, name, dataSurvey});
			}
		}
		return result;
	}

	/*
	 * Creates a decision node (if filter is non-empty) and links src → [dec →] dst.
	 * decSuffix distinguishes decision keys: "f" for forward, "tg" for task_group.
	 */
	private void linkWithOptionalDecision(WorkflowData data, LinkedHashMap<String, WorkflowItem> itemMap,
			String filter, int id, String decSuffix, String srcKey, String dstKey, boolean enabled) {
		if (filter != null && !filter.trim().isEmpty()) {
			String decKey = "decision:" + decSuffix + ":" + id;
			WorkflowItem dec = new WorkflowItem();
			dec.id      = decKey;
			dec.type    = TYPE_DECISION;
			dec.role    = ROLE_DECISION;
			dec.name    = filter;
			dec.enabled = enabled;
			itemMap.putIfAbsent(decKey, dec);
			addLinkIfAbsent(data, srcKey, decKey);
			addLinkIfAbsent(data, decKey, dstKey);
		} else {
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
}
