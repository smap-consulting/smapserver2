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
import java.util.LinkedHashMap;
import java.util.logging.Logger;

import org.smap.sdal.model.WorkflowData;
import org.smap.sdal.model.WorkflowItem;
import org.smap.sdal.model.WorkflowLink;

public class WorkflowManager {

	private static Logger log = Logger.getLogger(WorkflowManager.class.getName());

	/*
	 * Build the full set of workflow nodes and links accessible to the user.
	 *
	 * Each record in forward or task_group is split into:
	 *   - one trigger WorkflowItem  (role = "trigger")
	 *   - one action  WorkflowItem  (role = "action")
	 *   - one WorkflowLink connecting them
	 *
	 * Nodes are deduplicated: two forward records that both trigger from the same
	 * survey share a single "submission:s:<s_id>" trigger node, and two records
	 * that both create tasks via the same task group share a single
	 * "task:tg:<tg_id>" action node.
	 */
	public WorkflowData getWorkflowItems(Connection sd, String user) throws Exception {

		WorkflowData data = new WorkflowData();

		// Use LinkedHashMap to preserve insertion order while deduplicating by id
		LinkedHashMap<String, WorkflowItem> itemMap = new LinkedHashMap<>();

		// -----------------------------------------------------------------------
		// 1. forward table
		// -----------------------------------------------------------------------
		String sqlForward =
				"select f.id, f.name, f.trigger, f.target, f.enabled, "
				+ "f.s_id, f.bundle_ident, f.p_id, f.tg_id, f.alert_id, "
				+ "s_src.display_name as trigger_survey, "
				+ "s_bun.display_name as bundle_name, "
				+ "a.name as alert_name, "
				+ "tg.name as task_group_name "
				+ "from forward f "
				+ "left outer join survey s_src on s_src.s_id = f.s_id "
				+ "left outer join survey s_bun on s_bun.ident = f.bundle_ident "
				+ "left outer join cms_alert a on a.id = f.alert_id "
				+ "left outer join task_group tg on tg.tg_id = f.tg_id "
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
				int    fId          = rs.getInt("id");
				String fName        = rs.getString("name");
				String trigger      = rs.getString("trigger");
				String target       = rs.getString("target");
				boolean enabled     = rs.getBoolean("enabled");
				int    sId          = rs.getInt("s_id");
				String bundleIdent  = rs.getString("bundle_ident");
				int    pId          = rs.getInt("p_id");
				int    tgId         = rs.getInt("tg_id");
				int    alertId      = rs.getInt("alert_id");
				String triggerSurvey = rs.getString("trigger_survey");
				String bundleName   = rs.getString("bundle_name");
				String alertName    = rs.getString("alert_name");
				String tgName       = rs.getString("task_group_name");

				// -- Trigger node --
				String trigKey;
				String trigName;
				if (sId > 0) {
					trigKey  = "submission:s:" + sId;
					trigName = triggerSurvey != null ? triggerSurvey : fName;
				} else if (bundleIdent != null && !bundleIdent.isEmpty()) {
					trigKey  = "submission:bundle:" + bundleIdent;
					trigName = bundleName != null ? bundleName : bundleIdent;
				} else {
					trigKey  = (trigger != null ? trigger : "trigger") + ":f:" + fId;
					trigName = fName;
				}
				if (!itemMap.containsKey(trigKey)) {
					WorkflowItem trig = new WorkflowItem();
					trig.id      = trigKey;
					trig.type    = trigger != null ? trigger : "submission";
					trig.role    = "trigger";
					trig.name    = trigName;
					trig.enabled = enabled;
					itemMap.put(trigKey, trig);
				}

				// -- Action node --
				String actKey;
				String actName;
				if ("task".equals(target) && tgId > 0) {
					actKey  = "task:tg:" + tgId;
					actName = tgName != null ? tgName : fName;
				} else if ("case".equals(target) && alertId > 0) {
					actKey  = "case:a:" + alertId;
					actName = alertName != null ? alertName : fName;
				} else {
					actKey  = (target != null ? target : "action") + ":f:" + fId;
					actName = fName;
				}
				if (!itemMap.containsKey(actKey)) {
					WorkflowItem act = new WorkflowItem();
					act.id      = actKey;
					act.type    = target != null ? target : "task";
					act.role    = "action";
					act.name    = actName;
					act.enabled = enabled;
					itemMap.put(actKey, act);
				}

				// -- Link --
				addLinkIfAbsent(data, trigKey, actKey);
			}
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}

		// -----------------------------------------------------------------------
		// 2. task_group table
		// -----------------------------------------------------------------------
		String sqlTg =
				"select tg.tg_id, tg.name, tg.source_s_id, "
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
				String triggerSurvey = rs.getString("trigger_survey");
				String targetSurvey  = rs.getString("target_survey");

				// Trigger key matches forward submission triggers for the same source survey
				String trigKey = "submission:s:" + sourceSId;
				String actKey  = "task:tg:" + tgId;

				if (!itemMap.containsKey(trigKey)) {
					WorkflowItem trig = new WorkflowItem();
					trig.id      = trigKey;
					trig.type    = "submission";
					trig.role    = "trigger";
					trig.name    = triggerSurvey != null ? triggerSurvey : tgName;
					trig.enabled = true;
					itemMap.put(trigKey, trig);
				}
				if (!itemMap.containsKey(actKey)) {
					WorkflowItem act = new WorkflowItem();
					act.id      = actKey;
					act.type    = "task";
					act.role    = "action";
					act.name    = targetSurvey != null ? targetSurvey : tgName;
					act.enabled = true;
					itemMap.put(actKey, act);
				}
				addLinkIfAbsent(data, trigKey, actKey);
			}
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}

		data.items.addAll(itemMap.values());
		return data;
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
