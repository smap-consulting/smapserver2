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

	// WorkItem type constants
	private static final String TYPE_FORM      = "form";
	private static final String TYPE_TASK      = "task";
	private static final String TYPE_CASE      = "case";
	private static final String TYPE_PERIODIC  = "periodic";
	private static final String TYPE_REMINDER  = "reminder";
	private static final String TYPE_EMAIL     = "email";
	private static final String TYPE_SMS       = "sms";

	// WorkItem role constants (determines visual shape)
	private static final String ROLE_FORM         = "form";
	private static final String ROLE_TRIGGER      = "trigger";
	private static final String ROLE_NOTIFICATION = "notification";

	/*
	 * Build the full set of workflow nodes and links accessible to the user.
	 *
	 * Each record in forward / task_group is decomposed into two WorkflowItems
	 * (source + destination) plus a WorkflowLink between them.
	 *
	 * Items are deduplicated by id so that the same survey or task group
	 * referenced by multiple records produces a single shared node.
	 */
	public WorkflowData getWorkflowItems(Connection sd, String user) throws Exception {

		WorkflowData data = new WorkflowData();
		LinkedHashMap<String, WorkflowItem> itemMap = new LinkedHashMap<>();

		// -------------------------------------------------------------------
		// 1. forward table
		// -------------------------------------------------------------------
		String sqlForward =
				"select f.id, f.name, f.trigger, f.target, f.enabled, "
				+ "f.s_id, f.bundle_ident, f.p_id, f.tg_id, f.alert_id, "
				+ "s_src.display_name as trigger_survey, "
				+ "s_bun.display_name as bundle_name, "
				+ "a.name as alert_name, "
				+ "tg.name as task_group_name, "
				+ "s_tgt.display_name as target_survey "
				+ "from forward f "
				+ "left outer join survey s_src on s_src.s_id = f.s_id "
				+ "left outer join survey s_bun on s_bun.ident = f.bundle_ident "
				+ "left outer join cms_alert a on a.id = f.alert_id "
				+ "left outer join task_group tg on tg.tg_id = f.tg_id "
				+ "left outer join survey s_tgt on s_tgt.s_id = tg.target_s_id "
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
				int     fId          = rs.getInt("id");
				String  fName        = rs.getString("name");
				String  trigger      = rs.getString("trigger");
				String  target       = rs.getString("target");
				boolean enabled      = rs.getBoolean("enabled");
				int     sId          = rs.getInt("s_id");
				String  bundleIdent  = rs.getString("bundle_ident");
				int     tgId         = rs.getInt("tg_id");
				int     alertId      = rs.getInt("alert_id");
				String  triggerSurvey = rs.getString("trigger_survey");
				String  bundleName   = rs.getString("bundle_name");
				String  alertName    = rs.getString("alert_name");
				String  tgName       = rs.getString("task_group_name");
				String  targetSurvey = rs.getString("target_survey");

				// -- Source node --
				String srcKey;
				WorkflowItem src = new WorkflowItem();
				src.enabled = enabled;

				if ("submission".equals(trigger)) {
					if (sId > 0) {
						srcKey    = "form:s:" + sId;
						src.type  = TYPE_FORM;
						src.role  = ROLE_FORM;
						src.name  = triggerSurvey != null ? triggerSurvey : fName;
					} else if (bundleIdent != null && !bundleIdent.isEmpty()) {
						srcKey    = "form:bundle:" + bundleIdent;
						src.type  = TYPE_FORM;
						src.role  = ROLE_FORM;
						src.name  = bundleName != null ? bundleName : bundleIdent;
					} else {
						srcKey    = "form:f:" + fId;
						src.type  = TYPE_FORM;
						src.role  = ROLE_FORM;
						src.name  = fName;
					}
				} else if ("periodic".equals(trigger)) {
					srcKey   = "periodic:f:" + fId;
					src.type = TYPE_PERIODIC;
					src.role = ROLE_TRIGGER;
					src.name = fName;
				} else if ("reminder".equals(trigger)) {
					srcKey   = "reminder:f:" + fId;
					src.type = TYPE_REMINDER;
					src.role = ROLE_TRIGGER;
					src.name = fName;
				} else {
					srcKey   = "trigger:" + trigger + ":f:" + fId;
					src.type = trigger;
					src.role = ROLE_TRIGGER;
					src.name = fName;
				}
				src.id = srcKey;
				itemMap.putIfAbsent(srcKey, src);

				// -- Destination node --
				String dstKey;
				WorkflowItem dst = new WorkflowItem();
				dst.enabled = enabled;

				if ("task".equals(target)) {
					if (tgId > 0) {
						dstKey   = "task:tg:" + tgId;
						dst.name = targetSurvey != null ? targetSurvey : fName;
					} else {
						dstKey   = "task:f:" + fId;
						dst.name = fName;
					}
					dst.type = TYPE_TASK;
					dst.role = ROLE_FORM;
				} else if ("case".equals(target)) {
					if (alertId > 0) {
						dstKey   = "case:a:" + alertId;
						dst.name = targetSurvey != null ? targetSurvey : fName;
					} else {
						dstKey   = "case:f:" + fId;
						dst.name = targetSurvey != null ? targetSurvey : fName;
					}
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

				addLinkIfAbsent(data, srcKey, dstKey);
			}
		} finally {
			try { if (pstmt != null) pstmt.close(); } catch (SQLException e) {}
		}

		// -------------------------------------------------------------------
		// 2. task_group table — source_s_id (form) → tg_id (task)
		// -------------------------------------------------------------------
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

				// Source: the submission form — same key as forward submission triggers
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

				// Destination: the task form — same key as forward task actions for same tg
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

				addLinkIfAbsent(data, srcKey, dstKey);
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
