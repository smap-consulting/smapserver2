package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.CaseManagementSettings;
import org.smap.sdal.model.OpsIssue;
import org.smap.sdal.model.UpdateConflict;
import org.smap.sdal.model.UpdateConflicts;

import com.google.gson.Gson;

/*
 * The operational issues report: problems somebody needs to resolve, in the surveys of the projects the
 * user belongs to in their current organisation.  Each issue says where it can be fixed.
 *
 *   conflict              a conflicting update that has not been reviewed
 *   notification_error    a notification failed in the last 30 days, e.g. its filter could not be evaluated
 *   alert_error           a case management alert failed in the last 30 days
 *   submission_error      submissions that could not be loaded in the last 30 days
 *   notification_filter   an enabled notification whose filter does not work with its survey now
 *   alert_filter          a case management alert whose filter does not work with its bundle now
 *   notification_orphan   an enabled notification whose survey or bundle has been deleted
 *   task_deleted_user     open tasks assigned to a user that has been deleted
 *   task_not_sent         task emails that could not be sent
 *
 * Errors that repeat, such as a notification failing on every submission, are one issue with a count.
 */
public class IssuesManager {

	private static Logger log = Logger.getLogger(IssuesManager.class.getName());

	public static final String CONFLICT = "conflict";
	public static final String NOTIFICATION_ERROR = "notification_error";
	public static final String ALERT_ERROR = "alert_error";
	public static final String SUBMISSION_ERROR = "submission_error";
	public static final String NOTIFICATION_FILTER = "notification_filter";
	public static final String ALERT_FILTER = "alert_filter";
	public static final String NOTIFICATION_ORPHAN = "notification_orphan";
	public static final String TASK_DELETED_USER = "task_deleted_user";
	public static final String TASK_NOT_SENT = "task_not_sent";
	public static final String ASSIGNED_NOT_CASE = "assigned_not_case";
	public static final String EXPRESSION = "expression";			// A rule or server calculation that does not work

	private static final String PERIOD = "30 days";
	private static final int LIMIT = 1000;		// Per kind of issue

	// Projects the user belongs to in their current organisation.  One parameter: the user ident.
	private static final String USER_PROJECTS = "select p.id from project p, user_project up, users u "
			+ "where p.id = up.p_id and up.u_id = u.id and u.ident = ? and p.o_id = u.o_id";

	// Surveys in those projects.  One parameter: the user ident.
	private static final String USER_SURVEYS = "select s.s_id from survey s "
			+ "where not s.deleted and s.p_id in (" + USER_PROJECTS + ")";

	private ResourceBundle localisation;
	private String tz;

	public IssuesManager(ResourceBundle l, String tz) {
		localisation = l;
		this.tz = (tz == null || tz.trim().isEmpty()) ? "UTC" : tz;
	}

	public List<OpsIssue> getIssues(Connection sd, Connection cResults, String user) throws SQLException {
		List<OpsIssue> issues = new ArrayList<>();
		boolean securityManager = GeneralUtilityMethods.hasSecurityGroup(sd, user, Authorise.SECURITY_ID);

		/*
		 * Each kind is found separately so that a problem finding one does not hide the others
		 */
		try { addConflicts(sd, cResults, user, securityManager, issues); } catch (Exception e) { log.log(Level.SEVERE, "Issues: conflicts", e); }
		try { addLogErrors(sd, user, issues); } catch (Exception e) { log.log(Level.SEVERE, "Issues: log errors", e); }
		try { addSubmissionErrors(sd, user, issues); } catch (Exception e) { log.log(Level.SEVERE, "Issues: submission errors", e); }
		try { addExpressions(sd, cResults, user, issues); } catch (Exception e) { log.log(Level.SEVERE, "Issues: server expressions", e); }
		try { addOrphanNotifications(sd, user, issues); } catch (Exception e) { log.log(Level.SEVERE, "Issues: orphan notifications", e); }
		try { addTaskIssues(sd, user, issues); } catch (Exception e) { log.log(Level.SEVERE, "Issues: tasks", e); }
		try { addAssignedNotCases(sd, user, securityManager, issues); } catch (Exception e) { log.log(Level.SEVERE, "Issues: assigned records not cases", e); }

		for (OpsIssue i : issues) {
			i.typeLabel = label("iss_" + i.type, i.type);
		}
		return issues;
	}

	/*
	 * Conflicting updates not yet reviewed.  These concern records, so a user who is restricted by
	 * a role to some of a bundle's records only sees those of bundles they can see in full.
	 */
	private void addConflicts(Connection sd, Connection cResults, String user, boolean securityManager,
			List<OpsIssue> issues) throws SQLException {

		String sql = "select distinct on (e.id) e.id, e.key, e.instanceid, e.changes, "
				+ time("e.event_time") + " as event_time, "
				+ "s.s_id, s.display_name, p.name as project_name, f.table_name "
				+ "from record_event e "
				+ "join form f on f.table_name = e.table_name and f.parentform = 0 "
				+ "join survey s on s.s_id = f.s_id "
				+ "join project p on p.id = s.p_id "
				+ "where e.event = 'conflict' and e.status = 'unresolved' "
				+ "and s.s_id in (" + USER_SURVEYS + ") "
				+ (securityManager ? "" : bundleRbacSql("s.group_survey_ident"))
				+ "order by e.id desc, s.s_id "
				+ "limit " + LIMIT;

		Gson gson = new Gson();
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			int idx = 1;
			pstmt.setString(idx++, tz);
			pstmt.setString(idx++, user);
			if (!securityManager) {
				pstmt.setString(idx++, user);
			}
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				OpsIssue i = new OpsIssue(CONFLICT);
				i.sId = rs.getInt("s_id");
				i.survey = rs.getString("display_name");
				i.project = rs.getString("project_name");
				i.last = rs.getString("event_time");
				i.first = i.last;

				// The record is opened at its latest version, which is where the conflict is reviewed
				String instance = latestVersion(cResults, rs.getString("table_name"), rs.getString("key"));
				if (instance == null) {
					instance = rs.getString("instanceid");
				}
				i.item = instance;

				List<String> names = new ArrayList<>();
				try {
					UpdateConflicts uc = gson.fromJson(rs.getString("changes"), UpdateConflicts.class);
					if (uc != null && uc.items != null) {
						for (UpdateConflict c : uc.items) {
							String n = c.name != null ? c.name : c.repeat;
							if (n != null && !names.contains(n)) names.add(n);
						}
					}
				} catch (Exception e) {
					// An unreadable conflict is still an unresolved conflict
				}
				i.detail = label("iss_conflict_detail", "Answers changed by two people") + (names.isEmpty() ? "" : ": " + String.join(", ", names));
				i.fixUrl = "/app/tasks/managed_forms.html?id=" + i.sId + "&instanceid=" + urlEncode(instance);
				issues.add(i);
			}
		}
	}

	/*
	 * Notification and case management alert errors recorded in the log in the last 30 days, one issue per
	 * notification or alert.  Entries written before the log recorded what they were about are grouped by
	 * survey and message.
	 */
	private void addLogErrors(Connection sd, String user, List<OpsIssue> issues) throws SQLException {

		String sql = "select l.event, l.s_id, l.ref, s.display_name, p.id as p_id, p.name as project_name, count(*) as cnt, "
				+ time("min(l.log_time)") + " as first_time, "
				+ time("max(l.log_time)") + " as last_time, "
				+ "(array_agg(l.note order by l.log_time desc))[1] as note "
				+ "from log l "
				+ "join survey s on s.s_id = l.s_id "
				+ "join project p on p.id = s.p_id "
				+ "where l.log_time > now() - interval '" + PERIOD + "' "
				+ "and (l.event = ? or (l.event = ? and l.user_ident = 'alert')) "
				+ "and l.s_id in (" + USER_SURVEYS + ") "
				+ "group by l.event, l.s_id, l.ref, s.display_name, p.id, p.name, case when l.ref is null then l.note end "
				+ "order by max(l.log_time) desc "
				+ "limit " + LIMIT;

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			int idx = 1;
			pstmt.setString(idx++, tz);
			pstmt.setString(idx++, tz);
			pstmt.setString(idx++, LogManager.NOTIFICATION_ERROR);
			pstmt.setString(idx++, LogManager.CASE_MANAGEMENT);
			pstmt.setString(idx++, user);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				boolean alert = LogManager.CASE_MANAGEMENT.equals(rs.getString("event"));
				OpsIssue i = new OpsIssue(alert ? ALERT_ERROR : NOTIFICATION_ERROR);
				i.sId = rs.getInt("s_id");
				i.pId = rs.getInt("p_id");
				i.survey = rs.getString("display_name");
				i.project = rs.getString("project_name");
				i.count = rs.getInt("cnt");
				i.first = rs.getString("first_time");
				i.last = rs.getString("last_time");
				i.detail = rs.getString("note");

				int refId = refId(rs.getString("ref"), alert ? LogManager.REF_ALERT : LogManager.REF_NOTIFICATION);
				if (alert) {
					i.item = refId > 0 ? name(sd, "select name from cms_alert where id = ?", refId) : null;
					i.fixUrl = "/app/cases.html?id=" + i.sId + (refId > 0 ? "&alert_id=" + refId : "");
				} else {
					i.item = refId > 0 ? name(sd, "select name from forward where id = ?", refId) : null;
					i.fixUrl = notificationUrl(refId, i.pId);
				}
				if (refId > 0 && i.item == null) {
					i.item = label("iss_deleted", "Since deleted");		// Fixed by deleting it, still listed until it ages out
					i.fixUrl = null;
				}
				issues.add(i);
			}
		}
	}

	/*
	 * Submissions that could not be loaded in the last 30 days, one issue per survey and reason
	 */
	private void addSubmissionErrors(Connection sd, String user, List<OpsIssue> issues) throws SQLException {

		String sql = "select ue.s_id, s.display_name, p.name as project_name, "
				+ "coalesce(nullif(ue.db_reason, ''), ue.reason) as why, count(*) as cnt, "
				+ time("min(ue.upload_time)") + " as first_time, "
				+ time("max(ue.upload_time)") + " as last_time "
				+ "from upload_event ue "
				+ "join survey s on s.s_id = ue.s_id "
				+ "join project p on p.id = s.p_id "
				+ "where ue.upload_time > now() - interval '" + PERIOD + "' "
				+ "and (ue.status = 'error' or (" + UploadEventManager.SQL_ERROR + ")) "
				+ "and ue.s_id in (" + USER_SURVEYS + ") "
				+ "group by ue.s_id, s.display_name, p.name, coalesce(nullif(ue.db_reason, ''), ue.reason) "
				+ "order by max(ue.upload_time) desc "
				+ "limit " + LIMIT;

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			int idx = 1;
			pstmt.setString(idx++, tz);
			pstmt.setString(idx++, tz);
			pstmt.setString(idx++, user);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				OpsIssue i = new OpsIssue(SUBMISSION_ERROR);
				i.sId = rs.getInt("s_id");
				i.survey = rs.getString("display_name");
				i.project = rs.getString("project_name");
				i.count = rs.getInt("cnt");
				i.first = rs.getString("first_time");
				i.last = rs.getString("last_time");
				i.detail = rs.getString("why");
				i.fixUrl = "/app/fieldManager/monitor.html?survey=" + i.sId;
				issues.add(i);
			}
		}
	}

	/*
	 * Saved server expressions that do not work now, for example because a question they use has been
	 * deleted or renamed: notification and case alert filters, task rules, PDF template rules, role row
	 * filters, reference data filters and server calculations.  Found before they next fail.
	 */
	private void addExpressions(Connection sd, Connection cResults, String user, List<OpsIssue> issues) throws SQLException {

		String sql = "select distinct s.group_survey_ident from survey s "
				+ "where s.s_id in (" + USER_SURVEYS + ") and s.group_survey_ident is not null";
		ArrayList<String> bundles = new ArrayList<>();
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, user);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) bundles.add(rs.getString(1));
		}

		ServerExpressionValidator v = new ServerExpressionValidator(localisation);
		int count = 0;
		for (String bundle : bundles) {
			for (ServerExpressionValidator.Problem p : v.checkBundle(sd, cResults, bundle, null)) {
				if (++count > LIMIT) return;
				OpsIssue i;
				if (p.kind.equals(ServerExpressionValidator.KIND_NOTIFICATION)) {
					i = new OpsIssue(NOTIFICATION_FILTER);
				} else if (p.kind.equals(ServerExpressionValidator.KIND_ALERT)) {
					i = new OpsIssue(ALERT_FILTER);
				} else {
					i = new OpsIssue(EXPRESSION);
				}
				if (p.sId > 0) {
					setSurvey(sd, i, p.sId);
				}
				i.item = (i.type.equals(EXPRESSION) ? p.what + ": " : "") + (p.name == null ? "" : p.name);
				i.detail = p.expression == null ? p.error
						: localisation.getString("filter_error").replace("%s1", p.expression).replace("%s2", p.error);
				i.fixUrl = expressionUrl(sd, p, i.pId);
				issues.add(i);
			}
		}
	}

	// Where an expression is edited.  Pages that work on the user's current survey are opened as they are.
	private String expressionUrl(Connection sd, ServerExpressionValidator.Problem p, int pId) throws SQLException {
		switch (p.kind) {
		case ServerExpressionValidator.KIND_NOTIFICATION:
			return notificationUrl(p.id, pId);
		case ServerExpressionValidator.KIND_ALERT:
			return "/app/cases.html?id=" + p.sId + "&alert_id=" + p.id;
		case ServerExpressionValidator.KIND_TASK:
		case ServerExpressionValidator.KIND_TASK_ASSIGN:
			String tgProject = name(sd, "select p_id::text from task_group where tg_id = ?", p.id);
			return "/app/tasks/taskManagement.html?tg_id=" + p.id + (tgProject == null ? "" : "&project=" + tgProject);
		case ServerExpressionValidator.KIND_SERVER_CALC:
			return "/app/edit.html?id=" + p.sId;
		case ServerExpressionValidator.KIND_TEMPLATE:
			return "/app/templates.html";
		case ServerExpressionValidator.KIND_ROW_FILTER:
			return "/surveyRoles.html";
		case ServerExpressionValidator.KIND_REF_FILTER:
			return "/referenceFilters.html";
		default:
			return null;
		}
	}

	/*
	 * Enabled notifications whose survey, or every survey of whose bundle, has been deleted
	 */
	private void addOrphanNotifications(Connection sd, String user, List<OpsIssue> issues) throws SQLException {

		String sql = "select f.id, f.name, f.trigger, f.s_id, f.bundle, f.bundle_ident, "
				+ "(select s2.display_name from survey s2 where s2.s_id = f.s_id or (f.bundle and s2.ident = f.bundle_ident) limit 1) as survey_name, "
				+ "coalesce((select s3.p_id from survey s3 "
				+ "    where s3.s_id = f.s_id or (f.bundle and s3.group_survey_ident = f.bundle_ident) limit 1), f.p_id) as p_id "
				+ "from forward f "
				+ "where f.enabled "
				+ "and ((not f.bundle and f.s_id > 0 and not exists (select 1 from survey s where s.s_id = f.s_id and not s.deleted)) "
				+ "  or (f.bundle and not exists (select 1 from survey s where s.group_survey_ident = f.bundle_ident and not s.deleted))) "
				+ "and (f.p_id in (" + USER_PROJECTS + ") "
				+ "  or exists (select 1 from survey s4 where (s4.s_id = f.s_id or (f.bundle and s4.group_survey_ident = f.bundle_ident)) "
				+ "    and s4.p_id in (" + USER_PROJECTS + "))) "
				+ "order by f.id "
				+ "limit " + LIMIT;

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, user);
			pstmt.setString(2, user);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				OpsIssue i = new OpsIssue(NOTIFICATION_ORPHAN);
				i.survey = rs.getString("survey_name");
				i.pId = rs.getInt("p_id");
				i.project = i.pId > 0 ? name(sd, "select name from project where id = ?", i.pId) : null;
				i.item = rs.getString("name");
				i.detail = label(rs.getBoolean("bundle") ? "iss_orphan_bundle" : "iss_orphan_survey",
						"Its survey has been deleted, it will not be sent");
				i.fixUrl = notificationUrl(rs.getInt("id"), i.pId);
				issues.add(i);
			}
		}
	}

	/*
	 * Open tasks assigned to users who have since been deleted, and task emails that could not be sent,
	 * one issue per task group
	 */
	private void addAssignedNotCases(Connection sd, String user, boolean securityManager,
			List<OpsIssue> issues) throws SQLException {

		/*
		 * Records assigned to somebody in a survey bundle that is not set up for case management.  They reach the
		 * people they are assigned to but are not counted or shown as cases.  A bundle is set up for case management
		 * when its case settings name a status question.
		 */
		java.util.Set<String> caseBundles = new java.util.HashSet<>();
		Gson gson = new Gson();
		try (PreparedStatement pstmt = sd.prepareStatement("select group_survey_ident, settings from cms_setting "
				+ "where group_survey_ident is not null and settings is not null")) {
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				try {
					CaseManagementSettings cms = gson.fromJson(rs.getString(2), CaseManagementSettings.class);
					if (cms != null && cms.statusQuestion != null && cms.statusQuestion.trim().length() > 0) {
						caseBundles.add(rs.getString(1));
					}
				} catch (Exception e) {
					// Settings that cannot be read do not make a case bundle
				}
			}
		}

		String sql = "select s.s_id, s.p_id, s.display_name, p.name as project_name, ru.group_survey_ident, "
				+ "count(distinct ru.thread) as cnt, "
				+ time("min(ru.created_at)") + " as first_time, "
				+ time("max(ru.created_at)") + " as last_time "
				+ "from record_user ru "
				+ "join survey s on s.ident = ru.group_survey_ident and not s.deleted "
				+ "join project p on p.id = s.p_id "
				+ "where ru.access = 'owner' "
				+ "and s.s_id in (" + USER_SURVEYS + ") "
				+ (securityManager ? "" : bundleRbacSql("ru.group_survey_ident"))
				+ "group by s.s_id, s.p_id, s.display_name, p.name, ru.group_survey_ident "
				+ "order by p.name, s.display_name "
				+ "limit " + LIMIT;

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			int idx = 1;
			pstmt.setString(idx++, tz);
			pstmt.setString(idx++, tz);
			pstmt.setString(idx++, user);
			if (!securityManager) {
				pstmt.setString(idx++, user);
			}
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				if (caseBundles.contains(rs.getString("group_survey_ident"))) {
					continue;
				}
				OpsIssue i = new OpsIssue(ASSIGNED_NOT_CASE);
				i.sId = rs.getInt("s_id");
				i.pId = rs.getInt("p_id");
				i.survey = rs.getString("display_name");
				i.project = rs.getString("project_name");
				i.count = rs.getInt("cnt");
				i.first = rs.getString("first_time");
				i.last = rs.getString("last_time");
				i.detail = label("iss_assigned_not_case_detail",
						"Records are assigned to people but the survey bundle is not set up for case management, so they are not shown as cases");
				i.fixUrl = "/app/cases.html?id=" + i.sId;
				issues.add(i);
			}
		}
	}

	private void addTaskIssues(Connection sd, String user, List<OpsIssue> issues) throws SQLException {

		String sql = "select t.tg_id, p.id as p_id, tg.name as tg_name, p.name as project_name, "
				+ "case when a.status = 'error' then 'not_sent' else 'deleted_user' end as kind, "
				+ "count(*) as cnt, "
				+ time("min(a.assigned_date)") + " as first_time, "
				+ time("max(a.assigned_date)") + " as last_time "
				+ "from assignments a "
				+ "join tasks t on t.id = a.task_id "
				+ "join task_group tg on tg.tg_id = t.tg_id "
				+ "join project p on p.id = t.p_id "
				+ "where not t.deleted "
				+ "and t.p_id in (" + USER_PROJECTS + ") "
				+ "and ((a.status in ('new', 'accepted') and a.assignee > 0 "
				+ "    and not exists (select 1 from users u2 where u2.id = a.assignee)) "
				+ "  or a.status = 'error') "
				+ "group by t.tg_id, p.id, tg.name, p.name, case when a.status = 'error' then 'not_sent' else 'deleted_user' end "
				+ "order by t.tg_id "
				+ "limit " + LIMIT;

		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			int idx = 1;
			pstmt.setString(idx++, tz);
			pstmt.setString(idx++, tz);
			pstmt.setString(idx++, user);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				boolean notSent = "not_sent".equals(rs.getString("kind"));
				OpsIssue i = new OpsIssue(notSent ? TASK_NOT_SENT : TASK_DELETED_USER);
				i.project = rs.getString("project_name");
				i.item = rs.getString("tg_name");
				i.count = rs.getInt("cnt");
				i.first = rs.getString("first_time");
				i.last = rs.getString("last_time");
				i.detail = label(notSent ? "iss_task_not_sent_detail" : "iss_task_deleted_user_detail",
						notSent ? "Task emails could not be sent" : "Open tasks are assigned to a user who has been deleted");
				i.fixUrl = "/app/tasks/taskManagement.html?tg_id=" + rs.getInt("tg_id") + "&project=" + rs.getInt("p_id");
				issues.add(i);
			}
		}
	}

	/*
	 * Bundles the user may see the records of: not protected by enabled roles, or the user holds a
	 * role on it with no row filter.  The same rule as the operations dashboard.  One parameter: the user ident.
	 */
	private String bundleRbacSql(String col) {
		return " and ( not exists (select 1 from survey_role sr where (sr.survey_ident = " + col
				+ " or sr.group_survey_ident = " + col + ") and sr.enabled) "
				+ "or exists (select 1 from survey_role sr2, user_role ur2, users u2 "
				+ "where (sr2.survey_ident = " + col + " or sr2.group_survey_ident = " + col + ") and sr2.enabled "
				+ "and sr2.r_id = ur2.r_id and ur2.u_id = u2.id and u2.ident = ? "
				+ "and (sr2.row_filter is null or sr2.row_filter = '')) ) ";
	}

	// The latest version of a record, by its thread
	private String latestVersion(Connection cResults, String table, String thread) {
		if (table == null || thread == null) return null;
		String sql = "select instanceid from " + table + " where _thread = ? and not _bad order by prikey desc limit 1";
		try (PreparedStatement pstmt = cResults.prepareStatement(sql)) {
			pstmt.setString(1, thread);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		} catch (SQLException e) {
			return null;
		}
	}

	private void setSurvey(Connection sd, OpsIssue i, int sId) throws SQLException {
		i.sId = sId;
		String sql = "select s.display_name, p.name, p.id from survey s join project p on p.id = s.p_id where s.s_id = ?";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, sId);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				i.survey = rs.getString(1);
				i.project = rs.getString(2);
				i.pId = rs.getInt(3);
			}
		}
	}

	private String name(Connection sd, String sql, int id) throws SQLException {
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, id);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		}
	}

	// The notifications page lists one project's notifications, so the link names the project too
	private String notificationUrl(int nId, int pId) {
		StringBuilder url = new StringBuilder("/app/fieldManager/notifications.html");
		String sep = "?";
		if (nId > 0) { url.append(sep).append("fwd_id=").append(nId); sep = "&"; }
		if (pId > 0) { url.append(sep).append("project=").append(pId); }
		return url.toString();
	}

	private int refId(String ref, String prefix) {
		if (ref == null || !ref.startsWith(prefix)) return 0;
		try {
			return Integer.parseInt(ref.substring(prefix.length()));
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	// A time in the user's time zone.  One parameter: the time zone.
	private String time(String col) {
		return "to_char(timezone(?, " + col + "), 'YYYY-MM-DD HH24:MI')";
	}

	private String label(String key, String dflt) {
		try {
			return localisation.getString(key);
		} catch (Exception e) {
			return dflt;
		}
	}

	private String urlEncode(String s) {
		try {
			return java.net.URLEncoder.encode(s == null ? "" : s, "UTF-8");
		} catch (Exception e) {
			return "";
		}
	}

	/*
	 * The number of issues of each type, for a summary
	 */
	public static Map<String, Integer> countByType(List<OpsIssue> issues) {
		Map<String, Integer> counts = new LinkedHashMap<>();
		for (OpsIssue i : issues) {
			counts.merge(i.type, 1, Integer::sum);
		}
		return counts;
	}
}
