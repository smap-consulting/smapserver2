package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.OutputStream;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.CaseCount;
import org.smap.sdal.model.OpsAlert;
import org.smap.sdal.model.OpsCase;
import org.smap.sdal.model.OpsField;
import org.smap.sdal.model.OpsSettings;
import org.smap.sdal.model.User;
import org.smap.sdal.model.OpsBacklogPoint;
import org.smap.sdal.model.OpsItem;
import org.smap.sdal.model.OpsKpi;
import org.smap.sdal.model.OpsOverview;
import org.smap.sdal.model.OpsTrendPoint;
import org.smap.sdal.model.OpsUnit;
import org.smap.sdal.model.OpsUnitDetail;

/*
 * Builds the senior-manager Operations Overview (L0).
 *
 * Aggregates workload, trends, overdue / stale work and per-unit (role) performance
 * across an organisation, reusing existing tables only.  See
 * docs/manager-reporting-solution.md (Part C) for the design.
 */
public class OpsMonitorManager {

	private static Logger log = Logger.getLogger(OpsMonitorManager.class.getName());

	ResourceBundle localisation;

	// Defaults (B.0.1) - overridden per organisation by ops_settings (see loadSettings)
	private static final int DEFAULT_TREND_DAYS = 30;
	private static final int DEFAULT_STALE_DAYS = 14;
	private static final double DEFAULT_RAG_AMBER = 10.0;
	private static final double DEFAULT_RAG_RED = 25.0;
	private static final int ALERT_LIST_LIMIT = 20;

	// Effective per-org settings, loaded at the start of each build (defaults until then)
	private int trendDays = DEFAULT_TREND_DAYS;
	private int staleDays = DEFAULT_STALE_DAYS;
	private double ragAmber = DEFAULT_RAG_AMBER;
	private double ragRed = DEFAULT_RAG_RED;

	// Request context for RBAC. A security manager (SECURITY group) sees all surveys;
	// everyone else is restricted to bundles they have unfiltered role access to.
	private String requestingUser;
	private boolean securityManager;

	private String staleInterval() {
		return staleDays + " days";
	}

	/*
	 * Load per-org settings and the RBAC context for the requesting user.
	 * fullAccess (or a null user, e.g. the scheduled digest) grants security-manager access.
	 */
	private void loadContext(Connection sd, int oId, String user, boolean fullAccess) {
		loadSettings(sd, oId);
		requestingUser = user;
		if(fullAccess || user == null) {
			securityManager = true;
		} else {
			try {
				securityManager = GeneralUtilityMethods.hasSecurityGroup(sd, user, Authorise.SECURITY_ID);
			} catch (Exception e) {
				securityManager = false;
			}
		}
	}

	/*
	 * SQL fragment restricting a group-survey-ident column to bundles the requesting user
	 * may see: not protected by enabled roles, or the user holds a matching role that has
	 * no row filter. Adds one '?' (the user ident) to the statement when applied.
	 * Returns "" for a security manager (no restriction).
	 */
	private String bundleRbacSql(String col) {
		if(securityManager) {
			return "";
		}
		return " and ( not exists (select 1 from survey_role sr where (sr.survey_ident = " + col
				+ " or sr.group_survey_ident = " + col + ") and sr.enabled) "
				+ "or exists (select 1 from survey_role sr2, user_role ur2, users u2 "
				+ "where (sr2.survey_ident = " + col + " or sr2.group_survey_ident = " + col + ") and sr2.enabled "
				+ "and sr2.r_id = ur2.r_id and ur2.u_id = u2.id and u2.ident = ? "
				+ "and (sr2.row_filter is null or sr2.row_filter = '')) ) ";
	}

	/*
	 * True if the requesting user may see the given bundle (security manager, or unfiltered role access).
	 */
	private boolean isBundleAccessible(Connection sd, String groupSurveyIdent) throws SQLException {
		if(securityManager) {
			return true;
		}
		String sql = "select ( not exists (select 1 from survey_role sr where (sr.survey_ident = ? or sr.group_survey_ident = ?) and sr.enabled) "
				+ "or exists (select 1 from survey_role sr2, user_role ur2, users u2 "
				+ "where (sr2.survey_ident = ? or sr2.group_survey_ident = ?) and sr2.enabled "
				+ "and sr2.r_id = ur2.r_id and ur2.u_id = u2.id and u2.ident = ? "
				+ "and (sr2.row_filter is null or sr2.row_filter = ''))) as ok";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, groupSurveyIdent);
			pstmt.setString(2, groupSurveyIdent);
			pstmt.setString(3, groupSurveyIdent);
			pstmt.setString(4, groupSurveyIdent);
			pstmt.setString(5, requestingUser);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				return rs.getBoolean("ok");
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return false;
	}

	/*
	 * SQL fragment restricting alerts (alert a) to those whose survey the requesting user may
	 * see: no survey, not role-protected, or a matching role with no row filter. Adds one '?'
	 * (the user ident). Returns "" for a security manager.
	 */
	private String alertRbacSql() {
		if(securityManager) {
			return "";
		}
		return " and ( a.s_id is null "
				+ "or not exists (select 1 from survey sv, survey_role sr "
				+ "  where sv.s_id = a.s_id and (sr.survey_ident = sv.ident or sr.group_survey_ident = sv.group_survey_ident) and sr.enabled) "
				+ "or exists (select 1 from survey sv2, survey_role sr2, user_role ur2, users u2 "
				+ "  where sv2.s_id = a.s_id and (sr2.survey_ident = sv2.ident or sr2.group_survey_ident = sv2.group_survey_ident) and sr2.enabled "
				+ "  and sr2.r_id = ur2.r_id and ur2.u_id = u2.id and u2.ident = ? and (sr2.row_filter is null or sr2.row_filter = '')) ) ";
	}

	/*
	 * SQL fragment restricting a role id column to roles the requesting user is a member of.
	 * Adds one '?' (user ident). Returns "" for a security manager (all roles visible).
	 */
	private String roleMembershipSql(String roleIdCol) {
		if(securityManager) {
			return "";
		}
		return " and " + roleIdCol + " in (select ur_m.r_id from user_role ur_m "
				+ "join users u_m on u_m.id = ur_m.u_id where u_m.ident = ?) ";
	}

	/*
	 * True if the requesting user is a member of the named role (or a security manager).
	 */
	private boolean isRoleMember(Connection sd, int oId, String role) throws SQLException {
		if(securityManager) {
			return true;
		}
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select 1 from role r join user_role ur on ur.r_id = r.id "
					+ "join users u on u.id = ur.u_id where r.o_id = ? and r.name = ? and u.ident = ? limit 1");
			pstmt.setInt(1, oId);
			pstmt.setString(2, role);
			pstmt.setString(3, requestingUser);
			return pstmt.executeQuery().next();
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
	}

	/*
	 * Survey-level RBAC on a survey-ident column (e.g. tasks.survey_ident): not role-protected,
	 * or the user has a matching role with no row filter. Adds one '?' (user ident). "" for security manager.
	 */
	private String surveyIdentRbacSql(String identCol) {
		if(securityManager) {
			return "";
		}
		return " and ( " + identCol + " is null "
				+ "or not exists (select 1 from survey_role sr where sr.survey_ident = " + identCol + " and sr.enabled) "
				+ "or exists (select 1 from survey_role sr2, user_role ur2, users u2 "
				+ "where sr2.survey_ident = " + identCol + " and sr2.enabled and sr2.r_id = ur2.r_id and ur2.u_id = u2.id "
				+ "and u2.ident = ? and (sr2.row_filter is null or sr2.row_filter = '')) ) ";
	}

	/*
	 * Survey-level RBAC on a survey-id column (e.g. upload_event.s_id). Adds one '?' (user ident).
	 */
	private String surveyIdRbacSql(String sIdCol) {
		if(securityManager) {
			return "";
		}
		return " and ( " + sIdCol + " is null "
				+ "or not exists (select 1 from survey sv, survey_role sr where sv.s_id = " + sIdCol
				+ " and sr.survey_ident = sv.ident and sr.enabled) "
				+ "or exists (select 1 from survey sv2, survey_role sr2, user_role ur2, users u2 "
				+ "where sv2.s_id = " + sIdCol + " and sr2.survey_ident = sv2.ident and sr2.enabled "
				+ "and sr2.r_id = ur2.r_id and ur2.u_id = u2.id and u2.ident = ? and (sr2.row_filter is null or sr2.row_filter = '')) ) ";
	}

	// Per-org cache so reloads / polling do not re-scan results tables
	private static final long CACHE_TTL_MS = 60_000;
	private static class Cached {
		long builtAt;
		String json;		// the serialised OpsOverview
	}
	// Cache keys include the user ident so an RBAC-filtered view is never served to another user
	private static final ConcurrentHashMap<String, Cached> cache = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, Cached> unitCache = new ConcurrentHashMap<>();

	private static final int ATRISK_LIMIT = 100;

	// Result tables we have already ensured have the case-monitoring indexes (per process)
	private static final java.util.Set<String> indexedTables =
			java.util.Collections.synchronizedSet(new java.util.HashSet<String>());

	public OpsMonitorManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Return a cached serialised overview if it is still fresh, else null.
	 */
	public String getCachedOverview(int oId, String user) {
		Cached c = cache.get(oId + ":" + user);
		if(c != null && (System.currentTimeMillis() - c.builtAt) < CACHE_TTL_MS) {
			return c.json;
		}
		return null;
	}

	public void putCachedOverview(int oId, String user, String json) {
		Cached c = new Cached();
		c.builtAt = System.currentTimeMillis();
		c.json = json;
		cache.put(oId + ":" + user, c);
	}

	public String getCachedUnit(int oId, String role, String user) {
		Cached c = unitCache.get(oId + ":" + role + ":" + user);
		if(c != null && (System.currentTimeMillis() - c.builtAt) < CACHE_TTL_MS) {
			return c.json;
		}
		return null;
	}

	public void putCachedUnit(int oId, String role, String user, String json) {
		Cached c = new Cached();
		c.builtAt = System.currentTimeMillis();
		c.json = json;
		unitCache.put(oId + ":" + role + ":" + user, c);
	}

	/*
	 * Build the complete overview for an organisation.
	 */
	public OpsOverview getOverview(Connection sd, Connection cResults, int oId, String user) throws SQLException {

		loadContext(sd, oId, user, false);

		OpsOverview ov = new OpsOverview();
		ov.generatedAt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());

		// 1. Case figures (open / stale / unassigned) and backlog trend, summed over the org's case bundles
		ArrayList<String> bundles = getCaseBundles(sd, oId);
		int openCases = 0;
		int staleCases = 0;
		ov.unassignedCases = 0;
		int[][] trend = new int[trendDays + 1][2];		// [day][0]=opened [day][1]=closed
		String[] trendDayLabels = new String[trendDays + 1];

		CaseManager cm = new CaseManager(localisation);
		for(String groupSurveyIdent : bundles) {
			try {
				int[] counts = getCaseCounts(sd, cResults, groupSurveyIdent);
				openCases += counts[0];
				staleCases += counts[1];
				ov.unassignedCases += counts[2];

				ArrayList<CaseCount> cc = cm.getOpenClosed(sd, cResults, groupSurveyIdent, "day", trendDays, "day");
				for(int i = 0; i < cc.size() && i <= trendDays; i++) {
					trendDayLabels[i] = cc.get(i).day;
					trend[i][0] += cc.get(i).opened;
					trend[i][1] += cc.get(i).closed;
				}
			} catch (Exception e) {
				// One problem bundle should not break the whole overview
				log.log(Level.WARNING, "Ops overview: skipping bundle " + groupSurveyIdent + ": " + e.getMessage());
			}
		}

		// Build the cumulative backlog series
		int net = 0;
		for(int i = 0; i <= trendDays; i++) {
			if(trendDayLabels[i] == null) {
				continue;
			}
			net += (trend[i][0] - trend[i][1]);
			ov.backlog.add(new OpsBacklogPoint(trendDayLabels[i], trend[i][0], trend[i][1], net));
		}

		// 2. Task totals (org level)
		int[] taskTotals = getTaskTotals(sd, oId);	// [0]=inProgress [1]=overdue [2]=stale
		int staleItems = staleCases + taskTotals[2];

		// 3. Open alerts
		int openAlertCount = getOpenAlertCount(sd, oId);
		boolean anyHighAlert = false;
		ov.alerts = getOpenAlerts(sd, oId);
		for(OpsAlert al : ov.alerts) {
			if(al.priority == 1) { anyHighAlert = true; break; }
		}

		// 4. Submissions (30 day trend, value = last 7 days)
		ArrayList<OpsTrendPoint> subsTrend = getSubmissionsTrend(sd, oId);
		long subs7 = 0;
		int from = Math.max(0, subsTrend.size() - 7);
		for(int i = from; i < subsTrend.size(); i++) {
			subs7 += subsTrend.get(i).value;
		}

		// 5. KPI tiles
		ov.kpis.add(new OpsKpi("open_cases", localise("ops_open_cases", "Open cases"), openCases, "none"));
		OpsKpi unassignedKpi = new OpsKpi("unassigned_cases", localise("ops_unassigned", "Unassigned cases"),
				ov.unassignedCases, ov.unassignedCases > 0 ? "amber" : "green");
		ov.kpis.add(unassignedKpi);
		ov.kpis.add(new OpsKpi("tasks_in_progress", localise("ops_tasks_in_progress", "Tasks in progress"),
				taskTotals[0], "none"));
		ov.kpis.add(new OpsKpi("tasks_overdue", localise("ops_tasks_overdue", "Tasks overdue"),
				taskTotals[1], taskTotals[1] > 0 ? "red" : "green"));
		ov.kpis.add(new OpsKpi("stale_items", localise("ops_stale_items", "Stale items"),
				staleItems, staleItems > 0 ? "red" : "green"));
		OpsKpi alertKpi = new OpsKpi("open_alerts", localise("ops_open_alerts", "Open alerts"),
				openAlertCount, openAlertCount == 0 ? "green" : (anyHighAlert ? "red" : "amber"));
		ov.kpis.add(alertKpi);
		OpsKpi subsKpi = new OpsKpi("submissions_7d", localise("ops_submissions_7d", "Submissions (7 days)"),
				subs7, "none");
		subsKpi.trend = subsTrend;
		ov.kpis.add(subsKpi);

		// 6. Per-unit (role) rollup - only roles that own open work (incl. a "No unit" row)
		ov.units = getUnits(sd, cResults, oId);

		// Unassigned reconciliation row so the unit table accounts for the open-cases total
		if(ov.unassignedCases > 0) {
			OpsUnit ua = new OpsUnit(localise("ops_unit_unassigned", "Unassigned"));
			ua.openCases = ov.unassignedCases;
			ua.aggregate = true;
			ua.itemType = "unassigned";
			ua.rag = "none";
			ov.units.add(ua);
		}

		return ov;
	}

	/*
	 * Distinct case bundles (group survey idents) that belong to the organisation.
	 */
	private ArrayList<String> getCaseBundles(Connection sd, int oId) throws SQLException {
		ArrayList<String> bundles = new ArrayList<>();
		String sql = "select distinct cs.group_survey_ident "
				+ "from cms_setting cs "
				+ "where cs.group_survey_ident is not null "
				+ "and exists (select 1 from survey s, project p "
				+ "  where s.group_survey_ident = cs.group_survey_ident "
				+ "  and s.p_id = p.id and p.o_id = ? and not s.deleted)"
				+ bundleRbacSql("cs.group_survey_ident");
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			if(!securityManager) {
				pstmt.setString(2, requestingUser);
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				bundles.add(rs.getString(1));
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return bundles;
	}

	/*
	 * Open / stale / unassigned open-case counts for one bundle from its results table.
	 * Returns [open, stale, unassigned].
	 */
	private int[] getCaseCounts(Connection sd, Connection cResults, String groupSurveyIdent) throws SQLException {
		int[] counts = new int[3];
		String table = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, groupSurveyIdent);
		if(table == null) {
			return counts;
		}
		GeneralUtilityMethods.ensureTableCurrent(cResults, table, true);
		ensureCaseIndexes(cResults, table);

		String sql = "select "
				+ "count(*) filter (where _case_closed is null and not _bad) as open_count, "
				+ "count(*) filter (where _case_closed is null and not _bad and _thread_created < now() - ?::interval) as stale_count, "
				+ "count(*) filter (where _case_closed is null and not _bad and (_assigned is null or _assigned = '')) as unassigned_count "
				+ "from " + table;
		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql);
			pstmt.setString(1, staleInterval());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				counts[0] = rs.getInt("open_count");
				counts[1] = rs.getInt("stale_count");
				counts[2] = rs.getInt("unassigned_count");
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return counts;
	}

	/*
	 * Org-level task totals. Returns [inProgress, overdue, stale].
	 */
	private int[] getTaskTotals(Connection sd, int oId) throws SQLException {
		int[] totals = new int[3];
		String sql = "select "
				+ "count(*) filter (where a.status in ('unsent','accepted')) as in_progress, "
				+ "count(*) filter (where a.status in ('unsent','accepted') and t.schedule_finish is not null and t.schedule_finish < now()) as overdue, "
				+ "count(*) filter (where a.status in ('unsent','accepted') and t.created_at < now() - ?::interval) as stale_tasks "
				+ "from assignments a "
				+ "join tasks t on t.id = a.task_id and not t.deleted "
				+ "join project p on p.id = t.p_id and p.o_id = ?"
				+ surveyIdentRbacSql("t.survey_ident");
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, staleInterval());
			pstmt.setInt(2, oId);
			if(!securityManager) {
				pstmt.setString(3, requestingUser);
			}
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				totals[0] = rs.getInt("in_progress");
				totals[1] = rs.getInt("overdue");
				totals[2] = rs.getInt("stale_tasks");
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return totals;
	}

	private int getOpenAlertCount(Connection sd, int oId) throws SQLException {
		String sql = "select count(*) from alert a "
				+ "join users u on u.id = a.u_id and u.o_id = ? "
				+ "where a.status = 'open'"
				+ alertRbacSql();
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			if(!securityManager) {
				pstmt.setString(2, requestingUser);
			}
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				return rs.getInt(1);
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return 0;
	}

	private ArrayList<OpsAlert> getOpenAlerts(Connection sd, int oId) throws SQLException {
		ArrayList<OpsAlert> alerts = new ArrayList<>();
		String sql = "select a.id, a.message, a.priority, a.link, s.display_name as bundle, "
				+ "extract(epoch from (now() - a.created_time)) as since "
				+ "from alert a "
				+ "join users u on u.id = a.u_id and u.o_id = ? "
				+ "left join survey s on s.s_id = a.s_id "
				+ "where a.status = 'open' "
				+ alertRbacSql()
				+ "order by a.priority asc, a.created_time asc "
				+ "limit " + ALERT_LIST_LIMIT;
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			if(!securityManager) {
				pstmt.setString(2, requestingUser);
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				alerts.add(new OpsAlert(
						rs.getInt("id"),
						rs.getString("message"),
						rs.getInt("priority"),
						rs.getString("bundle"),
						(long) rs.getDouble("since"),
						rs.getString("link")));
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return alerts;
	}

	private ArrayList<OpsTrendPoint> getSubmissionsTrend(Connection sd, int oId) throws SQLException {
		ArrayList<OpsTrendPoint> trend = new ArrayList<>();
		String sql = "with days as (select generate_series("
				+ "date_trunc('day', now()) - '" + trendDays + " day'::interval, "
				+ "date_trunc('day', now()), '1 day'::interval) as day) "
				+ "select to_char(days.day, 'YYYY-MM-DD') as day, count(ue.ue_id) as cnt "
				+ "from days left join upload_event ue "
				+ "on date_trunc('day', ue.upload_time) = days.day "
				+ "and ue.o_id = ? and ue.status = 'success' and not ue.incomplete "
				+ surveyIdRbacSql("ue.s_id")
				+ "group by 1 order by 1 asc";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			if(!securityManager) {
				pstmt.setString(2, requestingUser);
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				trend.add(new OpsTrendPoint(rs.getString("day"), rs.getLong("cnt")));
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return trend;
	}

	/*
	 * Per-unit (role) workload. Open cases come from the results tables (same population as the
	 * KPI - excludes closed/bad), attributed to roles via each owner's user_role; tasks come from
	 * assignments. An owner in several roles contributes to each. Owners in no role roll up into a
	 * synthetic "No unit" row. Only roles that currently own open work are returned.
	 */
	private ArrayList<OpsUnit> getUnits(Connection sd, Connection cResults, int oId) throws SQLException {

		Map<String, OpsUnit> units = new LinkedHashMap<>();

		// 1. Open-case counts per owner, from the results tables (accessible bundles only)
		Map<String, Integer> ownerCounts = new HashMap<>();
		for(String gsi : getCaseBundles(sd, oId)) {
			try {
				String table = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, gsi);
				if(table == null) {
					continue;
				}
				GeneralUtilityMethods.ensureTableCurrent(cResults, table, true);
				ensureCaseIndexes(cResults, table);
				PreparedStatement p = null;
				try {
					p = cResults.prepareStatement("select _assigned, count(*) as cnt from " + table
							+ " where not _bad and _case_closed is null and _assigned is not null and _assigned <> '' "
							+ "group by _assigned");
					ResultSet rs = p.executeQuery();
					while(rs.next()) {
						ownerCounts.merge(rs.getString("_assigned"), rs.getInt("cnt"), Integer::sum);
					}
				} finally {
					try { if(p != null) p.close(); } catch(Exception e) {}
				}
			} catch (Exception e) {
				log.log(Level.WARNING, "Ops units: skipping bundle " + gsi + ": " + e.getMessage());
			}
		}

		// 2. Owner ident -> roles held (org)
		Map<String, ArrayList<String>> ownerRoles = new HashMap<>();
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select u.ident, r.name from users u "
					+ "join user_role ur on ur.u_id = u.id join role r on r.id = ur.r_id where u.o_id = ?");
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				ownerRoles.computeIfAbsent(rs.getString("ident"), k -> new ArrayList<>()).add(rs.getString("name"));
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}

		// 3. Roles the viewer may see (non security manager only)
		java.util.Set<String> viewerRoles = securityManager ? null : getViewerRoleNames(sd, oId);

		// 4. Attribute owner case counts to roles; owners in no role -> "No unit"
		int noUnit = 0;
		for(Map.Entry<String, Integer> e : ownerCounts.entrySet()) {
			ArrayList<String> roles = ownerRoles.get(e.getKey());
			if(roles == null || roles.isEmpty()) {
				noUnit += e.getValue();
			} else {
				for(String role : roles) {
					if(viewerRoles != null && !viewerRoles.contains(role)) {
						continue;	// hide roles the viewer is not in
					}
					units.computeIfAbsent(role, OpsUnit::new).openCases += e.getValue();
				}
			}
		}

		// 5. Open / overdue tasks by role (restricted to the viewer's roles unless security manager)
		String taskSql = "select r.name as role, "
				+ "count(*) filter (where a.status in ('unsent','accepted')) as open_tasks, "
				+ "count(*) filter (where a.status in ('unsent','accepted') and t.schedule_finish is not null and t.schedule_finish < now()) as overdue "
				+ "from assignments a "
				+ "join tasks t on t.id = a.task_id and not t.deleted "
				+ "join project p on p.id = t.p_id and p.o_id = ? "
				+ "join users u on u.id = a.assignee "
				+ "join user_role ur on ur.u_id = u.id "
				+ "join role r on r.id = ur.r_id "
				+ "where true "
				+ roleMembershipSql("r.id")
				+ "group by r.name";
		pstmt = null;
		try {
			pstmt = sd.prepareStatement(taskSql);
			int tIdx = 1;
			pstmt.setInt(tIdx++, oId);
			if(!securityManager) {
				pstmt.setString(tIdx++, requestingUser);	// roleMembershipSql
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				OpsUnit unit = units.computeIfAbsent(rs.getString("role"), OpsUnit::new);
				unit.openTasks = rs.getInt("open_tasks");
				unit.overdue = rs.getInt("overdue");
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}

		// 6. Build output (hide idle roles)
		ArrayList<OpsUnit> out = new ArrayList<>();
		for(OpsUnit unit : units.values()) {
			if(unit.openCases == 0 && unit.openTasks == 0) {
				continue;
			}
			if(unit.openTasks > 0) {
				unit.overduePct = (unit.overdue * 100.0) / unit.openTasks;
			}
			unit.rag = ragForPct(unit.overduePct);
			out.add(unit);
		}

		// 7. No-unit reconciliation row (owners in no role)
		if(noUnit > 0) {
			OpsUnit nu = new OpsUnit(localise("ops_unit_none", "No unit"));
			nu.openCases = noUnit;
			nu.aggregate = true;
			nu.itemType = "no_unit";
			nu.rag = "none";
			out.add(nu);
		}

		return out;
	}

	private java.util.Set<String> getViewerRoleNames(Connection sd, int oId) throws SQLException {
		java.util.Set<String> names = new java.util.HashSet<>();
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select r.name from role r join user_role ur on ur.r_id = r.id "
					+ "join users u on u.id = ur.u_id where r.o_id = ? and u.ident = ?");
			pstmt.setInt(1, oId);
			pstmt.setString(2, requestingUser);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				names.add(rs.getString(1));
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return names;
	}

	// ---------------------------------------------------------------------------
	// Operations Summary XLSX (scheduled email digest - Phase 4)
	// ---------------------------------------------------------------------------

	/*
	 * Write the org Operations Summary as an XLSX workbook (Summary, Units, At-risk, Alerts).
	 * Reuses getOverview + getItems. Used by the periodic notification digest.
	 */
	public void writeSummaryXlsx(Connection sd, Connection cResults, int oId, OutputStream out) throws Exception {

		OpsOverview ov = getOverview(sd, cResults, oId, null);		// digest: org-level report, full access
		ArrayList<OpsItem> items = getItems(sd, cResults, oId, "all", null);

		SXSSFWorkbook wb = new SXSSFWorkbook(10);
		try {
			// Summary (KPIs)
			Sheet summary = wb.createSheet(localise("ops_xls_summary", "Summary"));
			int r = 0;
			Row gen = summary.createRow(r++);
			setCell(gen, 0, localise("ops_generated", "Updated"));
			setCell(gen, 1, ov.generatedAt);
			r++;
			Row sh = summary.createRow(r++);
			setCell(sh, 0, localise("ops_item", "Item"));
			setCell(sh, 1, localise("ops_xls_value", "Value"));
			setCell(sh, 2, localise("ops_xls_status", "Status"));
			for(OpsKpi k : ov.kpis) {
				Row row = summary.createRow(r++);
				setCell(row, 0, k.label);
				setCell(row, 1, k.value);
				setCell(row, 2, k.rag);
			}

			// Units
			Sheet units = wb.createSheet(localise("ops_units", "Units (roles)"));
			r = 0;
			Row uh = units.createRow(r++);
			setCell(uh, 0, localise("ops_role", "Role"));
			setCell(uh, 1, localise("ops_open_cases", "Open cases"));
			setCell(uh, 2, localise("ops_open_tasks", "Open tasks"));
			setCell(uh, 3, localise("ops_tasks_overdue", "Overdue"));
			setCell(uh, 4, localise("ops_overdue_pct", "Overdue %"));
			setCell(uh, 5, localise("ops_xls_status", "Status"));
			for(OpsUnit u : ov.units) {
				Row row = units.createRow(r++);
				setCell(row, 0, u.role);
				setCell(row, 1, u.openCases);
				setCell(row, 2, u.openTasks);
				setCell(row, 3, u.overdue);
				setCell(row, 4, Math.round(u.overduePct));
				setCell(row, 5, u.rag);
			}

			// At-risk
			Sheet atRisk = wb.createSheet(localise("ops_at_risk", "At-risk items"));
			r = 0;
			Row ah = atRisk.createRow(r++);
			setCell(ah, 0, localise("ops_xls_type", "Type"));
			setCell(ah, 1, localise("ops_item", "Item"));
			setCell(ah, 2, localise("ops_xls_bundle", "Bundle"));
			setCell(ah, 3, localise("ops_assignee", "Assignee"));
			setCell(ah, 4, localise("ops_age_days", "Age (days)"));
			setCell(ah, 5, localise("ops_xls_status", "Status"));
			for(OpsItem it : items) {
				Row row = atRisk.createRow(r++);
				setCell(row, 0, it.type);
				setCell(row, 1, it.title);
				setCell(row, 2, it.bundle);
				setCell(row, 3, it.assignee);
				setCell(row, 4, it.ageDays);
				setCell(row, 5, it.status);
			}

			// Alerts
			Sheet alerts = wb.createSheet(localise("ops_alerts", "Open alerts"));
			r = 0;
			Row alh = alerts.createRow(r++);
			setCell(alh, 0, localise("ops_xls_priority", "Priority"));
			setCell(alh, 1, localise("ops_xls_message", "Message"));
			setCell(alh, 2, localise("ops_xls_bundle", "Bundle"));
			setCell(alh, 3, localise("ops_age_days", "Age (days)"));
			for(OpsAlert al : ov.alerts) {
				Row row = alerts.createRow(r++);
				setCell(row, 0, al.priority);
				setCell(row, 1, al.message);
				setCell(row, 2, al.bundle);
				setCell(row, 3, al.sinceSeconds / 86400);
			}

			wb.write(out);
		} finally {
			try { wb.dispose(); } catch (Exception e) {}
			try { wb.close(); } catch (Exception e) {}
		}
	}

	private void setCell(Row row, int col, String value) {
		Cell c = row.createCell(col);
		c.setCellValue(value == null ? "" : value);
	}

	private void setCell(Row row, int col, long value) {
		Cell c = row.createCell(col);
		c.setCellValue(value);
	}

	// ---------------------------------------------------------------------------
	// Org settings
	// ---------------------------------------------------------------------------

	private void loadSettings(Connection sd, int oId) {
		OpsSettings s = getSettings(sd, oId);
		trendDays = s.trendDays > 0 ? s.trendDays : DEFAULT_TREND_DAYS;
		staleDays = s.staleIntervalDays > 0 ? s.staleIntervalDays : DEFAULT_STALE_DAYS;
		ragAmber = s.ragAmberPct;
		ragRed = s.ragRedPct;
	}

	public OpsSettings getSettings(Connection sd, int oId) {
		OpsSettings s = new OpsSettings();
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select settings from ops_settings where o_id = ?");
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String j = rs.getString(1);
				if(j != null) {
					OpsSettings parsed = new Gson().fromJson(j, OpsSettings.class);
					if(parsed != null) {
						s = parsed;
					}
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "Ops settings read failed: " + e.getMessage());
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return s;
	}

	public void saveSettings(Connection sd, int oId, OpsSettings s, String user) throws SQLException {
		String json = new GsonBuilder().disableHtmlEscaping().create().toJson(s);
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("insert into ops_settings (o_id, settings, changed_by, changed_ts) "
					+ "values (?, ?::jsonb, ?, now()) "
					+ "on conflict (o_id) do update set settings = excluded.settings, "
					+ "changed_by = excluded.changed_by, changed_ts = excluded.changed_ts");
			pstmt.setInt(1, oId);
			pstmt.setString(2, json);
			pstmt.setString(3, user);
			pstmt.executeUpdate();
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		// Invalidate any cached snapshots for this org (all users)
		cache.keySet().removeIf(k -> k.startsWith(oId + ":"));
		unitCache.keySet().removeIf(k -> k.startsWith(oId + ":"));
	}

	// ---------------------------------------------------------------------------
	// L2 - org-wide at-risk record list
	// ---------------------------------------------------------------------------

	/*
	 * Org-wide record list behind an L0 tile. type =
	 *   overdue       - overdue tasks
	 *   in_progress   - open (unsent/accepted) tasks
	 *   stale         - open cases older than the stale interval
	 *   open_cases    - all open cases
	 *   unassigned    - open cases with no owner
	 *   alerts        - open case-management alerts
	 *   all (default) - overdue tasks + stale cases (used by the digest)
	 */
	public ArrayList<OpsItem> getItems(Connection sd, Connection cResults, int oId, String type, String user) throws SQLException {
		loadContext(sd, oId, user, false);
		ArrayList<OpsItem> items = new ArrayList<>();
		if(type == null) {
			type = "all";
		}

		if(type.equals("overdue")) {
			getTaskItems(sd, oId, items, true);
		} else if(type.equals("in_progress")) {
			getTaskItems(sd, oId, items, false);
		} else if(type.equals("alerts")) {
			getAlertItemsOrg(sd, oId, items);
		} else if(type.equals("no_unit")) {
			getNoUnitItems(sd, cResults, oId, items);
		} else if(type.equals("stale") || type.equals("open_cases") || type.equals("unassigned")) {
			collectCasesAllBundles(sd, cResults, oId, items, type);
		} else {	// all (legacy / digest)
			getTaskItems(sd, oId, items, true);
			collectCasesAllBundles(sd, cResults, oId, items, "stale");
		}
		return items;
	}

	private void collectCasesAllBundles(Connection sd, Connection cResults, int oId,
			java.util.List<OpsItem> items, String mode) throws SQLException {
		ArrayList<String> bundles = getCaseBundles(sd, oId);
		for(String gsi : bundles) {
			try {
				String table = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, gsi);
				if(table == null) {
					continue;
				}
				GeneralUtilityMethods.ensureTableCurrent(cResults, table, true);
				ensureCaseIndexes(cResults, table);
				collectCaseItemsOrg(sd, cResults, table, gsi, items, mode);
			} catch (Exception e) {
				log.log(Level.WARNING, "Ops items: skipping bundle " + gsi + ": " + e.getMessage());
			}
		}
	}

	/*
	 * Tasks for the org. overdueOnly=true -> past due; false -> all in-progress.
	 */
	private void getTaskItems(Connection sd, int oId, java.util.List<OpsItem> items, boolean overdueOnly) throws SQLException {
		StringBuilder sql = new StringBuilder("select t.title, t.tg_id, t.survey_name, a.assignee_name, "
				+ "(t.schedule_finish is not null and t.schedule_finish < now()) as overdue, "
				+ "extract(epoch from (now() - t.schedule_finish)) as overdue_age, "
				+ "extract(epoch from (now() - t.created_at)) as created_age "
				+ "from assignments a join tasks t on t.id = a.task_id and not t.deleted "
				+ "join project p on p.id = t.p_id and p.o_id = ? "
				+ "where a.status in ('unsent','accepted') ");
		if(overdueOnly) {
			sql.append("and t.schedule_finish is not null and t.schedule_finish < now() ");
		}
		sql.append(surveyIdentRbacSql("t.survey_ident"));
		sql.append("order by t.schedule_finish asc nulls last limit " + ATRISK_LIMIT);
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql.toString());
			pstmt.setInt(1, oId);
			if(!securityManager) {
				pstmt.setString(2, requestingUser);
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String title = rs.getString("title");
				String surveyName = rs.getString("survey_name");
				if(title == null || title.trim().isEmpty()) {
					title = surveyName;
				}
				boolean overdue = rs.getBoolean("overdue");
				long ageDays = (long) ((overdue ? rs.getDouble("overdue_age") : rs.getDouble("created_age")) / 86400);
				String link = "/app/tasks/taskManagement.html?tg_id=" + rs.getInt("tg_id");
				OpsItem it = new OpsItem("task", title, surveyName, rs.getString("assignee_name"), ageDays, overdue, link);
				it.status = overdue ? "overdue" : "in_progress";
				items.add(it);
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
	}

	/*
	 * Open cases for one bundle. mode = stale | open_cases | unassigned.
	 */
	private void collectCaseItemsOrg(Connection sd, Connection cResults, String table, String gsi,
			java.util.List<OpsItem> items, String mode) throws SQLException {
		String bundleName = getSurveyDisplayName(sd, gsi);
		StringBuilder sql = new StringBuilder("select prikey, instancename, _hrk, _assigned, instanceid, _s_id, "
				+ "extract(epoch from (now() - _thread_created)) as age "
				+ "from " + table + " where not _bad and _case_closed is null ");
		boolean stale = mode.equals("stale");
		if(stale) {
			sql.append("and _thread_created < now() - ?::interval ");
		} else if(mode.equals("unassigned")) {
			sql.append("and (_assigned is null or _assigned = '') ");
		}
		sql.append("order by _thread_created asc limit " + ATRISK_LIMIT);
		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql.toString());
			if(stale) {
				pstmt.setString(1, staleInterval());
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String instanceName = rs.getString("instancename");
				String hrk = rs.getString("_hrk");
				int prikey = rs.getInt("prikey");
				String label = (bundleName == null ? "" : bundleName) + " - ";
				if(instanceName != null && instanceName.trim().length() > 0) {
					label += instanceName;
				} else if(hrk != null && hrk.trim().length() > 0) {
					label += hrk;
				} else {
					label += prikey;
				}
				long ageDays = (long) (rs.getDouble("age") / 86400);
				OpsItem it = new OpsItem("case", label, bundleName, rs.getString("_assigned"),
						ageDays, false, caseLink(gsi, rs.getString("instanceid")));
				if(mode.equals("unassigned")) {
					it.status = "unassigned";
				} else if(stale) {
					it.status = "stale";
				} else {	// open_cases - flag the old ones as stale
					it.status = ageDays >= staleDays ? "stale" : "open";
				}
				items.add(it);
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
	}

	/*
	 * Open cases (org-wide) whose owner is in no role - the records behind the "No unit" row.
	 */
	private void getNoUnitItems(Connection sd, Connection cResults, int oId, java.util.List<OpsItem> items) throws SQLException {
		java.util.Set<String> withRole = getUsersWithRole(sd, oId);
		for(String gsi : getCaseBundles(sd, oId)) {
			try {
				String table = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, gsi);
				if(table == null) {
					continue;
				}
				GeneralUtilityMethods.ensureTableCurrent(cResults, table, true);
				ensureCaseIndexes(cResults, table);
				String bundleName = getSurveyDisplayName(sd, gsi);
				PreparedStatement p = null;
				try {
					p = cResults.prepareStatement("select prikey, instancename, _hrk, _assigned, instanceid, "
							+ "extract(epoch from (now() - _thread_created)) as age "
							+ "from " + table + " where not _bad and _case_closed is null "
							+ "and _assigned is not null and _assigned <> '' "
							+ "order by _thread_created asc limit " + ATRISK_LIMIT);
					ResultSet rs = p.executeQuery();
					while(rs.next()) {
						String owner = rs.getString("_assigned");
						if(withRole.contains(owner)) {
							continue;	// owner has a role -> belongs to a unit, not "No unit"
						}
						String instanceName = rs.getString("instancename");
						String hrk = rs.getString("_hrk");
						int prikey = rs.getInt("prikey");
						String label = (bundleName == null ? "" : bundleName) + " - ";
						if(instanceName != null && instanceName.trim().length() > 0) {
							label += instanceName;
						} else if(hrk != null && hrk.trim().length() > 0) {
							label += hrk;
						} else {
							label += prikey;
						}
						long ageDays = (long) (rs.getDouble("age") / 86400);
						OpsItem it = new OpsItem("case", label, bundleName, owner,
								ageDays, false, caseLink(gsi, rs.getString("instanceid")));
						it.status = ageDays >= staleDays ? "stale" : "open";
						items.add(it);
					}
				} finally {
					try { if(p != null) p.close(); } catch(Exception e) {}
				}
			} catch (Exception e) {
				log.log(Level.WARNING, "Ops no-unit items: skipping bundle " + gsi + ": " + e.getMessage());
			}
		}
	}

	private java.util.Set<String> getUsersWithRole(Connection sd, int oId) throws SQLException {
		java.util.Set<String> idents = new java.util.HashSet<>();
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select distinct u.ident from users u "
					+ "join user_role ur on ur.u_id = u.id where u.o_id = ?");
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				idents.add(rs.getString(1));
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return idents;
	}

	/*
	 * Open case-management alerts presented as items.
	 */
	private void getAlertItemsOrg(Connection sd, int oId, java.util.List<OpsItem> items) throws SQLException {
		String sql = "select a.message, a.priority, a.link, s.display_name as bundle, "
				+ "extract(epoch from (now() - a.created_time)) as age "
				+ "from alert a "
				+ "join users u on u.id = a.u_id and u.o_id = ? "
				+ "left join survey s on s.s_id = a.s_id "
				+ "where a.status = 'open' "
				+ alertRbacSql()
				+ "order by a.priority asc, a.created_time asc limit " + ATRISK_LIMIT;
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			if(!securityManager) {
				pstmt.setString(2, requestingUser);
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				long ageDays = (long) (rs.getDouble("age") / 86400);
				OpsItem it = new OpsItem("alert", rs.getString("message"), rs.getString("bundle"),
						null, ageDays, false, rs.getString("link"));
				it.status = "alert";
				items.add(it);
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
	}

	// ---------------------------------------------------------------------------
	// L1 - single unit (role) drill-down
	// ---------------------------------------------------------------------------

	private static class RoleMembers {
		ArrayList<String> idents = new ArrayList<>();
		ArrayList<Integer> ids = new ArrayList<>();
	}

	/*
	 * Build the L1 detail for one role: open figures, throughput (cases + tasks),
	 * average cycle times, 30-day trend and the at-risk item list.
	 */
	public OpsUnitDetail getUnitDetail(Connection sd, Connection cResults, int oId, String role, String user) throws SQLException {

		loadContext(sd, oId, user, false);

		OpsUnitDetail d = new OpsUnitDetail(role);
		d.generatedAt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());

		// A non security manager may only view a unit (role) they belong to
		if(!isRoleMember(sd, oId, role)) {
			d.rag = ragForPct(0);
			return d;
		}

		RoleMembers m = getRoleMembers(sd, oId, role);

		// Tasks: open / overdue / avg cycle / completed trend / overdue at-risk items
		if(!m.ids.isEmpty()) {
			getUnitTaskFigures(sd, d, m);
		}

		// Cases: per-bundle results scans filtered to the role's user idents
		int n = trendDays + 1;
		long[] opened = new long[n];
		long[] closed = new long[n];
		String[] daysArr = new String[n];
		long cycleSumSec = 0;
		long cycleCnt = 0;

		if(!m.idents.isEmpty()) {
			ArrayList<String> bundles = getCaseBundles(sd, oId);
			for(String gsi : bundles) {
				try {
					String table = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, gsi);
					if(table == null) {
						continue;
					}
					GeneralUtilityMethods.ensureTableCurrent(cResults, table, true);
					ensureCaseIndexes(cResults, table);

					d.openCases += countOpenCasesForUsers(cResults, table, m.idents);
					accumulateCaseSeries(cResults, table, m.idents, daysArr, opened, closed);
					long[] cyc = caseCycleForUsers(cResults, table, m.idents);
					cycleSumSec += cyc[0];
					cycleCnt += cyc[1];
					collectOpenCasesForRole(sd, cResults, table, gsi, m.idents, d.atRisk);
				} catch (Exception e) {
					log.log(Level.WARNING, "Ops unit: skipping bundle " + gsi + ": " + e.getMessage());
				}
			}
		}

		int net = 0;
		for(int i = 0; i < n; i++) {
			if(daysArr[i] == null) {
				continue;
			}
			d.casesClosed.add(new OpsTrendPoint(daysArr[i], closed[i]));
			net += (int) (opened[i] - closed[i]);
			d.backlog.add(new OpsBacklogPoint(daysArr[i], (int) opened[i], (int) closed[i], net));
		}
		if(cycleCnt > 0) {
			d.avgCycleDaysCases = (cycleSumSec / (double) cycleCnt) / 86400.0;
		}

		if(d.openTasks > 0) {
			d.overduePct = (d.overdue * 100.0) / d.openTasks;
		}
		d.rag = ragForPct(d.overduePct);

		if(d.atRisk.size() > ATRISK_LIMIT) {
			d.atRisk = new ArrayList<>(d.atRisk.subList(0, ATRISK_LIMIT));
		}

		return d;
	}

	private RoleMembers getRoleMembers(Connection sd, int oId, String role) throws SQLException {
		RoleMembers m = new RoleMembers();
		String sql = "select u.id, u.ident from users u "
				+ "join user_role ur on ur.u_id = u.id "
				+ "join role r on r.id = ur.r_id "
				+ "where r.o_id = ? and r.name = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, role);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				m.ids.add(rs.getInt(1));
				m.idents.add(rs.getString(2));
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return m;
	}

	private void getUnitTaskFigures(Connection sd, OpsUnitDetail d, RoleMembers m) throws SQLException {
		String ph = placeholders(m.ids.size());

		// counts + avg cycle
		String sql = "select "
				+ "count(*) filter (where a.status in ('unsent','accepted')) as open_tasks, "
				+ "count(*) filter (where a.status in ('unsent','accepted') and t.schedule_finish is not null and t.schedule_finish < now()) as overdue, "
				+ "coalesce(extract(epoch from avg(a.completed_date - a.assigned_date) "
				+ "  filter (where a.status = 'submitted' and a.completed_date is not null and a.assigned_date is not null)), 0) as cyc_sec "
				+ "from assignments a join tasks t on t.id = a.task_id and not t.deleted "
				+ "where a.assignee in (" + ph + ")";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			setInts(pstmt, 1, m.ids);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				d.openTasks = rs.getInt("open_tasks");
				d.overdue = rs.getInt("overdue");
				d.avgCycleDaysTasks = rs.getLong("cyc_sec") / 86400.0;
			}
			pstmt.close();

			// completed trend (30 days)
			String trendSql = "with days as (select generate_series("
					+ "date_trunc('day', now()) - '" + trendDays + " day'::interval, "
					+ "date_trunc('day', now()), '1 day'::interval) as day) "
					+ "select to_char(days.day, 'YYYY-MM-DD') as day, count(a.id) as cnt "
					+ "from days left join assignments a "
					+ "on date_trunc('day', a.completed_date) = days.day and a.status = 'submitted' and a.assignee in (" + ph + ") "
					+ "group by 1 order by 1 asc";
			pstmt = sd.prepareStatement(trendSql);
			setInts(pstmt, 1, m.ids);
			rs = pstmt.executeQuery();
			while(rs.next()) {
				d.tasksCompleted.add(new OpsTrendPoint(rs.getString("day"), rs.getLong("cnt")));
			}
			pstmt.close();

			// All open tasks for the unit (overdue ones flagged) - listed in full so the list
			// reconciles with the open-task / overdue counts above
			String itemSql = "select t.title, t.tg_id, t.survey_name, a.assignee_name, "
					+ "(t.schedule_finish is not null and t.schedule_finish < now()) as overdue, "
					+ "extract(epoch from (now() - t.schedule_finish)) as overdue_age, "
					+ "extract(epoch from (now() - t.created_at)) as created_age "
					+ "from assignments a join tasks t on t.id = a.task_id and not t.deleted "
					+ "where a.assignee in (" + ph + ") and a.status in ('unsent','accepted') "
					+ "order by t.schedule_finish asc nulls last limit " + ATRISK_LIMIT;
			pstmt = sd.prepareStatement(itemSql);
			setInts(pstmt, 1, m.ids);
			rs = pstmt.executeQuery();
			while(rs.next()) {
				String title = rs.getString("title");
				String surveyName = rs.getString("survey_name");
				if(title == null || title.trim().isEmpty()) {
					title = surveyName;
				}
				boolean overdue = rs.getBoolean("overdue");
				long ageDays = (long) ((overdue ? rs.getDouble("overdue_age") : rs.getDouble("created_age")) / 86400);
				String link = "/app/tasks/taskManagement.html?tg_id=" + rs.getInt("tg_id");
				OpsItem it = new OpsItem("task", title, surveyName, rs.getString("assignee_name"), ageDays, overdue, link);
				it.status = overdue ? "overdue" : "in_progress";
				d.atRisk.add(it);
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
	}

	private int countOpenCasesForUsers(Connection cResults, String table, ArrayList<String> idents) throws SQLException {
		String ph = placeholders(idents.size());
		String sql = "select count(*) from " + table + " where not _bad and _case_closed is null and _assigned in (" + ph + ")";
		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql);
			setStrings(pstmt, 1, idents);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				return rs.getInt(1);
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return 0;
	}

	private void accumulateCaseSeries(Connection cResults, String table, ArrayList<String> idents,
			String[] daysArr, long[] opened, long[] closed) throws SQLException {
		String ph = placeholders(idents.size());
		String cte = "with days as (select generate_series("
				+ "date_trunc('day', now()) - '" + trendDays + " day'::interval, "
				+ "date_trunc('day', now()), '1 day'::interval) as day) ";
		String openedSql = cte + "select to_char(days.day,'YYYY-MM-DD') as day, count(t.prikey) as cnt from days left join "
				+ table + " t on date_trunc('day', t._thread_created) = days.day and t._assigned in (" + ph + ") group by 1 order by 1 asc";
		String closedSql = cte + "select to_char(days.day,'YYYY-MM-DD') as day, count(t.prikey) as cnt from days left join "
				+ table + " t on date_trunc('day', t._case_closed) = days.day and t._assigned in (" + ph + ") group by 1 order by 1 asc";
		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(openedSql);
			setStrings(pstmt, 1, idents);
			ResultSet rs = pstmt.executeQuery();
			int i = 0;
			while(rs.next() && i <= trendDays) {
				daysArr[i] = rs.getString("day");
				opened[i] += rs.getLong("cnt");
				i++;
			}
			pstmt.close();
			pstmt = cResults.prepareStatement(closedSql);
			setStrings(pstmt, 1, idents);
			rs = pstmt.executeQuery();
			i = 0;
			while(rs.next() && i <= trendDays) {
				closed[i] += rs.getLong("cnt");
				i++;
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
	}

	private long[] caseCycleForUsers(Connection cResults, String table, ArrayList<String> idents) throws SQLException {
		String ph = placeholders(idents.size());
		String sql = "select coalesce(extract(epoch from sum(_case_closed - _thread_created)),0) as sum_sec, count(*) as cnt "
				+ "from " + table + " where _assigned in (" + ph + ") and _case_closed is not null and _thread_created is not null";
		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql);
			setStrings(pstmt, 1, idents);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				return new long[] { (long) rs.getDouble("sum_sec"), rs.getLong("cnt") };
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return new long[] { 0, 0 };
	}

	/*
	 * List ALL open cases owned by the role's members in one bundle (status open, or stale when
	 * older than the stale interval). Listing every open case keeps the list consistent with the
	 * unit's open-case count.
	 */
	private void collectOpenCasesForRole(Connection sd, Connection cResults, String table, String gsi,
			ArrayList<String> idents, java.util.List<OpsItem> items) throws SQLException {
		String bundleName = getSurveyDisplayName(sd, gsi);
		String ph = placeholders(idents.size());
		String sql = "select prikey, instancename, _hrk, _assigned, instanceid, "
				+ "extract(epoch from (now() - _thread_created)) as age "
				+ "from " + table + " where not _bad and _case_closed is null and _assigned in (" + ph + ") "
				+ "order by _thread_created asc limit " + ATRISK_LIMIT;
		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement(sql);
			setStrings(pstmt, 1, idents);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String instanceName = rs.getString("instancename");
				String hrk = rs.getString("_hrk");
				int prikey = rs.getInt("prikey");
				String label = (bundleName == null ? "" : bundleName) + " - ";
				if(instanceName != null && instanceName.trim().length() > 0) {
					label += instanceName;
				} else if(hrk != null && hrk.trim().length() > 0) {
					label += hrk;
				} else {
					label += prikey;
				}
				long ageDays = (long) (rs.getDouble("age") / 86400);
				OpsItem it = new OpsItem("case", label, bundleName, rs.getString("_assigned"),
						ageDays, false, caseLink(gsi, rs.getString("instanceid")));
				it.status = ageDays >= staleDays ? "stale" : "open";
				items.add(it);
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
	}

	/*
	 * Deep link to a specific case record in the managed-forms console.
	 * managed_forms.html parses id (survey) + instanceid and opens the single-record view,
	 * deriving the project from the survey. Falls back to the console list if either is missing.
	 */
	private String caseLink(String groupSurveyIdent, String instanceid) {
		if(groupSurveyIdent != null && instanceid != null && !instanceid.isEmpty()) {
			return "/app/operations_case.html?survey=" + groupSurveyIdent + "&instanceid=" + instanceid;
		}
		return "/app/operations_case.html";
	}

	// ---------------------------------------------------------------------------
	// Org-scoped single-case viewer (Phase 5)
	// ---------------------------------------------------------------------------

	// Result-table columns not shown as data fields in the case viewer
	private static final java.util.Set<String> CASE_SKIP_COLS = new java.util.HashSet<>(java.util.Arrays.asList(
			"prikey", "parkey", "instanceid", "the_geom"));

	/*
	 * Fetch one case record (org membership must be validated by the caller).
	 */
	public OpsCase getCase(Connection sd, Connection cResults, int oId, String groupSurveyIdent, String instanceid, String user) throws Exception {
		loadContext(sd, oId, user, false);
		if(!isBundleAccessible(sd, groupSurveyIdent)) {
			throw new org.smap.sdal.Utilities.AuthorisationException();
		}
		OpsCase c = new OpsCase(groupSurveyIdent, instanceid);
		c.bundle = getSurveyDisplayName(sd, groupSurveyIdent);

		String table = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, groupSurveyIdent);
		if(table == null) {
			return c;
		}
		GeneralUtilityMethods.ensureTableCurrent(cResults, table, true);

		String instanceName = null;
		String hrk = null;
		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement("select * from " + table + " where instanceid = ? and not _bad limit 1");
			pstmt.setString(1, instanceid);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				ResultSetMetaData md = rs.getMetaData();
				for(int i = 1; i <= md.getColumnCount(); i++) {
					String col = md.getColumnName(i);
					String val = rs.getString(i);
					if(col.equals("_assigned")) {
						c.assignee = val;
					} else if(col.equals("_case_survey")) {
						c.caseSurveyIdent = val;
					} else if(col.equals("_thread")) {
						c.thread = val;
					} else if(col.equals("_case_closed")) {
						c.closed = val != null;
					} else if(col.equals("prikey")) {
						c.prikey = rs.getInt(i);
					} else if(col.equals("instancename")) {
						instanceName = val;
					} else if(col.equals("_hrk")) {
						hrk = val;
					}
					if(!CASE_SKIP_COLS.contains(col) && !col.startsWith("_") && val != null && !val.isEmpty()) {
						c.fields.add(new OpsField(col, val));
					}
				}
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}

		String t = (c.bundle == null ? "" : c.bundle) + " - ";
		if(instanceName != null && instanceName.trim().length() > 0) {
			t += instanceName;
		} else if(hrk != null && hrk.trim().length() > 0) {
			t += hrk;
		} else {
			t += c.prikey;
		}
		c.title = t;
		if(c.caseSurveyIdent == null) {
			c.caseSurveyIdent = groupSurveyIdent;
		}
		return c;
	}

	/*
	 * Assign or release a case (org membership validated by the caller).
	 * type = assign | release.
	 */
	public int assignCase(Connection sd, Connection cResults, int oId, String groupSurveyIdent, String instanceid,
			String assignTo, String type, String requestingUser) throws Exception {
		loadContext(sd, oId, requestingUser, false);
		if(!isBundleAccessible(sd, groupSurveyIdent)) {
			throw new org.smap.sdal.Utilities.AuthorisationException();
		}
		String table = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, groupSurveyIdent);
		if(table == null) {
			return 0;
		}
		String caseSurvey = groupSurveyIdent;
		PreparedStatement pstmt = null;
		try {
			pstmt = cResults.prepareStatement("select _case_survey from " + table + " where instanceid = ? limit 1");
			pstmt.setString(1, instanceid);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next() && rs.getString(1) != null) {
				caseSurvey = rs.getString(1);
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}

		CaseManager cm = new CaseManager(localisation);
		return cm.assignRecord(sd, cResults, localisation, table, instanceid, assignTo, type, caseSurvey, null, requestingUser);
	}

	/*
	 * Org users that a case can be assigned to.
	 */
	public ArrayList<User> getAssignableUsers(Connection sd, int oId) throws SQLException {
		ArrayList<User> users = new ArrayList<>();
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select ident, name from users where o_id = ? order by name asc");
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				User u = new User();
				u.ident = rs.getString("ident");
				u.name = rs.getString("name");
				users.add(u);
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return users;
	}

	private String getSurveyDisplayName(Connection sd, String ident) throws SQLException {
		String name = null;
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select display_name from survey where ident = ? limit 1");
			pstmt.setString(1, ident);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				name = rs.getString(1);
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		return name;
	}

	private String placeholders(int n) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < n; i++) {
			sb.append(i == 0 ? "?" : ",?");
		}
		return sb.toString();
	}

	private void setStrings(PreparedStatement p, int start, ArrayList<String> vals) throws SQLException {
		for(int i = 0; i < vals.size(); i++) {
			p.setString(start + i, vals.get(i));
		}
	}

	private void setInts(PreparedStatement p, int start, ArrayList<Integer> vals) throws SQLException {
		for(int i = 0; i < vals.size(); i++) {
			p.setInt(start + i, vals.get(i));
		}
	}

	/*
	 * Lazily index the date columns the trend / stale queries aggregate on, once per table
	 * per process. CREATE INDEX IF NOT EXISTS is a cheap catalog check when already present.
	 */
	private void ensureCaseIndexes(Connection cResults, String table) {
		if(indexedTables.contains(table)) {
			return;
		}
		PreparedStatement pstmt = null;
		try {
			String tc = "create index if not exists " + table + "_tc_idx on " + table + " (_thread_created)";
			String cc = "create index if not exists " + table + "_cc_idx on " + table + " (_case_closed)";
			pstmt = cResults.prepareStatement(tc);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = cResults.prepareStatement(cc);
			pstmt.executeUpdate();
		} catch (Exception e) {
			log.log(Level.WARNING, "Ops overview: could not ensure indexes on " + table + ": " + e.getMessage());
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}
		indexedTables.add(table);
	}

	private String ragForPct(double pct) {
		if(pct >= ragRed) {
			return "red";
		} else if(pct >= ragAmber) {
			return "amber";
		}
		return "green";
	}

	private String localise(String key, String fallback) {
		try {
			if(localisation != null && localisation.containsKey(key)) {
				return localisation.getString(key);
			}
		} catch (Exception e) {
		}
		return fallback;
	}
}
