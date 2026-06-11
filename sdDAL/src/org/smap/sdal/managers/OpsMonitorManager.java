package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.CaseCount;
import org.smap.sdal.model.OpsAlert;
import org.smap.sdal.model.OpsBacklogPoint;
import org.smap.sdal.model.OpsKpi;
import org.smap.sdal.model.OpsOverview;
import org.smap.sdal.model.OpsTrendPoint;
import org.smap.sdal.model.OpsUnit;

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

	// Agreed Phase 1 defaults (org-configurable in a later phase) - see B.0.1
	private static final int TREND_DAYS = 30;
	private static final String STALE_INTERVAL = "14 days";	// anything open older than this is "stale"
	private static final double RAG_AMBER_PCT = 10.0;		// overdue % of open work
	private static final double RAG_RED_PCT = 25.0;
	private static final int ALERT_LIST_LIMIT = 20;

	// Per-org cache so reloads / polling do not re-scan results tables
	private static final long CACHE_TTL_MS = 60_000;
	private static class Cached {
		long builtAt;
		String json;		// the serialised OpsOverview
	}
	private static final ConcurrentHashMap<Integer, Cached> cache = new ConcurrentHashMap<>();

	// Result tables we have already ensured have the case-monitoring indexes (per process)
	private static final java.util.Set<String> indexedTables =
			java.util.Collections.synchronizedSet(new java.util.HashSet<String>());

	public OpsMonitorManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Return a cached serialised overview if it is still fresh, else null.
	 */
	public String getCachedOverview(int oId) {
		Cached c = cache.get(oId);
		if(c != null && (System.currentTimeMillis() - c.builtAt) < CACHE_TTL_MS) {
			return c.json;
		}
		return null;
	}

	public void putCachedOverview(int oId, String json) {
		Cached c = new Cached();
		c.builtAt = System.currentTimeMillis();
		c.json = json;
		cache.put(oId, c);
	}

	/*
	 * Build the complete overview for an organisation.
	 */
	public OpsOverview getOverview(Connection sd, Connection cResults, int oId) throws SQLException {

		OpsOverview ov = new OpsOverview();
		ov.generatedAt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());

		// 1. Case figures (open / stale / unassigned) and backlog trend, summed over the org's case bundles
		ArrayList<String> bundles = getCaseBundles(sd, oId);
		int openCases = 0;
		int staleCases = 0;
		ov.unassignedCases = 0;
		int[][] trend = new int[TREND_DAYS + 1][2];		// [day][0]=opened [day][1]=closed
		String[] trendDays = new String[TREND_DAYS + 1];

		CaseManager cm = new CaseManager(localisation);
		for(String groupSurveyIdent : bundles) {
			try {
				int[] counts = getCaseCounts(sd, cResults, groupSurveyIdent);
				openCases += counts[0];
				staleCases += counts[1];
				ov.unassignedCases += counts[2];

				ArrayList<CaseCount> cc = cm.getOpenClosed(sd, cResults, groupSurveyIdent, "day", TREND_DAYS, "day");
				for(int i = 0; i < cc.size() && i <= TREND_DAYS; i++) {
					trendDays[i] = cc.get(i).day;
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
		for(int i = 0; i <= TREND_DAYS; i++) {
			if(trendDays[i] == null) {
				continue;
			}
			net += (trend[i][0] - trend[i][1]);
			ov.backlog.add(new OpsBacklogPoint(trendDays[i], trend[i][0], trend[i][1], net));
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

		// 6. Per-unit (role) rollup - only roles that own open work
		ov.units = getUnits(sd, oId);

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
				+ "  and s.p_id = p.id and p.o_id = ? and not s.deleted)";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
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
			pstmt.setString(1, STALE_INTERVAL);
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
				+ "join project p on p.id = t.p_id and p.o_id = ?";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, STALE_INTERVAL);
			pstmt.setInt(2, oId);
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
				+ "where a.status = 'open'";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
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
				+ "order by a.priority asc, a.created_time asc "
				+ "limit " + ALERT_LIST_LIMIT;
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
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
				+ "date_trunc('day', now()) - '" + TREND_DAYS + " day'::interval, "
				+ "date_trunc('day', now()), '1 day'::interval) as day) "
				+ "select to_char(days.day, 'YYYY-MM-DD') as day, count(ue.ue_id) as cnt "
				+ "from days left join upload_event ue "
				+ "on date_trunc('day', ue.upload_time) = days.day "
				+ "and ue.o_id = ? and ue.status = 'success' and not ue.incomplete "
				+ "group by 1 order by 1 asc";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
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
	 * Per-unit (role) workload. Cases come from the record_user owner index (assigned cases only);
	 * tasks come from assignments. A user in several roles contributes to each of those roles.
	 * Only roles that currently own open work are returned.
	 */
	private ArrayList<OpsUnit> getUnits(Connection sd, int oId) throws SQLException {

		Map<String, OpsUnit> units = new LinkedHashMap<>();

		// Assigned cases by role
		String caseSql = "select r.name as role, count(*) as cnt "
				+ "from record_user ru "
				+ "join users u on u.ident = ru.assignee_ident and u.o_id = ? "
				+ "join user_role ur on ur.u_id = u.id "
				+ "join role r on r.id = ur.r_id "
				+ "where ru.access = 'owner' "
				+ "group by r.name";

		// Open / overdue tasks by role
		String taskSql = "select r.name as role, "
				+ "count(*) filter (where a.status in ('unsent','accepted')) as open_tasks, "
				+ "count(*) filter (where a.status in ('unsent','accepted') and t.schedule_finish is not null and t.schedule_finish < now()) as overdue "
				+ "from assignments a "
				+ "join tasks t on t.id = a.task_id and not t.deleted "
				+ "join project p on p.id = t.p_id and p.o_id = ? "
				+ "join users u on u.id = a.assignee "
				+ "join user_role ur on ur.u_id = u.id "
				+ "join role r on r.id = ur.r_id "
				+ "group by r.name";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(caseSql);
			pstmt.setInt(1, oId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String role = rs.getString("role");
				OpsUnit unit = units.computeIfAbsent(role, OpsUnit::new);
				unit.openCases = rs.getInt("cnt");
			}
			pstmt.close();

			pstmt = sd.prepareStatement(taskSql);
			pstmt.setInt(1, oId);
			rs = pstmt.executeQuery();
			while(rs.next()) {
				String role = rs.getString("role");
				OpsUnit unit = units.computeIfAbsent(role, OpsUnit::new);
				unit.openTasks = rs.getInt("open_tasks");
				unit.overdue = rs.getInt("overdue");
			}
		} finally {
			try { if(pstmt != null) pstmt.close(); } catch(Exception e) {}
		}

		ArrayList<OpsUnit> out = new ArrayList<>();
		for(OpsUnit unit : units.values()) {
			if(unit.openCases == 0 && unit.openTasks == 0) {
				continue;	// no open work - hide idle roles
			}
			if(unit.openTasks > 0) {
				unit.overduePct = (unit.overdue * 100.0) / unit.openTasks;
			}
			unit.rag = ragForPct(unit.overduePct);
			out.add(unit);
		}
		return out;
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
		if(pct >= RAG_RED_PCT) {
			return "red";
		} else if(pct >= RAG_AMBER_PCT) {
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
