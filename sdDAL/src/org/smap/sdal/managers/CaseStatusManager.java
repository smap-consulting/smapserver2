package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.CaseManagementSettings;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Whether a case is open or closed follows from its data: a case is closed while its status question has one of
 * the bundle's closed values.  Every change to a record, however it is made, ends here so that closing and
 * opening again have the same effects:
 *
 *   closed    _case_closed is set, the history records who had the case, and they are all taken off it
 *   reopened  _case_closed is cleared and _case_reopened set, the history records it, the people who had the
 *             case when it closed get it back, and its alerts can fire again, timed from the reopening
 *
 * The caller reads the record's state before the change with read(), makes the change, then calls apply()
 * with the new version of the record, which may be the same row.
 */
public class CaseStatusManager {

	private static Logger log = Logger.getLogger(CaseStatusManager.class.getName());

	public static final String NONE = "none";
	public static final String CLOSED = "closed";
	public static final String REOPENED = "reopened";

	public static final String REOPENED_COLUMN = "_case_reopened";

	/*
	 * A record's case state
	 */
	public static class State {
		public String status;
		public Timestamp closed;
		public Timestamp reopened;
	}

	/*
	 * What a closed or reopened event keeps, beside its description
	 */
	private static class Details {
		String instance;			// The version of the record the change made
		String from;
		String to;
		String caseSurvey;
		List<String> owners;		// Closed: who had the case.  Reopened: who got it back
		List<String> notRestored;	// Reopened: who had it but can no longer be given it
	}

	private ResourceBundle localisation;
	private Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	public CaseStatusManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * The case settings of the bundle a results table belongs to, or null if its cases do not close
	 */
	public CaseManagementSettings settingsForTable(Connection sd, String table) throws SQLException {
		String groupSurveyIdent = groupForTable(sd, table);
		if(groupSurveyIdent == null) {
			return null;
		}
		CMS cms = new CaseManager(localisation).getCaseManagementSettings(sd, groupSurveyIdent);
		if(cms == null || cms.settings == null || !cms.settings.hasClosedStatus()) {
			return null;
		}
		return cms.settings;
	}

	/*
	 * The case state of a row, before it is changed.  Null if there is no such row.
	 */
	public State read(Connection cResults, String table, int prikey, CaseManagementSettings settings) throws SQLException {
		if(settings == null || prikey <= 0 || !GeneralUtilityMethods.hasColumn(cResults, table, settings.statusQuestion)) {
			return null;
		}
		boolean hasReopened = GeneralUtilityMethods.hasColumn(cResults, table, REOPENED_COLUMN);
		String sql = "select cast(" + settings.statusQuestion + " as text), _case_closed"
				+ (hasReopened ? ", " + REOPENED_COLUMN : "")
				+ " from " + table + " where prikey = ?";
		try (PreparedStatement pstmt = cResults.prepareStatement(sql)) {
			pstmt.setInt(1, prikey);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				State s = new State();
				s.status = rs.getString(1);
				s.closed = rs.getTimestamp(2);
				s.reopened = hasReopened ? rs.getTimestamp(3) : null;
				return s;
			}
		}
		return null;
	}

	/*
	 * Bring a changed record's case state into line with its status.  before is its state before the change,
	 * null for a new record.  Returns NONE, CLOSED or REOPENED.
	 */
	public String apply(Connection sd, Connection cResults, String table, int prikey, State before,
			CaseManagementSettings settings, String user, String surveyIdent) throws SQLException {

		if(settings == null || prikey <= 0) {
			return NONE;
		}
		State now = read(cResults, table, prikey, settings);
		if(now == null) {
			return NONE;
		}
		GeneralUtilityMethods.ensureCaseReopenedColumn(cResults, table);

		boolean wasClosed = before != null && before.closed != null;
		boolean isClosed = settings.isClosed(now.status);
		String result = NONE;

		Timestamp closed;
		Timestamp reopened = before == null ? now.reopened : before.reopened;	// A new version does not copy it
		if(isClosed) {
			// Closing again keeps the time it first closed
			closed = wasClosed ? before.closed : new Timestamp(System.currentTimeMillis());
			if(!wasClosed) {
				result = CLOSED;
			}
		} else {
			closed = null;
			if(wasClosed) {
				reopened = new Timestamp(System.currentTimeMillis());
				result = REOPENED;
			}
		}

		try (PreparedStatement pstmt = cResults.prepareStatement("update " + table + " set _case_closed = ?, "
				+ REOPENED_COLUMN + " = ? where prikey = ?")) {
			pstmt.setTimestamp(1, closed);
			pstmt.setTimestamp(2, reopened);
			pstmt.setInt(3, prikey);
			pstmt.executeUpdate();
		}

		if(result.equals(NONE)) {
			return result;
		}

		String instanceId = instanceId(cResults, table, prikey);
		String thread = GeneralUtilityMethods.getThread(cResults, table, instanceId);
		CaseManager cm = new CaseManager(localisation);
		RecordEventManager rem = new RecordEventManager();
		Details d = new Details();
		d.instance = instanceId;
		d.from = before == null ? null : before.status;
		d.to = now.status;

		if(result.equals(CLOSED)) {
			d.owners = cm.getOwners(sd, cResults, table, instanceId);
			d.caseSurvey = caseSurvey(cResults, table, prikey);
			rem.writeEvent(sd, cResults, RecordEventManager.CASE_CLOSED, RecordEventManager.STATUS_SUCCESS, user,
					table, instanceId, gson.toJson(d), null, null, null, describe("cs_closed", d), 0, surveyIdent, 0, 0);
			cm.assignRecord(sd, cResults, localisation, table, instanceId, user, "release",
					null, localisation.getString("cm_auto_release"), user, true, null);
		} else {
			Details lastClose = lastClose(sd, thread);
			d.owners = new ArrayList<> ();
			d.notRestored = new ArrayList<> ();
			d.caseSurvey = lastClose == null ? null : lastClose.caseSurvey;
			if(lastClose != null && lastClose.owners != null) {
				for(String u : lastClose.owners) {
					if(canHaveCase(sd, u, d.caseSurvey == null ? surveyIdent : d.caseSurvey)) {
						d.owners.add(u);
					} else {
						d.notRestored.add(u);
					}
				}
			}
			rem.writeEvent(sd, cResults, RecordEventManager.CASE_REOPENED, RecordEventManager.STATUS_SUCCESS, user,
					table, instanceId, gson.toJson(d), null, null, null, describe("cs_reopened", d), 0, surveyIdent, 0, 0);
			if(!d.owners.isEmpty()) {
				String note = localisation.getString("cs_given_back");
				if(cm.allowsMultipleAssignees(sd, table)) {
					cm.addOwners(sd, cResults, localisation, table, instanceId, d.owners, d.caseSurvey, null, user, note);
				} else {
					cm.replaceOwners(sd, cResults, localisation, table, instanceId, d.owners.subList(0, 1), d.caseSurvey,
							null, user, note);
				}
			}
			// Its alerts can fire again
			try (PreparedStatement pstmt = cResults.prepareStatement(
					"delete from case_alert_triggered where table_name = ? and thread = ?")) {
				pstmt.setString(1, table);
				pstmt.setString(2, thread);
				pstmt.executeUpdate();
			} catch (SQLException e) {
				log.log(Level.SEVERE, "Clear alerts of a reopened case", e);
			}
		}
		return result;
	}

	/*
	 * The event, in words: closed (from x to y), had by ...  or  reopened (from x to y), given back to ...
	 */
	private String describe(String key, Details d) {
		boolean noFrom = d.from == null || d.from.trim().isEmpty();
		StringBuilder b = new StringBuilder(localisation.getString(noFrom ? key + "_set" : key)
				.replace("%s1", noFrom ? "" : d.from)
				.replace("%s2", d.to == null ? "" : d.to));
		if(d.owners != null && !d.owners.isEmpty()) {
			b.append(" ").append(localisation.getString(key.equals("cs_closed") ? "cs_had" : "cs_back")
					.replace("%s1", String.join(", ", d.owners)));
		}
		if(d.notRestored != null && !d.notRestored.isEmpty()) {
			b.append(" ").append(localisation.getString("cs_not_back").replace("%s1", String.join(", ", d.notRestored)));
		}
		return b.toString();
	}

	/*
	 * Who had the case the last time it closed
	 */
	private Details lastClose(Connection sd, String thread) throws SQLException {
		if(thread == null) {
			return null;
		}
		try (PreparedStatement pstmt = sd.prepareStatement("select changes from record_event "
				+ "where key = ? and event = ? order by id desc limit 1")) {
			pstmt.setString(1, thread);
			pstmt.setString(2, RecordEventManager.CASE_CLOSED);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next() && rs.getString(1) != null) {
				try {
					return gson.fromJson(rs.getString(1), Details.class);
				} catch (Exception e) {
					log.log(Level.SEVERE, "Read a closed event", e);
				}
			}
		}
		return null;
	}

	/*
	 * Whether a user can still be given a case of a survey: a member of its project, and of one of its roles
	 * when it has roles.  The same rule as the list of people a case can be assigned to.
	 */
	private boolean canHaveCase(Connection sd, String user, String surveyIdent) throws SQLException {
		if(user == null || surveyIdent == null) {
			return false;
		}
		String sql = "select count(*) from survey s, users u, user_project up "
				+ "where u.id = up.u_id "
				+ "and s.p_id = up.p_id "
				+ "and s.ident = ? "
				+ "and u.ident = ? "
				+ "and not u.temporary "
				+ "and not s.deleted "
				+ "and ((s.ident not in (select survey_ident from survey_role where enabled = true)) "
				+ "or (s.ident in (select sr.survey_ident from user_role ur, survey_role sr "
				+ "    where sr.enabled = true and u.id = ur.u_id and ur.r_id = sr.r_id)) "
				+ "or exists (select 1 from user_group ug where ug.u_id = u.id and (ug.g_id = 6 or ug.g_id = 4)))";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, surveyIdent);
			pstmt.setString(2, user);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() && rs.getInt(1) > 0;
		}
	}

	private String instanceId(Connection cResults, String table, int prikey) throws SQLException {
		try (PreparedStatement pstmt = cResults.prepareStatement("select instanceid from " + table + " where prikey = ?")) {
			pstmt.setInt(1, prikey);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		}
	}

	private String caseSurvey(Connection cResults, String table, int prikey) throws SQLException {
		if(!GeneralUtilityMethods.hasColumn(cResults, table, "_case_survey")) {
			return null;
		}
		try (PreparedStatement pstmt = cResults.prepareStatement("select _case_survey from " + table + " where prikey = ?")) {
			pstmt.setInt(1, prikey);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		}
	}

	private String groupForTable(Connection sd, String table) throws SQLException {
		try (PreparedStatement pstmt = sd.prepareStatement("select s.group_survey_ident from form f, survey s "
				+ "where f.s_id = s.s_id and f.table_name = ? and f.parentform = 0 and not s.deleted "
				+ "order by s.s_id limit 1")) {
			pstmt.setString(1, table);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		}
	}
}
