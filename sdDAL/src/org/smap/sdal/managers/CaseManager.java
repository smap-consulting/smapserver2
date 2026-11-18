package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.Case;
import org.smap.sdal.model.CaseCount;
import org.smap.sdal.model.CaseManagementAlert;
import org.smap.sdal.model.CaseManagementSettings;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*****************************************************************************

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

 ******************************************************************************/

/*
 * This class supports access to Case Management information in the database
 * All surveys in a bundle share the same case management set up
 */
public class CaseManager {
	
	private static Logger log =
			 Logger.getLogger(CaseManager.class.getName());

	LogManager lm = new LogManager(); // Application log
	ResourceBundle localisation = null;
	Gson gson;
	
	public CaseManager(ResourceBundle l) {
		localisation = l;
		gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
	}

	/*
	 * Get Case Management settings
	 */
	public CMS getCaseManagementSettings(Connection sd, String groupSurveyIdent) throws SQLException {
		
		PreparedStatement pstmt = null;
		CMS cms = null;
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		try {
			String sql = null;
			ResultSet rs = null;
			
			/*
			 * Get the settings for this survey group
			 */
			CaseManagementSettings settings = new CaseManagementSettings();
			ArrayList<CaseManagementAlert> alerts = new ArrayList<>();
			
			sql = "select id, "
					+ "settings, "
					+ "changed_by as changed_by,"
					+ "changed_ts as changed_ts "
					+ "from cms_setting "
					+ "where group_survey_ident = ? ";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  groupSurveyIdent);
			log.fine("Get case management settings: " + pstmt.toString());
			rs = pstmt.executeQuery();
							
			if(rs.next()) {
				String sString = rs.getString("settings");
				
				if(sString != null ) {
					settings = gson.fromJson(sString, CaseManagementSettings.class); 
				} 
			}
			
			if(settings != null) {
				/*
				 * Get the alerts for this survey group
				 * Note the alerts are stored in separate records in the database so
				 * they can be used in queries to find active notification alerts
				 */
				sql = "select id, name, period, filter "
						+ "from cms_alert "
						+ "where group_survey_ident = ? "
						+ "order by name asc";
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1,  groupSurveyIdent);
				
				rs = pstmt.executeQuery();
				while(rs.next()) {
					alerts.add(new CaseManagementAlert(rs.getInt("id"), 
							groupSurveyIdent, rs.getString("name"), 
							rs.getString("period"),
							rs.getString("filter")));
				}
				
				// Create the combined settings object
				cms = new CMS(settings, alerts, groupSurveyIdent);
			}
					    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return cms;
	}

	/*
	 * Create a new Case Management Alert
	 */
	public void createAlert(Connection sd, 
			String user,
			CaseManagementAlert alert) throws Exception {
		
		String sql = "insert into cms_alert (group_survey_ident, name, period, filter, changed_by, changed_ts) " +
				" values (?, ?, ?, ?, ?, now());";
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, alert.group_survey_ident);
			pstmt.setString(2, alert.name);
			pstmt.setString(3,  alert.period);
			pstmt.setString(4,  alert.filter);
			pstmt.setString(5,  user);

			log.fine("SQL: " + pstmt.toString());
			pstmt.executeUpdate();
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}
		
		return;

	}
	
	/*
	 * Update a Case Management Setting
	 */
	public void updateAlert(Connection sd, 
			String user,
			CaseManagementAlert alert) throws Exception {
		
		String sql = "update cms_alert "
				+ "set group_survey_ident = ?, "
				+ "name = ?,"
				+ "period = ?,"
				+ "filter = ?,"
				+ "changed_by = ?,"
				+ "changed_ts = now() "
				+ "where id = ?"; 
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setString(1, alert.group_survey_ident);
			pstmt.setString(2, alert.name);
			pstmt.setString(3, alert.period);
			pstmt.setString(4, alert.filter);
			pstmt.setString(5, user);
			pstmt.setInt(6, alert.id);

			log.fine("SQL: " + pstmt.toString());
			pstmt.executeUpdate();
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}

	}
	
	/*
	 * Update a Case Management Setting
	 */
	public void updateSettings(Connection sd, 
			String user,
			String group_survey_ident,
			CaseManagementSettings settings) throws Exception {
		
		// The status and criticality questions are column names
		for(String name : new String[] {settings.statusQuestion, settings.criticalityQuestion}) {
			if(name != null && name.trim().length() > 0 && !GeneralUtilityMethods.isValidQuestionName(name)) {
				throw new ApplicationException(localisation.getString("cm_inv_q").replace("%s1", GeneralUtilityMethods.getSafeText(name, true)));
			}
		}
		
		/*
		 * A save that does not say whether the bundle allows multiple assignees keeps what it has
		 */
		if(settings.multipleAssignees == null) {
			CMS existing = getCaseManagementSettings(sd, group_survey_ident);
			settings.multipleAssignees = existing != null && existing.settings != null
					&& existing.settings.allowsMultipleAssignees() ? Boolean.TRUE : null;
		}
		
		String sql = "update cms_setting "
				+ "set settings = ?, "
				+ "changed_by = ?,"
				+ "changed_ts = now() "
				+ "where group_survey_ident = ?"; 
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setString(1, gson.toJson(settings));
			pstmt.setString(2, user);
			pstmt.setString(3, group_survey_ident);

			log.fine("SQL: " + pstmt.toString());
			int count = pstmt.executeUpdate();
			if(count < 1) {
				try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
				
				sql = "insert into cms_setting "
						+ "(settings, changed_by, group_survey_ident, changed_ts) "
						+ "values(?,?, ?, now())"; 
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, gson.toJson(settings));
				pstmt.setString(2, user);
				pstmt.setString(3, group_survey_ident);
				log.fine("SQL: " + pstmt.toString());
				pstmt.executeUpdate();
			}
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}

	}
	
	/*
	 * Delete a case management alert
	 */
	public void deleteAlert(Connection sd, 
			int id) throws Exception {
		
		String sql = "delete from cms_alert where id = ? ";
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setInt(1, id);

			log.fine("SQL: " + pstmt.toString());
			pstmt.executeUpdate();
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	/*
	 * Get cases for a user and survey
	 */
	public ArrayList<Case> getCases(
			Connection sd, 
			Connection cResults,
			String sIdent,
			String sName,
			String groupSurveyIdent,
			String user,
			int sId) throws Exception {
		
		PreparedStatement pstmt = null;
			
		ArrayList<Case> cases = new ArrayList<>();
		
		try {
			/*
			 * Return all cases assigned to a user that are to be completed by the provided survey ident
			 * For transition match against the last submitting survey id if _case_survey is null)
			 * Cases should always use the same updateid hence set it to the thread value.  By doing this downloading the latest
			 *  instance of a case to fieldTask will not look like a new case has been created
			 */
			String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, sIdent);
			if(tableName != null) {

				GeneralUtilityMethods.ensureTableCurrent(cResults, tableName, true);		// Temporary - remove 2023

				StringBuilder sql = new StringBuilder("select instanceid, _thread, prikey, instancename, _hrk from ")
						.append(tableName)
						.append(" where not _bad and _assigned = ? and (_case_survey = ? or (_case_survey is null and _s_id = ?)) ");
				pstmt = cResults.prepareStatement(sql.toString());
				pstmt.setString(1, user);
				pstmt.setString(2,  sIdent);
				pstmt.setInt(3, sId);
				//log.fine("Get cases: " + pstmt.toString());
				ResultSet rs = pstmt.executeQuery();

				while(rs.next()) {
					String title = null;
					int prikey = rs.getInt("prikey");
					String instanceName = rs.getString("instancename");
					String thread = rs.getString("_thread");
					if(thread == null) {
						thread = rs.getString("instanceid");
					}
					String hrk = rs.getString("_hrk");
					title = sName + " - ";
					if(instanceName != null && instanceName.trim().length() > 0) {
						title = title + instanceName;
					} else if(hrk != null && hrk.trim().length() > 0) {
						title = title + hrk;
					} else {
						title = title + String.valueOf(prikey);
					}

					cases.add(new Case(prikey, title, thread));
				}
			}

		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		return cases;
	}
	
	/*
	 * The display title plus the assignee of a case / referenced record, read live from its
	 * current (latest, not bad) instance.
	 */
	public static class CaseListItem {
		public String title;
		public String assigned;		// User ident in _assigned, null if unassigned
		public long uploadTime;		// _upload_time of the latest record (epoch ms); used by the device to skip unchanged cases/references
	}

	/*
	 * Build the list entry (title and assignee) for a case / referenced record from its current
	 * (latest, not bad) instance.
	 * Returns null if no live record exists for the thread so callers can skip stale index rows.
	 */
	public CaseListItem getCaseListItem(Connection sd, Connection cResults, String sIdent, String sName, String thread)
			throws Exception {

		CaseListItem item = null;
		String tableName = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, sIdent);
		if(tableName == null) {
			return null;
		}

		PreparedStatement pstmt = null;
		try {
			String sql = "select instancename, _hrk, prikey, _assigned, _upload_time from " + tableName
					+ " where not _bad and _thread = ? order by prikey desc limit 1";
			pstmt = cResults.prepareStatement(sql);
			pstmt.setString(1, thread);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String instanceName = rs.getString("instancename");
				String hrk = rs.getString("_hrk");
				int prikey = rs.getInt("prikey");
				item = new CaseListItem();
				item.title = sName + " - ";
				if(instanceName != null && instanceName.trim().length() > 0) {
					item.title = item.title + instanceName;
				} else if(hrk != null && hrk.trim().length() > 0) {
					item.title = item.title + hrk;
				} else {
					item.title = item.title + String.valueOf(prikey);
				}
				String assigned = rs.getString("_assigned");
				if(assigned != null && assigned.trim().length() > 0) {
					item.assigned = assigned;
				}
				Timestamp uploadTime = rs.getTimestamp("_upload_time");
				if(uploadTime != null) {
					item.uploadTime = uploadTime.getTime();
				}
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		return item;
	}

	public ArrayList<CaseCount> getOpenClosed(Connection sd, Connection cResults,
			String sIdent, String interval, int intervalCount, String aggregationInterval, String tz) throws SQLException {

		String table = GeneralUtilityMethods.getMainResultsTableSurveyIdent(sd, cResults, sIdent);
		GeneralUtilityMethods.ensureTableCurrent(cResults, table, true);

		// Bucket by the organisation's local day, not UTC, so the day boundaries match
		// what users see. _thread_created / _case_closed are timestamptz; timezone(tz, ts)
		// converts to local wall-clock before truncating.
		if(tz == null || tz.trim().length() == 0) { tz = "UTC"; }

		StringBuilder cte = new StringBuilder("with days as (select generate_series(")
				.append("date_trunc('day', timezone(?, now())) - '").append(intervalCount).append(" day'::interval,")
				.append("date_trunc('day', timezone(?, now())),")
				.append("'1 day'::interval")
				.append(") as day) ");

		// Exclude superseded (_bad) versions: an edit/replace copies _thread_created onto the
		// new row and marks the previous one _bad, so counting all rows would count each edit
		// as another case opened on the original day. Count only the surviving version per thread.
		StringBuilder sqlOpened = new StringBuilder("select days.day, count(")
				.append(table).append(".prikey) as opened from days left join ")
				.append(table)
				.append(" on date_trunc('day', timezone(?, _thread_created)) = days.day and not ")
				.append(table).append("._bad ")
				.append("group by 1 order by 1 asc");

		StringBuilder sqlClosed = new StringBuilder("select days.day, count(")
				.append(table).append(".prikey) as closed from days left join ")
				.append(table)
				.append(" on date_trunc('day', timezone(?, _case_closed)) = days.day and not ")
				.append(table).append("._bad ")
				.append("group by 1 order by 1 asc");

		ArrayList<CaseCount> cc = new ArrayList<>();

		if(table != null) {
			PreparedStatement pstmtOpened = null;
			PreparedStatement pstmtClosed = null;

			try {

				pstmtOpened = cResults.prepareStatement(cte.toString() + sqlOpened.toString());
				pstmtOpened.setString(1, tz);
				pstmtOpened.setString(2, tz);
				pstmtOpened.setString(3, tz);
				pstmtClosed = cResults.prepareStatement(cte.toString() + sqlClosed.toString());
				pstmtClosed.setString(1, tz);
				pstmtClosed.setString(2, tz);
				pstmtClosed.setString(3, tz);
				//log.fine("Open: " + pstmtOpened.toString());
				//log.fine("Closed: " + pstmtClosed.toString());
				ResultSet rs = pstmtOpened.executeQuery();
				ResultSet rsc = pstmtClosed.executeQuery();
				while(rs.next()) {
					rsc.next();
					String day = rs.getString(1);
					String [] dayComp = day.split(" ");
					cc.add(new CaseCount(dayComp[0], rs.getInt(2), rsc.getInt(2)));
				}
			} finally {
				if(pstmtOpened != null) {try {pstmtOpened.close();} catch(Exception e) {}}
				if(pstmtClosed != null) {try {pstmtClosed.close();} catch(Exception e) {}}
			}
		}
		return cc;
	}
	
	/*
	 * Who owns a case.  A case has a set of owners, held in record_user (access 'owner').  Unless its bundle
	 * allows multiple assignees there is at most one, which is how cases have always worked.
	 *
	 * _assigned on the record is kept as a copy of the owners, sorted and comma separated, so that filters,
	 * exports and anything else that reads it keep working.  With one owner it is exactly the user ident it
	 * has always been.
	 */

	// What made a person an owner, in record_user.source
	public static final String SOURCE_CONSOLE = "console";
	public static final String SOURCE_ESCALATION = "escalation";
	public static final String SOURCE_DEVICE = "device";
	public static final String SOURCE_OPS = "ops";
	public static final String SOURCE_MCP = "mcp";

	private static class Owner {
		String ident;
		String source;
		Owner(String ident, String source) { this.ident = ident; this.source = source; }
	}

	/*
	 * A change to a case's owners, worked out from the owners it has now.  Returns false if the change
	 * cannot be made, for example a lock on a case somebody already holds.
	 */
	private interface OwnerChange {
		boolean plan(List<Owner> current, List<String> add, List<String> remove);
	}

	/*
	 * Method to assign a record to a user
	 * A release only succeeds if the requesting user holds the record
	 */
	public int assignRecord(Connection sd,
			Connection cResults,
			ResourceBundle localisation,
			String tablename,
			String instanceId,
			String assignTo,
			String type,					// lock || release || assign
			String surveyIdent,
			String note,
			String requestingUser
			) throws SQLException {
		return assignRecord(sd, cResults, localisation, tablename, instanceId, assignTo, type, surveyIdent,
				note, requestingUser, false, SOURCE_CONSOLE);
	}

	public int assignRecord(Connection sd,
			Connection cResults,
			ResourceBundle localisation,
			String tablename,
			String instanceId,
			String assignTo,
			String type,
			String surveyIdent,
			String note,
			String requestingUser,
			boolean anyHolder
			) throws SQLException {
		return assignRecord(sd, cResults, localisation, tablename, instanceId, assignTo, type, surveyIdent,
				note, requestingUser, anyHolder, SOURCE_CONSOLE);
	}

	/*
	 * Assign, lock or release a case, as callers always have.
	 *   lock     the requesting user takes the case.  Only if nobody holds it, unless the bundle allows
	 *            multiple assignees, when they join whoever does
	 *   assign   assignTo holds the case; with multiple assignees they are added to its owners.  An assignTo
	 *            of null or "_none" leaves nobody holding it
	 *   release  the requesting user gives the case up; with anyHolder everybody does, as when it is closed
	 * Returns 1 if the case exists and the change was made or was already in effect, 0 otherwise.
	 */
	public int assignRecord(Connection sd,
			Connection cResults,
			ResourceBundle localisation,
			String tablename,
			String instanceId,
			String assignTo,
			String type,
			String surveyIdent,
			String note,
			String requestingUser,
			boolean anyHolder,
			String source
			) throws SQLException {

		if(assignTo != null) {
			assignTo = assignTo.trim();
			if(assignTo.isEmpty() || assignTo.equals("_none")) {
				assignTo = null;
			}
		}
		final String who = assignTo;
		boolean multiple = allowsMultipleAssignees(sd, tablename);

		if(type.equals("lock")) {
			return changeOwners(sd, cResults, localisation, tablename, instanceId, surveyIdent, source, requestingUser,
					note, "lock", (current, add, remove) -> {
						if(contains(current, requestingUser)) return true;		// Already holds it
						if(!multiple && !current.isEmpty()) return false;		// Somebody else does
						add.add(requestingUser);
						return true;
					});
		} else if(type.equals("release")) {
			return changeOwners(sd, cResults, localisation, tablename, instanceId, null, source, requestingUser,
					note, "release", (current, add, remove) -> {
						if(anyHolder) {
							for(Owner o : current) remove.add(o.ident);
							return !current.isEmpty();
						}
						if(!contains(current, requestingUser)) return false;	// Only a holder can release
						remove.add(requestingUser);
						return true;
					});
		} else if(who == null) {
			return removeOwners(sd, cResults, localisation, tablename, instanceId, null, requestingUser, note);
		} else if(multiple) {
			return addOwners(sd, cResults, localisation, tablename, instanceId, List.of(who), surveyIdent, source,
					requestingUser, note);
		} else {
			return replaceOwners(sd, cResults, localisation, tablename, instanceId, List.of(who), surveyIdent, source,
					requestingUser, note);
		}
	}

	/*
	 * Make these users owners of the case as well as those it has
	 */
	public int addOwners(Connection sd, Connection cResults, ResourceBundle localisation, String tablename,
			String instanceId, List<String> users, String caseSurvey, String source, String requestingUser,
			String note) throws SQLException {
		return changeOwners(sd, cResults, localisation, tablename, instanceId, caseSurvey, source, requestingUser,
				note, "assign", (current, add, remove) -> {
					for(String u : users) {
						if(u != null && !contains(current, u) && !add.contains(u)) add.add(u);
					}
					return true;
				});
	}

	/*
	 * Stop these users owning the case, or everybody if users is null
	 */
	public int removeOwners(Connection sd, Connection cResults, ResourceBundle localisation, String tablename,
			String instanceId, List<String> users, String requestingUser, String note) throws SQLException {
		return changeOwners(sd, cResults, localisation, tablename, instanceId, null, null, requestingUser,
				note, "unassign", (current, add, remove) -> {
					for(Owner o : current) {
						if(users == null || users.contains(o.ident)) remove.add(o.ident);
					}
					return true;
				});
	}

	/*
	 * Make these users the case's only owners
	 */
	public int replaceOwners(Connection sd, Connection cResults, ResourceBundle localisation, String tablename,
			String instanceId, List<String> users, String caseSurvey, String source, String requestingUser,
			String note) throws SQLException {
		return changeOwners(sd, cResults, localisation, tablename, instanceId, caseSurvey, source, requestingUser,
				note, "assign", (current, add, remove) -> {
					for(Owner o : current) {
						if(!users.contains(o.ident)) remove.add(o.ident);
					}
					for(String u : users) {
						if(u != null && !contains(current, u) && !add.contains(u)) add.add(u);
					}
					return true;
				});
	}

	/*
	 * Replace the owners that one source added, keeping those added any other way.  An escalation uses this
	 * to take back the people an earlier escalation assigned while leaving those assigned by hand.
	 */
	public int replaceOwnersFrom(Connection sd, Connection cResults, ResourceBundle localisation, String tablename,
			String instanceId, List<String> users, String caseSurvey, String source, String requestingUser,
			String note) throws SQLException {
		return changeOwners(sd, cResults, localisation, tablename, instanceId, caseSurvey, source, requestingUser,
				note, "assign", (current, add, remove) -> {
					for(Owner o : current) {
						if(source.equals(o.source) && !users.contains(o.ident)) remove.add(o.ident);
					}
					for(String u : users) {
						if(u != null && !contains(current, u) && !add.contains(u)) add.add(u);
					}
					return true;
				});
	}

	/*
	 * The users who own a case, sorted
	 */
	public List<String> getOwners(Connection sd, Connection cResults, String tablename, String instanceId) throws SQLException {
		List<String> owners = new ArrayList<>();
		String thread = GeneralUtilityMethods.getThread(cResults, tablename, instanceId);
		String groupSurveyIdent = groupSurveyIdentForTable(sd, tablename);
		if(thread != null && groupSurveyIdent != null) {
			for(Owner o : readOwners(sd, groupSurveyIdent, thread)) owners.add(o.ident);
		}
		return owners;
	}

	/*
	 * Whether the bundle a results table belongs to allows a case to have more than one owner
	 */
	public boolean allowsMultipleAssignees(Connection sd, String tablename) throws SQLException {
		String groupSurveyIdent = groupSurveyIdentForTable(sd, tablename);
		if(groupSurveyIdent == null) {
			return false;
		}
		CMS cms = getCaseManagementSettings(sd, groupSurveyIdent);
		return cms != null && cms.settings != null && cms.settings.allowsMultipleAssignees();
	}

	/*
	 * Change a case's owners.  Everything that says who owns it changes together, under a lock on the case
	 * so that two changes at once cannot lose one another:
	 *   record_user, which is who owns it
	 *   _assigned and _case_survey on the record
	 *   the record's history, an entry for each person added or removed
	 *   each person's count of cases, and their device, which is told to refresh
	 */
	private int changeOwners(Connection sd, Connection cResults, ResourceBundle localisation, String tablename,
			String instanceId, String caseSurvey, String source, String requestingUser, String note, String type,
			OwnerChange change) throws SQLException {

		String thread = GeneralUtilityMethods.getThread(cResults, tablename, instanceId);
		if(thread == null) {
			log.fine("Assign: no record " + instanceId + " in " + tablename);
			return 0;
		}
		String groupSurveyIdent = groupSurveyIdentForTable(sd, tablename);
		if(groupSurveyIdent == null) {
			return 0;
		}

		// Join a transaction the caller has open, otherwise make one
		boolean ownTransaction = sd.getAutoCommit();
		if(ownTransaction) {
			sd.setAutoCommit(false);
		}
		List<String> added = new ArrayList<>();
		List<String> removed = new ArrayList<>();
		try {
			try (PreparedStatement pstmt = sd.prepareStatement("select pg_advisory_xact_lock(3, hashtext(?))")) {
				pstmt.setString(1, groupSurveyIdent + ":" + thread);
				pstmt.executeQuery();
			}

			List<Owner> current = readOwners(sd, groupSurveyIdent, thread);
			if(!change.plan(current, added, removed)) {
				if(ownTransaction) sd.rollback();
				return 0;
			}

			if(!added.isEmpty() || !removed.isEmpty()) {
				try (PreparedStatement del = sd.prepareStatement("delete from record_user "
						+ "where group_survey_ident = ? and thread = ? and access = 'owner' and assignee_ident = ?");
					PreparedStatement ins = sd.prepareStatement("insert into record_user "
						+ "(assignee, assignee_ident, group_survey_ident, survey_ident, thread, access, read_only, created_by, source) "
						+ "values ((select id from users where ident = ?), ?, ?, ?, ?, 'owner', false, ?, ?) "
						+ "on conflict (assignee_ident, group_survey_ident, thread) do update set "		// A reference becomes an owner
						+ "access = 'owner', read_only = false, survey_ident = excluded.survey_ident, "
						+ "source = excluded.source, created_by = excluded.created_by")) {
					for(String u : removed) {
						del.setString(1, groupSurveyIdent);
						del.setString(2, thread);
						del.setString(3, u);
						del.executeUpdate();
					}
					for(String u : added) {
						ins.setString(1, u);
						ins.setString(2, u);
						ins.setString(3, groupSurveyIdent);
						ins.setString(4, caseSurvey);
						ins.setString(5, thread);
						ins.setString(6, requestingUser);
						ins.setString(7, source);
						ins.executeUpdate();
					}
				}

				// The record's copy of who owns it
				List<Owner> now = readOwners(sd, groupSurveyIdent, thread);
				List<String> idents = new ArrayList<>();
				for(Owner o : now) idents.add(o.ident);
				String summary = idents.isEmpty() ? null : String.join(", ", idents);
				String surveyNow = idents.isEmpty() ? null
						: (caseSurvey != null && !added.isEmpty() ? caseSurvey : currentCaseSurvey(cResults, tablename, thread));
				try (PreparedStatement upd = cResults.prepareStatement("update " + tablename
						+ " set _assigned = ?, _case_survey = ? where _thread = ?")) {
					upd.setString(1, summary);
					upd.setString(2, surveyNow);
					upd.setString(3, thread);
					upd.executeUpdate();
				}

				UserManager um = new UserManager(null);
				RecordEventManager rem = new RecordEventManager();
				for(String u : removed) {
					um.decrementTotalTasks(sd, u);
					rem.writeEvent(sd, cResults, RecordEventManager.ASSIGNED, "success", requestingUser, tablename,
							instanceId, null, null, null, null, details(localisation, type, u, note, false), 0, null, 0, 0);
				}
				for(String u : added) {
					um.incrementTotalTasks(sd, u);
					rem.writeEvent(sd, cResults, RecordEventManager.ASSIGNED, "success", requestingUser, tablename,
							instanceId, null, null, null, null, details(localisation, type, u, note, true), 0, null, 0, 0);
				}
				// Tell the devices of the people taken off the case; the history entry tells those who own it
				MessagingManager mm = new MessagingManager(null);
				for(String u : removed) {
					mm.userChange(sd, u);
				}
			}

			if(ownTransaction) sd.commit();
			return 1;

		} catch (SQLException e) {
			if(ownTransaction) {
				try { sd.rollback(); } catch (SQLException ex) {}
			}
			throw e;
		} finally {
			if(ownTransaction) {
				try { sd.setAutoCommit(true); } catch (SQLException ex) {}
			}
		}
	}

	// The history entry for a person added to or taken off a case
	private String details(ResourceBundle localisation, String type, String user, String note, boolean added) {
		String d;
		if(added) {
			d = (type.equals("lock") ? localisation.getString("cm_lock") : localisation.getString("assigned_to"))
					.replace("%s1", user);
		} else if(type.equals("release")) {
			d = localisation.getString("cm_release") + ": " + user;
		} else {
			d = localisation.getString("cm_unassigned_user").replace("%s1", user);
		}
		if(note != null && note.trim().length() > 0) {
			d += " - " + note;
		}
		return d;
	}

	private List<Owner> readOwners(Connection sd, String groupSurveyIdent, String thread) throws SQLException {
		List<Owner> owners = new ArrayList<>();
		try (PreparedStatement pstmt = sd.prepareStatement("select assignee_ident, source from record_user "
				+ "where group_survey_ident = ? and thread = ? and access = 'owner' order by assignee_ident")) {
			pstmt.setString(1, groupSurveyIdent);
			pstmt.setString(2, thread);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				owners.add(new Owner(rs.getString(1), rs.getString(2)));
			}
		}
		return owners;
	}

	private String currentCaseSurvey(Connection cResults, String tablename, String thread) throws SQLException {
		try (PreparedStatement pstmt = cResults.prepareStatement("select _case_survey from " + tablename
				+ " where _thread = ? and not _bad limit 1")) {
			pstmt.setString(1, thread);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		}
	}

	// The bundle a main results table belongs to
	private String groupSurveyIdentForTable(Connection sd, String tablename) throws SQLException {
		try (PreparedStatement pstmt = sd.prepareStatement("select s.group_survey_ident from form f "
				+ "join survey s on s.s_id = f.s_id "
				+ "where f.table_name = ? and f.parentform = 0 and s.group_survey_ident is not null "
				+ "order by s.deleted, s.s_id limit 1")) {
			pstmt.setString(1, tablename);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		}
	}

	private static boolean contains(List<Owner> owners, String ident) {
		for(Owner o : owners) {
			if(o.ident.equals(ident)) return true;
		}
		return false;
	}
}
