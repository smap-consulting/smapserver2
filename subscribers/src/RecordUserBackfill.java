import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

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
 * One off backfill that populates the record_user owner index from the existing _assigned
 * values on results records.
 *
 * Run automatically by the forward subscriber on startup. The server.record_user_backfilled
 * flag records that it has completed so it is only done once. The work itself is also
 * idempotent (rows inserted with "on conflict do nothing") so a partial run is safe to retry.
 */
public class RecordUserBackfill {

	private static Logger log = Logger.getLogger(RecordUserBackfill.class.getName());

	/*
	 * Run the backfill once. Checks the server.record_user_backfilled flag and does nothing
	 * if it has already completed. Sets the flag on success.
	 */
	public static void runIfNeeded(Connection sd, Connection cResults) {

		PreparedStatement pstmtFlag = null;
		try {
			boolean done = false;
			pstmtFlag = sd.prepareStatement("select record_user_backfilled from server");
			ResultSet rs = pstmtFlag.executeQuery();
			if(rs.next()) {
				done = rs.getBoolean(1);
			}
			pstmtFlag.close();
			pstmtFlag = null;

			if(done) {
				return;		// Already completed
			}

			log.info("Starting one off record_user backfill");
			int inserted = backfill(sd, cResults);
			log.info("record_user backfill complete. Owner rows inserted: " + inserted);

			// The cached task counts predate the backfill, force each user's badge count to be
			// recalculated the next time it is read
			pstmtFlag = sd.prepareStatement("update users set reset_total_tasks = true");
			pstmtFlag.executeUpdate();
			pstmtFlag.close();
			pstmtFlag = null;

			pstmtFlag = sd.prepareStatement("update server set record_user_backfilled = true");
			pstmtFlag.executeUpdate();
		} catch (Exception e) {
			// Leave the flag unset so the backfill is retried on the next startup
			log.log(java.util.logging.Level.SEVERE, "record_user backfill failed: " + e.getMessage(), e);
		} finally {
			try {if (pstmtFlag != null) {pstmtFlag.close();}} catch (Exception e) {}
		}
	}

	/*
	 * For every top level results table, copy assigned records into record_user as owner rows.
	 * group_survey_ident comes from the survey, survey_ident from _case_survey (fallback survey ident).
	 */
	public static int backfill(Connection sd, Connection cResults) throws Exception {

		int inserted = 0;

		PreparedStatement pstmtSurveys = null;
		PreparedStatement pstmtInsert = null;
		try {
			pstmtSurveys = sd.prepareStatement("select s.ident, s.group_survey_ident, f.table_name "
					+ "from survey s, form f "
					+ "where s.s_id = f.s_id and f.parentform = 0 "
					+ "and not s.deleted "
					+ "and s.group_survey_ident is not null");

			pstmtInsert = sd.prepareStatement("insert into record_user "
					+ "(assignee, assignee_ident, group_survey_ident, survey_ident, thread, access, read_only) "
					+ "values ((select id from users where ident = ?), ?, ?, ?, ?, 'owner', false) "
					+ "on conflict (assignee_ident, group_survey_ident, thread) do nothing");

			ResultSet rsSurveys = pstmtSurveys.executeQuery();
			while(rsSurveys.next()) {
				String surveyIdent = rsSurveys.getString("ident");
				String groupSurveyIdent = rsSurveys.getString("group_survey_ident");
				String tableName = rsSurveys.getString("table_name");

				if(tableName == null || !GeneralUtilityMethods.tableExists(cResults, tableName)
						|| !GeneralUtilityMethods.hasColumn(cResults, tableName, "_assigned")) {
					continue;
				}

				boolean hasCaseSurvey = GeneralUtilityMethods.hasColumn(cResults, tableName, "_case_survey");
				String caseSurveyCol = hasCaseSurvey ? "_case_survey" : "null as _case_survey";

				PreparedStatement pstmtRecs = null;
				try {
					pstmtRecs = cResults.prepareStatement("select distinct _thread, _assigned, " + caseSurveyCol
							+ " from " + tableName
							+ " where _assigned is not null and not _bad and _thread is not null");
					ResultSet rsRecs = pstmtRecs.executeQuery();
					while(rsRecs.next()) {
						String thread = rsRecs.getString("_thread");
						String assigned = rsRecs.getString("_assigned");
						String caseSurvey = rsRecs.getString("_case_survey");
						if(caseSurvey == null || caseSurvey.trim().length() == 0) {
							caseSurvey = surveyIdent;
						}

						pstmtInsert.setString(1, assigned);
						pstmtInsert.setString(2, assigned);
						pstmtInsert.setString(3, groupSurveyIdent);
						pstmtInsert.setString(4, caseSurvey);
						pstmtInsert.setString(5, thread);
						inserted += pstmtInsert.executeUpdate();
					}
				} finally {
					try {if (pstmtRecs != null) {pstmtRecs.close();}} catch (Exception e) {}
				}
			}
		} finally {
			try {if (pstmtSurveys != null) {pstmtSurveys.close();}} catch (Exception e) {}
			try {if (pstmtInsert != null) {pstmtInsert.close();}} catch (Exception e) {}
		}

		return inserted;
	}
}
