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

package org.smap.sdal.managers;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.smap.sdal.model.UploadEvent;

public class UploadEventManager {
	
	PreparedStatement pstmtInsert = null;
	String sql = "insert into upload_event ("
			+ "ue_id, "
			+ "upload_time, "
			+ "user_name, "
			+ "file_name, "
			+ "survey_name, "
			+ "imei, "
			+ "status, "
			+ "reason, "
			+ "location,"
			+ "server_name,"
			+ "s_id,"
			+ "p_id,"
			+ "o_id,"
			+ "e_id,"
			+ "form_status,"
			+ "file_path,"
			+ "orig_survey_ident,"
			+ "update_id,"
			+ "ident,"
			+ "incomplete,"
			+ "instanceid,"
			+ "assignment_id,"
			+ "survey_notes,"
			+ "location_trigger,"
			+ "audit_file_path,"
			+ "start_time,"
			+ "end_time,"
			+ "instance_name,"
			+ "scheduled_start,"
			+ "temporary_user,"
			+ "results_db_applied,"
			+ "submission_type, "
			+ "agent) "
			+ "values (nextval('ue_seq'), now(), ?, ?, ?, ?, ?, ?, ?, ?, ?"
			+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
			+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
	
	
	
	/*
	 * Constructor
	 */
	public UploadEventManager(Connection sd) throws SQLException {
		pstmtInsert = sd.prepareStatement(sql);
	}
	
	/*
	 * Write the upload event to the database
	 */
	public void write(UploadEvent ue, boolean results_db_applied) throws SQLException {
		pstmtInsert.setString(1, ue.getUserName());
		pstmtInsert.setString(2, ue.getFileName());
		pstmtInsert.setString(3,  ue.getSurveyName());
		pstmtInsert.setString(4, ue.getImei());
		pstmtInsert.setString(5, ue.getStatus());
		pstmtInsert.setString(6, ue.getReason());
		pstmtInsert.setString(7, ue.getLocation());
		pstmtInsert.setString(8, ue.getServerName());
		pstmtInsert.setInt(9, ue.getSurveyId());
		pstmtInsert.setInt(10,  ue.getProjectId());
		pstmtInsert.setInt(11,  ue.getOrganisationId());
		pstmtInsert.setInt(12,  ue.getEnterpriseId());
		pstmtInsert.setString(13, ue.getFormStatus());
		pstmtInsert.setString(14, ue.getFilePath());
		pstmtInsert.setString(15, ue.getOrigSurveyIdent());
		pstmtInsert.setString(16,  ue.getUpdateId());
		pstmtInsert.setString(17,  ue.getIdent());
		pstmtInsert.setBoolean(18, ue.getIncomplete());
		pstmtInsert.setString(19, ue.getInstanceId());
		pstmtInsert.setInt(20, ue.getAssignmentId());
		pstmtInsert.setString(21, ue.getSurveyNotes());
		pstmtInsert.setString(22, ue.getLocationTrigger());
		pstmtInsert.setString(23, ue.getAuditFilePath());
		pstmtInsert.setTimestamp(24, ue.getStart());
		pstmtInsert.setTimestamp(25, ue.getEnd());
		pstmtInsert.setString(26, ue.getInstanceName());
		pstmtInsert.setTimestamp(27, ue.getScheduledStart());
		pstmtInsert.setBoolean(28, ue.getTemporaryUser());
		pstmtInsert.setBoolean(29, results_db_applied);
		pstmtInsert.setString(30, ue.getType());
		/*
		 * Appended rather than slotted in beside imei, because every parameter here is positional
		 * and inserting one would move twenty six others by hand.
		 */
		pstmtInsert.setString(31, ue.getAgent());
	
		pstmtInsert.executeUpdate();
	}
	
	/*
	 * Close prepared statements
	 */
	/*
	 * Queue failed submissions for a survey to be applied again.
	 *
	 * Clearing results_db_applied and the subscriber's status is all it takes: the batch processor
	 * picks up anything not yet applied.  Duplicates are left alone because applying them again
	 * would only fail again.  An error with no reason is retried too - "not like" is never true of
	 * a null, so writing the condition without the null check skipped those silently.
	 *
	 * ueIds narrows it to particular uploads; null or empty means every failed upload of the
	 * survey.  Returns the uploads that were queued.
	 */
	public static List<Integer> retryFailed(Connection sd, String surveyIdent, List<Integer> ueIds)
			throws SQLException {

		boolean some = ueIds != null && !ueIds.isEmpty();
		String sql = "update upload_event "
				+ "set results_db_applied = 'false', "
				+ "db_status = null, "
				+ "db_reason = null "
				+ "where db_status = 'error' "
				+ "and (db_reason is null or db_reason not like 'Duplicate survey:%') "
				+ "and ident = ? "
				+ (some ? "and ue_id = any(?) " : "")
				+ "returning ue_id";

		List<Integer> queued = new ArrayList<>();
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setString(1, surveyIdent);
			if(some) {
				Array ids = sd.createArrayOf("integer", ueIds.toArray());
				pstmt.setArray(2, ids);
			}
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				queued.add(rs.getInt(1));
			}
		}
		return queued;
	}

	public void close() {
		try {if(pstmtInsert != null) {pstmtInsert.close();}} catch(Exception e) {};
	}
	
}
