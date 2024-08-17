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

package JdbcManagers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.smap.server.entities.UploadEvent;

public class JdbcUploadEventManager {
	
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
			+ "results_db_applied) "
			+ "values (nextval('ue_seq'), now(), ?, ?, ?, ?, ?, ?, ?, ?, ?"
			+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
			+ ", ?, ?, ?, ?, ?, ?, ?, ?);";
	
	String sqlGet = "select "
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
			+ "temporary_user,"
			+ "restore, "
			+ "submission_type,"
			+ "payload "
			+ "from upload_event "
				+ "where status = 'success' "
				+ "and s_id is not null "
				+ "and not incomplete "
				+ "and not results_db_applied "
				+ "and not queued "
				+ "order by ue_id asc";
	
	PreparedStatement pstmtGetPending = null;
	
	/*
	 * Constructor
	 */
	public JdbcUploadEventManager(Connection sd) throws SQLException {
		pstmtInsert = sd.prepareStatement(sql);
		pstmtGetPending = sd.prepareStatement(sqlGet);
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
	
		pstmtInsert.executeUpdate();
	}
	

	/*
	 * Get Uploads that have not been processed
	 */
	public List<UploadEvent> getPending() throws SQLException {
		return getUploadEventList(pstmtGetPending);
	}
	
	/*
	 * Close prepared statements
	 */
	public void close() {
		try {if(pstmtInsert != null) {pstmtInsert.close();}} catch(Exception e) {};
		try {if(pstmtGetPending != null) {pstmtGetPending.close();}} catch(Exception e) {};
	}
	
	private List <UploadEvent> getUploadEventList(PreparedStatement pstmt) throws SQLException {
		ArrayList <UploadEvent> ueList = new ArrayList<UploadEvent> ();
		
		ResultSet rs = pstmt.executeQuery();
		while(rs.next()) {
			UploadEvent ue = new UploadEvent();
			ue.setId(rs.getInt(1));
			ue.setUploadTime(rs.getTimestamp(2));
			ue.setUserName(rs.getString(3));
			ue.setFileName(rs.getString(4));
			ue.setSurveyName(rs.getString(5));
			ue.setImei(rs.getString(6));
			ue.setStatus(rs.getString(7));
			ue.setReason(rs.getString(8));
			ue.setLocation(rs.getString(9));
			ue.setServerName(rs.getString(10));
			ue.setSurveyId(rs.getInt(11));
			ue.setProjectId(rs.getInt(12));
			ue.setFormStatus(rs.getString(13));
			ue.setFilePath(rs.getString(14));
			ue.setOrigSurveyIdent(rs.getString(15));
			ue.setUpdateId(rs.getString(16));
			ue.setIdent(rs.getString(17));
			ue.setIncomplete(rs.getBoolean(18));
			ue.setInstanceId(rs.getString(19));
			ue.setAssignmentId(rs.getInt(20));
			ue.setSurveyNotes(rs.getString(21));
			ue.setLocationTrigger(rs.getString(22));
			ue.setAuditFilePath(rs.getString(23));
			ue.setTemporaryUser(rs.getBoolean(24));
			ue.setRestore(rs.getBoolean(25));
			ue.setType(rs.getString("submission_type"));
			ue.setPayload(rs.getString("payload"));
			
			ueList.add(ue);
		}
		return ueList;
	}
}
