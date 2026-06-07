package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
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
 * Manage referenced cases - read only access for one or more users to a record.
 * References are stored as record_user rows with access = 'reference' and read_only = true.
 * Unlike an owned case they do not touch _assigned on the record and many users can
 * reference the same record.
 */
public class ReferenceManager {

	private static Logger log = Logger.getLogger(ReferenceManager.class.getName());

	ResourceBundle localisation = null;

	public ReferenceManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Give a list of users read only access to a record
	 */
	public int addReferences(Connection sd, Connection cResults, String tableName, String instanceId,
			String surveyIdent, List<String> userIdents, String requestingUser) throws SQLException {

		int count = 0;
		String thread = GeneralUtilityMethods.getThread(cResults, tableName, instanceId);
		if(thread == null) {
			return 0;
		}
		String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdentFromIdent(sd, surveyIdent);

		String sql = "insert into record_user "
				+ "(assignee, assignee_ident, group_survey_ident, survey_ident, thread, access, read_only, created_by) "
				+ "values ((select id from users where ident = ?), ?, ?, ?, ?, 'reference', true, ?) "
				+ "on conflict (assignee_ident, group_survey_ident, thread) do nothing";

		PreparedStatement pstmt = null;
		RecordEventManager rem = new RecordEventManager();
		try {
			pstmt = sd.prepareStatement(sql);
			for(String userIdent : userIdents) {
				if(userIdent == null || userIdent.trim().length() == 0) {
					continue;
				}
				userIdent = userIdent.trim();
				pstmt.setString(1, userIdent);
				pstmt.setString(2, userIdent);
				pstmt.setString(3, groupSurveyIdent);
				pstmt.setString(4, surveyIdent);
				pstmt.setString(5, thread);
				pstmt.setString(6, requestingUser);
				int added = pstmt.executeUpdate();
				count += added;

				if(added > 0) {
					String details = localisation != null ? localisation.getString("c_referenced") : "Referenced";
					details = details.replace("%s1", userIdent);
					rem.writeEvent(sd, cResults, RecordEventManager.ASSIGNED, "success", requestingUser,
							tableName, instanceId, null, null, null, null, details, 0, null, 0, 0);
				}
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
		return count;
	}

	/*
	 * Remove read only access for a list of users (or all users if userIdents is null/empty)
	 */
	public int removeReferences(Connection sd, Connection cResults, String tableName, String instanceId,
			List<String> userIdents, String requestingUser) throws SQLException {

		int count = 0;
		String thread = GeneralUtilityMethods.getThread(cResults, tableName, instanceId);
		if(thread == null) {
			return 0;
		}

		PreparedStatement pstmt = null;
		RecordEventManager rem = new RecordEventManager();
		try {
			if(userIdents == null || userIdents.size() == 0) {
				pstmt = sd.prepareStatement("delete from record_user where thread = ? and access = 'reference'");
				pstmt.setString(1, thread);
				count = pstmt.executeUpdate();
			} else {
				pstmt = sd.prepareStatement(
						"delete from record_user where thread = ? and assignee_ident = ? and access = 'reference'");
				for(String userIdent : userIdents) {
					if(userIdent == null || userIdent.trim().length() == 0) {
						continue;
					}
					pstmt.setString(1, thread);
					pstmt.setString(2, userIdent.trim());
					count += pstmt.executeUpdate();
				}
			}
			if(count > 0) {
				String details = localisation != null ? localisation.getString("c_unreferenced") : "Reference removed";
				rem.writeEvent(sd, cResults, RecordEventManager.ASSIGNED, "success", requestingUser,
						tableName, instanceId, null, null, null, null, details, 0, null, 0, 0);
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
		return count;
	}

	/*
	 * Get the list of users who currently reference a record
	 */
	public ArrayList<String> getReferences(Connection sd, Connection cResults, String tableName, String instanceId)
			throws SQLException {

		ArrayList<String> users = new ArrayList<>();
		String thread = GeneralUtilityMethods.getThread(cResults, tableName, instanceId);
		if(thread == null) {
			return users;
		}

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(
					"select assignee_ident from record_user where thread = ? and access = 'reference' order by assignee_ident");
			pstmt.setString(1, thread);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				users.add(rs.getString(1));
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}
		return users;
	}

	/*
	 * Get the instance ids of the records that the given user references in a survey bundle.
	 * Used to indicate referenced records in the console without querying each record.
	 */
	public ArrayList<String> getReferencedInstances(Connection sd, Connection cResults, String tableName,
			String groupSurveyIdent, String userIdent) throws SQLException {

		ArrayList<String> instances = new ArrayList<>();

		// Threads referenced by this user (survey_definitions database)
		ArrayList<String> threads = new ArrayList<>();
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement("select thread from record_user "
					+ "where assignee_ident = ? and access = 'reference' and group_survey_ident = ?");
			pstmt.setString(1, userIdent);
			pstmt.setString(2, groupSurveyIdent);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				threads.add(rs.getString(1));
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (Exception e) {}
		}

		if(threads.size() == 0 || tableName == null
				|| !GeneralUtilityMethods.tableExists(cResults, tableName)) {
			return instances;
		}

		// Current instance id for each referenced thread (results database)
		PreparedStatement pstmtRes = null;
		try {
			pstmtRes = cResults.prepareStatement("select instanceid from " + tableName
					+ " where not _bad and _thread = any(?)");
			pstmtRes.setArray(1, cResults.createArrayOf("text", threads.toArray()));
			ResultSet rs = pstmtRes.executeQuery();
			while(rs.next()) {
				instances.add(rs.getString(1));
			}
		} finally {
			try {if (pstmtRes != null) {pstmtRes.close();}} catch (Exception e) {}
		}
		return instances;
	}
}
