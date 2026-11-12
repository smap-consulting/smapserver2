package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;

/*
 * Make a new version of a record before it is changed
 *
 * A change made by a submission is stored as a new version of the record, sharing its thread,
 * with the version it replaced kept and marked bad.  A change made in the console is stored the
 * same way so that the version somebody opened for editing stays as it was when they opened it.
 * That is what lets a later save tell what they changed from what somebody else changed.
 */
public class RecordVersionManager {

	private static Logger log = Logger.getLogger(RecordVersionManager.class.getName());

	/*
	 * The advisory lock taken while a record is merged or versioned, one per bundle
	 * Shared with the subscriber so a console change and a submission do not version a record at once
	 */
	public static final int RECORD_LOCK = 2;

	private ResourceBundle localisation;

	public RecordVersionManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Lock changes to records in a bundle
	 * Released by unlock, on the same connection
	 */
	public static void lock(Connection sd, int bundleSurveyId) throws SQLException {
		try (PreparedStatement pstmt = sd.prepareStatement("select pg_advisory_lock(?,?)")) {
			pstmt.setInt(1, RECORD_LOCK);
			pstmt.setInt(2, bundleSurveyId);
			pstmt.execute();
		}
	}

	public static void unlock(Connection sd, int bundleSurveyId) {
		try (PreparedStatement pstmt = sd.prepareStatement("select pg_advisory_unlock(?,?)")) {
			pstmt.setInt(1, RECORD_LOCK);
			pstmt.setInt(2, bundleSurveyId);
			pstmt.execute();
		} catch (Exception e) {
			log.warning("Releasing record lock: " + e.getMessage());
		}
	}

	/*
	 * Copy the latest version of a record to a new version and supersede the old one
	 * Repeat rows move to the new version, so their primary keys stay the same, and the old
	 * version keeps copies of them
	 * Returns the instance id of the new version, which is to be changed
	 * A deleted record is not versioned, its instance id is returned
	 */
	public String newVersion(Connection sd, Connection cResults, int sId, String groupSurveyIdent,
			int topFormId, String table, String instanceId, String user, String tz) throws Exception {

		String latest = GeneralUtilityMethods.getLatestInstanceId(cResults, table, instanceId);

		int curKey = 0;
		boolean bad = false;
		try (PreparedStatement pstmt = cResults.prepareStatement("select prikey, _bad from " + table + " where instanceid = ?")) {
			pstmt.setString(1, latest);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				curKey = rs.getInt(1);
				bad = rs.getBoolean(2);
			}
		}
		if(curKey == 0 || bad) {
			return latest;
		}

		/*
		 * Copy the record
		 */
		String newInstanceId = "uuid:" + UUID.randomUUID().toString();
		StringBuilder cols = new StringBuilder();
		StringBuilder vals = new StringBuilder();
		try (PreparedStatement pstmt = cResults.prepareStatement(
				"select column_name from information_schema.columns where table_name = ? and column_name != 'prikey' "
				+ "order by ordinal_position asc")) {
			pstmt.setString(1, table);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String col = rs.getString(1);
				if(cols.length() > 0) {
					cols.append(",");
					vals.append(",");
				}
				cols.append(col);
				if(col.equals("instanceid")) {
					vals.append("?");
				} else if(col.equals("_bad") || col.equals("_modified")) {
					vals.append("false");
				} else if(col.equals("_bad_reason")) {
					vals.append("null");
				} else {
					vals.append(col);
				}
			}
		}
		int newKey = 0;
		String sqlCopy = "insert into " + table + " (" + cols + ") select " + vals + " from " + table + " where prikey = ?";
		try (PreparedStatement pstmt = cResults.prepareStatement(sqlCopy, Statement.RETURN_GENERATED_KEYS)) {
			pstmt.setString(1, newInstanceId);
			pstmt.setInt(2, curKey);
			log.fine("New version of record: " + pstmt.toString());
			pstmt.executeUpdate();
			ResultSet rs = pstmt.getGeneratedKeys();
			if(rs.next()) {
				newKey = rs.getInt(1);
			}
		}
		if(newKey == 0) {
			throw new Exception("New version of record " + instanceId + " not created");
		}

		/*
		 * Move the repeat rows to the new version and give the old version copies
		 */
		for(String childTable : getChildTables(sd, sId, groupSurveyIdent)) {
			if(!GeneralUtilityMethods.tableExists(cResults, childTable)) {
				continue;
			}
			ArrayList<Integer> keys = new ArrayList<>();
			try (PreparedStatement pstmt = cResults.prepareStatement("select prikey from " + childTable + " where parkey = ? order by prikey asc")) {
				pstmt.setInt(1, curKey);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					keys.add(rs.getInt(1));
				}
			}
			if(keys.isEmpty()) {
				continue;
			}
			try (PreparedStatement pstmt = cResults.prepareStatement("update " + childTable + " set parkey = ? where parkey = ?")) {
				pstmt.setInt(1, newKey);
				pstmt.setInt(2, curKey);
				pstmt.executeUpdate();
			}
			try (PreparedStatement pstmt = cResults.prepareStatement(getCopySql(cResults, childTable, curKey))) {
				for(int key : keys) {
					pstmt.setInt(1, key);
					pstmt.executeUpdate();
				}
			}
		}

		/*
		 * Copy the markers of compound geometries, which are kept in a table of their own
		 */
		try (PreparedStatement pstmt = sd.prepareStatement("select column_name from question where f_id = ? and qtype = 'geocompound' and not soft_deleted")) {
			pstmt.setInt(1, topFormId);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				String pointsTable = table + "_" + rs.getString(1);
				if(GeneralUtilityMethods.tableExists(cResults, pointsTable)) {
					try (PreparedStatement pstmtCopy = cResults.prepareStatement("insert into " + pointsTable
							+ " (parkey, properties, locn) select ?, properties, locn from " + pointsTable + " where parkey = ? order by prikey asc")) {
						pstmtCopy.setInt(1, newKey);
						pstmtCopy.setInt(2, curKey);
						pstmtCopy.executeUpdate();
					}
				}
			}
		}

		/*
		 * Supersede the old version
		 */
		UtilityMethodsEmail.markRecord(cResults, sd, localisation, table,
				true, localisation.getString("sub_mw") + " " + newKey, curKey, sId, topFormId, true, false, user, true, tz, false, false);

		return newInstanceId;
	}

	/*
	 * The tables of the repeats in the main form of every survey in the bundle
	 */
	private ArrayList<String> getChildTables(Connection sd, int sId, String groupSurveyIdent) throws SQLException {
		ArrayList<String> tables = new ArrayList<>();
		String sql = "select distinct table_name from form "
				+ "where reference = 'false' "
				+ "and parentform in (select f_id from form where parentform = 0 "
				+ "and (s_id = ? or s_id in (select s_id from survey where group_survey_ident = ? and deleted='false'))) ";
		try (PreparedStatement pstmt = sd.prepareStatement(sql)) {
			pstmt.setInt(1, sId);
			pstmt.setString(2, groupSurveyIdent);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				tables.add(rs.getString(1));
			}
		}
		return tables;
	}

	/*
	 * Insert a copy of a row with a new parent
	 */
	public static String getCopySql(Connection cResults, String tableName, int parkey) throws SQLException {
		StringBuilder cols = new StringBuilder();
		try (PreparedStatement pstmt = cResults.prepareStatement("select column_name from information_schema.columns where table_name = ? "
				+ "and column_name != 'prikey' and column_name != 'parkey' order by ordinal_position asc")) {
			pstmt.setString(1, tableName);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				cols.append(",").append(rs.getString(1));
			}
		}
		return "insert into " + tableName + " (parkey" + cols + ") select " + parkey + cols + " from " + tableName + " where prikey = ?";
	}

	/*
	 * Copy a repeat row to a new parent record, moving its own repeat rows to the copy
	 */
	public static int copyRow(Connection sd, Connection cRel, String tableName, int key, int newParent) throws SQLException {
		int newKey = 0;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtMove = null;
		try {
			pstmt = cRel.prepareStatement(getCopySql(cRel, tableName, newParent), Statement.RETURN_GENERATED_KEYS);
			pstmt.setInt(1, key);
			pstmt.executeUpdate();
			ResultSet rs = pstmt.getGeneratedKeys();
			if(rs.next()) {
				newKey = rs.getInt(1);
				for(String child : getRepeatTables(sd, tableName)) {
					if(GeneralUtilityMethods.tableExists(cRel, child)) {
						if(pstmtMove != null) {try{pstmtMove.close();} catch(Exception e) {}}
						pstmtMove = cRel.prepareStatement("update " + child + " set parkey = ? where parkey = ?");
						pstmtMove.setInt(1, newKey);
						pstmtMove.setInt(2, key);
						pstmtMove.executeUpdate();
					}
				}
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
			if(pstmtMove != null) {try{pstmtMove.close();} catch(Exception e) {}}
		}
		return newKey;
	}

	/*
	 * Delete a repeat row and its own repeat rows
	 */
	public static void deleteRow(Connection sd, Connection cRel, String tableName, int key) throws SQLException {
		PreparedStatement pstmt = null;
		PreparedStatement pstmtKeys = null;
		try {
			for(String child : getRepeatTables(sd, tableName)) {
				if(GeneralUtilityMethods.tableExists(cRel, child)) {
					if(pstmtKeys != null) {try{pstmtKeys.close();} catch(Exception e) {}}
					pstmtKeys = cRel.prepareStatement("select prikey from " + child + " where parkey = ?");
					pstmtKeys.setInt(1, key);
					ResultSet rs = pstmtKeys.executeQuery();
					ArrayList<Integer> childKeys = new ArrayList<>();
					while(rs.next()) {
						childKeys.add(rs.getInt(1));
					}
					for(int childKey : childKeys) {
						deleteRow(sd, cRel, child, childKey);
					}
				}
			}
			pstmt = cRel.prepareStatement("delete from " + tableName + " where prikey = ?");
			pstmt.setInt(1, key);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
			if(pstmtKeys != null) {try{pstmtKeys.close();} catch(Exception e) {}}
		}
	}

	/*
	 * The tables of the repeats inside a repeat
	 */
	public static ArrayList<String> getRepeatTables(Connection sd, String tableName) throws SQLException {
		ArrayList<String> tables = new ArrayList<>();
		String sql = "select distinct table_name from form "
				+ "where reference = 'false' "
				+ "and parentform in (select f_id from form where table_name = ?)";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, tableName);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				tables.add(rs.getString(1));
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
		return tables;
	}

}
