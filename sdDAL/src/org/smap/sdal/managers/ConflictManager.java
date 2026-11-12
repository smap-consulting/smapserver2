package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.DataItemChange;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.SubFormRowChange;
import org.smap.sdal.model.UpdateConflict;
import org.smap.sdal.model.UpdateConflicts;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Review the answers two people changed at the same time on a record
 * The answers of the person who saved last were applied.  Reviewing an answer either keeps it
 * or puts back the answer of the other person, as a new version of the record
 */
public class ConflictManager {

	private static Logger log = Logger.getLogger(ConflictManager.class.getName());

	public static final String USE_APPLIED = "applied";
	public static final String USE_THEIRS = "theirs";

	private ResourceBundle localisation;
	private String tz;

	public ConflictManager(ResourceBundle l, String tz) {
		localisation = l;
		this.tz = tz == null ? "UTC" : tz;
	}

	/*
	 * Apply the choices made for the answers of a conflict
	 * choices maps the index of an answer in the conflict to applied or theirs
	 * Returns the instance id of the latest version of the record
	 */
	public String resolve(Connection sd, Connection cResults, int sId, int eventId,
			HashMap<Integer, String> choices, String user) throws Exception {

		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

		String surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
		String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdent(sd, sId);
		int bundleSurveyId = GeneralUtilityMethods.getSurveyId(sd, groupSurveyIdent);
		if(bundleSurveyId <= 0) {
			bundleSurveyId = sId;
		}
		String table = GeneralUtilityMethods.getMainResultsTable(sd, cResults, sId);

		/*
		 * Get the conflict, it must be on a record of this survey
		 */
		UpdateConflicts uc = null;
		String instanceId = null;
		try (PreparedStatement pstmt = sd.prepareStatement("select instanceid, changes from record_event "
				+ "where id = ? and event = ? and table_name = ?")) {
			pstmt.setInt(1, eventId);
			pstmt.setString(2, RecordEventManager.CONFLICT);
			pstmt.setString(3, table);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				instanceId = rs.getString(1);
				uc = gson.fromJson(rs.getString(2), UpdateConflicts.class);
			}
		}
		if(uc == null || uc.items == null) {
			throw new ApplicationException(localisation.getString("rec_na"));
		}

		RoleManager rm = new RoleManager(localisation);
		if(!rm.canAccessRecord(sd, cResults, surveyIdent, table, instanceId, user, tz)) {
			throw new ApplicationException(localisation.getString("rec_na"));
		}

		/*
		 * The answers to be put back
		 */
		ArrayList<UpdateConflict> theirs = new ArrayList<>();
		for(Integer idx : choices.keySet()) {
			String choice = choices.get(idx);
			if(idx == null || idx < 0 || idx >= uc.items.size()) {
				throw new ApplicationException("Invalid conflict item");
			}
			if(!USE_APPLIED.equals(choice) && !USE_THEIRS.equals(choice)) {
				throw new ApplicationException("Invalid choice");
			}
			UpdateConflict c = uc.items.get(idx);
			if(c.resolution != null) {
				continue;		// Already reviewed
			}
			if(USE_THEIRS.equals(choice)) {
				theirs.add(c);
			}
			c.resolution = choice;
		}

		String latestInstanceId = GeneralUtilityMethods.getLatestInstanceId(cResults, table, instanceId);
		boolean autoCommit = cResults.getAutoCommit();
		RecordVersionManager.lock(sd, bundleSurveyId);
		try {
			cResults.setAutoCommit(false);

			if(theirs.size() > 0) {
				Form topForm = GeneralUtilityMethods.getTopLevelForm(sd, sId);
				RecordVersionManager rvm = new RecordVersionManager(localisation);
				String newInstanceId = rvm.newVersion(sd, cResults, sId, groupSurveyIdent, topForm.id, table, instanceId, user, tz);
				latestInstanceId = newInstanceId;
				int newKey = getKey(cResults, table, newInstanceId);
				if(newKey == 0) {
					throw new ApplicationException(localisation.getString("rec_na"));
				}

				ArrayList<DataItemChange> changes = new ArrayList<>();
				HashMap<String, ArrayList<SubFormRowChange>> repeatChanges = new HashMap<>();
				HashMap<String, String> repeatNames = new HashMap<>();

				for(UpdateConflict c : theirs) {
					if(c.repeat == null) {
						if(UpdateConflict.VALUE.equals(c.kind)) {
							checkColumn(sd, table, c.col);
							String current = getValue(cResults, table, c.col, newKey, c.type);
							copyColumn(cResults, table, c.col, c.theirsKey, newKey);
							changes.add(new DataItemChange(c.col, c.name, c.type, c.theirs, current));
						}
					} else {
						checkRepeatTable(sd, table, c.repeatTable);
						ArrayList<SubFormRowChange> rows = repeatChanges.get(c.repeatTable);
						if(rows == null) {
							rows = new ArrayList<>();
							repeatChanges.put(c.repeatTable, rows);
							repeatNames.put(c.repeatTable, c.repeat);
						}
						int rowKey = c.rowId == null ? 0 : getRowKey(cResults, c.repeatTable, newKey, c.rowId);
						if(UpdateConflict.VALUE.equals(c.kind)) {
							if(rowKey > 0) {
								checkColumn(sd, c.repeatTable, c.col);
								String current = getValue(cResults, c.repeatTable, c.col, rowKey, c.type);
								copyColumn(cResults, c.repeatTable, c.col, c.theirsKey, rowKey);
								ArrayList<DataItemChange> rowChange = new ArrayList<>();
								rowChange.add(new DataItemChange(c.col, c.name, c.type, c.theirs, current));
								rows.add(new SubFormRowChange(SubFormRowChange.CHANGED, rowChange));
							}
						} else if(UpdateConflict.THEIRS_DELETED.equals(c.kind)) {
							// The other person deleted the row, delete it
							if(rowKey > 0) {
								RecordVersionManager.deleteRow(sd, cResults, c.repeatTable, rowKey);
								rows.add(new SubFormRowChange(SubFormRowChange.DELETED_RECORD, c.values));
							}
						} else if(UpdateConflict.MINE_DELETED.equals(c.kind)) {
							// The other person changed the row, put it back
							if(c.theirsKey > 0) {
								RecordVersionManager.copyRow(sd, cResults, c.repeatTable, c.theirsKey, newKey);
								rows.add(new SubFormRowChange(SubFormRowChange.NEW_RECORD, c.values));
							}
						}
					}
				}
				for(String repeatTable : repeatChanges.keySet()) {
					ArrayList<SubFormRowChange> rows = repeatChanges.get(repeatTable);
					if(rows.size() > 0) {
						changes.add(new DataItemChange(repeatTable, repeatNames.get(repeatTable), rows));
					}
				}

				if(changes.size() > 0) {
					RecordEventManager rem = new RecordEventManager();
					rem.writeEvent(sd, cResults, RecordEventManager.CHANGES, RecordEventManager.STATUS_SUCCESS, user,
							table, newInstanceId, gson.toJson(changes), null, null, null,
							localisation.getString("conflict_theirs"), sId, null, 0, 0);
				}
			}

			/*
			 * Record the review, the conflict is resolved when every answer has been reviewed
			 */
			boolean resolved = true;
			for(UpdateConflict c : uc.items) {
				if(c.resolution == null) {
					resolved = false;
					break;
				}
			}
			if(resolved) {
				uc.resolvedBy = user;
				uc.resolvedTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
			}
			try (PreparedStatement pstmt = sd.prepareStatement("update record_event set changes = ?, status = ? where id = ?")) {
				pstmt.setString(1, gson.toJson(uc));
				pstmt.setString(2, resolved ? RecordEventManager.STATUS_RESOLVED : RecordEventManager.STATUS_UNRESOLVED);
				pstmt.setInt(3, eventId);
				pstmt.executeUpdate();
			}

			cResults.commit();
			log.info("Conflict " + eventId + " reviewed by " + user + (resolved ? ", resolved" : ""));
			return latestInstanceId;

		} catch (Exception e) {
			try {cResults.rollback();} catch (Exception ex) {}
			throw e;
		} finally {
			try {cResults.setAutoCommit(autoCommit);} catch (Exception ex) {}
			RecordVersionManager.unlock(sd, bundleSurveyId);
		}
	}

	private int getKey(Connection cResults, String table, String instanceId) throws SQLException {
		try (PreparedStatement pstmt = cResults.prepareStatement("select prikey from " + table + " where instanceid = ?")) {
			pstmt.setString(1, instanceId);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		}
	}

	/*
	 * The row of a repeat in a version of a record
	 */
	private int getRowKey(Connection cResults, String repeatTable, int parkey, String rowId) throws SQLException {
		if(!GeneralUtilityMethods.hasColumn(cResults, repeatTable, GetXForm.ROW_ID)) {
			return 0;
		}
		try (PreparedStatement pstmt = cResults.prepareStatement("select prikey from " + repeatTable
				+ " where parkey = ? and " + GetXForm.ROW_ID + " = ?")) {
			pstmt.setInt(1, parkey);
			pstmt.setString(2, rowId);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		}
	}

	private String getValue(Connection cResults, String table, String col, int key, String type) throws SQLException {
		String sql = GeneralUtilityMethods.isGeometry(type)
				? "select ST_AsGeoJSON(" + col + ") from " + table + " where prikey = ?"
				: "select " + col + " from " + table + " where prikey = ?";
		try (PreparedStatement pstmt = cResults.prepareStatement(sql)) {
			pstmt.setInt(1, key);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getString(1) : null;
		}
	}

	private void copyColumn(Connection cResults, String table, String col, int fromKey, int toKey) throws SQLException {
		try (PreparedStatement pstmt = cResults.prepareStatement("update " + table
				+ " set " + col + " = (select " + col + " from " + table + " where prikey = ?) "
				+ "where prikey = ?")) {
			pstmt.setInt(1, fromKey);
			pstmt.setInt(2, toKey);
			pstmt.executeUpdate();
		}
	}

	/*
	 * The names in a conflict are stored data, check them before they are used in sql
	 */
	private void checkColumn(Connection sd, String table, String col) throws SQLException, ApplicationException {
		boolean valid = false;
		if(col != null) {
			try (PreparedStatement pstmt = sd.prepareStatement("select count(*) from question q, form f "
					+ "where q.f_id = f.f_id and f.table_name = ? and q.column_name = ?")) {
				pstmt.setString(1, table);
				pstmt.setString(2, col);
				ResultSet rs = pstmt.executeQuery();
				valid = rs.next() && rs.getInt(1) > 0;
			}
		}
		if(!valid) {
			throw new ApplicationException("Invalid question");
		}
	}

	private void checkRepeatTable(Connection sd, String table, String repeatTable) throws SQLException, ApplicationException {
		boolean valid = false;
		if(repeatTable != null) {
			try (PreparedStatement pstmt = sd.prepareStatement("select count(*) from form "
					+ "where table_name = ? and parentform in (select f_id from form where table_name = ?)")) {
				pstmt.setString(1, repeatTable);
				pstmt.setString(2, table);
				ResultSet rs = pstmt.executeQuery();
				valid = rs.next() && rs.getInt(1) > 0;
			}
		}
		if(!valid) {
			throw new ApplicationException("Invalid repeat");
		}
	}
}
