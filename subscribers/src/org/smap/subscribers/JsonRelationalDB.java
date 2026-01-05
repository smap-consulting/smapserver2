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

package org.smap.subscribers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;

import org.smap.sdal.Utilities.AdvisoryLock;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.legacy.SurveyInstance;
import org.smap.sdal.legacy.UtilityMethods;
import org.smap.sdal.managers.SubmissionEventManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.DatabaseConnections;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.MediaChange;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.SubscriberEvent;
import org.smap.sdal.model.Survey;
import org.smap.server.entities.HostUnreachableException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/*
 * JSON submission processor
 * Phase 2: INSERT and UPDATE support
 */
public class JsonRelationalDB extends Subscriber {

	private ResourceBundle localisation;
	private String tz = "UTC";
	private Logger log;
	private AdvisoryLock lockTableChange;

	@Override
	public String getDest() {
		return "relational database";
	}

	@Override
	public ArrayList<MediaChange> upload(Logger log, SurveyInstance instance, InputStream is,
			String submittingUser, boolean temporaryUser, String server, String device,
			SubscriberEvent se, String confFilePath, String formStatus, String basePath,
			String filePath, String updateId, int ue_id, Date uploadTime, String surveyNotes,
			String locationTrigger, String auditFilePath, ResourceBundle l, Survey survey)
			throws HostUnreachableException {

		this.localisation = l;
		this.log = log;
		ArrayList<MediaChange> mediaChanges = new ArrayList<>();

		DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
		DatabaseConnections dbc = new DatabaseConnections();
		SubmissionEventManager sem = new SubmissionEventManager();

		PreparedStatement pstmt = null;
		PreparedStatement pstmtUpdate = null;

		try {
			GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);

			// Read JSON from file
			JsonObject json = readJsonFromFile(filePath);

			// Extract survey identifier and get survey structure
			String surveyIdent = json.get("_survey").getAsString();
			int surveyId = GeneralUtilityMethods.getSurveyId(dbc.sd, surveyIdent);

			// Initialize advisory lock for table creation
			lockTableChange = new AdvisoryLock(dbc.sd, 1, surveyId);

			SurveyManager sm = new SurveyManager(localisation, tz);
			Survey fullSurvey = sm.getById(
					dbc.sd,
					dbc.results,
					submittingUser,
					false,  // temporary user
					surveyId,
					true,   // full survey with questions
					basePath,
					null,   // instance id
					false,  // get results
					false,  // generate dummy values
					true,   // get property type questions
					false,  // get soft deleted
					false,  // get HRK
					"internal", // external options
					false,  // get change history
					false,  // get roles
					false,  // super user
					"geojson", // geom format
					false,  // reference surveys
					false,  // only get launched
					false   // merge default set value
			);

			// Get survey version (latest)
			int version = fullSurvey.surveyData.version;

			// Get top-level form
			if (fullSurvey.surveyData.forms == null || fullSurvey.surveyData.forms.isEmpty()) {
				throw new Exception("Survey has no forms");
			}
			Form topForm = fullSurvey.surveyData.forms.get(0);
			String tableName = topForm.tableName;

			// Extract instanceid and device from JSON
			String instanceId = json.has("instanceid") ? json.get("instanceid").getAsString() : null;
			String deviceId = json.has("_device") ? json.get("_device").getAsString() : null;

			// Build question name to Question object map
			HashMap<String, Question> questionMap = new HashMap<>();
			for (Question q : topForm.questions) {
				questionMap.put(q.name, q);
			}

			// Ensure tables exist (create if first submission)
			UtilityMethods.createSurveyTables(dbc.sd, dbc.results, localisation,
					surveyId, surveyIdent, tz, lockTableChange);

			// Check if record exists
			boolean recordExists = false;
			if (instanceId != null && !instanceId.trim().isEmpty()) {
				recordExists = checkInstanceIdExists(dbc.results, tableName, instanceId);
			}

			// Build and execute appropriate statement
			int prikey = 0;
			if (recordExists) {
				// UPDATE existing record
				pstmt = buildUpdateStatement(dbc.results, tableName, instanceId, json,
						questionMap, uploadTime, surveyId);
				pstmt.executeUpdate();
			} else {
				// INSERT new record
				pstmt = buildInsertStatement(dbc.results, tableName, instanceId, json,
						questionMap, submittingUser, uploadTime, surveyId, version);
				pstmt.executeUpdate();

				// Get generated primary key
				ResultSet rs = pstmt.getGeneratedKeys();
				if (rs.next()) {
					prikey = rs.getInt(1);
				}
			}

			// Update upload_event - set results_db_applied = true
			String updateSql = "UPDATE upload_event SET results_db_applied = 'true', "
					+ "processed_time = now(), queued = false WHERE ue_id = ?";
			pstmtUpdate = dbc.sd.prepareStatement(updateSql);
			pstmtUpdate.setInt(1, ue_id);
			pstmtUpdate.executeUpdate();

			// Create submission event for notifications
			sem.writeToQueue(log, dbc.sd, ue_id, null, String.valueOf(prikey));

		} catch (Exception e) {
			log.severe("Error processing JSON submission: " + e.getMessage());
			e.printStackTrace();

			// Update upload_event with error
			try {
				String updateSql = "UPDATE upload_event SET db_status = ?, db_reason = ? WHERE ue_id = ?";
				pstmtUpdate = dbc.sd.prepareStatement(updateSql);
				pstmtUpdate.setString(1, "error");
				pstmtUpdate.setString(2, e.getMessage());
				pstmtUpdate.setInt(3, ue_id);
				pstmtUpdate.executeUpdate();
			} catch (Exception e2) {
				log.severe("Error updating upload_event with error status: " + e2.getMessage());
			}

		} finally {
			try {
				if (lockTableChange != null) {
					lockTableChange.release("json upload");
					lockTableChange.close("json upload");
				}
			} catch (Exception e) {}
			try { if (pstmt != null) pstmt.close(); } catch (Exception e) {}
			try { if (pstmtUpdate != null) pstmtUpdate.close(); } catch (Exception e) {}
			try { if (dbc.sd != null) dbc.sd.close(); } catch (Exception e) {}
			try { if (dbc.results != null) dbc.results.close(); } catch (Exception e) {}
		}

		return mediaChanges;
	}

	/*
	 * Read JSON from file
	 */
	private JsonObject readJsonFromFile(String filePath) throws Exception {
		StringBuilder jsonString = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
			String line;
			while ((line = reader.readLine()) != null) {
				jsonString.append(line);
			}
		}
		return JsonParser.parseString(jsonString.toString()).getAsJsonObject();
	}

	/*
	 * Check if instanceid exists in results table
	 */
	private boolean checkInstanceIdExists(Connection cResults, String tableName, String instanceId) throws Exception {
		if (instanceId == null || instanceId.trim().isEmpty()) {
			return false;
		}

		String sql = "SELECT prikey FROM " + tableName + " WHERE instanceid = ? LIMIT 1";
		try (PreparedStatement pstmt = cResults.prepareStatement(sql)) {
			pstmt.setString(1, instanceId);
			try (ResultSet rs = pstmt.executeQuery()) {
				return rs.next();
			}
		}
	}

	/*
	 * Build INSERT statement for new record
	 */
	private PreparedStatement buildInsertStatement(Connection cResults, String tableName,
			String instanceId, JsonObject json, HashMap<String, Question> questionMap,
			String submittingUser, Date uploadTime, int surveyId, int version) throws Exception {

		StringBuilder cols = new StringBuilder();
		StringBuilder vals = new StringBuilder();
		ArrayList<ColumnValue> columnValues = new ArrayList<>();

		// Add metadata columns
		addColumn(cols, vals, columnValues, "parkey", "0", "int");
		addColumn(cols, vals, columnValues, "_user", submittingUser, "string");
		addColumn(cols, vals, columnValues, "_complete", "true", "boolean");
		addColumn(cols, vals, columnValues, SmapServerMeta.UPLOAD_TIME_NAME,
				String.valueOf(new Timestamp(uploadTime.getTime())), "timestamp");
		addColumn(cols, vals, columnValues, SmapServerMeta.SURVEY_ID_NAME, String.valueOf(surveyId), "int");
		addColumn(cols, vals, columnValues, "_version", String.valueOf(version), "int");

		if (instanceId != null) {
			addColumn(cols, vals, columnValues, "instanceid", instanceId, "string");
		}

		// Add question columns
		for (String key : json.keySet()) {
			// Skip reserved keys
			if (key.startsWith("_") || key.equals("instanceid")) {
				continue;
			}

			// Check if question exists
			Question q = questionMap.get(key);
			if (q == null) {
				continue; // Silently ignore unknown keys
			}

			String value = json.get(key).getAsString();
			String columnName = q.columnName;
			String type = q.type;

			addColumn(cols, vals, columnValues, columnName, value, type);
		}

		// Build INSERT statement
		String sql = "INSERT INTO " + tableName + " (" + cols + ") VALUES (" + vals + ")";
		PreparedStatement pstmt = cResults.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

		// Set parameter values
		int idx = 1;
		for (ColumnValue cv : columnValues) {
			setParameterValue(pstmt, idx++, cv);
		}

		return pstmt;
	}

	/*
	 * Build UPDATE statement for existing record
	 */
	private PreparedStatement buildUpdateStatement(Connection cResults, String tableName,
			String instanceId, JsonObject json, HashMap<String, Question> questionMap,
			Date uploadTime, int surveyId) throws Exception {

		StringBuilder setClauses = new StringBuilder();
		ArrayList<ColumnValue> columnValues = new ArrayList<>();

		// Add metadata columns to update
		addUpdateColumn(setClauses, columnValues, SmapServerMeta.UPLOAD_TIME_NAME,
				String.valueOf(new Timestamp(uploadTime.getTime())), "timestamp");

		// Add question columns
		for (String key : json.keySet()) {
			// Skip reserved keys
			if (key.startsWith("_") || key.equals("instanceid")) {
				continue;
			}

			// Check if question exists
			Question q = questionMap.get(key);
			if (q == null) {
				continue; // Silently ignore unknown keys
			}

			String value = json.get(key).getAsString();
			String columnName = q.columnName;
			String type = q.type;

			addUpdateColumn(setClauses, columnValues, columnName, value, type);
		}

		// Build UPDATE statement
		String sql = "UPDATE " + tableName + " SET " + setClauses + " WHERE instanceid = ?";
		PreparedStatement pstmt = cResults.prepareStatement(sql);

		// Set parameter values
		int idx = 1;
		for (ColumnValue cv : columnValues) {
			setParameterValue(pstmt, idx++, cv);
		}
		// Set instanceid for WHERE clause
		pstmt.setString(idx, instanceId);

		return pstmt;
	}

	/*
	 * Add column to INSERT statement
	 */
	private void addColumn(StringBuilder cols, StringBuilder vals, ArrayList<ColumnValue> columnValues,
			String columnName, String value, String type) {
		if (cols.length() > 0) {
			cols.append(", ");
		}
		cols.append(columnName);

		if (vals.length() > 0) {
			vals.append(", ");
		}

		// Handle geometry types specially
		if (type.equals("geopoint")) {
			vals.append("ST_GeomFromText(?, 4326)");
		} else if (type.equals("geoshape")) {
			vals.append("ST_GeomFromText(?, 4326)");
		} else if (type.equals("geotrace") || type.equals("geocompound")) {
			vals.append("ST_GeomFromText(?, 4326)");
		} else {
			vals.append("?");
		}

		columnValues.add(new ColumnValue(columnName, value, type));
	}

	/*
	 * Add column to UPDATE statement
	 */
	private void addUpdateColumn(StringBuilder setClauses, ArrayList<ColumnValue> columnValues,
			String columnName, String value, String type) {
		if (setClauses.length() > 0) {
			setClauses.append(", ");
		}

		// Handle geometry types specially
		if (type.equals("geopoint") || type.equals("geoshape") ||
		    type.equals("geotrace") || type.equals("geocompound")) {
			setClauses.append(columnName).append(" = ST_GeomFromText(?, 4326)");
		} else {
			setClauses.append(columnName).append(" = ?");
		}

		columnValues.add(new ColumnValue(columnName, value, type));
	}

	/*
	 * Set prepared statement parameter value with type conversion
	 */
	private void setParameterValue(PreparedStatement pstmt, int idx, ColumnValue cv) throws Exception {
		if (cv.value == null || cv.value.isEmpty()) {
			pstmt.setNull(idx, java.sql.Types.VARCHAR);
			return;
		}

		try {
			if (cv.type.equals("int") || cv.type.equals("integer")) {
				pstmt.setInt(idx, Integer.parseInt(cv.value));
			} else if (cv.type.equals("double") || cv.type.equals("decimal")) {
				pstmt.setDouble(idx, Double.parseDouble(cv.value));
			} else if (cv.type.equals("boolean")) {
				pstmt.setBoolean(idx, Boolean.parseBoolean(cv.value));
			} else if (cv.type.equals("timestamp") || cv.type.equals("dateTime")) {
				pstmt.setTimestamp(idx, GeneralUtilityMethods.getTimestamp(cv.value));
			} else if (cv.type.equals("date")) {
				pstmt.setDate(idx, java.sql.Date.valueOf(LocalDate.parse(cv.value)));
			} else if (cv.type.equals("time")) {
				pstmt.setTime(idx, java.sql.Time.valueOf(cv.value));
			} else if (cv.type.equals("geopoint")) {
				// Convert "lat lon" to "POINT(lon lat)"
				String[] parts = cv.value.trim().split("\\s+");
				if (parts.length >= 2) {
					String wkt = "POINT(" + parts[1] + " " + parts[0] + ")";
					pstmt.setString(idx, wkt);
				} else {
					pstmt.setNull(idx, java.sql.Types.VARCHAR);
				}
			} else if (cv.type.equals("geoshape")) {
				// Convert to POLYGON WKT format
				// For now, pass as-is and let PostGIS handle it
				pstmt.setString(idx, cv.value);
			} else if (cv.type.equals("geotrace") || cv.type.equals("geocompound")) {
				// Convert to LINESTRING WKT format
				pstmt.setString(idx, cv.value);
			} else {
				// Default: string types (text, select_one, select_multiple, etc.)
				pstmt.setString(idx, cv.value);
			}
		} catch (Exception e) {
			throw new Exception("Type conversion error for column " + cv.columnName +
					" (type: " + cv.type + ", value: " + cv.value + "): " + e.getMessage());
		}
	}

	/*
	 * Helper class to store column values
	 */
	private static class ColumnValue {
		String columnName;
		String value;
		String type;

		ColumnValue(String columnName, String value, String type) {
			this.columnName = columnName;
			this.value = value;
			this.type = type;
		}
	}
}
