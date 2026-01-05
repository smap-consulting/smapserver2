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

package surveyMobileAPI;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.Date;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.FileUtils;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.Survey;
import org.smap.server.entities.UploadEvent;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import JdbcManagers.JdbcUploadEventManager;

/*
 * Process JSON form submissions
 * Phase 1: Single JSON object only (no batch arrays)
 */
public class JsonFormData {

	private static Logger log = Logger.getLogger(JsonFormData.class.getName());

	public void loadJson(Connection sd, ResourceBundle localisation, HttpServletRequest request, String user) throws Exception {

		String basePath = GeneralUtilityMethods.getBasePath(request);
		String serverName = request.getServerName();

		// Read JSON from request body
		StringBuilder jsonString = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream(), "UTF-8"))) {
			String line;
			while ((line = reader.readLine()) != null) {
				jsonString.append(line);
			}
		} catch (IOException e) {
			log.severe("Error reading JSON from request: " + e.getMessage());
			throw new Exception("Failed to read JSON from request");
		}

		// Parse JSON
		JsonObject json;
		try {
			json = JsonParser.parseString(jsonString.toString()).getAsJsonObject();
		} catch (Exception e) {
			log.severe("Invalid JSON: " + e.getMessage());
			throw new Exception("Invalid JSON format");
		}

		// Extract required fields
		if (!json.has("_survey")) {
			throw new Exception("Missing required field: _survey");
		}
		String surveyIdent = json.get("_survey").getAsString();

		// Extract optional instanceid (generate UUID if not present)
		String instanceId;
		if (json.has("instanceid")) {
			instanceId = json.get("instanceid").getAsString();
		} else {
			instanceId = UUID.randomUUID().toString();
		}

		// Extract optional device ID
		String deviceId = null;
		if (json.has("_device")) {
			deviceId = json.get("_device").getAsString();
		}

		// Get survey details
		int surveyId = GeneralUtilityMethods.getSurveyId(sd, surveyIdent);
		SurveyManager sm = new SurveyManager(localisation, "UTC");
		Survey survey = sm.getSurveyId(sd, surveyIdent);
		if (survey == null) {
			throw new Exception("Survey not found: " + surveyIdent);
		}

		// Check if survey is deleted or blocked
		if (survey.surveyData.deleted) {
			throw new Exception("Survey has been deleted: " + surveyIdent);
		}
		if (survey.surveyData.blocked) {
			throw new Exception("Survey is blocked: " + surveyIdent);
		}

		// Check organization submission limits
		// TODO: Add organization limit check if needed (see XFormData.java line 340+)

		// Save JSON to disk
		String surveyPath = basePath + "/uploadedSurveys/" + surveyIdent;
		String instancePath = surveyPath + "/" + instanceId;

		// Create directories
		File folder = new File(surveyPath);
		FileUtils.forceMkdir(folder);
		folder = new File(instancePath);
		FileUtils.forceMkdir(folder);

		String filePath = instancePath + "/" + instanceId + ".json";
		String fileName = instanceId + ".json";

		// Write JSON to file
		try {
			File jsonFile = new File(filePath);
			FileUtils.writeStringToFile(jsonFile, jsonString.toString(), "UTF-8");
			log.info("Saved JSON file: " + filePath);
		} catch (IOException e) {
			log.severe("Error saving JSON file: " + e.getMessage());
			throw new Exception("Failed to save JSON file");
		}

		// Create and write upload event
		UploadEvent ue = new UploadEvent();
		ue.setUserName(user);
		ue.setServerName(serverName);
		ue.setSurveyId(surveyId);
		ue.setIdent(surveyIdent);
		ue.setFilePath(filePath);
		ue.setProjectId(survey.getPId());
		ue.setOrganisationId(survey.surveyData.o_id);
		ue.setEnterpriseId(survey.surveyData.e_id);
		ue.setUploadTime(new Date());
		ue.setFileName(fileName);
		ue.setSurveyName(survey.getDisplayName());
		ue.setInstanceId(instanceId);
		ue.setImei(deviceId);
		ue.setOrigSurveyIdent(surveyIdent);
		ue.setStatus("success");
		ue.setIncomplete(false);

		JdbcUploadEventManager uem = null;
		try {
			uem = new JdbcUploadEventManager(sd);
			uem.write(ue, false);  // results_db_applied = false
			log.info("Created upload_event for instanceId: " + instanceId);
		} finally {
			if (uem != null) {
				uem.close();
			}
		}

		log.info("userevent: " + user + " : JSON upload : " + survey.getDisplayName());
	}
}
