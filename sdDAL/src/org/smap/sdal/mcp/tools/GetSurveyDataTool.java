package org.smap.sdal.mcp.tools;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.mcp.IMCPTool;
import org.smap.sdal.managers.DataManager;
import org.smap.sdal.model.MCPToolDefinition;
import org.smap.sdal.model.MCPToolResult;

/**
 * Tool to get survey data in hierarchical format
 * Uses the getRecordHierarchy method from DataManager to return survey data
 * with repeating groups nested within their parent records
 */
public class GetSurveyDataTool implements IMCPTool {

	private static Logger log = Logger.getLogger(GetSurveyDataTool.class.getName());

	@Override
	public MCPToolDefinition getDefinition() {
		Map<String, Object> inputSchema = new HashMap<>();
		inputSchema.put("type", "object");

		Map<String, Object> properties = new HashMap<>();

		// Required survey_id parameter
		Map<String, Object> surveyIdProperty = new HashMap<>();
		surveyIdProperty.put("type", "integer");
		surveyIdProperty.put("description", "Survey ID to get data from");
		properties.put("survey_id", surveyIdProperty);

		// Optional uuid parameter
		Map<String, Object> uuidProperty = new HashMap<>();
		uuidProperty.put("type", "string");
		uuidProperty.put("description", "Optional UUID of a specific record. If omitted, returns all submissions");
		properties.put("uuid", uuidProperty);

		// Optional include_meta parameter
		Map<String, Object> includeMetaProperty = new HashMap<>();
		includeMetaProperty.put("type", "boolean");
		includeMetaProperty.put("description", "Include metadata fields (prikey, hrk, etc.). Default: false");
		properties.put("include_meta", includeMetaProperty);

		inputSchema.put("properties", properties);

		// survey_id is required
		ArrayList<String> required = new ArrayList<>();
		required.add("survey_id");
		inputSchema.put("required", required);

		return new MCPToolDefinition(
			"get_survey_data",
			"Gets survey data in hierarchical format with repeating groups nested within parent records. Returns all submissions or a specific record by UUID.",
			inputSchema
		);
	}

	@Override
	public MCPToolResult execute(Connection sd, String username, Map<String, Object> arguments) throws Exception {

		/*
		 * Add authorisations
		 */
		ArrayList<String> authorisations = new ArrayList<String>();
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.ADMIN);
		Authorise a = new Authorise(authorisations, null);
		a.isAuthorised(sd, username);

		/*
		 * Parse arguments
		 */
		if (arguments.get("survey_id") == null) {
			throw new Exception("survey_id is required");
		}

		int surveyId;
		Object surveyIdObj = arguments.get("survey_id");
		if (surveyIdObj instanceof Number) {
			surveyId = ((Number) surveyIdObj).intValue();
		} else {
			throw new Exception("survey_id must be a number");
		}

		// Get survey identifier and validate it's in user's organisation
		String surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, surveyId);
		a.surveyInUsersOrganisation(sd, username, surveyIdent);

		// Optional UUID parameter
		String uuid = null;
		if (arguments.get("uuid") != null) {
			uuid = arguments.get("uuid").toString();
		}

		// Optional include_meta parameter
		boolean includeMeta = false;
		if (arguments.get("include_meta") != null) {
			Object includeMetaObj = arguments.get("include_meta");
			if (includeMetaObj instanceof Boolean) {
				includeMeta = (Boolean) includeMetaObj;
			}
		}

		// Get results database connection
		Connection cResults = null;
		String connectionString = "MCP-GetSurveyData";

		try {
			cResults = ResultsDataSource.getConnection(connectionString);

			// Get locale and resource bundle
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, null, username));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			// Get timezone for the user's organisation
			int oId = GeneralUtilityMethods.getOrganisationId(sd, username);
			String tz = GeneralUtilityMethods.getOrganisationTZ(sd, oId);
			if (tz == null) {
				tz = "UTC";
			}

			// Create DataManager
			DataManager dm = new DataManager(localisation, tz);

			// Call getRecordHierarchy
			Response response = dm.getRecordHierarchy(
				sd,
				cResults,
				username,
				surveyIdent,
				surveyId,
				uuid,
				"no",           // merge - don't merge select multiples
				localisation,
				tz,
				includeMeta,
				null,           // urlprefix
				null,           // attachmentPrefix
				false    		// Do not poll
			);

			// Extract the response entity as a string
			String responseData = response.getEntity().toString();

			// Format result for MCP
			StringBuilder result = new StringBuilder();
			result.append("Survey Data (ID: ").append(surveyId).append("):\n\n");
			result.append(responseData);

			return new MCPToolResult(result.toString());

		} finally {
			if (cResults != null) {
				try {
					ResultsDataSource.closeConnection(connectionString, cResults);
				} catch (Exception e) {
					log.severe("Error closing results connection: " + e.getMessage());
				}
			}
		}
	}
}
