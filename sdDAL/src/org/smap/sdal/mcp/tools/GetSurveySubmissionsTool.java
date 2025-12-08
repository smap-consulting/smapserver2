package org.smap.sdal.mcp.tools;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.mcp.IMCPTool;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.MCPToolDefinition;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Survey;


/**
 * Tool to get submission statistics for surveys
 */
public class GetSurveySubmissionsTool implements IMCPTool {

	private static Logger log = Logger.getLogger(IMCPTool.class.getName());
	
	@Override
	public MCPToolDefinition getDefinition() {
		Map<String, Object> inputSchema = new HashMap<>();
		inputSchema.put("type", "object");

		Map<String, Object> properties = new HashMap<>();

		// Optional survey_id parameter
		Map<String, Object> surveyIdProperty = new HashMap<>();
		surveyIdProperty.put("type", "integer");
		surveyIdProperty.put("description", "Optional survey ID to get submissions for a specific survey. Omit to get counts for all surveys.");
		properties.put("survey_id", surveyIdProperty);

		// Optional project_id parameter
		Map<String, Object> projectIdProperty = new HashMap<>();
		projectIdProperty.put("type", "integer");
		projectIdProperty.put("description", "Optional project ID to filter surveys. Set to 0 or omit to get all surveys.");
		properties.put("project_id", projectIdProperty);

		inputSchema.put("properties", properties);

		return new MCPToolDefinition(
			"get_survey_submissions",
			"Gets submission counts and statistics for surveys. Can filter by survey ID or project ID.",
			inputSchema
		);
	}

	@Override
	public MCPToolResult execute(Connection sd, String username, Map<String, Object> arguments) throws Exception {
		
		/*
		 * Add authorisations
		 * Allow administrators and analysts
		 */
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.ADMIN);
		Authorise a = new Authorise(authorisations, null);
		a.isAuthorised(sd, username);
		

		/*
		 * Parse arguments
		 * Validate that the resources requested (project or survey) are in the users organisation
		 * The user does not need to have explicit access to the resource when accessing via the MCP end point
		 */
		Integer surveyId = null;
		if (arguments.get("survey_id") != null) {
			Object surveyIdObj = arguments.get("survey_id");
			if (surveyIdObj instanceof Number) {
				surveyId = ((Number) surveyIdObj).intValue();
				a.surveyInUsersOrganisation(sd, username, GeneralUtilityMethods.getSurveyIdent(sd, surveyId));
			}
		}

		int projectId = 0;
		if (arguments.get("project_id") != null) {
			Object projectIdObj = arguments.get("project_id");
			if (projectIdObj instanceof Number) {
				projectId = ((Number) projectIdObj).intValue();
				a.projectInUsersOrganisation(sd, username, projectId);
			}
		}

		// Get surveys using SurveyManager
		SurveyManager sm = new SurveyManager(null, "UTC");
		ArrayList<Survey> surveys = sm.getSurveys(
			sd,
			username,
			false,           // getDeleted
			false,           // getBlocked
			projectId,       // projectId (0 = all projects)
			false,           // superUser
			false,           // onlyGroup
			false,           // getGroupDetails
			false,           // onlyDataSurvey
			false,           // links
			null             // urlprefix
		);

		// Filter by survey ID if provided
		if (surveyId != null) {
			final int targetId = surveyId;
			surveys.removeIf(survey -> survey.getId() != targetId);
		}

		// Get submission counts for each survey
		StringBuilder result = new StringBuilder();

		if (surveys.isEmpty()) {
			result.append("No surveys found.");
		} else {
			result.append("Survey Submission Statistics:\n\n");

			int totalSubmissions = 0;
			for (Survey survey : surveys) {
				int count = getSubmissionCount(sd, survey.getIdent());
				totalSubmissions += count;

				result.append("- ").append(survey.getDisplayName());
				result.append(" (ID: ").append(survey.getId()).append(")");
				result.append("\n  Project: ").append(survey.getProjectName());
				result.append("\n  Submissions: ").append(count);
				result.append("\n");
			}

			result.append("\nTotal surveys: ").append(surveys.size());
			result.append("\nTotal submissions: ").append(totalSubmissions);
		}

		return new MCPToolResult(result.toString());
	}

	/**
	 * Get the submission count for a survey
	 * @param sd Database connection
	 * @param surveyIdent Survey identifier
	 * @return Number of submissions
	 */
	private int getSubmissionCount(Connection sd, String surveyIdent) {
		int count = 0;

		// Note: We need to use a different connection for the results database
		// For now, we'll try to get it from upload_event table which tracks submissions
		String sqlCount = "SELECT COUNT(*) FROM upload_event "
				+ "WHERE ident = ? AND status != 'error'";

		try (PreparedStatement pstmtCount = sd.prepareStatement(sqlCount)) {
			pstmtCount.setString(1, surveyIdent);

			try (ResultSet rsCount = pstmtCount.executeQuery()) {
				if (rsCount.next()) {
					count = rsCount.getInt(1);
				}
			}
		} catch (Exception e) {
			// Return 0 if we can't get the count
			log.log(Level.SEVERE, e.getMessage(), e);
		} 

		return count;
	}
}
