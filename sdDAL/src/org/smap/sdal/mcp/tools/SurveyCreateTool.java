package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Project;

/*
 * Start a new survey, empty or as a copy of one that exists.
 *
 * Creating and duplicating are one call in Smap and are one tool here for the same reason: the
 * difference is whether an existing survey is named, and splitting them would be two tools wrapping
 * the same manager with the same arguments.
 *
 * This is how a model builds a form.  The XLSForm path is not exposed, because an agent cannot
 * produce a spreadsheet it has never seen; it makes a survey here and then puts questions in it with
 * survey_add_question, which is the same thing the online editor does.
 *
 * It also makes a form that works on the SAME RECORD as an existing one, which is what a case
 * management process is built out of.  A bail application is registered at reception, annotated by
 * the Public Prosecutor, given a hearing date at listings and heard by the bail officer - four
 * stages, four forms, one record.  The forms share the record by being in one **bundle**, which
 * shares a results table.
 *
 * Each stage's form lives in the project whose members perform that stage, and that is the whole
 * routing mechanism: somebody sees the form for their step because they are in its project.  A
 * process built as one form with several sections works and assigns correctly and then nobody
 * assigned can open it, because they are not in the single project that one form lives in.
 *
 * **The bundle is fixed here and cannot be changed afterwards.**  It decides how the results tables
 * are built, so there is no later operation that moves a survey into a bundle - getting it wrong
 * means building the forms again.
 */
public class SurveyCreateTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_create";
	}

	@Override
	public String getTitle() {
		return "Create a survey";
	}

	@Override
	public String getDescription() {
		return "Creates a survey in a project: empty, a copy of another, or a form that works on the "
				+ "SAME RECORD as another - which is how the stages of a case management process are "
				+ "built, each in the project whose people do that stage. Sharing a record cannot be "
				+ "changed afterwards. Add questions with survey_add_question.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.WRITE;
	}

	@Override
	public boolean isMutating() {
		return true;
	}

	@Override
	public String getReversal() {
		return "survey_delete removes it, and survey_undelete would bring it back again";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"name", property("string", "What the survey is called"),
				"project_id", property("integer",
						"The project it belongs to, from project_list"),
				"copy_of_survey_id", property("integer",
						"Optional. Copy this survey's design instead of starting empty. The copy "
						+ "gets the questions and none of the answers."),
				"same_record_as_survey_id", property("integer",
						"Optional. Make this a further stage of an existing process: the new form "
						+ "works on the SAME RECORD as that survey, so one is filled in after the "
						+ "other and both read and write the same answers. It starts as a copy of "
						+ "that survey's design. Cannot be changed afterwards."));
		schema.put("required", new String[] { "name", "project_id" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String name = stringArg(arguments, "name");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A name is required.", true);
		}
		name = name.trim();

		int projectId = intArg(arguments, "project_id", 0);
		if(projectId <= 0) {
			return new MCPToolResult("A project_id is required. Use project_list to find one.", true);
		}
		/*
		 * The project has to be one this user is a member of.  createNewSurvey is given the project
		 * as a number and does not ask whether the caller may put a survey in it, so it is asked
		 * here, the same way every other tool decides what the caller can reach.
		 */
		boolean inProject = false;
		String projectName = null;
		for(Project p : new org.smap.sdal.managers.ProjectManager(ctx.localisation)
				.getProjects(ctx.sd, ctx.user, false, false, null, false, false)) {
			if(p.id == projectId) {
				inProject = true;
				projectName = p.name;
				break;
			}
		}
		if(!inProject) {
			return new MCPToolResult("No such project, or you are not a member of it. project_list "
					+ "shows the ones you can use.", true);
		}

		int copyOf = intArg(arguments, "copy_of_survey_id", 0);
		int sameRecordAs = intArg(arguments, "same_record_as_survey_id", 0);

		/*
		 * Sharing a record is always also a copy: the bundle is read from the survey being joined,
		 * and createNewSurvey only looks it up when it has been given one to start from.  That suits
		 * what these forms are - a later stage usually opens with the same identifying questions as
		 * the stage before it, and then diverges.
		 */
		if(sameRecordAs > 0 && copyOf > 0 && sameRecordAs != copyOf) {
			return new MCPToolResult("copy_of_survey_id and same_record_as_survey_id name different "
					+ "surveys. A form that shares a record is created from the survey it shares it "
					+ "with, so give one or the other.", true);
		}
		boolean shareRecord = sameRecordAs > 0;
		if(shareRecord) {
			copyOf = sameRecordAs;
		}

		String copiedFrom = null;
		if(copyOf > 0) {
			org.smap.sdal.model.Survey source = org.smap.sdal.mcp.McpData.surveyById(ctx, copyOf);
			if(source == null) {
				return new MCPToolResult(shareRecord
						? "No such survey to share a record with, or you do not have access to it."
						: "No such survey to copy, or you do not have access to it.", true);
			}
			copiedFrom = source.getDisplayName();
		}

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		int sId;
		try {
			sId = sm.createNewSurvey(ctx.sd, name, projectId,
					copyOf > 0,		// existing
					copyOf,			// existingSurveyId
					shareRecord,	// sharedResults - one bundle, one results table, one record
					ctx.user,
					false);			// superUser - the caller's own rights
		} catch (Exception e) {
			/*
			 * Joining a bundle whose surveys carry role filters is refused for anybody but a server
			 * owner, because the new form would read records the filters were written to restrict.
			 * That refusal is an answer rather than a failure, so it is passed on as one.
			 */
			String msg = e.getMessage() == null ? "it could not be created" : e.getMessage();
			return new MCPToolResult("\"" + name + "\" was not created: " + msg, true);
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("created", Boolean.TRUE);
		data.put("survey_id", sId);
		data.put("name", name);
		data.put("project", projectName);
		if(copiedFrom != null) {
			data.put("copiedFrom", copiedFrom);
		}
		data.put("sharesRecordWith", shareRecord ? copiedFrom : "");

		StringBuilder text = new StringBuilder();
		text.append("Created \"").append(name).append("\" in ").append(projectName)
				.append(" as survey ").append(sId).append(".");
		if(shareRecord) {
			text.append(" It works on the same record as \"").append(copiedFrom)
					.append("\" - one bundle, one results table - so a record registered there is "
							+ "read and written by this form too. It starts with that survey's "
							+ "questions; add the ones this stage needs and delete the ones it does "
							+ "not.");
			text.append("\n\nWhoever performs this stage reaches it by being in ")
					.append(projectName)
					.append(". That is what decides who sees it.");
		} else if(copiedFrom != null) {
			text.append(" It is a copy of \"").append(copiedFrom)
					.append("\", with that survey's questions and none of its answers.");
		} else {
			text.append(" It has no questions yet - add them with survey_add_question.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
