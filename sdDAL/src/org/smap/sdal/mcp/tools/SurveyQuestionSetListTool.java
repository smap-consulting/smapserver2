package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpSurveyLayout;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.ChangeItem;
import org.smap.sdal.model.ChangeSet;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.PropertyChange;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * Point a select question at a choice list, or move it to a different one.
 *
 * The same act either way - a select question holds a list id and this changes it - but the two cases
 * feel different to whoever is asking, so both are named in the description.
 *
 * On a question that has already been answered this is worth understanding before doing.  The answers
 * already stored are values, not references: nothing rewrites them and nothing checks them again.  So
 * a record answered "faso" keeps "faso" whether or not the new list offers it, and it will show as a
 * code with no label behind it if it does not.  Rather than refuse - moving a question to a corrected
 * list is a legitimate repair - this reports which stored answers the new list does not account for,
 * which is the thing a caller cannot easily work out and would otherwise find out from a report.
 */
public class SurveyQuestionSetListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_question_set_list";
	}

	@Override
	public String getTitle() {
		return "Set a question's choices";
	}

	@Override
	public String getDescription() {
		return "Points a select question at a choice list, or moves it to a different one. Answers "
				+ "already collected are not rewritten, so if the new list does not offer a value "
				+ "somebody already gave, that answer shows as a code with no label - this reports "
				+ "which. Make or change a list with survey_options_edit.";
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
		return "survey_question_set_list again with the previous list, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ANALYST);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to change, from survey_list"),
				"name", property("string",
						"The select question to change, from survey_questions"),
				"list_name", property("string",
						"The choice list it should use, from survey_options. It has to exist - make "
								+ "one with survey_options_edit."));
		schema.put("required", new String[] { "survey_id", "name", "list_name" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}

		String name = stringArg(arguments, "name");
		String listName = stringArg(arguments, "list_name");
		if(name == null || name.trim().isEmpty() || listName == null || listName.trim().isEmpty()) {
			return new MCPToolResult("A question name and a list_name are both required.", true);
		}
		name = name.trim();
		listName = listName.trim();

		/*
		 * Both halves: the question comes from the forms, the list from the option lists, and the
		 * loader that fills one leaves the other empty.
		 */
		Survey s = McpData.design(ctx, surveyId);
		if(s == null) {
			return new MCPToolResult("No survey with id " + surveyId + " that you can reach.", true);
		}

		/* The question, and what it uses now */
		Question found = null;
		Form itsForm = null;
		for(Form f : s.surveyData.forms) {
			if(f.questions == null) {
				continue;
			}
			for(Question q : f.questions) {
				if(name.equalsIgnoreCase(q.name)) {
					found = q;
					itsForm = f;
					break;
				}
			}
			if(found != null) {
				break;
			}
		}
		if(found == null) {
			/*
			 * Say what was actually looked through.  "No question called x" is indistinguishable
			 * between the question being absent and the survey arriving with nothing loaded, and
			 * that ambiguity has now cost two rounds of guessing.
			 */
			int formCount = s.surveyData.forms == null ? 0 : s.surveyData.forms.size();
			int questionCount = 0;
			if(s.surveyData.forms != null) {
				for(Form f : s.surveyData.forms) {
					questionCount += f.questions == null ? 0 : f.questions.size();
				}
			}
			return new MCPToolResult("This survey has no question called \"" + name
					+ "\". survey_questions lists them. (Looked through " + questionCount
					+ " question(s) in " + formCount + " form(s).)", true);
		}
		if(found.type == null || !(found.type.startsWith("select") || found.type.equals("rank"))) {
			return new MCPToolResult("\"" + name + "\" is a " + found.type + ". Only a select, a "
					+ "select1 or a rank question offers choices.", true);
		}

		OptionList target = s.surveyData.optionLists == null
				? null : s.surveyData.optionLists.get(listName);
		if(target == null) {
			return new MCPToolResult("This survey has no choice list called \"" + listName
					+ "\". survey_options lists the ones it has, and survey_options_edit makes a new "
					+ "one.", true);
		}

		String was = found.list_name;
		if(listName.equals(was)) {
			return new MCPToolResult("\"" + name + "\" already uses \"" + listName + "\", so "
					+ "nothing was changed.", false);
		}

		/*
		 * Which stored answers the new list does not account for.  Only worth asking when the
		 * question has a column to read, which is what published means.
		 */
		List<String> orphaned = new ArrayList<>();
		McpSurveyLayout layout = new McpSurveyLayout(ctx.sd, surveyId);
		McpSurveyLayout.Item item = layout.find(name);
		if(item != null && item.published && itsForm.tableName != null) {
			List<String> offered = new ArrayList<>();
			if(target.options != null) {
				for(Option o : target.options) {
					offered.add(o.value);
				}
			}
			orphaned = unmatchedAnswers(ctx, itsForm.tableName, found.columnName == null
					? name : found.columnName, offered);
		}

		PropertyChange pc = new PropertyChange();
		pc.qId = found.id;
		pc.name = name;
		pc.type = "question";
		pc.prop = "list_name";
		pc.oldVal = was == null ? "" : was;
		pc.newVal = listName;
		pc.fId = itsForm.id;

		ChangeItem ci = new ChangeItem();
		ci.property = pc;
		ci.source = "mcp";

		ChangeSet cs = new ChangeSet();
		cs.changeType = "property";
		cs.type = "question";
		cs.action = "update";
		cs.source = "mcp";
		cs.items = new ArrayList<>();
		cs.items.add(ci);

		ArrayList<ChangeSet> changes = new ArrayList<>();
		changes.add(cs);

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		sm.applyChangeSetArray(ctx.sd, ctx.cResults, surveyId, ctx.user, changes, true);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("name", name);
		data.put("list_name", listName);
		data.put("previous", was == null ? "" : was);
		data.put("answersNotOffered", orphaned);

		StringBuilder text = new StringBuilder();
		text.append(was == null || was.isEmpty()
				? "\"" + name + "\" now offers the choices in \"" + listName + "\"."
				: "\"" + name + "\" now uses \"" + listName + "\" instead of \"" + was + "\".");

		if(!orphaned.isEmpty()) {
			text.append("\n\n**").append(orphaned.size())
					.append(orphaned.size() == 1 ? " value already stored is not offered by the new "
							+ "list: " : " values already stored are not offered by the new list: ")
					.append(String.join(", ", orphaned))
					.append(".** Those records keep what they were given - nothing rewrote them - "
							+ "but the answers now show as codes with no label. Either add them to "
							+ "the list with survey_options_edit, or correct the records.");
		} else if(item != null && item.published) {
			text.append("\n\nEvery answer already stored is offered by the new list, so nothing "
					+ "collected so far is left without a label.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	/*
	 * The distinct answers in the results column that the new list does not offer.
	 *
	 * Asked of the data rather than reasoned about, because whether this change strands anything
	 * depends entirely on what people happened to answer.
	 */
	private List<String> unmatchedAnswers(McpToolContext ctx, String tableName, String columnName,
			List<String> offered) {

		List<String> out = new ArrayList<>();
		try {
			if(!org.smap.sdal.Utilities.GeneralUtilityMethods.hasColumn(ctx.cResults, tableName,
					columnName)) {
				return out;
			}
			String sql = "select distinct " + columnName + " from " + tableName
					+ " where " + columnName + " is not null and " + columnName + " != ''";
			try (java.sql.PreparedStatement pstmt = ctx.cResults.prepareStatement(sql)) {
				java.sql.ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					String v = rs.getString(1);
					if(v != null && !offered.contains(v) && !out.contains(v)) {
						out.add(v);
					}
				}
			}
		} catch (Exception e) {
			/*
			 * Not knowing is reported as nothing rather than failing the change: a select stored in
			 * some other shape is not a reason to refuse a design change that is otherwise valid.
			 */
			return new ArrayList<>();
		}
		return out;
	}
}
