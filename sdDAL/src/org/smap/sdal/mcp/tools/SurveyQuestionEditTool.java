package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.ChangeItem;
import org.smap.sdal.model.ChangeSet;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.PropertyChange;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * Change the logic on a question that already exists.
 *
 * Everything else here could add a question or take one away, and nothing could correct one.  A
 * calculation copied from the question above it, a parameter pointing at a survey since deleted -
 * both are one wrong word, and both meant going to the console or rebuilding the form from an
 * XLSForm to fix a field that was otherwise right.
 *
 * The properties are the ones the editor treats as a question's logic rather than its wording:
 * relevance, constraint, calculation, appearance and parameters.  They share a code path in
 * SurveyManager, which is the reason they are offered together - each is written, validated and
 * logged exactly as the online editor writes it.
 *
 * Wording is deliberately absent.  A label or a hint exists once per language, so changing one is a
 * different operation with a language to name, and doing it badly leaves a form that reads correctly
 * in one language and is blank in another.
 */
public class SurveyQuestionEditTool extends AbstractMcpTool {

	/* What may be changed, and the name the editor stores each under */
	private static final List<String> PROPS = Arrays.asList(
			"calculation", "parameters", "relevant", "constraint", "appearance");

	@Override
	public String getName() {
		return "survey_question_edit";
	}

	@Override
	public String getTitle() {
		return "Change a question's logic";
	}

	@Override
	public String getDescription() {
		return "Changes the logic on a question that already exists: its calculation, its parameters, when "
				+ "it is asked, what its answer must satisfy, and how it is presented. Anything not named "
				+ "is left as it is; an empty string takes a property off. To change what a question asks, "
				+ "or its type, use the console.";
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
		return "survey_question_edit again with the previous values, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to change, from survey_list"),
				"name", property("string", "The question to change, from survey_questions"),
				"calculation", property("string",
						"Optional. The expression whose result becomes the answer, such as "
								+ "${price} * ${quantity}. An empty string takes the calculation off."),
				"parameters", property("string",
						"Optional. The question's parameters, written key=value and separated by "
								+ "semicolons, such as form_identifier=s10_149. What they mean depends "
								+ "on the question type - a parent_form names the survey it launches, a "
								+ "repeat names the survey it reads rows from. An empty string takes "
								+ "them all off."),
				"relevant", property("string",
						"Optional. When the question is asked, such as ${age} > 18. An empty string "
								+ "means always."),
				"constraint", property("string",
						"Optional. What the answer must satisfy, such as . < 100. An empty string "
								+ "means anything."),
				"appearance", property("string",
						"Optional. How the question is presented, such as autocomplete w4."));
		schema.put("required", new String[] { "survey_id", "name" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}
		String name = stringArg(arguments, "name");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A question name is required. survey_questions lists them.", true);
		}
		name = name.trim();

		Survey s = McpData.questions(ctx, surveyId);
		if(s == null) {
			return new MCPToolResult("No survey with id " + surveyId + " that you can reach.", true);
		}

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
			return new MCPToolResult("This survey has no question called \"" + name
					+ "\". survey_questions lists them.", true);
		}

		/*
		 * What each property is now.  Read before anything is written so the change can be described
		 * and undone, and because the editor checks the old value it was given against what is
		 * stored - a change built on a stale reading is refused rather than applied blindly.
		 */
		Map<String, String> before = new LinkedHashMap<>();
		before.put("calculation", found.calculation);
		before.put("parameters", GeneralUtilityMethods.convertParametersToString(found.paramArray));
		before.put("relevant", found.relevant);
		before.put("constraint", found.constraint);
		before.put("appearance", found.appearance);

		ArrayList<ChangeSet> changes = new ArrayList<>();
		List<String> changed = new ArrayList<>();
		Map<String, Object> was = new LinkedHashMap<>();

		for(String prop : PROPS) {
			/*
			 * Absent means leave it alone; given means set it, and an empty string takes it off.
			 * The distinction matters most here, where most calls change one property of five.
			 */
			if(!arguments.containsKey(prop) || arguments.get(prop) == null) {
				continue;
			}
			String newVal = stringArg(arguments, prop);
			newVal = newVal == null ? "" : newVal.trim();
			String oldVal = before.get(prop) == null ? "" : before.get(prop).trim();
			if(newVal.equals(oldVal)) {
				continue;
			}

			PropertyChange pc = new PropertyChange();
			pc.qId = found.id;
			pc.name = name;
			pc.type = "question";
			pc.qType = found.type;		// the repeat handling for parameters reads this
			pc.prop = prop;
			pc.oldVal = oldVal;
			pc.newVal = newVal;
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
			changes.add(cs);

			changed.add(prop);
			was.put(prop, oldVal);
		}

		if(changes.isEmpty()) {
			return new MCPToolResult("\"" + name + "\" already reads that way, so nothing was "
					+ "changed. Name a property to change it - " + String.join(", ", PROPS)
					+ " - and give an empty string to take one off.", false);
		}

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		sm.applyChangeSetArray(ctx.sd, ctx.cResults, surveyId, ctx.user, changes, true);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("survey", s.getDisplayName());
		data.put("name", name);
		data.put("changed", changed);
		data.put("was", was);

		StringBuilder text = new StringBuilder();
		text.append("Changed ").append(String.join(", ", changed)).append(" on \"").append(name)
				.append("\" in \"").append(s.getDisplayName()).append("\".\n");
		for(String prop : changed) {
			String old = (String) was.get(prop);
			text.append("\n").append(prop).append(": ")
					.append(old == null || old.isEmpty() ? "(none)" : old)
					.append(" -> ");
			String now = stringArg(arguments, prop);
			now = now == null ? "" : now.trim();
			text.append(now.isEmpty() ? "(none)" : now);
		}
		text.append("\n\nThe previous values are above, and this tool sets them back.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
