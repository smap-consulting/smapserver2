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
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.ChangeItem;
import org.smap.sdal.model.ChangeSet;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.Survey;

/*
 * Add and remove the choices a select question offers, making the list if it does not exist yet.
 *
 * **Add and remove, never replace.**  Everything else in this server that writes a list replaces it,
 * because the screens it was built for submit the whole thing back, and that has been the single
 * richest source of silent destruction here.  A choice list has no reason to follow: naming what to
 * add and what to take away says exactly what was meant, and a list sent whole by a caller working
 * from a half remembered survey would quietly drop the choices it had forgotten.
 *
 * Removing a choice that has been answered is the thing to be careful about.  The records keep the
 * value they were given - nothing rewrites them - but the choice is no longer offered, so the answer
 * shows as a code with no label behind it and the question can never be answered that way again.
 * That is sometimes exactly right, when a choice was a mistake, and it is reported rather than
 * refused because only the caller knows which.
 */
public class SurveyOptionsEditTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_options_edit";
	}

	@Override
	public String getTitle() {
		return "Change a choice list";
	}

	@Override
	public String getDescription() {
		return "Adds or removes the choices a select question offers, making the list if it does not "
				+ "exist. Name what to add and what to remove - the list is never replaced wholesale. "
				+ "Removing a choice people have already answered leaves those answers showing a code "
				+ "with no label. Attach a list to a question with survey_question_set_list.";
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
		return "survey_options_edit again, adding back what was removed - this reports what went, "
				+ "though a removed choice returns at the end of the list rather than where it was";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ANALYST);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> choice = new LinkedHashMap<>();
		choice.put("type", "object");
		Map<String, Object> choiceProps = new LinkedHashMap<>();
		choiceProps.put("value", property("string", "What is stored when this choice is picked"));
		choiceProps.put("label", property("string", "What the person answering sees"));
		choice.put("properties", choiceProps);

		Map<String, Object> add = property("array", "Choices to add, each with a value and a label.");
		add.put("items", choice);

		Map<String, Object> remove = property("array",
				"Values of choices to take away. The label is not needed.");
		remove.put("items", property("string", "The stored value of a choice"));

		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to change, from survey_list"),
				"list_name", property("string",
						"The choice list, from survey_questions or survey_options. A name with no "
								+ "list behind it is created."),
				"add", add,
				"remove", remove);
		schema.put("required", new String[] { "survey_id", "list_name" });
		return schema;
	}

	@SuppressWarnings("unchecked")
	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}

		String listName = stringArg(arguments, "list_name");
		if(listName == null || listName.trim().isEmpty()) {
			return new MCPToolResult("A list_name is required. survey_questions reports the list "
					+ "each select question uses.", true);
		}
		listName = listName.trim();

		Object addArg = arguments.get("add");
		Object removeArg = arguments.get("remove");
		List<Object> addList = addArg instanceof List ? (List<Object>) addArg : new ArrayList<>();
		List<Object> removeList = removeArg instanceof List
				? (List<Object>) removeArg : new ArrayList<>();
		if(addList.isEmpty() && removeList.isEmpty()) {
			return new MCPToolResult("Name some choices to add or remove.", true);
		}

		Survey s = McpData.optionLists(ctx, surveyId);
		if(s == null) {
			return new MCPToolResult("No survey with id " + surveyId + " that you can reach.", true);
		}

		OptionList existing = s.surveyData.optionLists == null
				? null : s.surveyData.optionLists.get(listName);
		boolean creating = existing == null;
		if(creating && !removeList.isEmpty()) {
			return new MCPToolResult("This survey has no choice list called \"" + listName
					+ "\", so there is nothing to remove from it. survey_options lists the ones it "
					+ "has.", true);
		}

		/* What is there now, so a duplicate is refused rather than quietly doubling a choice */
		Map<String, String> current = new LinkedHashMap<>();
		int nextSeq = 0;
		Map<String, Boolean> answered = new LinkedHashMap<>();
		if(existing != null && existing.options != null) {
			for(Option o : existing.options) {
				current.put(o.value, o.display_name == null ? o.value : o.display_name);
				answered.put(o.value, o.published);
				if(o.seq >= nextSeq) {
					nextSeq = o.seq + 1;
				}
			}
		}

		int languages = s.surveyData.languages == null || s.surveyData.languages.isEmpty()
				? 1 : s.surveyData.languages.size();

		ArrayList<Option> toAdd = new ArrayList<>();
		List<String> addedNames = new ArrayList<>();
		for(Object o : addList) {
			if(!(o instanceof Map)) {
				return new MCPToolResult("Each choice to add is an object with a value and a label.",
						true);
			}
			Map<String, Object> c = (Map<String, Object>) o;
			String value = c.get("value") == null ? null : c.get("value").toString().trim();
			String label = c.get("label") == null ? null : c.get("label").toString().trim();
			if(value == null || value.isEmpty()) {
				return new MCPToolResult("Every choice needs a value - what is stored when it is "
						+ "picked.", true);
			}
			if(label == null || label.isEmpty()) {
				label = value;
			}
			if(current.containsKey(value)) {
				return new MCPToolResult("\"" + listName + "\" already offers \"" + value
						+ "\" (" + current.get(value) + "). Nothing was changed.", true);
			}
			for(String already : addedNames) {
				if(already.equals(value)) {
					return new MCPToolResult("\"" + value + "\" is listed twice in add.", true);
				}
			}

			Option opt = new Option();
			opt.value = value;
			opt.display_name = label;
			opt.optionList = listName;
			opt.seq = nextSeq++;
			opt.labels = new ArrayList<>();
			for(int i = 0; i < languages; i++) {
				Label l = new Label();
				l.text = label;
				opt.labels.add(l);
			}
			toAdd.add(opt);
			addedNames.add(value);
		}

		ArrayList<Option> toRemove = new ArrayList<>();
		List<String> removedNames = new ArrayList<>();
		List<String> removedAnswered = new ArrayList<>();
		for(Object o : removeList) {
			if(o == null) {
				continue;
			}
			String value = o.toString().trim();
			if(value.isEmpty()) {
				continue;
			}
			if(!current.containsKey(value)) {
				return new MCPToolResult("\"" + listName + "\" does not offer \"" + value
						+ "\". survey_options shows what it offers. Nothing was changed.", true);
			}
			Option opt = new Option();
			opt.value = value;
			opt.optionList = listName;
			toRemove.add(opt);
			removedNames.add(value);
			if(Boolean.TRUE.equals(answered.get(value))) {
				removedAnswered.add(value);
			}
		}

		ArrayList<ChangeSet> changes = new ArrayList<>();
		if(!toAdd.isEmpty()) {
			changes.add(set("add", toAdd));
		}
		if(!toRemove.isEmpty()) {
			changes.add(set("delete", toRemove));
		}

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		sm.applyChangeSetArray(ctx.sd, ctx.cResults, surveyId, ctx.user, changes, true);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("list_name", listName);
		data.put("created", creating);
		data.put("added", addedNames);
		data.put("removed", removedNames);
		data.put("removedWithAnswers", removedAnswered);

		StringBuilder text = new StringBuilder();
		if(creating) {
			text.append("Created the choice list \"").append(listName).append("\" with ")
					.append(addedNames.size())
					.append(addedNames.size() == 1 ? " choice." : " choices.");
			text.append("\n\nNothing uses it yet. Point a select question at it with "
					+ "survey_question_set_list, or name it when adding one with "
					+ "survey_add_question.");
		} else {
			text.append("Changed the choice list \"").append(listName).append("\".");
			if(!addedNames.isEmpty()) {
				text.append("\n- added: ").append(String.join(", ", addedNames));
			}
			if(!removedNames.isEmpty()) {
				text.append("\n- removed: ").append(String.join(", ", removedNames));
			}
		}

		if(!removedAnswered.isEmpty()) {
			/*
			 * Said afterwards rather than refused beforehand, because removing a choice that turned
			 * out to be a mistake is a real thing to want.  What is not acceptable is not knowing.
			 */
			text.append("\n\n**").append(String.join(", ", removedAnswered))
					.append(removedAnswered.size() == 1 ? " had already been answered." : " had "
							+ "already been answered.")
					.append(" Those records keep the value they were given - nothing rewrote them - "
							+ "but it is no longer offered, so the answer now shows as a code with "
							+ "no label behind it.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	private ChangeSet set(String action, ArrayList<Option> options) {
		ChangeSet cs = new ChangeSet();
		cs.changeType = "option";
		cs.type = "option";
		cs.action = action;
		cs.source = "mcp";
		cs.items = new ArrayList<>();
		for(Option o : options) {
			ChangeItem ci = new ChangeItem();
			ci.option = o;
			ci.source = "mcp";
			ci.name = o.optionList;
			cs.items.add(ci);
		}
		return cs;
	}
}
