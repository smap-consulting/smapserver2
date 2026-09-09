package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.ReferenceFilterManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.ReferenceFilter;
import org.smap.sdal.model.Survey;

/*
 * Narrow what one survey may read of another's records.
 *
 * The filter is a pseudo-SQL expression over the source survey's questions, and the cap limits how
 * many of its records can come across.  ReferenceFilterManager validates the expression before
 * saving it and clears the generated files afterwards, so a bad filter is refused rather than
 * silently producing an empty list on somebody's phone.
 *
 * The previous filter is reported back, because it is what the caller needs to put this change back
 * and there is nowhere else to read it once it has been overwritten.  Same reasoning as the old
 * values on a record change: a tool that says a change can be undone has to hand over the thing the
 * undo needs.
 */
public class ReferenceFilterSetTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "reference_filter_set";
	}

	@Override
	public String getTitle() {
		return "Set a reference data filter";
	}

	@Override
	public String getDescription() {
		return "Sets the filter on the reference data one survey reads from another: an expression "
				+ "narrowing which of the source survey's records come across, and a cap on how "
				+ "many. reference_filter_list shows the connections and what is on them now. The "
				+ "previous setting is returned, so it can be put back.";
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
		return "reference_filter_set again with the previous filter, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer",
						"The survey that reads the reference data, from survey_list"),
				"source_survey_ident", property("string",
						"The survey it reads from, as reported by reference_filter_list"),
				"filter", property("string",
						"The expression narrowing which records come across, such as "
						+ "${status} = 'open'. An empty string removes the filter and lets every "
						+ "record through."),
				"max_records", property("integer",
						"Optional. The most records this connection may carry. 0 means no cap."),
				"enabled", property("boolean",
						"Optional. Whether the filter is applied. Default true. Turning it off "
						+ "leaves it recorded but lets every record through."));
		schema.put("required", new String[] { "survey_id", "source_survey_ident" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}
		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}
		String source = stringArg(arguments, "source_survey_ident");
		if(source == null || source.trim().isEmpty()) {
			return new MCPToolResult("A source_survey_ident is required. reference_filter_list "
					+ "shows which surveys this one reads from.", true);
		}
		source = source.trim();

		String groupIdent = GeneralUtilityMethods.getGroupSurveyIdent(ctx.sd, surveyId);
		ReferenceFilterManager rfm = new ReferenceFilterManager(ctx.localisation);

		/*
		 * The connection has to exist.  A filter on a survey this one does not read from would sit
		 * in the table doing nothing, and the caller would have no way of telling that from a filter
		 * that works.
		 */
		boolean linked = false;
		String sourceName = source;
		for(ReferenceFilter candidate : rfm.getLinkableSources(ctx.sd, groupIdent)) {
			if(source.equals(candidate.linkedSIdent)) {
				linked = true;
				sourceName = candidate.linkedSName == null ? source : candidate.linkedSName;
				break;
			}
		}
		if(!linked) {
			return new MCPToolResult("\"" + survey.getDisplayName() + "\" does not read reference "
					+ "data from " + source + ". reference_filter_list shows the surveys it does "
					+ "read from.", true);
		}

		/* What it was, so the caller can put it back */
		ReferenceFilter existing = rfm.getFilter(ctx.sd, groupIdent, source, false);
		Map<String, Object> previous = new LinkedHashMap<>();
		previous.put("filter", existing == null ? null : existing.filter);
		previous.put("maxRecords", existing == null ? 0 : existing.maxRecords);
		previous.put("enabled", existing == null ? true : existing.enabled);

		ReferenceFilter rf = new ReferenceFilter();
		rf.linkerSIdent = groupIdent;
		rf.linkedSIdent = source;
		rf.filter = stringArg(arguments, "filter");
		if(rf.filter == null) {
			rf.filter = existing == null ? "" : existing.filter;
		}
		rf.maxRecords = intArg(arguments, "max_records",
				existing == null ? 0 : existing.maxRecords);
		rf.enabled = boolArg(arguments, "enabled",
				existing == null ? true : existing.enabled);

		/*
		 * validateFilter inside saveFilter refuses an expression that does not name questions the
		 * source survey has, so a filter that would quietly match nothing is refused instead.
		 */
		try {
			rfm.saveFilter(ctx.sd, rf);
		} catch (Exception e) {
			return new MCPToolResult("That filter was not accepted: " + e.getMessage()
					+ ". The expression is written over the source survey's own question names, "
					+ "which survey_questions will list for " + source + ".", true);
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("set", Boolean.TRUE);
		data.put("survey", survey.getDisplayName());
		data.put("source", source);
		data.put("filter", rf.filter);
		data.put("maxRecords", rf.maxRecords > 0 ? (Object) rf.maxRecords : "no cap");
		data.put("enabled", rf.enabled);
		data.put("previous", previous);

		StringBuilder text = new StringBuilder();
		text.append("\"").append(survey.getDisplayName()).append("\" reading ").append(sourceName)
				.append(": ");
		if(rf.filter == null || rf.filter.trim().isEmpty()) {
			text.append(rf.maxRecords > 0
					? "no filter, but at most " + rf.maxRecords + " record(s)"
					: "no filter and no cap, so every record is available");
		} else {
			text.append(rf.filter);
			if(rf.maxRecords > 0) {
				text.append(", and at most ").append(rf.maxRecords).append(" record(s)");
			}
		}
		if(!rf.enabled) {
			text.append(" (turned off, so it is not being applied)");
		}
		text.append("\n\nIt was: ").append(existing == null || existing.filter == null
				|| existing.filter.trim().isEmpty()
				? "no filter" : existing.filter);
		if(existing != null && existing.maxRecords > 0) {
			text.append(", at most ").append(existing.maxRecords).append(" record(s)");
		}
		text.append(". Set it back with those values to undo this.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
