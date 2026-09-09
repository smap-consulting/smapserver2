package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.ReferenceFilterManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.ReferenceFilter;
import org.smap.sdal.model.Survey;

/*
 * Where a survey gets its reference data from, and what it is allowed to see of it.
 *
 * A survey can read another survey's submitted records as the choices for a question - pulldata, or
 * a select from an external source.  A reference filter narrows what comes across that connection,
 * and caps how many records it can carry.
 *
 * Every connection is reported, not only the filtered ones.  A source with no filter hands over
 * everything it holds, and that is the answer somebody asking this question most needs to see; a
 * tool that listed only the filters would show an empty list for a survey pulling an entire register
 * of people and look reassuring.
 *
 * Filters belong to the survey group rather than the survey, in the same way roles do, so the group
 * ident is looked up rather than taken from the listed survey - a list query leaves what a list does
 * not need unset, and reading a blank ident here would report a survey as having no connections at
 * all.
 */
public class ReferenceFilterListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "reference_filter_list";
	}

	@Override
	public String getTitle() {
		return "Reference data connections";
	}

	@Override
	public String getDescription() {
		return "The other surveys this one reads reference data from, and the filter on each: the "
				+ "expression narrowing which of their records come across, and any cap on how "
				+ "many. Connections with no filter are listed too, because those hand over "
				+ "everything the source survey holds.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer",
						"The survey that pulls the reference data, from survey_list"));
		schema.put("required", new String[] { "survey_id" });
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
		String groupIdent = GeneralUtilityMethods.getGroupSurveyIdent(ctx.sd, surveyId);

		ReferenceFilterManager rfm = new ReferenceFilterManager(ctx.localisation);
		List<ReferenceFilter> sources = rfm.getLinkableSources(ctx.sd, groupIdent);
		List<ReferenceFilter> filters = rfm.getFilters(ctx.sd, groupIdent);

		/* The filter for each source, so a source with none can be reported as unrestricted */
		Map<String, ReferenceFilter> bySource = new LinkedHashMap<>();
		for(ReferenceFilter rf : filters) {
			bySource.put(rf.linkedSIdent, rf);
		}

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		if(sources.isEmpty()) {
			text.append("\"").append(survey.getDisplayName())
					.append("\" does not read reference data from any other survey.");
		} else {
			text.append("\"").append(survey.getDisplayName()).append("\" reads reference data from ")
					.append(sources.size()).append(" survey(s):");

			for(ReferenceFilter source : sources) {
				ReferenceFilter rf = bySource.get(source.linkedSIdent);
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("source", source.linkedSIdent);
				row.put("sourceName", source.linkedSName);
				row.put("filtered", rf != null && rf.enabled
						&& rf.filter != null && !rf.filter.trim().isEmpty());

				text.append("\n- ").append(source.linkedSName == null
						? source.linkedSIdent : source.linkedSName);

				if(rf == null) {
					row.put("filter", null);
					text.append(": no filter, every record is available");
				} else {
					row.put("filter", rf.filter);
					row.put("enabled", rf.enabled);
					/*
					 * Zero means no cap rather than nothing, which is the opposite reading, so it
					 * is reported as a word rather than left as a number to be misread.
					 */
					row.put("maxRecords", rf.maxRecords > 0 ? (Object) rf.maxRecords : "no cap");

					if(rf.filter != null && !rf.filter.trim().isEmpty()) {
						text.append(": ").append(rf.filter);
					} else {
						text.append(": no filter, every record is available");
					}
					if(!rf.enabled) {
						text.append(" (turned off, so it is not being applied)");
					}
					if(rf.maxRecords > 0) {
						text.append(", at most ").append(rf.maxRecords).append(" record(s)");
					}
				}
				rows.add(row);
			}
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("connections", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
