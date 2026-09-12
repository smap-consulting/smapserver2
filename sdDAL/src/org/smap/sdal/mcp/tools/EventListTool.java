package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.LogItemDt;
import org.smap.sdal.model.MCPToolResult;

/*
 * What has happened on this server lately.
 *
 * The organisation's log: surveys created and deleted, users added, errors, security refusals,
 * everything Smap records as worth keeping.  This is the tool that answers "what happened last
 * week", which is a question an agent is good at and a person usually has to go looking for.
 *
 * Scoped to the caller's own organisation and nothing wider.  getLogEntries takes an organisation
 * and a flag for entries belonging to no organisation at all - server level things - and that flag
 * is false here: an organisation administrator is not a server administrator, and the difference is
 * exactly the sort that gets lost when one tool answers both questions.
 */
public class EventListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "event_list";
	}

	@Override
	public String getTitle() {
		return "Recent events";
	}

	@Override
	public String getDescription() {
		return "What has happened in this organisation recently: surveys created and changed, users "
				+ "added, errors, refused access. Newest first. Use this to answer what happened "
				+ "last week, or to find out why something stopped working.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"limit", property("integer",
						"Optional. How many events to return, newest first. Default 50."),
				"contains", property("string",
						"Optional. Only events whose description or user mentions this."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int limit = ctx.cap(intArg(arguments, "limit", 50));
		String contains = stringArg(arguments, "contains");
		if(contains != null) {
			contains = contains.trim().toLowerCase();
			if(contains.isEmpty()) {
				contains = null;
			}
		}

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		/*
		 * Asked for more than is wanted when a filter is given, because the filter is applied here
		 * rather than in the query: fetching exactly the limit and then discarding most of it would
		 * return a handful of matches and call it all of them.
		 */
		int fetch = contains == null ? limit : ctx.cap(limit * 10);

		ArrayList<LogItemDt> entries = new LogManager().getLogEntries(ctx.sd, ctx.localisation,
				oId,
				"desc",
				0,				// start
				"log_time",
				fetch,
				false,			// forHtml
				false);			// getNonOrgEntries - server level entries are not this caller's

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		for(LogItemDt item : entries) {
			if(rows.size() >= limit) {
				break;
			}
			if(contains != null) {
				String haystack = ((item.note == null ? "" : item.note) + " "
						+ (item.userIdent == null ? "" : item.userIdent) + " "
						+ (item.event == null ? "" : item.event) + " "
						+ (item.sName == null ? "" : item.sName)).toLowerCase();
				if(!haystack.contains(contains)) {
					continue;
				}
			}
			Map<String, Object> row = new LinkedHashMap<>();
			row.put("when", item.log_time == null ? null : item.log_time.toString());
			row.put("event", item.event);
			row.put("user", item.userIdent);
			row.put("survey", item.sName);
			row.put("detail", item.note);
			rows.add(row);

			text.append("\n- ").append(item.log_time == null ? "" : item.log_time.toString())
					.append("  ").append(item.event);
			if(item.userIdent != null) {
				text.append("  by ").append(item.userIdent);
			}
			if(item.sName != null) {
				text.append("  on ").append(item.sName);
			}
			if(item.note != null && !item.note.isEmpty()) {
				text.append("\n    ").append(item.note);
			}
		}

		if(rows.isEmpty()) {
			text.setLength(0);
			text.append(contains == null
					? "Nothing has been logged."
					: "Nothing logged mentions \"" + stringArg(arguments, "contains") + "\".");
		} else {
			text.insert(0, rows.size() + " event(s), newest first:");
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("events", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
