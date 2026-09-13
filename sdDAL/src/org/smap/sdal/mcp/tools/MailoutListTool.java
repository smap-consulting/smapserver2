package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.MailoutManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Mailout;
import org.smap.sdal.model.MailoutPerson;
import org.smap.sdal.model.Survey;

/*
 * Campaigns waiting to go out, and how far each has got.
 *
 * A mailout is a survey sent to a list of people, each with their own link.  The people are reported
 * by what state they are in - not yet sent, sent, replied, unsubscribed - because that is the
 * question anyone asks of a campaign, and because the count of "not yet sent" is the number that
 * would leave if somebody pressed send.
 *
 * Nothing here sends one.  An email cannot be recalled, so MCP prepares a mailout and a person sends
 * it from the console; there is no mailout_send and there will not be one.
 */
public class MailoutListTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "mailout_list";
	}

	@Override
	public String getTitle() {
		return "Mailouts";
	}

	@Override
	public String getDescription() {
		return "The mailout campaigns set up for a survey, and how many people are in each state: "
				+ "waiting to be sent to, already sent, replied, unsubscribed. Nothing here sends "
				+ "anything - a person sends a mailout from the console, because email cannot be "
				+ "recalled.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to look at, from survey_list"));
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
		int oId = org.smap.sdal.Utilities.GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		MailoutManager mm = new MailoutManager(ctx.localisation);
		ArrayList<Mailout> mailouts = mm.getMailouts(ctx.sd, survey.getIdent(), false, null);

		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();

		if(mailouts == null || mailouts.isEmpty()) {
			text.append("\"").append(survey.getDisplayName()).append("\" has no mailouts.");
		} else {
			text.append("\"").append(survey.getDisplayName()).append("\" has ")
					.append(mailouts.size()).append(" mailout(s):");

			for(Mailout m : mailouts) {
				/*
				 * Counted per state rather than reported per person.  A campaign can hold thousands
				 * of addresses and the question is nearly always how many, not who.
				 */
				Map<String, Integer> states = new LinkedHashMap<>();
				int total = 0;
				for(MailoutPerson p : mm.getMailoutPeople(ctx.sd, m.id, oId, false)) {
					String status = p.status == null ? "unknown" : p.status;
					states.put(status, states.containsKey(status) ? states.get(status) + 1 : 1);
					total++;
				}

				Map<String, Object> row = new LinkedHashMap<>();
				row.put("mailout_id", m.id);
				row.put("name", m.name);
				row.put("subject", m.subject);
				row.put("people", total);
				row.put("byStatus", states);
				row.put("anonymous", m.anonymous);
				row.put("multipleSubmit", m.multiple_submit);
				rows.add(row);

				text.append("\n- ").append(m.name).append(" (").append(m.id).append("): ")
						.append(total).append(" people");
				if(!states.isEmpty()) {
					StringBuilder byState = new StringBuilder();
					for(Map.Entry<String, Integer> e : states.entrySet()) {
						if(byState.length() > 0) {
							byState.append(", ");
						}
						byState.append(e.getValue()).append(" ").append(e.getKey());
					}
					text.append(" - ").append(byState);
				}
			}
			text.append("\n\nNothing here sends a mailout. A person sends one from the console, "
					+ "because an email cannot be called back.");
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("mailouts", rows);
		data.put("count", rows.size());

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
