package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.DSARManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * What this server holds about one person.
 *
 * The first half of answering a data subject access request: before anybody exports anything they
 * have to know whether there is anything, where it is, and which field their name is actually in.
 * That is a question in words, and this answers it in words.
 *
 * **It reports where the data is, not what it says.**  Project, survey, form, which personal data
 * columns matched and how many rows - and no values.  The request is answered by sending the person
 * their data, not by reading it into a chat transcript, and the two are easy to conflate when the
 * tool that finds it could just as easily print it.  The spreadsheet is produced from the console,
 * which is also where it can be handed over properly.
 *
 * Only columns marked as personal data are searched, which is the same definition the export uses
 * because it is the same walk.  A survey whose name field was never marked will not be found by
 * either, and that is worth knowing rather than assuming: a nil return here means nothing marked as
 * personal data matched, not that the person is absent from the server.
 */
public class DsarFindTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "dsar_find";
	}

	@Override
	public String getTitle() {
		return "Find a person's data";
	}

	@Override
	public String getDescription() {
		return "Finds where personal data about somebody is held: which projects, surveys and forms "
				+ "mention them, in which fields, and how many records. Reports locations and counts "
				+ "only, never the values - the data itself is exported from the console. Use it to "
				+ "answer a data subject access request, or to check before one.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.PRIVACY;
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.DPO, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"identifier", property("string",
						"What identifies the person - a name, an email address, a phone number"),
				"field", property("string",
						"Optional. Only look in this question, by name."),
				"partial", property("boolean",
						"Optional. Match anywhere in the value rather than the whole of it. "
								+ "Default false."));
		schema.put("required", new String[] { "identifier" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		String identifier = stringArg(arguments, "identifier");
		if(identifier == null || identifier.trim().isEmpty()) {
			return new MCPToolResult("An identifier is required - a name, an email address or a "
					+ "phone number to look for.", true);
		}
		identifier = identifier.trim();

		String field = stringArg(arguments, "field");
		boolean partial = boolArg(arguments, "partial", false);

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);

		/*
		 * Logged before the search, the same way the console's export is, and for the same reason:
		 * looking somebody up is itself the thing a data protection officer is accountable for, and
		 * a search that failed part way through is still a search that happened.
		 */
		StringBuilder note = new StringBuilder("DSAR find: identifier='").append(identifier)
				.append("'");
		if(field != null) {
			note.append(", field='").append(field).append("'");
		}
		if(partial) {
			note.append(", partial=true");
		}
		note.append(", via MCP");
		new LogManager().writeLogOrganisation(ctx.sd, oId, ctx.user, LogManager.DSAR,
				note.toString(), 0);

		/*
		 * superUser false: answered from what this person can see, which is the rule everywhere else
		 * here.  A data protection officer who cannot reach a survey does not get a fuller answer
		 * from an agent than from the console.
		 */
		DSARManager dm = new DSARManager();
		List<DSARManager.Target> targets =
				dm.findTargets(ctx.sd, ctx.cResults, ctx.user, field, false);

		List<Map<String, Object>> hits = new ArrayList<>();
		int totalRecords = 0;
		int searched = 0;
		for(DSARManager.Target t : targets) {
			searched++;
			Map<String, Integer> counts = dm.countMatches(ctx.cResults, t, identifier, partial);
			if(counts.isEmpty()) {
				continue;
			}
			int records = 0;
			List<Map<String, Object>> fields = new ArrayList<>();
			for(Map.Entry<String, Integer> e : counts.entrySet()) {
				Map<String, Object> f = new LinkedHashMap<>();
				f.put("field", e.getKey());
				f.put("records", e.getValue());
				fields.add(f);
				if(e.getValue() > records) {
					records = e.getValue();
				}
			}
			Map<String, Object> hit = new LinkedHashMap<>();
			hit.put("project", t.projectName);
			hit.put("survey", t.surveyName);
			hit.put("form", t.formName);
			hit.put("fields", fields);
			hits.add(hit);
			totalRecords += records;
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("identifier", identifier);
		data.put("partial", partial);
		data.put("found", hits);
		data.put("formsSearched", searched);
		data.put("formsWithData", hits.size());

		StringBuilder text = new StringBuilder();
		if(hits.isEmpty()) {
			text.append("Nothing marked as personal data matches \"").append(identifier)
					.append("\" in the ").append(searched)
					.append(searched == 1 ? " form" : " forms")
					.append(" you can see.");
			if(!partial) {
				text.append("\n\nThat was an exact match. Try partial to look for it inside longer "
						+ "values.");
			}
			text.append("\n\nWorth knowing: only fields marked as personal data are searched. A "
					+ "survey whose name field was never marked will not be found by this, or by the "
					+ "export.");
		} else {
			text.append("\"").append(identifier).append("\" appears in ").append(hits.size())
					.append(hits.size() == 1 ? " form" : " forms")
					.append(", of ").append(searched).append(" searched:");
			for(Map<String, Object> hit : hits) {
				text.append("\n\n").append(hit.get("project")).append(" - ").append(hit.get("survey"))
						.append(" - ").append(hit.get("form"));
				for(Object f : (List<?>) hit.get("fields")) {
					Map<?, ?> fm = (Map<?, ?>) f;
					text.append("\n  - ").append(fm.get("field")).append(": ").append(fm.get("records"))
							.append(" record(s)");
				}
			}
			text.append("\n\nRoughly ").append(totalRecords)
					.append(" record(s) in all - forms are counted separately, so a person in a "
							+ "repeating group is counted in each.");
			text.append("\n\nThese are locations, not values. Export the data itself from the "
					+ "console, where it can be handed over as a file.");
		}

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
