package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.ResourceManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;

/*
 * How much this organisation has used, month by month.
 *
 * Submissions and the metered services - SMS, transcription, translation, image recognition - which
 * are the numbers somebody wants when asking whether usage is going up, or why a month looks
 * unusual.
 *
 * Submissions are counted from upload_event and everything else from the log's measures, which are
 * two different sources answering two different questions: one is what arrived, the other is what
 * was spent doing something with it.  They are reported side by side rather than added together,
 * because a total across them would be a number of nothing in particular.
 *
 * Nothing here is money.  What a month cost depends on the plan, and a tool that multiplied a count
 * by a rate it guessed would produce a figure someone might repeat.
 */
public class UsageReportTool extends AbstractMcpTool {

	/* The metered services, by the name the log records them under */
	private static final String[][] MEASURES = {
		{ LogManager.SMS, "SMS messages" },
		{ LogManager.REKOGNITION, "Image recognition" },
		{ LogManager.TRANSCRIBE, "Transcription" },
		{ LogManager.TRANSLATE, "Translation" },
	};

	private static final int MAX_MONTHS = 24;

	@Override
	public String getName() {
		return "usage_report";
	}

	@Override
	public String getTitle() {
		return "Usage";
	}

	@Override
	public String getDescription() {
		return "How much this organisation has used month by month: submissions received, and the "
				+ "metered services such as SMS, transcription and translation. Use it to see "
				+ "whether usage is rising or why a month looks unusual. Counts only - what a "
				+ "month cost depends on the plan and is not worked out here.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"months", property("integer",
						"Optional. How many months back to report, ending with this one. Default "
						+ "6, at most " + MAX_MONTHS + "."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int months = intArg(arguments, "months", 6);
		if(months < 1) {
			months = 1;
		}
		if(months > MAX_MONTHS) {
			months = MAX_MONTHS;
		}

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		ResourceManager rm = new ResourceManager();

		/*
		 * Walked back from this month with a calendar rather than by subtracting from a month
		 * number, so that crossing a year boundary does not produce month zero.
		 */
		Calendar cal = Calendar.getInstance();
		List<Map<String, Object>> rows = new ArrayList<>();
		StringBuilder text = new StringBuilder();
		long totalSubmissions = 0;

		for(int i = 0; i < months; i++) {
			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH) + 1;		// Calendar counts from zero, Smap from one

			int submissions = rm.getUsageSubmissionsMeasure(ctx.sd, oId, month, year);
			totalSubmissions += submissions;

			Map<String, Object> row = new LinkedHashMap<>();
			row.put("month", String.format("%04d-%02d", year, month));
			row.put("submissions", submissions);

			StringBuilder line = new StringBuilder();
			line.append("\n- ").append(String.format("%04d-%02d", year, month)).append(": ")
					.append(submissions).append(" submission(s)");

			Map<String, Object> services = new LinkedHashMap<>();
			for(String[] measure : MEASURES) {
				int used = rm.getUsageMeasure(ctx.sd, oId, month, year, measure[0]);
				if(used > 0) {
					services.put(measure[1], used);
					line.append(", ").append(used).append(" ").append(measure[1].toLowerCase());
				}
			}
			if(!services.isEmpty()) {
				row.put("services", services);
			}
			rows.add(row);
			text.append(line);

			cal.add(Calendar.MONTH, -1);
		}

		text.insert(0, "Usage over the last " + months + " month(s), most recent first:");
		text.append("\n\n").append(totalSubmissions).append(" submission(s) in total over that "
				+ "period. These are counts, not costs.");

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("months", rows);
		data.put("totalSubmissions", totalSubmissions);

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
