package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * What changing a question's type would do to the data already collected.
 *
 * Reads only.  Changing the type is left to a person in the console, and this exists so that the
 * person deciding has the numbers in front of them: how many answers would not survive, and which
 * ones.  Working that out is what an agent is good at; the act that cannot be undone stays with
 * whoever is accountable for it.
 *
 * A question with no column in the results table has never stored an answer, so its type can be
 * changed freely.  That is the exemption, and it is decided by the column rather than by the
 * published flag.
 *
 * published exists for speed: it saves the editor a look at the results table on every change, and
 * it means the column exists.  The two should always agree.  This tool is a read only diagnostic
 * making one information_schema lookup, so the speed the flag protects is not in play here and the
 * column can be asked directly - and since it reads both, it is the one place that can notice when
 * they have come apart, which is worth saying rather than quietly working around.
 *
 * Where there is a column, two separate facts get confused and the report keeps them apart.
 *
 *  1. Smap changes the definition and does not touch the results column.  There is no
 *     ALTER COLUMN ... TYPE anywhere in Smap; only add column.  So after a type change the answers
 *     already stored are exactly as they were, in a column of the old type.
 *  2. The damage, when there is any, is in the two directions that follow: answers already stored
 *     may not survive if anyone ever does convert the column, and answers the new type produces may
 *     not fit the column that is still there.
 *
 * The second is the one that bites first and is the one nobody expects, because the type change
 * appears to work.
 */
public class SurveyCheckTypeChangeTool extends AbstractMcpTool {

	/* Where a distinct offending value stops being an example and starts being a data dump */
	private static final int SAMPLES = 5;

	@Override
	public String getName() {
		return "survey_check_type_change";
	}

	@Override
	public String getTitle() {
		return "Check a question type change";
	}

	@Override
	public String getDescription() {
		return "Reports what changing a question's type would do to the answers already collected: "
				+ "how many would not convert, examples of the ones that would not, and whether "
				+ "answers of the new type will still fit the column that holds them. Reads only - "
				+ "the change itself is made in the console, deliberately, because it cannot be "
				+ "undone. Ask this before proposing a type change on a survey that has data.";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ANALYST, Authorise.ADMIN);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey the question belongs to"),
				"name", property("string", "The question whose type would change"),
				"new_type", property("string",
						"The type it would change to, such as string, int, decimal, date, dateTime"),
				"form", property("string",
						"Optional. The form it is in. Defaults to the main form."));
		schema.put("required", new String[] { "survey_id", "name", "new_type" });
		return schema;
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}
		Survey listed = McpData.surveyById(ctx, surveyId);
		if(listed == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}
		String name = stringArg(arguments, "name");
		String newType = stringArg(arguments, "new_type");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A question name is required.", true);
		}
		if(newType == null || newType.trim().isEmpty()) {
			return new MCPToolResult("A new_type is required. It is the type you are considering "
					+ "changing the question to.", true);
		}
		name = name.trim();
		newType = newType.trim();

		Survey s = McpData.questions(ctx, surveyId);
		String formWanted = stringArg(arguments, "form");
		Form form = null;
		for(Form f : s.surveyData.forms) {
			if(formWanted == null ? f.parentform == 0 : formWanted.equalsIgnoreCase(f.name)) {
				form = f;
				break;
			}
		}
		if(form == null) {
			return new MCPToolResult(formWanted == null
					? "This survey has no main form, which should not happen."
					: "This survey has no form called \"" + formWanted + "\".", true);
		}
		Question q = null;
		if(form.questions != null) {
			for(Question candidate : form.questions) {
				if(name.equalsIgnoreCase(candidate.name)) {
					q = candidate;
					break;
				}
			}
		}
		if(q == null) {
			return new MCPToolResult("\"" + form.name + "\" has no question called " + name
					+ ". survey_questions lists the ones it has.", true);
		}
		if(newType.equals(q.type)) {
			return new MCPToolResult(name + " is already a " + q.type + " question. Nothing would "
					+ "change.", false);
		}

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("question", name);
		data.put("form", form.name);
		data.put("currentType", q.type);
		data.put("proposedType", newType);
		data.put("published", q.published);

		StringBuilder text = new StringBuilder();
		text.append("Changing ").append(name).append(" from ").append(q.type)
				.append(" to ").append(newType).append(" in \"")
				.append(listed.getDisplayName()).append("\".\n\n");

		/*
		 * The column decides this, not the published flag.
		 *
		 * If no column has been created in the results table then the question has never stored an
		 * answer and its type can be changed freely.  published is Smap's record of the same fact
		 * and is usually the same answer, but it is a flag and the column is the thing itself: a
		 * question marked published whose column is still pending has nothing to lose either, and a
		 * flag that has fallen out of step must not be able to report a column full of answers as
		 * safe to convert.
		 */
		String tableName = form.tableName;
		String columnName = q.columnName != null ? q.columnName : q.name;
		if(!SurveyManager.isValidTableName(tableName) || !columnName.matches("[A-Za-z_][A-Za-z0-9_]*")) {
			return new MCPToolResult("The results table for this form cannot be read safely.", true);
		}

		String currentColType = GeneralUtilityMethods.columnType(ctx.cResults, tableName, columnName);

		/*
		 * The flag says a column exists and none does, or none should and one does.  Neither stops
		 * this tool answering, because it goes on the column either way, but a flag out of step with
		 * the table is worth reporting to whoever can look into it.
		 */
		if(currentColType != null && !q.published) {
			data.put("publishedFlagDisagrees", "The question is not marked published but its column "
					+ "exists in " + tableName + ". The answer below goes on the column.");
		}

		if(currentColType == null) {
			data.put("verdict", "safe");
			data.put("reason", "No column has been created for this question yet");
			text.append("Safe. No column has been created for this question in the data tables, so "
					+ "there is nothing stored to convert and nothing to be left behind. The type "
					+ "can be changed freely.");
			if(q.published) {
				text.append(" The question is marked as published, but the column that would hold "
						+ "its answers has not been added yet - survey_history shows that change as "
						+ "pending.");
			}
			MCPToolResult result = new MCPToolResult(text.toString());
			result.setStructuredContent(data);
			return result;
		}

		String proposedColType = normalise(GeneralUtilityMethods.getPostgresColType(newType));
		String currentNormalised = normalise(currentColType);

		data.put("currentColumnType", currentColType);
		data.put("proposedColumnType", proposedColType);

		text.append("Smap will change the question and leave the column alone. There is no "
				+ "ALTER COLUMN in Smap, only add column, so the answers already stored stay "
				+ "exactly as they are, in a ").append(currentColType).append(" column.\n\n");

		if(proposedColType.equals(currentNormalised)) {
			data.put("verdict", "safe");
			data.put("reason", "Both types are stored the same way");
			text.append("Safe. ").append(q.type).append(" and ").append(newType)
					.append(" are both stored as ").append(currentColType)
					.append(", so nothing about the stored answers changes at all.");
			MCPToolResult result = new MCPToolResult(text.toString());
			result.setStructuredContent(data);
			return result;
		}

		/* How many answers there are to lose, before asking whether they would convert */
		long stored = count(ctx, "select count(" + columnName + ") from " + tableName
				+ " where not coalesce(_bad, false)");
		data.put("answersStored", stored);

		/*
		 * Whether each stored answer could be read as the new type.  pg_input_is_valid answers
		 * exactly that and does it without raising, which is the whole difficulty of the question;
		 * it arrived in PostgreSQL 16, so on anything older the answer is the honest one that the
		 * count cannot be given rather than a guess dressed as a number.
		 */
		boolean canTest = serverVersionNum(ctx) >= 160000;
		if(!canTest) {
			data.put("verdict", "unknown");
			data.put("reason", "This server's PostgreSQL is older than 16, so the conversion cannot "
					+ "be tested without risking an error");
			text.append(stored).append(" answer(s) are stored. Whether they would convert cannot be "
					+ "tested on this server, which runs a PostgreSQL older than 16. Look at them "
					+ "with data_query before deciding.");
			MCPToolResult result = new MCPToolResult(text.toString());
			result.setStructuredContent(data);
			return result;
		}

		long bad = count(ctx, "select count(*) from " + tableName
				+ " where not coalesce(_bad, false) and " + columnName + " is not null "
				+ "and not pg_input_is_valid(" + columnName + "::text, " + literal(proposedColType) + ")");
		data.put("answersThatWouldNotConvert", bad);

		List<String> samples = new ArrayList<>();
		if(bad > 0) {
			String sql = "select distinct " + columnName + "::text from " + tableName
					+ " where not coalesce(_bad, false) and " + columnName + " is not null "
					+ "and not pg_input_is_valid(" + columnName + "::text, " + literal(proposedColType) + ") "
					+ "limit " + SAMPLES;
			try (PreparedStatement pstmt = ctx.cResults.prepareStatement(sql)) {
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					samples.add(rs.getString(1));
				}
			}
			data.put("examples", samples);
		}

		/*
		 * The direction that bites first.  A text column takes anything, so widening is harmless;
		 * anything else is a column that will refuse the values the new type starts producing, and
		 * it refuses them at submission time, on somebody's device, long after this change looked
		 * like it worked.
		 */
		boolean newValuesFit = currentNormalised.equals("text");
		data.put("newAnswersWillFitTheColumn", newValuesFit);

		String verdict;
		if(bad == 0 && newValuesFit) {
			verdict = "safe";
		} else if(bad == 0) {
			verdict = "risky";
		} else {
			verdict = "would lose answers";
		}
		data.put("verdict", verdict);

		text.append(stored).append(" answer(s) are stored. ");
		if(bad == 0) {
			text.append("All of them could be read as ").append(newType).append(".");
		} else {
			text.append(bad).append(" of them could not be read as ").append(newType);
			if(!samples.isEmpty()) {
				text.append(", such as: ").append(String.join(", ", samples));
			}
			text.append(". Converting the column would lose those answers.");
		}

		if(!newValuesFit) {
			text.append("\n\nThe column is ").append(currentColType).append(" and would stay that "
					+ "way, so answers given after the change may be refused when a record is "
					+ "submitted - on a device, after this change appeared to work. This is the "
					+ "part that usually goes wrong.");
		}

		text.append("\n\nMCP will not make this change. If it is the right thing to do, make it in "
				+ "the console knowing it is one way. The alternative that keeps everything is to "
				+ "add a new question of the type you want with survey_add_question and leave this "
				+ "one holding its answers.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	private long count(McpToolContext ctx, String sql) throws Exception {
		try (PreparedStatement pstmt = ctx.cResults.prepareStatement(sql)) {
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getLong(1) : 0;
		}
	}

	private int serverVersionNum(McpToolContext ctx) throws Exception {
		try (PreparedStatement pstmt = ctx.cResults.prepareStatement(
				"select current_setting('server_version_num')::int")) {
			ResultSet rs = pstmt.executeQuery();
			return rs.next() ? rs.getInt(1) : 0;
		}
	}

	/*
	 * A single quoted literal for a type name.  The name never comes from the caller - it is what
	 * getPostgresColType returned for a type this tool recognises - but it is built into SQL rather
	 * than bound, so it is quoted here rather than trusted.
	 */
	private String literal(String type) {
		return "'" + type.replace("'", "''") + "'";
	}

	/*
	 * information_schema and getPostgresColType do not spell the same type the same way: one says
	 * integer where the other says int, and time without time zone where the other says time.
	 * Comparing them unnormalised reports a difference between a column and itself.
	 */
	private String normalise(String type) {
		if(type == null) {
			return "";
		}
		String t = type.trim().toLowerCase();
		if(t.equals("int") || t.equals("int4") || t.equals("integer")) {
			return "integer";
		}
		if(t.equals("float8") || t.equals("double precision")) {
			return "double precision";
		}
		if(t.equals("timestamp") || t.equals("timestamptz")
				|| t.equals("timestamp with time zone") || t.equals("timestamp without time zone")) {
			return "timestamp with time zone";
		}
		if(t.equals("time") || t.equals("time without time zone")) {
			return "time without time zone";
		}
		if(t.equals("varchar") || t.equals("character varying")) {
			return "text";
		}
		return t;
	}
}
