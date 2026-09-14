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
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * Move a question: up or down the form, into a group or repeat, or back out of one.
 *
 * All of that is one operation to a person and two underneath, which is why it is one tool:
 *
 *   Moving within a form, or into or out of a **group**, only changes a sequence number.  A group is
 *   a begin and an end with questions lying between them, so "inside" means nothing more than a
 *   sequence in that range.
 *
 *   Moving into or out of a **repeat** changes which form the question belongs to, and a form owns a
 *   results table.  So the answers already collected do not come with it.
 *
 * That second case is the one worth refusing rather than explaining afterwards.  A question that has
 * been published has a column in its form's table with data in it; moving it to another form makes a
 * new column in the new table and leaves every existing answer behind in the old one, unreachable
 * from the question that now exists.  Nothing errors, and the data looks deleted.  So it is refused,
 * and the answer says what to do instead.
 */
public class SurveyMoveQuestionTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "survey_move_question";
	}

	@Override
	public String getTitle() {
		return "Move a question";
	}

	@Override
	public String getDescription() {
		return "Moves a question within a form, into a group or repeat, or back out of one. Give "
				+ "after to put it in a particular place, or into to put it at the end of a group or "
				+ "repeat. Moving a question that already holds answers into or out of a repeat is "
				+ "refused, because its answers would be left behind in the old table.";
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
		return "survey_move_question again, back to where it was - this reports where that was";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ANALYST);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> schema = schema(
				"survey_id", property("integer", "The survey to change, from survey_list"),
				"name", property("string", "The question to move, from survey_questions"),
				"after", property("string",
						"Optional. Put it immediately after this question, wherever that is - "
								+ "inside a group, inside a repeat, or at the top level."),
				"into", property("string",
						"Optional. Put it at the end of this group or repeat. Use an empty string "
								+ "to take it out of whatever it is in, to the end of the form."));
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
			return new MCPToolResult("A question name is required. survey_questions lists them.",
					true);
		}
		name = name.trim();

		Survey s = McpData.outline(ctx, surveyId);
		if(s == null) {
			return new MCPToolResult("No survey with id " + surveyId + " that you can reach.", true);
		}

		McpSurveyLayout layout = new McpSurveyLayout(ctx.sd, surveyId);
		McpSurveyLayout.Item q = layout.find(name);
		if(q == null) {
			return new MCPToolResult("This survey has no question called \"" + name
					+ "\". survey_questions lists them.", true);
		}
		if(q.isEndGroup()) {
			return new MCPToolResult("\"" + name + "\" is the end of a group, not a question. Move "
					+ "the group itself and its end comes with it.", true);
		}

		String afterName = stringArg(arguments, "after");
		Object intoArg = arguments.get("into");
		boolean intoGiven = intoArg != null;
		String intoName = intoGiven ? intoArg.toString().trim() : null;

		if((afterName == null || afterName.trim().isEmpty()) && !intoGiven) {
			return new MCPToolResult("Give after, to put it in a particular place, or into, to put "
					+ "it at the end of a group or repeat.", true);
		}

		int targetFId;
		int targetSeq;
		String where;

		if(afterName != null && !afterName.trim().isEmpty()) {
			McpSurveyLayout.Item after = layout.find(afterName.trim());
			if(after == null) {
				return new MCPToolResult("This survey has no question called \"" + afterName
						+ "\".", true);
			}
			if(after.name.equalsIgnoreCase(q.name)) {
				return new MCPToolResult("A question cannot be moved after itself.", true);
			}
			/*
			 * After a group means after the whole thing.  Landing between a group's begin and its end
			 * would put the question inside a group the caller named as a neighbour, not a container.
			 */
			McpSurveyLayout.Item afterEnd = layout.endOf(after);
			targetFId = after.fId;
			targetSeq = (afterEnd == null ? after.seq : afterEnd.seq) + 1;
			McpSurveyLayout.Item container = layout.groupContaining(after);
			where = "after " + after.name
					+ (container == null ? "" : ", inside " + container.name);
		} else if(intoName.isEmpty()) {
			/* Out of whatever it is in, to the end of the form it is already in */
			targetFId = q.fId;
			targetSeq = layout.lastSeq(q.fId) + 1;
			where = "at the end of " + q.formName;
		} else {
			McpSurveyLayout.Item into = layout.find(intoName);
			if(into == null) {
				return new MCPToolResult("This survey has no group or repeat called \"" + intoName
						+ "\".", true);
			}
			if(into.isGroup()) {
				McpSurveyLayout.Item end = layout.endOf(into);
				if(end == null) {
					return new MCPToolResult("\"" + intoName + "\" has no end, so there is no inside "
							+ "to move anything into. That group needs repairing in the console "
							+ "first.", true);
				}
				targetFId = into.fId;
				targetSeq = end.seq;			// Immediately before the end, so the last thing inside
				where = "at the end of the group " + into.name;
			} else if(into.isRepeat()) {
				/*
				 * The repeat's own form, found by the question that created it.  A repeat question and
				 * the form it made are different rows; moving into the repeat means moving into that
				 * form, not to a sequence beside the repeat question.
				 */
				Integer repeatFId = repeatForm(ctx, surveyId, into);
				if(repeatFId == null) {
					return new MCPToolResult("\"" + intoName + "\" is a repeat but its form could "
							+ "not be found, which should not happen.", true);
				}
				targetFId = repeatFId;
				targetSeq = layout.lastSeq(repeatFId) + 1;
				where = "at the end of the repeat " + into.name;
			} else {
				return new MCPToolResult("\"" + intoName + "\" is a " + into.type
						+ ", not a group or a repeat. Only those can hold questions.", true);
			}
		}

		if(targetFId == q.fId && targetSeq == q.seq) {
			return new MCPToolResult("\"" + name + "\" is already there, so nothing was changed.",
					false);
		}

		/*
		 * Between forms is between tables.  A published question carries a column of answers that
		 * does not travel with it, so the move would look successful and read as data loss.
		 */
		if(targetFId != q.fId && q.published) {
			return new MCPToolResult("\"" + name + "\" already holds answers, and moving it "
					+ (q.isRepeat() ? "" : "into or out of a repeat ")
					+ "would move it to another table. The answers already collected would stay "
					+ "behind in the old one, reachable by nothing. Nothing was changed.\n\nTo "
					+ "reorganise a form that has data, add the question in its new place and move "
					+ "the answers across deliberately.", true);
		}

		/*
		 * Moving a group moves its contents: QuestionManager expands a begin group into every
		 * question between it and its end.  Said up front because "move the group" and "move the
		 * group heading, leaving its questions behind" are both plausible readings of the request.
		 */
		List<String> carried = new ArrayList<>();
		if(q.isGroup()) {
			for(McpSurveyLayout.Item i : layout.contentsOf(q)) {
				carried.add(i.name);
				if(targetFId != q.fId && i.published) {
					return new MCPToolResult("\"" + name + "\" contains " + i.name + ", which "
							+ "already holds answers, and this move would take it to another table "
							+ "and leave those answers behind. Nothing was changed.", true);
				}
			}
		}

		String wasIn = q.formName;
		McpSurveyLayout.Item wasInGroup = layout.groupContaining(q);
		int wasSeq = q.seq;

		Question move = new Question();
		move.id = q.qId;
		move.name = q.name;
		move.columnName = q.name;
		move.type = q.type;
		move.fId = targetFId;
		move.sourceFormId = q.fId;
		move.seq = targetSeq;
		move.sourceSeq = q.seq;
		/*
		 * The index of each form within the survey, not -1.
		 *
		 * The move is carried out with form ids, so the indexes look redundant - but they are written
		 * into the change log, and the changes page reads forms[sourceFormIndex].name to say where a
		 * question came from.  Minus one meant forms[-1], which is undefined, and the page failed on
		 * the first such entry and rendered none of the others.
		 */
		move.formIndex = formIndex(s, targetFId);
		move.sourceFormIndex = formIndex(s, q.fId);

		ChangeItem ci = new ChangeItem();
		ci.question = move;
		ci.source = "mcp";

		ChangeSet cs = new ChangeSet();
		cs.changeType = "question";
		cs.type = "question";
		cs.action = "move";
		cs.source = "mcp";
		cs.items = new ArrayList<>();
		cs.items.add(ci);

		ArrayList<ChangeSet> changes = new ArrayList<>();
		changes.add(cs);

		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone, ctx.clientId);
		sm.applyChangeSetArray(ctx.sd, ctx.cResults, surveyId, ctx.user, changes, true);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("moved", Boolean.TRUE);
		data.put("name", q.name);
		data.put("to", where);
		data.put("carried", carried);
		Map<String, Object> from = new LinkedHashMap<>();
		from.put("form", wasIn);
		from.put("group", wasInGroup == null ? "" : wasInGroup.name);
		from.put("position", wasSeq);
		data.put("previous", from);

		StringBuilder text = new StringBuilder();
		text.append("Moved ").append(q.name).append(" ").append(where).append(".");
		if(!carried.isEmpty()) {
			text.append("\n\nIts ").append(carried.size())
					.append(carried.size() == 1 ? " question came with it: " : " questions came with "
							+ "it: ")
					.append(String.join(", ", carried));
		}
		text.append("\n\nIt was in ").append(wasIn);
		if(wasInGroup != null) {
			text.append(", inside ").append(wasInGroup.name);
		}
		text.append(", at position ").append(wasSeq).append(".");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	/* Where a form sits in the survey's own list, which is what the change log records */
	private int formIndex(Survey s, int fId) {
		if(s.surveyData.forms != null) {
			for(int i = 0; i < s.surveyData.forms.size(); i++) {
				if(s.surveyData.forms.get(i).id == fId) {
					return i;
				}
			}
		}
		return 0;
	}

	/* The form a repeat question created, which is the form whose parentquestion is that question */
	private Integer repeatForm(McpToolContext ctx, int sId, McpSurveyLayout.Item repeat)
			throws Exception {
		try (java.sql.PreparedStatement pstmt = ctx.sd.prepareStatement(
				"select f_id from form where s_id = ? and parentquestion = ?")) {
			pstmt.setInt(1, sId);
			pstmt.setInt(2, repeat.qId);
			java.sql.ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				return rs.getInt(1);
			}
		}
		return null;
	}
}
