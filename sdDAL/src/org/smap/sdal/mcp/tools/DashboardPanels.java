package org.smap.sdal.mcp.tools;

import java.sql.Date;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.managers.DashboardManager;
import org.smap.sdal.managers.DashboardManager.Panel;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * What the dashboard tools share: whose panels are being managed, and turning a caller's description of
 * a graph or table panel into a panel the Analysis page can show.
 *
 * Every name is checked against the survey and every setting against the values the Analysis page
 * offers, as a panel holding something the page does not expect shows up blank rather than failing.
 * The rules for functions and grouping are the ones the page applies when a panel is set up there.
 */
class DashboardPanels {

	static final List<String> TIME_GROUPS = Arrays.asList("none", "hour", "day", "week", "month", "year");
	static final List<String> CHART_TYPES = Arrays.asList("histogram", "pie");
	static final List<String> GROUP_TYPES = Arrays.asList("geopoint", "geopolygon", "geolinestring", "geoshape",
			"geotrace", "geocompound", "string");

	/* Thrown with a message for the caller when a panel cannot be made as described */
	static class Refusal extends Exception {
		private static final long serialVersionUID = 1L;
		Refusal(String msg) {
			super(msg);
		}
	}

	static Map<String, Object> userProperty() {
		Map<String, Object> p = new LinkedHashMap<>();
		p.put("type", "string");
		p.put("description", "Optional. The ident of the user whose dashboard this is. Defaults to your own; "
				+ "an administrator can name a user of their own organisation.");
		return p;
	}

	/* The user whose panels these are */
	static String user(McpToolContext ctx, String onBehalfOf) throws Exception {
		String user = DashboardManager.effectiveUser(ctx.sd, ctx.user,
				onBehalfOf == null ? null : onBehalfOf.trim());
		if(user == null) {
			throw new Refusal("You can only manage your own dashboard, or as an administrator the dashboard "
					+ "of a user in your organisation. Nothing was changed.");
		}
		return user;
	}

	/*
	 * Fill in a panel from the arguments, over the values it has when one is being changed.  A setting not
	 * given keeps the value the panel has; an empty string takes an optional one off
	 */
	static void apply(McpToolContext ctx, String user, Panel p, Map<String, Object> args, boolean isNew) throws Exception {

		String type = str(args, "type");
		if(type != null) {
			p.type = type.toLowerCase();
		}
		if(!DashboardManager.GRAPH.equals(p.type) && !DashboardManager.TABLE.equals(p.type)) {
			throw new Refusal("type is graph or table. Nothing was changed.");
		}

		if(args.get("survey_id") != null) {
			p.sId = intOf(args.get("survey_id"));
		}
		Survey survey = McpData.surveyById(ctx, p.sId);
		if(survey == null || !DashboardManager.surveyReachable(ctx.sd, user, p.sId)) {
			throw new Refusal("No such survey, or it is not in a project " + (user.equals(ctx.user) ? "you are"
					: user + " is") + " a member of. Nothing was changed.");
		}
		p.sName = survey.getDisplayName();
		Survey design = McpData.questions(ctx, p.sId);
		p.lang = design.surveyData.languages != null && !design.surveyData.languages.isEmpty()
				? design.surveyData.languages.get(0).name : "none";

		// The question shown
		if(args.containsKey("question")) {
			String name = str(args, "question");
			if(name == null || name.isEmpty()) {
				p.qname = null;
				p.question = null;
			} else {
				Question q = question(design, name);
				if(q == null) {
					throw new Refusal(survey.getDisplayName() + " has no question called " + name
							+ ". Nothing was changed.");
				}
				p.qname = q.name;
				p.question = label(q);
			}
		} else if(isNew) {
			p.qname = null;
		}
		if(p.qname == null && DashboardManager.GRAPH.equals(p.type)) {
			throw new Refusal("A graph needs a question. Nothing was changed.");
		}
		Question shown = p.qname == null ? null : question(design, p.qname);
		if(p.qname != null && shown == null) {
			throw new Refusal(survey.getDisplayName() + " no longer has the question " + p.qname
					+ ". Name one in question. Nothing was changed.");
		}

		// The function, which has to suit the question
		String fn = str(args, "function");
		if(fn != null && !fn.isEmpty()) {
			p.fn = fn.toLowerCase();
		} else if(fn != null || isNew || shown == null) {
			p.fn = shown == null ? "none" : defaultFunction(shown.type);
		}
		List<String> allowed = functions(shown == null ? null : shown.type);
		if(!allowed.contains(p.fn)) {
			throw new Refusal("function for " + (shown == null ? "a table with no question" : p.qname
					+ ", a " + shown.type + " question,") + " is one of " + String.join(", ", allowed)
					+ ". Nothing was changed.");
		}

		// What it is grouped by
		if(args.containsKey("group_by")) {
			String name = str(args, "group_by");
			if(name == null || name.isEmpty()) {
				p.groupQuestionName = null;
				p.groupQuestionText = null;
				p.groupType = "normal";
			} else {
				Question g = question(design, name);
				if(g == null) {
					throw new Refusal(survey.getDisplayName() + " has no question called " + name
							+ ". Nothing was changed.");
				}
				p.groupQuestionName = g.name;
				p.groupQuestionText = label(g);
				p.groupType = GROUP_TYPES.contains(g.type) ? g.type : "normal";
			}
		} else if(isNew) {
			p.groupType = "normal";
		}

		// The dates
		if(args.containsKey("date_question")) {
			String name = str(args, "date_question");
			if(name == null || name.isEmpty()) {
				p.dateQuestionName = null;
			} else {
				Question d = question(design, name);
				if(d == null || !("date".equals(d.type) || "dateTime".equals(d.type))) {
					throw new Refusal("date_question is a date or dateTime question of "
							+ survey.getDisplayName() + ". Nothing was changed.");
				}
				p.dateQuestionName = d.name;
			}
		}
		if(args.containsKey("from_date")) {
			p.fromDate = date(str(args, "from_date"), "from_date");
		}
		if(args.containsKey("to_date")) {
			p.toDate = date(str(args, "to_date"), "to_date");
		}
		if(p.fromDate != null && p.toDate != null && p.toDate.before(p.fromDate)) {
			throw new Refusal("to_date is before from_date. Nothing was changed.");
		}
		String timeGroup = str(args, "time_group");
		if(timeGroup != null) {
			p.timeGroup = timeGroup.isEmpty() ? "none" : timeGroup.toLowerCase();
		} else if(p.timeGroup == null) {
			p.timeGroup = "none";
		}
		if(!TIME_GROUPS.contains(p.timeGroup)) {
			throw new Refusal("time_group is one of " + String.join(", ", TIME_GROUPS) + ". Nothing was changed.");
		}
		if(!"none".equals(p.timeGroup) && p.dateQuestionName == null) {
			throw new Refusal("Grouping by time needs date_question. Nothing was changed.");
		}

		String chartType = str(args, "chart_type");
		if(chartType != null && !chartType.isEmpty()) {
			p.chartType = chartType.toLowerCase();
		} else if(p.chartType == null) {
			p.chartType = "histogram";
		}
		if(!CHART_TYPES.contains(p.chartType)) {
			throw new Refusal("chart_type is histogram or pie. Nothing was changed.");
		}

		if(args.containsKey("title")) {
			String title = str(args, "title");
			try {
				p.title = title == null ? "" : HtmlSanitise.checkCleanName(title, ctx.localisation);
			} catch (Exception e) {
				throw new Refusal("A title cannot contain < or >, or a web address. Nothing was changed.");
			}
		} else if(p.title == null) {
			p.title = "";
		}
	}

	/* The project a survey is in */
	static int projectOf(McpToolContext ctx, int sId) throws Exception {
		return GeneralUtilityMethods.getProjectIdFromSurveyIdent(ctx.sd, GeneralUtilityMethods.getSurveyIdent(ctx.sd, sId));
	}

	/* A panel as a caller reads it */
	static Map<String, Object> describe(Panel p) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put("panel_id", p.id);
		m.put("type", p.type);
		m.put("title", p.title == null ? "" : p.title);
		m.put("survey_id", p.sId);
		m.put("survey", p.sName);
		m.put("question", p.qname == null ? "" : p.qname);
		m.put("function", p.fn == null ? "" : p.fn);
		m.put("group_by", p.groupQuestionName == null ? "" : p.groupQuestionName);
		m.put("date_question", p.dateQuestionName == null ? "" : p.dateQuestionName);
		m.put("from_date", p.fromDate == null ? "" : p.fromDate.toString());
		m.put("to_date", p.toDate == null ? "" : p.toDate.toString());
		m.put("time_group", p.timeGroup == null ? "none" : p.timeGroup);
		m.put("chart_type", p.chartType == null ? "" : p.chartType);
		return m;
	}

	/* The functions the Analysis page offers for a question of this type */
	static List<String> functions(String qType) {
		if(qType == null) {
			return Arrays.asList("none");
		}
		if(qType.startsWith("select")) {
			return Arrays.asList("none", "count", "percent");
		}
		if(qType.equals("int") || qType.equals("decimal")) {
			return Arrays.asList("none", "average", "total");
		}
		if(qType.equals("calculate") || qType.equals("string") || qType.equals("server_calculate")) {
			return Arrays.asList("none", "count", "percent", "average", "total");
		}
		return Arrays.asList("none");
	}

	private static String defaultFunction(String qType) {
		if(qType != null && qType.startsWith("select")) {
			return "percent";
		}
		if("int".equals(qType) || "decimal".equals(qType)) {
			return "average";
		}
		return "none";
	}

	private static Question question(Survey design, String name) {
		String wanted = name.trim();
		for(Form f : design.surveyData.forms) {
			if(f.questions == null) {
				continue;
			}
			for(Question q : f.questions) {
				if(q.soft_deleted || q.type == null || q.type.startsWith("begin") || q.type.startsWith("end")
						|| q.type.equals("note")) {
					continue;
				}
				if(wanted.equalsIgnoreCase(q.name)) {
					return q;
				}
			}
		}
		return null;
	}

	private static String label(Question q) {
		if(q.labels != null && !q.labels.isEmpty() && q.labels.get(0).text != null
				&& !q.labels.get(0).text.trim().isEmpty()) {
			return q.labels.get(0).text;
		}
		return q.name;
	}

	private static Date date(String v, String name) throws Refusal {
		if(v == null || v.trim().isEmpty()) {
			return null;
		}
		try {
			return Date.valueOf(v.trim());
		} catch (IllegalArgumentException e) {
			throw new Refusal(name + " is a date in the form yyyy-mm-dd. Nothing was changed.");
		}
	}

	private static String str(Map<String, Object> args, String name) {
		Object v = args.get(name);
		return v == null ? null : v.toString().trim();
	}

	private static int intOf(Object v) throws Refusal {
		try {
			return v instanceof Number ? ((Number) v).intValue() : Integer.parseInt(v.toString().trim());
		} catch (NumberFormatException e) {
			throw new Refusal("survey_id is a number. Nothing was changed.");
		}
	}
}
