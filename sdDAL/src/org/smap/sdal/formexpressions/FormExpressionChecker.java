package org.smap.sdal.formexpressions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.regex.Pattern;

import org.smap.sdal.model.Form;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.Survey;

/*
 * Checks the expressions of a survey's questions, and the things a device needs to be able to start the form:
 * that the expressions can be read, use functions that exist with the right number of arguments, that no
 * calculation or relevance depends on itself, and that every select has choices.
 *
 * Problems that the JavaRosa check this replaces also refused are errors.  Those it did not find, such as an
 * unknown function in a constraint, are warnings for now, so a survey that works today is not refused when it
 * is next changed.
 */
public class FormExpressionChecker {

	public static final String RELEVANT = "relevant";
	public static final String CONSTRAINT = "constraint";
	public static final String CALCULATION = "calculation";
	public static final String CHOICE_FILTER = "choice_filter";
	public static final String REQUIRED = "required";
	public static final String READ_ONLY = "read_only";
	public static final String REPEAT_COUNT = "repeat_count";

	// Expressions a device evaluates when the form starts, where JavaRosa found unknown functions
	private static final HashSet<String> EVALUATED_AT_START = new HashSet<> (java.util.Arrays.asList(
			RELEVANT, CALCULATION, REQUIRED, REPEAT_COUNT));

	private static final Pattern DATE = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}([T ].*)?$");

	public static class Problem {
		public boolean error;		// false for a warning
		public String question;
		public String column;		// One of the column constants, null for a problem with the question itself
		public String expression;
		public String message;		// The problem in words, without saying where
		public int position;		// The character the problem is at, -1 if none

		/*
		 * The problem, with the column and question it is in
		 */
		public String describe(ResourceBundle l) {
			String where = column == null ? question
					: l.getString("fe_where").replace("%s1", column).replace("%s2", question);
			return where + ": " + message;
		}
	}

	private ResourceBundle localisation;
	private FormExpressionSpec spec = FormExpressionSpec.get();

	public FormExpressionChecker(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Check one expression on its own: that it can be read and the functions it uses.  Returns the problems found.
	 */
	public List<Problem> checkExpression(String question, String column, String expression) {
		List<Problem> problems = new ArrayList<> ();
		check(problems, question, column, expression, null);
		return problems;
	}

	/*
	 * Check every expression in a survey, the loops between them, and the choices of its selects
	 */
	public List<Problem> checkSurvey(Survey s) {
		return checkSurvey(s, true);
	}

	/*
	 * Only the problems that involve more than one expression: loops, and selects with no choices.  For a caller that
	 * has already checked each expression on its own.
	 */
	public List<Problem> checkLoopsAndChoices(Survey s) {
		return checkSurvey(s, false);
	}

	private List<Problem> checkSurvey(Survey s, boolean expressions) {
		List<Problem> problems = new ArrayList<> ();
		List<Problem> ignored = new ArrayList<> ();
		LinkedHashMap<String, List<String>> dependsOn = new LinkedHashMap<> ();	// For loops

		for(Form f : s.surveyData.forms) {
			for(Question q : f.questions) {
				if(q.soft_deleted || q.name == null) {
					continue;
				}
				List<Problem> found = expressions ? problems : ignored;
				HashSet<String> refs = new HashSet<> ();
				check(found, q.name, RELEVANT, q.relevant, refs);
				check(found, q.name, CALCULATION, q.calculation, refs);
				List<String> deps = new ArrayList<> (refs);
				if(expressions) {
					check(problems, q.name, CONSTRAINT, q.constraint, null);
					check(problems, q.name, CHOICE_FILTER, q.choice_filter, null);
					check(problems, q.name, REQUIRED, expressionOnly(q.required_expression), null);
					check(problems, q.name, READ_ONLY, expressionOnly(q.readonly_expression), null);
					check(problems, q.name, REPEAT_COUNT, q.repeatCount, null);
				}
				if(!deps.isEmpty()) {
					dependsOn.put(q.name, deps);
				}
				checkChoices(problems, s, q);
			}
		}
		findLoops(problems, dependsOn);
		return problems;
	}

	/*
	 * required and read only hold true or false when they are not an expression
	 */
	private String expressionOnly(String v) {
		if(v == null) {
			return null;
		}
		String t = v.trim();
		if(t.isEmpty() || t.equalsIgnoreCase("yes") || t.equalsIgnoreCase("no") || t.equalsIgnoreCase("true")
				|| t.equalsIgnoreCase("false") || t.equals("true()") || t.equals("false()")) {
			return null;
		}
		return v;
	}

	private void check(List<Problem> problems, String question, String column, String expression, HashSet<String> refs) {
		if(expression == null || expression.trim().isEmpty()) {
			return;
		}
		ExprNode root;
		try {
			root = new FormExpressionParser().parse(expression);
		} catch (FormExpressionException e) {
			problems.add(problem(true, question, column, expression, message(e.key, e.params), e.position));
			return;
		}
		walk(problems, question, column, expression, root, refs);
	}

	private void walk(List<Problem> problems, String question, String column, String expression, ExprNode n,
			HashSet<String> refs) {

		if(ExprNode.QUESTION.equals(n.kind) && refs != null) {
			refs.add(n.value);
		} else if(ExprNode.FUNCTION.equals(n.kind)) {
			boolean error = EVALUATED_AT_START.contains(column);
			FormExpressionSpec.Function f = spec.getFunction(n.value);
			if(f == null) {
				String like = spec.closest(n.value);
				String msg = like == null ? message("fe_fn_unknown", n.value) : message("fe_fn_suggest", n.value, like);
				problems.add(problem(error, question, column, expression, msg, n.position));
			} else {
				String args = argumentProblem(f, n.children.size());
				if(args != null) {
					problems.add(problem(error, question, column, expression, args, n.position));
				}
				if(f.name.equals("indexed-repeat") && !n.children.isEmpty()) {
					String k = n.children.get(0).kind;
					if(!ExprNode.QUESTION.equals(k) && !ExprNode.PATH.equals(k)) {
						problems.add(problem(error, question, column, expression, message("fe_ir_first"),
								n.children.get(0).position));
					}
				}
				if(f.name.equals("date") && n.children.size() == 1 && ExprNode.STRING.equals(n.children.get(0).kind)
						&& !DATE.matcher(n.children.get(0).value).matches()) {
					problems.add(problem(error, question, column, expression,
							message("fe_date", n.children.get(0).value), n.children.get(0).position));
				}
			}
		}
		for(ExprNode c : n.children) {
			walk(problems, question, column, expression, c, refs);
		}
	}

	/*
	 * Whether a number of arguments suits a function, in words if it does not
	 */
	private String argumentProblem(FormExpressionSpec.Function f, int count) {
		int max = f.maxArgs == null ? Integer.MAX_VALUE : f.maxArgs;
		boolean ok = count >= f.minArgs && count <= max;
		String takes;
		if(f.name.equals("indexed-repeat")) {
			ok = ok && count % 2 == 1;
			takes = "3, 5, 7, 9 " + localisation.getString("fe_or") + " 11";
		} else if(f.name.equals("weighted-checklist")) {
			ok = ok && count % 2 == 0;
			takes = localisation.getString("fe_n_even");
		} else if(f.maxArgs == null) {
			takes = localisation.getString("fe_n_more").replace("%s1", String.valueOf(f.minArgs));
		} else if(f.minArgs == max) {
			takes = String.valueOf(max);
		} else if(f.minArgs + 1 == max) {
			takes = f.minArgs + " " + localisation.getString("fe_or") + " " + max;
		} else {
			takes = localisation.getString("fe_n_range").replace("%s1", String.valueOf(f.minArgs))
					.replace("%s2", String.valueOf(max));
		}
		if(ok) {
			return null;
		}
		return message("fe_fn_args", f.name, takes, String.valueOf(count));
	}

	/*
	 * A select whose list has no choices cannot be shown.  Choices that come from a file or a lookup are not known here.
	 */
	private void checkChoices(List<Problem> problems, Survey s, Question q) {
		if(q.type == null || !(q.type.equals("select") || q.type.equals("select1") || q.type.equals("rank"))
				|| q.list_name == null || q.external_choices) {
			return;
		}
		String app = q.appearance == null ? "" : q.appearance;
		if(app.contains("search(") || app.contains("lookup_choices(")) {
			return;
		}
		OptionList ol = s.surveyData.optionLists.get(q.list_name);
		if(ol != null && ol.options.isEmpty()) {
			problems.add(problem(true, q.name, null, null, message("fe_no_choices", q.list_name), -1));
		}
	}

	/*
	 * Calculations and relevance that depend on themselves, directly or through other questions
	 */
	private void findLoops(List<Problem> problems, Map<String, List<String>> dependsOn) {
		HashMap<String, Integer> state = new HashMap<> ();	// 1 being visited, 2 done
		HashSet<String> reported = new HashSet<> ();
		for(String q : dependsOn.keySet()) {
			ArrayList<String> stack = new ArrayList<> ();
			visit(q, dependsOn, state, stack, problems, reported);
		}
	}

	private void visit(String q, Map<String, List<String>> dependsOn, HashMap<String, Integer> state,
			ArrayList<String> stack, List<Problem> problems, HashSet<String> reported) {
		Integer st = state.get(q);
		if(st != null && st == 2) {
			return;
		}
		if(st != null && st == 1) {
			List<String> loop = new ArrayList<> (stack.subList(stack.indexOf(q), stack.size()));
			loop.add(q);
			String key = new java.util.TreeSet<> (loop).toString();
			if(reported.add(key)) {
				StringBuilder b = new StringBuilder();
				for(String n : loop) {
					if(b.length() > 0) {
						b.append(" → ");
					}
					b.append("${").append(n).append("}");
				}
				problems.add(problem(true, loop.get(0), null, null, message("fe_loop", b.toString()), -1));
			}
			return;
		}
		state.put(q, 1);
		stack.add(q);
		List<String> deps = dependsOn.get(q);
		if(deps != null) {
			for(String d : deps) {
				visit(d, dependsOn, state, stack, problems, reported);
			}
		}
		stack.remove(stack.size() - 1);
		state.put(q, 2);
	}

	private Problem problem(boolean error, String question, String column, String expression, String message, int position) {
		Problem p = new Problem();
		p.error = error;
		p.question = question;
		p.column = column;
		p.expression = expression;
		p.position = position;
		p.message = message;
		if(expression != null && position >= 0) {
			p.message += " " + message("fe_at", String.valueOf(position + 1), expression);
		}
		return p;
	}

	private String message(String key, String ... params) {
		String m = localisation.getString(key);
		for(int i = 0; i < params.length; i++) {
			m = m.replace("%s" + (i + 1), params[i] == null ? "" : params[i]);
		}
		return m;
	}
}
