package org.smap.sdal.managers;

/*
This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

SMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with SMAP.  If not, see <http://www.gnu.org/licenses/>.

 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.Stack;
import java.util.regex.Pattern;

import org.javarosa.xpath.XPathParseTool;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.ApplicationWarning;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.constants.SmapQuestionTypes;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.QuestionForm;
import org.smap.sdal.model.SetValue;
import org.smap.sdal.model.Survey;

/*
 * Checks on a survey definition that is being uploaded.  Used by both the XLSForm and the
 * XForm uploads so that a survey is held to the same rules whichever way it arrives.
 *
 * A location is an integer.  For an XLSForm it is the row in the work sheet, for an XForm it
 * is an index into the list of question paths the XForm loader has registered.  The
 * ErrorFormatter turns the location into the text of the message.
 */
public class SurveyUploadValidator {

	/*
	 * Creates the exception reported to the user for a failed check
	 */
	public interface ErrorFormatter {
		ApplicationException error(String code, int location, String sheet, String p1, String p2, String p3);
	}

	/*
	 * XLSForm errors give the row and the work sheet
	 */
	public static class XlsErrorFormatter implements ErrorFormatter {
		private ResourceBundle localisation;
		public XlsErrorFormatter(ResourceBundle l) {
			localisation = l;
		}
		public ApplicationException error(String code, int location, String sheet, String p1, String p2, String p3) {
			return XLSUtilities.getApplicationException(localisation, code, location, sheet, p1, p2, p3);
		}
	}

	private class FunctionCheck {
		String name;
		int args;
		String template;

		FunctionCheck(String n, int a, String t) {
			name = n;
			args = a;
			template = t;
		}
	}

	private ResourceBundle localisation;
	private ArrayList<ApplicationWarning> warnings;
	private ErrorFormatter fmt;
	private Survey survey;
	private ArrayList<FunctionCheck> functions = new ArrayList<> ();

	HashMap<String, Integer> qNameMap = new HashMap<> ();							// Use in question name validation
	HashMap<String, Integer> qNameMapCaseInsensitive = new HashMap<> ();			// Use in question name uniqueness
	HashMap<String, HashMap<String, Integer>> oNameMap = new HashMap<> ();		// Use in option name validation
	HashMap<Integer, Stack<Question>> groupStackMap = new HashMap<>();			// Keep track of groups in forms
	boolean inFieldList = false;												// Only some questions are allowed inside a field list

	Pattern validChoiceName = Pattern.compile("^[A-Za-z0-9_@&\\-\\.\\+%,():/ ]*$");
	Pattern validListName = Pattern.compile("^[A-Za-z0-9_@&\\-\\.\\+%,():/]*$");	// Like choices but spaces are not valid

	public SurveyUploadValidator(ResourceBundle l, ArrayList<ApplicationWarning> w, ErrorFormatter fmt) {
		localisation = l;
		warnings = w;
		this.fmt = fmt;

		// Initialise Function Check array
		functions.add(new FunctionCheck("count", 1, "count(nodeset)"));
		functions.add(new FunctionCheck("if", 3, "if(condition, a, b)"));

		// Initialise question name map with internal names
		qNameMap.put("_user", -1);
		qNameMap.put(SmapServerMeta.UPLOAD_TIME_NAME, -1);
		qNameMap.put("_hrk", -1);
		qNameMap.put("_version", -1);
	}

	public void setSurvey(Survey s) {
		survey = s;
	}

	/*
	 * Record a name that expressions may refer to but which is not checked as a question, for
	 * example a preload
	 */
	public void addName(String name, int location) {
		qNameMap.put(name, location);
		qNameMapCaseInsensitive.put(name.toLowerCase(), location);
	}

	public Integer getLocation(String name) {
		return qNameMap.get(name);
	}

	public Stack<Question> getGroupStack(Integer formIdx) {
		Stack<Question> gs = null;
		gs = groupStackMap.get(formIdx);
		if(gs == null) {
			gs = new Stack<Question> ();
			groupStackMap.put(formIdx, gs);
		}
		return gs;
	}

	public void validateForm(int rowNumber, Form f) throws Exception {

		if(f.questions.size() == 0) {
			// Form must have at least one question
			throw fmt.error("tu_er", rowNumber, "survey", null, null, null);
		} else {
			// Questions must be visible
			boolean hasVisibleQuestion = false;
			for(Question qx : f.questions) {
				if(!qx.type.equals("calculate")) {
					hasVisibleQuestion = true;
					break;
				}
			}

			if(!hasVisibleQuestion) {
				ApplicationException e = fmt.error("tu_er", rowNumber, "survey", null, null, null);
				warnings.add(new ApplicationWarning(e.getMessage()));
			}
		}

		/*
		 * Validate groups
		 */
		for(int i = 0; i < f.questions.size(); i++) {
			Question q = f.questions.get(i);
			if(q.type.equals("begin group")) {
				validateGroup(f.questions, q, i);
			}
		}

	}

	private int validateGroup(ArrayList<Question> questions, Question groupQuestion, int start) throws ApplicationException {

		Question q;
		String name = groupQuestion.name;
		int i;
		boolean hasVisibleQuestion = false;
		for(i = start + 1; i < questions.size(); i++) {
			q = questions.get(i);
			if(q.type.equals("begin group")) {
				hasVisibleQuestion = true;		// Count another group as a visible question, as long as this embedded group has a visible question then all is good
				validateGroup(questions, q, i);	// recursive validation
			} else if(q.type.equals("end group")) {
				break;
			} else {
				hasVisibleQuestion = true;
			}
		}

		if(!hasVisibleQuestion) {
			Integer rowNumber = qNameMap.get(name);
			ApplicationException e = fmt.error("tu_er_g", rowNumber == null ? -1 : rowNumber, "survey", null, null, null);
			warnings.add(new ApplicationWarning(e.getMessage()));
		}
		return i + 1;
	}

	public void validateQuestion(Question q, int rowNumber, int formIndex) throws Exception {

		/*
		 * Check Name
		 */
		if (q.name == null || q.name.trim().length() == 0) {
			// Check for a missing name
			throw fmt.error("tu_mn", rowNumber, "survey", null, null, null);

		} else if(!GeneralUtilityMethods.isValidQuestionName(q.name)) {
			// Check for a valid name
			throw fmt.error("tu_qn", rowNumber, "survey", q.name, null, null);

		} else if(!q.type.equals("end group") && !q.type.equals("end matrix") && qNameMapCaseInsensitive.get(q.columnName) != null) {
			// Check for a duplicate name
			// Temporary - allow multiple question names of the_geom
			if(!q.name.equals("the_geom")) {
				throw fmt.error("tu_dq", rowNumber, "survey", q.name, null, null);
			}
		}

		if(!q.type.equals("end group") && !q.type.equals("end matrix")) {
			qNameMap.put(q.name, rowNumber);
			qNameMapCaseInsensitive.put(q.columnName, rowNumber);
		}

		// check relevance
		if(q.relevant != null) {
			ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.relevant);
			if(refs.contains(q.name)) {		// Circular references
				throw fmt.error("tu_cr", rowNumber, "survey", "relevant", q.name, q.relevant);
			}

			checkParentheses(q.relevant, rowNumber, "survey", "relevant", q.name);
			try {
				XPathParseTool.parseXPath(GeneralUtilityMethods.convertAllxlsNamesToPseudoXPath(q.relevant));
			} catch (Exception e) {
				throw fmt.error("tu_jr", rowNumber, "survey", "relevant", e.getMessage(), null);
			}
			testXExprFunctions(q.relevant, true, rowNumber, "relevant");
		}

		// check constraint
		if(q.constraint != null) {
			checkParentheses(q.constraint, rowNumber, "survey", "constraint", q.name);
			try {
				XPathParseTool.parseXPath(GeneralUtilityMethods.convertAllxlsNamesToPseudoXPath(q.constraint));
			} catch (Exception e) {
				throw fmt.error("tu_jr", rowNumber, "survey", "constraint", e.getMessage(), null);
			}
			testXExprFunctions(q.constraint, true, rowNumber, "constraint");
		}

		// check calculate
		if(q.calculation != null) {
			checkParentheses(q.calculation, rowNumber, "survey", "calculation", q.name);
			checkCalculationCircularReferences(q.calculation, rowNumber, "survey", "calculation", q.name);
			try {
				XPathParseTool.parseXPath(GeneralUtilityMethods.convertAllxlsNamesToPseudoXPath(q.calculation));
			} catch (Exception e) {
				throw fmt.error("tu_jr", rowNumber, "survey", "calculation", e.getMessage(), null);
			}
			testXExprFunctions(q.calculation, true, rowNumber, "calculation");
		}

		// check appearance
		if(q.appearance != null) {
			checkParentheses(q.appearance, rowNumber, "survey", "appearance", q.name);
			testXExprFunctions(q.appearance, true, rowNumber, "appearance");
		}

		// Check choice filter
		if(q.choice_filter != null) {
			checkParentheses(q.choice_filter, rowNumber, "survey", "choice_filter", q.name);
			try {
				XPathParseTool.parseXPath(GeneralUtilityMethods.convertAllxlsNamesToPseudoXPath(q.choice_filter));
			} catch (Exception e) {
				throw fmt.error("tu_jr", rowNumber, "survey", "choice_filter", e.getMessage(), null);
			}
			testXExprFunctions(q.choice_filter, true, rowNumber, "choice_filter");
		}

		// Check intent
		if(q.intent != null ) {
			boolean valid = false;
			if(q.type.equals("begin group")) {
				if(q.appearance != null && q.appearance.contains("field-list")) {
					valid = true;
				}
			}
			if(!valid) {
				throw fmt.error("tu_int", rowNumber, "survey", null, null, null);
			}
		}

		// invalid question in field-list
		if(inFieldList) {
			if(q.type.equals("end group")) {
				inFieldList = false;
			} else if(q.type.equals("begin repeat")) {		// Tentatively should be ok to have a group inside a fieldlist group
				Stack<Question> groupStack = getGroupStack(formIndex);
				String groupName = groupStack.isEmpty() ? "" : groupStack.pop().name;
				throw fmt.error("tu_fl", rowNumber, "survey", q.type, groupName, null);
			}
		} else {
			if(q.type.equals("begin group")) {
				if(q.appearance != null && q.appearance.contains("field-list")) {
					inFieldList = true;
				}
			}
		}

		// List name not in choices
		if(q.list_name != null  && !q.list_name.startsWith("$")) {
			if(survey.surveyData.optionLists.get(q.list_name) == null) {
				throw fmt.error("tu_lnf", rowNumber, "survey", q.list_name, null, null);
			}
		}

		// Matrix
		if(q.type.equals("begin matrix")) {
			// Missing list
			if(q.list_name == null) {
				throw fmt.error("tu_mat_list", rowNumber, "survey", null, null, null);
			}
		}

		// check parameters
		if(q.paramArray != null) {
			ArrayList<KeyValueSimp> noDups = new ArrayList<KeyValueSimp> ();
			HashMap<String, String> paramHashMap = new HashMap<> ();
			for(KeyValueSimp kv : q.paramArray) {
				String existing = paramHashMap.get(kv.k);
				if(existing == null) {
					noDups.add(kv);
					paramHashMap.put(kv.k, kv.v);
				} else {
					if(existing.equals(kv.v)) {
						// Its a duplicate just discard by not adding to the noDups output
					} else {
						// Conflicting values
						throw fmt.error("tu_cf", rowNumber, "survey", kv.k, null, null);
					}
				}
			}
			q.paramArray = noDups;

		}

		// Check that parent and child forms have the form_identifier parameter
		if(q.type.equals("parent_form") || q.type.equals(SmapQuestionTypes.CHILD_FORM)) {
			boolean hasFormIdentifier = false;
			if(q.paramArray != null) {
				for(KeyValueSimp kv : q.paramArray) {
					if(kv.k.equals("form_identifier")) {
						hasFormIdentifier = true;
						break;
					}
				}
			}
			if(!hasFormIdentifier) {
				throw fmt.error("tu_form_launch", rowNumber, "survey", null, null, null);
			}
		}

	}

	/*
	 * When a survey is added to a bundle, or replaces a survey, a question that is already in the
	 * bundle must keep a compatible database column type
	 */
	public void validateTypeCompatible(Question q, HashMap<String, QuestionForm> questionNames, int rowNumber) throws ApplicationException {
		if(questionNames != null) {
			QuestionForm qt = questionNames.get(q.name);
			if(qt != null) {
				if(q.source != null && qt.published) {
					String newColType = GeneralUtilityMethods.getPostgresColType(q.type);
					String oldColType = GeneralUtilityMethods.getPostgresColType(qt.qType);
					if(!newColType.equals(oldColType) &&
							!newColType.equals("end repeat") &&	// Not sure why these are needed as source type should be null
							!oldColType.equals("end repeat")&&
							!newColType.equals("end group") &&
							!oldColType.equals("end group")
							) {
						throw fmt.error("tu_it", rowNumber, "survey", q.name, q.type, qt.qType);
					}
				}
			}
		}
	}

	private void checkParentheses(String expression, int rowNumber, String sheet, String column, String name) throws ApplicationException {
		checkMatchedParenthesies('{', '}', expression, rowNumber, sheet, column);
		checkMatchedParenthesies('(', ')', expression, rowNumber, sheet, column);
	}

	private void checkMatchedParenthesies(char p1, char p2, String expression, int rowNumber, String sheet, String column) throws ApplicationException {
		int depth = 0;
		int locn = 0;
		if(expression != null) {
			for(int i = 0; i < expression.length(); i++) {
				char c = expression.charAt(i);
				if( c == p1) {
					depth++;
					locn = i;
				} else if( c == p2) {
					depth--;
					locn = i;
				}
				if(depth < 0) {
					break;
				}
			}

			if(depth != 0) {
				if(p1 == '(') {
					throw fmt.error("tu_mbs", rowNumber, sheet, column, String.valueOf(locn), null);
				} else if(p1 == '{') {
					throw fmt.error("tu_mbc", rowNumber, sheet, column, String.valueOf(locn), null);
				}

			}
		}
	}

	private void checkCalculationCircularReferences(String expression, int rowNumber, String sheet, String column, String name) throws ApplicationException {

		Pattern pattern = Pattern.compile("\\$\\{.+?\\}");
		java.util.regex.Matcher matcher = pattern.matcher(expression);
		while (matcher.find()) {
			String matched = matcher.group();
			String qname = matched.substring(2, matched.length() - 1);
			if(qname.equals(name)) {
				throw fmt.error("tu_cr", rowNumber, sheet, column, name, expression);
			}
		}
	}

	public void validateSurvey() throws Exception {

		// Validate forms and questions
		for(Form f : survey.surveyData.forms) {
			if(f.reference) {
				Integer rowNumber = qNameMap.get(f.name);
				int row = rowNumber == null ? -1 : rowNumber;
				if(f.name.equals(f.referenceName)) {
					throw fmt.error("tu_ref_self", row, "survey", f.name, null, null);
				} else {
					boolean validRef = false;
					for(Form refForm : survey.surveyData.forms) {
						if(refForm.name.equals(f.referenceName)) {
							if(refForm.reference) {
								throw fmt.error("tu_ref_ref", row, "survey", f.name, refForm.name, null);
							} else {
								validRef = true;
								break;
							}
						}
					}
					if(!validRef) {
						throw fmt.error("tu_ref_nf", row, "survey", f.referenceName, f.name, null);
					}
				}
			}
			for(Question q : f.questions) {
				if(q.relevant != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.relevant);
					if(refs.size() > 0) {
						questionInSurvey(refs, "relevant", q);
					}
				}
				if(q.constraint != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.constraint);
					if(refs.size() > 0) {
						questionInSurvey(refs, "constraint", q);
					}
				}
				if(q.repeatCount != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.repeatCount);
					if(refs.size() > 0) {
						questionInSurvey(refs, "repeat_count", q);
					}
					// Make sure there is not a question with a name that will clash with the automatically generated repeat count name
					repeatCountClash(q);
				}
				if(q.choice_filter != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.choice_filter);
					if(refs.size() > 0) {
						questionInSurvey(refs, "choice_filter", q);
					}
				}
				if(q.labels != null) {
					int idx = 0;
					for(Label l : q.labels) {
						ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(l.text);
						if(refs.size() > 0) {
							questionInSurvey(refs, "label::" + survey.surveyData.languages.get(idx).name, q);

							if(refs.contains(q.name)) {		// Check for self reference
								Integer rowNumber = qNameMap.get(q.name);
								throw fmt.error("tu_cr", rowNumber == null ? -1 : rowNumber, "survey",
										"label::" + survey.surveyData.languages.get(idx).name, q.name, q.relevant);
							}
						}
						idx++;
					}
				}
				if(q.calculation != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.calculation);
					if(refs.size() > 0) {
						questionInSurvey(refs, "calculation", q);
					}
				}
				if(q.appearance != null) {
					ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(q.appearance);
					if(refs.size() > 0) {
						questionInSurvey(refs, "appearance", q);
					}
				}
				// check default
				if(q.setValues != null) {
					for(SetValue sv : q.setValues) {
						if(sv.value != null) {
							if(sv.value.contains("last-saved#")) {
								int idx1 = sv.value.indexOf('#');
								int idx2 = sv.value.indexOf('}', idx1);
								if(idx2 > 0) {
									String sourceQuestion = sv.value.substring(idx1 + 1, idx2);
									if(sourceQuestion != null) {
										ArrayList<String> refs = new ArrayList<String> ();
										refs.add(sourceQuestion);
										questionInSurvey(refs, "default", q);
									}
								}
							}
						}
					}
				}
			}
		}

		// Validate Settings
		if(survey.surveyData.instanceNameDefn != null) {
			ArrayList<String> refs = GeneralUtilityMethods.getXlsNames(survey.surveyData.instanceNameDefn );
			if(refs.size() > 0) {
				settingsQuestionInSurvey(refs, "instance_name");
			}
		}

		// Validate groups
		for(Integer formIndex : groupStackMap.keySet()) {
			Stack<Question> groupStack = groupStackMap.get(formIndex);

			if(!groupStack.isEmpty()) {
				String groupName = groupStack.pop().name;
				Integer rowNumber = qNameMap.get(groupName);
				throw fmt.error("tu_meg", rowNumber == null ? -1 : rowNumber, "survey", groupName, null, null);
			}
		}

	}

	public void validateOption(Option o, int rowNumber) throws ApplicationException {

		HashMap<String, Integer> listMap = oNameMap.get(o.optionList);
		if(listMap == null) {
			listMap = new HashMap<String, Integer> ();
			oNameMap.put(o.optionList, listMap);
		}

		if(o.value == null || o.value.trim().length() == 0) {
			// Check for a missing value
			throw fmt.error("tu_vr", rowNumber, "choices", o.optionList, null, null);

		} else if(!validChoiceName.matcher(o.value).matches()) {
			// Check for a valid value
			throw fmt.error("tu_cn",rowNumber, "choices", o.value, null, null);

		} else if(!validListName.matcher(o.optionList).matches()) {
			// Check for a valid value
			throw fmt.error("tu_ln",rowNumber, "choices", o.optionList, null, null);

		} else if(listMap.get(o.value) != null) {
			// Check for a duplicate value
			ApplicationException e = fmt.error("tu_do", rowNumber, "choices", o.value, o.optionList, null);
			warnings.add(new ApplicationWarning(e.getMessage()));
		}

		listMap.put(o.value, rowNumber);

	}

	private void questionInSurvey(ArrayList<String> names, String context, Question q) throws ApplicationException {
		for(String name : names) {
			if(qNameMap.get(name) == null) {
				Integer rowNumber = qNameMap.get(q.name);
				throw fmt.error("tu_mq", rowNumber == null ? -1 : rowNumber, "survey", context, name, null);
			}
		}
	}

	private void repeatCountClash(Question q) throws ApplicationException {
		String name = q.name.toLowerCase() + "_count";
		if(qNameMap.get(name) != null) {
			Integer rowNumber = qNameMap.get(name);
			throw fmt.error("tu_rc", rowNumber, "survey", name, q.name, null);
		}
	}

	public void settingsQuestionInSurvey(ArrayList<String> names, String colname) throws ApplicationException {
		for(String name : names) {
			if(qNameMap.get(name) == null) {
				String msg = localisation.getString("tu_mq");
				msg = msg.replace("%s1", colname);
				msg = msg.replace("%s2", "settings");
				msg = msg.replace("%s3", name);
				throw new ApplicationException(msg);
			}
		}
	}

	/*
	 * Test for valid javarosa functions in an XPath expression
	 * If the call is for an appearance then only the search function is valid and "search" without parameters is also valid
	 */
	private void testXExprFunctions(String in, boolean isAppearance, int rowNumber, String column) throws Exception {

		// 1. remove any text inside quotes
		boolean inside = false;
		StringBuffer noText = new StringBuffer("");
		for(int i = 0; i < in.length(); i++) {
			if(in.charAt(i) == '\'') {
				inside = !inside;
			} else if(!inside) {
				noText.append(in.charAt(i));
			}
		}

		if(noText.length() > 0) {
			String process = noText.toString();
			for(FunctionCheck f : functions) {
				Pattern pattern = Pattern.compile(f.name + "[\\s]*\\(");
				java.util.regex.Matcher matcher = pattern.matcher(process);

				while (matcher.find()) {

					StringBuffer toTest = new StringBuffer("");

					// remove sub functions
					int depth = 0;
					boolean end = false;
					for(int i = matcher.start(); i < noText.length(); i++) {
						if(noText.charAt(i) == '(') {
							depth++;
						} else if(noText.charAt(i) == ')') {
							depth--;
							if(depth == 0) {
								end = true;
							}
						}
						if(depth <= 1) {
							toTest.append(noText.charAt(i));
						}
						if(end) {
							break;
						}
					}

					String[] args = toTest.toString().split(",");
					if(args.length != f.args) {
						throw fmt.error("tu_args", rowNumber, "survey", column,
								f.name, f.template);
					}
				}
			}
		}
	}
}
