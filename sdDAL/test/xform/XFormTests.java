import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.ApplicationWarning;
import org.smap.sdal.managers.XFormTemplateUploadManager;
import org.smap.sdal.managers.XLSTemplateUploadManager;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.ServerCalculation;
import org.smap.sdal.model.StyleList;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.SurveyDAO;

/*
 * Tests of the XForm upload that do not need a database.  Run them with run.sh in this
 * directory after a build of sdDAL
 *
 *  - every field of the survey, form and question models is classified for an XForm replace
 *  - an XForm in the style pyxform creates, and one in the style Smap creates, load into the
 *    survey model with the values an XLSForm would give
 *  - XForms that the old loader failed on, or accepted when it should not have
 *  - settings an XForm cannot hold are kept when it replaces a survey
 *  - the XLSForm checks still work now that they are shared with the XForm upload
 */
public class XFormTests {

	static int failures = 0;
	static ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", Locale.ENGLISH);
	static String dir = "test/xform/";

	public static void main(String[] args) throws Exception {

		System.out.println("Field classification");
		classification();

		System.out.println("pyxform style XForm");
		household();

		System.out.println("Smap style XForm");
		smap();

		System.out.println("XForms with problems");
		problems();

		System.out.println("Replacing a survey keeps the settings an XForm does not have");
		keepSettings();

		System.out.println("XLSForm checks");
		xlsChecks();

		System.out.println(failures == 0 ? "\nALL PASSED" : "\n" + failures + " FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}

	/*
	 * ------------------------------------------------------------------------------------
	 */
	static void classification() {
		checkClassified(SurveyDAO.class, XFormTemplateUploadManager.SURVEY_FROM_XFORM, XFormTemplateUploadManager.SURVEY_XFORM_OR_KEPT,
				XFormTemplateUploadManager.SURVEY_KEPT, XFormTemplateUploadManager.SURVEY_KEPT_BY_IDENT,
				XFormTemplateUploadManager.SURVEY_NOT_WRITTEN);
		checkClassified(Question.class, XFormTemplateUploadManager.QUESTION_FROM_XFORM, XFormTemplateUploadManager.QUESTION_XFORM_OR_KEPT,
				XFormTemplateUploadManager.QUESTION_KEPT, XFormTemplateUploadManager.QUESTION_NOT_WRITTEN);
		checkClassified(Form.class, XFormTemplateUploadManager.FORM_FROM_XFORM, XFormTemplateUploadManager.FORM_KEPT,
				XFormTemplateUploadManager.FORM_NOT_WRITTEN);
	}

	@SafeVarargs
	static void checkClassified(Class<?> c, List<String>... lists) {
		HashSet<String> fields = new HashSet<> ();
		for(Field f : c.getDeclaredFields()) {
			if(!Modifier.isStatic(f.getModifiers())) {
				fields.add(f.getName());
			}
		}
		HashSet<String> listed = new HashSet<> ();
		for(List<String> l : lists) {
			for(String n : l) {
				check(c.getSimpleName() + "." + n + " listed once", listed.add(n));
				check(c.getSimpleName() + "." + n + " is a field", fields.contains(n));
			}
		}
		for(String f : fields) {
			check(c.getSimpleName() + "." + f + " is classified for an XForm replace", listed.contains(f));
		}
	}

	/*
	 * ------------------------------------------------------------------------------------
	 */
	static void household() throws Exception {
		ArrayList<ApplicationWarning> warnings = new ArrayList<> ();
		XFormTemplateUploadManager xum = new XFormTemplateUploadManager(localisation, warnings);
		Survey s = xum.getSurvey(1, new FileInputStream(dir + "household.xml"), null, 1, null, null, false, 0);

		equal("name from title", s.surveyData.displayName, "Household");
		equal("forms", s.surveyData.forms.size(), 2);
		equal("languages", s.surveyData.languages.size(), 2);
		equal("language name", s.surveyData.languages.get(0).name, "English");
		equal("language code", s.surveyData.languages.get(1).code, "fr");
		equal("default language", s.surveyData.def_lang, "English");
		equal("style", s.surveyData.surveyClass, "pages");
		equal("instance name", s.surveyData.instanceNameDefn, "concat( ${name} , '-', ${age} )");
		check("audit", s.surveyData.timing_data);
		check("track changes", s.surveyData.track_changes);
		check("start preload", hasMeta(s, "start", "start"));
		check("end preload", hasMeta(s, "end", "end"));
		check("device preload", hasMeta(s, "deviceid", "deviceid"));
		equal("no extra preloads", countPreloads(s), 3);

		Form main = s.surveyData.forms.get(0);
		equal("question order", names(main), "name,age,likes,member,fav,details,photo,note1,score,calc,details,yn,stamp");

		Question name = q(s, "name");
		equal("name type", name.type, "string");
		check("name required", name.required);
		equal("name label", name.labels.get(0).text, "Name");
		equal("name label fr", name.labels.get(1).text, "Nom");

		Question age = q(s, "age");
		equal("age type", age.type, "int");
		equal("age label output", age.labels.get(0).text, "Age of ${name}");
		equal("age hint", age.labels.get(0).hint, "In years");
		equal("age guidance", age.labels.get(0).guidance_hint, "Ask for a birth certificate");
		equal("age hint fr filled", age.labels.get(1).hint, "-");
		equal("age constraint", age.constraint, ". < 120");
		equal("age constraint msg", age.labels.get(0).constraint_msg, "Too old");
		equal("age constraint msg fr", age.labels.get(1).constraint_msg, "Trop vieux");
		equal("age relevant", age.relevant, "${name}  != ''");
		equal("age default", age.defaultanswer, "10");
		equal("age column", age.columnName, "age");

		Question likes = q(s, "likes");
		equal("likes type", likes.type, "select");
		equal("likes list", likes.list_name, "likes");
		equal("likes appearance", likes.appearance, "minimal");
		check("likes required expression", likes.required && "${age}  > 5".equals(likes.required_expression));
		equal("likes image", likes.labels.get(0).image, "likes.png");
		OptionList ol = s.surveyData.optionLists.get("likes");
		equal("likes choices", ol.options.size(), 2);
		equal("likes choice label fr", ol.options.get(0).labels.get(1).text, "Pommes");

		Question member = q(s, "member");
		equal("repeat type", member.type, "begin repeat");
		equal("repeat count", member.repeatCount, "${age}  div 5");
		equal("repeat label", member.labels.get(0).text, "Member");
		Form mf = s.surveyData.forms.get(1);
		equal("repeat form", mf.name, "member");
		equal("repeat parent", mf.parentFormIndex, 0);
		equal("repeat parent question", main.questions.get(mf.parentQuestionIndex).name, "member");
		equal("repeat questions", names(mf), "mname,older");

		Question older = q(s, "older");
		equal("relative relevant", older.relevant, "${mname}  != ''");
		equal("relative output", older.labels.get(0).text, "Older than ${age}?");
		equal("instance list", older.list_name, "yesno");
		equal("instance list label", s.surveyData.optionLists.get("yesno").options.get(0).labels.get(1).text, "Oui");

		Question fav = q(s, "fav");
		equal("choices from repeat", fav.list_name, "${mname}");
		equal("repeat choice filter", fav.choice_filter, "mname != ''");

		Question details = q(s, "details");
		equal("group type", details.type, "begin group");
		equal("group appearance", details.appearance, "field-list");
		equal("group relevant", details.relevant, "selected( ${likes} , 'a')");

		Question photo = q(s, "photo");
		equal("photo type", photo.type, "image");
		equal("photo pixels", param(photo, "max-pixels"), "1024");

		equal("note type", q(s, "note1").type, "note");

		Question score = q(s, "score");
		equal("range type", score.type, "range");
		equal("range start", param(score, "start"), "1");
		equal("range end", param(score, "end"), "10");

		Question calc = q(s, "calc");
		equal("calculate type", calc.type, "calculate");
		check("calculate hidden", !calc.visible);
		equal("quoted path", calc.calculation, "jr:choice-name( ${likes} ,'${likes}')");

		equal("identical lists merged", q(s, "yn").list_name, "yesno");

		Question stamp = q(s, "stamp");
		check("set value", stamp.setValues != null && stamp.setValues.size() == 1
				&& "now()".equals(stamp.setValues.get(0).value));

		equal("no warnings", warnings.size(), 0);
		for(ApplicationWarning w : warnings) {
			System.out.println("    warning: " + w.getMessage());
		}
	}

	/*
	 * ------------------------------------------------------------------------------------
	 */
	static void smap() throws Exception {
		ArrayList<ApplicationWarning> warnings = new ArrayList<> ();
		XFormTemplateUploadManager xum = new XFormTemplateUploadManager(localisation, warnings);
		Survey s = xum.getSurvey(1, new FileInputStream(dir + "smap.xml"), "Visits upload", 1, null, null, false, 0);

		equal("name entered", s.surveyData.displayName, "Visits upload");
		equal("default language", s.surveyData.languages.get(0).name, "language");
		equal("instance name", s.surveyData.instanceNameDefn, "concat(${country}, ' ', ${city})");
		check("not audited", !s.surveyData.timing_data);
		check("start geopoint", hasMeta(s, "loc", "start-geopoint"));
		check("start", hasMeta(s, "_start", "start"));

		Form main = s.surveyData.forms.get(0);
		equal("question order", names(main), "country,city,n,visits,amount,total,secret");
		equal("forms", s.surveyData.forms.size(), 2);
		equal("repeat copy skipped", names(s.surveyData.forms.get(1)), "vdate");

		Question country = q(s, "country");
		equal("pii", country.pii, "pii");
		equal("list", country.list_name, "countries");
		equal("list label", s.surveyData.optionLists.get("countries").options.get(1).labels.get(0).text, "Fiji");

		Question city = q(s, "city");
		equal("cascade list", city.list_name, "cities");
		equal("choice filter", city.choice_filter, "country= ${country}");
		equal("cascade filter column", s.surveyData.optionLists.get("cities").options.get(0).cascade_filters.get("country"), "au");
		equal("required expression", city.required_expression, "${country} = 'au'");
		equal("required msg", city.labels.get(0).required_msg, "A city is needed");

		Question n = q(s, "n");
		equal("constraint msg", n.constraint_msg, "Must be positive");

		Question visits = q(s, "visits");
		equal("repeat label", visits.labels.get(0).text, "Visits");
		equal("repeat count from _count", visits.repeatCount, "${n}");
		equal("repeat relevant", visits.relevant, "${n} > 0");

		Question amount = q(s, "amount");
		equal("decimal", amount.type, "decimal");
		check("readonly expression", amount.readonly && "${n} > 5".equals(amount.readonly_expression));

		Question total = q(s, "total");
		equal("trigger", total.trigger, "${amount}");
		equal("trigger value", total.calculation, "${amount} * 2");

		equal("no warnings", warnings.size(), 0);
		for(ApplicationWarning w : warnings) {
			System.out.println("    warning: " + w.getMessage());
		}
	}

	/*
	 * ------------------------------------------------------------------------------------
	 */
	static final String HEAD = "<?xml version=\"1.0\"?><h:html xmlns=\"http://www.w3.org/2002/xforms\" xmlns:h=\"http://www.w3.org/1999/xhtml\" "
			+ "xmlns:jr=\"http://openrosa.org/javarosa\"><h:head><h:title>T</h:title><model>";

	static void problems() throws Exception {

		// Duplicate names in different groups use the same column, the old loader created a broken table
		expectError("duplicate names across groups", HEAD
				+ "<instance><data><g1><a/></g1><g2><a/></g2></data></instance>"
				+ "</model></h:head><h:body><group ref=\"/data/g1\"><input ref=\"/data/g1/a\"><label>A</label></input></group>"
				+ "<group ref=\"/data/g2\"><input ref=\"/data/g2/a\"><label>A</label></input></group></h:body></h:html>",
				"already used");

		// A question named like a system column gets a column of its own
		Survey s = load("system column name", HEAD
				+ "<instance><data><_bad/><prikey/></data></instance>"
				+ "</model></h:head><h:body><input ref=\"/data/_bad\"><label>B</label></input>"
				+ "<input ref=\"/data/prikey\"><label>P</label></input></h:body></h:html>", null);
		if(s != null) {
			check("_bad column renamed", !"_bad".equals(q(s, "_bad").columnName));
			check("prikey column renamed", !"prikey".equals(q(s, "prikey").columnName));
		}

		// bind with ref instead of nodeset, and a body element that refers to its bind
		s = load("bind ref and body bind", HEAD
				+ "<instance><data><a/></data></instance><bind id=\"ba\" ref=\"/data/a\" type=\"int\"/>"
				+ "</model></h:head><h:body><input bind=\"ba\"><label>A</label></input></h:body></h:html>", null);
		if(s != null) {
			equal("bind ref type", q(s, "a").type, "int");
			equal("body bind label", q(s, "a").labels.get(0).text, "A");
		}

		// A label that refers to a node rather than itext, the old loader threw
		load("label ref to a node", HEAD
				+ "<instance><data><a/><b/></data></instance>"
				+ "</model></h:head><h:body><input ref=\"/data/a\"><label ref=\"/data/b\"/></input>"
				+ "<input ref=\"/data/b\"><label>B</label></input></h:body></h:html>", null);

		// A group without a ref and with a label, the old loader garbled it
		load("group without ref", HEAD
				+ "<instance><data><a/></data></instance>"
				+ "</model></h:head><h:body><group><label ref=\"/data/a\"/><input ref=\"/data/a\"><label>A</label></input></group></h:body></h:html>", null);

		// Choices from a csv file are not part of the XForm
		ArrayList<ApplicationWarning> warnings = new ArrayList<> ();
		s = load("external choices", HEAD
				+ "<instance><data><a/></data></instance><instance id=\"towns\" src=\"jr://file-csv/towns.csv\"/>"
				+ "</model></h:head><h:body><select1 ref=\"/data/a\"><label>A</label>"
				+ "<itemset nodeset=\"instance('towns')/root/item\"><value ref=\"name\"/><label ref=\"label\"/></itemset>"
				+ "</select1></h:body></h:html>", warnings);
		if(s != null) {
			check("external choices warning", warnings.size() == 1 && warnings.get(0).getMessage().contains("towns"));
			check("external choices empty list", s.surveyData.optionLists.get(q(s, "a").list_name) != null);
		}

		// A reference that is not to a question is left as it is with a warning
		warnings = new ArrayList<> ();
		s = load("unknown reference", HEAD
				+ "<instance><data><a/></data></instance><bind nodeset=\"/data/a\" type=\"string\" relevant=\"/other/x = 1\"/>"
				+ "</model></h:head><h:body><input ref=\"/data/a\"><label>A</label></input></h:body></h:html>", warnings);
		if(s != null) {
			equal("unknown reference kept", q(s, "a").relevant, "/other/x = 1");
			check("unknown reference warning", warnings.size() == 1);
		}

		expectError("no body", HEAD + "<instance><data><a/></data></instance></model></h:head></h:html>", "body");
		expectError("no instance", HEAD + "</model></h:head><h:body/></h:html>", "instance");
		expectError("doctype refused", "<?xml version=\"1.0\"?><!DOCTYPE x [<!ENTITY e \"x\">]>" + HEAD.substring(HEAD.indexOf("<h:html"))
				+ "<instance><data><a>&e;</a></data></instance></model></h:head><h:body/></h:html>", "DOCTYPE");
		expectError("bad question name", HEAD
				+ "<instance><data><caf\u00e9/></data></instance>"
				+ "</model></h:head><h:body><input ref=\"/data/caf\u00e9\"><label>A</label></input></h:body></h:html>", "invalid");
		expectError("missing question in expression", HEAD
				+ "<instance><data><a/></data></instance><bind nodeset=\"/data/a\" type=\"string\" relevant=\"${zz} = 1\"/>"
				+ "</model></h:head><h:body><input ref=\"/data/a\"><label>A</label></input></h:body></h:html>", "zz");
		// Text from the XForm is encoded in messages
		warnings = new ArrayList<> ();
		load("unknown type", HEAD
				+ "<instance><data><a/></data></instance><bind nodeset=\"/data/a\" type=\"x&lt;script&gt;\"/>"
				+ "</model></h:head><h:body><input ref=\"/data/a\"><label>A</label></input></h:body></h:html>", warnings);
		check("unknown type warning encoded", warnings.size() == 1 && warnings.get(0).getMessage().contains("&lt;script")
				&& !warnings.get(0).getMessage().contains("<script"));
	}

	static Survey load(String test, String xml, ArrayList<ApplicationWarning> warnings) {
		try {
			if(warnings == null) {
				warnings = new ArrayList<> ();
			}
			XFormTemplateUploadManager xum = new XFormTemplateUploadManager(localisation, warnings);
			Survey s = xum.getSurvey(1, new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), null, 1, null, null, false, 0);
			check(test, true);
			return s;
		} catch (Exception e) {
			check(test + ": " + e.getClass().getSimpleName() + " " + e.getMessage(), false);
			return null;
		}
	}

	static void expectError(String test, String xml, String contains) {
		try {
			XFormTemplateUploadManager xum = new XFormTemplateUploadManager(localisation, new ArrayList<> ());
			xum.getSurvey(1, new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), null, 1, null, null, false, 0);
			check(test + " (no error)", false);
		} catch (Exception e) {
			String msg = String.valueOf(e.getMessage());
			boolean ok = contains == null || msg.contains(contains);
			ok = ok && !(e instanceof NullPointerException) && !(e instanceof StringIndexOutOfBoundsException);
			check(test + " (" + e.getClass().getSimpleName() + ": " + msg + ")", ok);
		}
	}

	/*
	 * ------------------------------------------------------------------------------------
	 */
	static void keepSettings() throws Exception {

		// The survey being replaced
		Survey e = new Survey();
		e.surveyData.uk.key_policy = "merge";
		e.surveyData.uk.key = "${country}";
		e.surveyData.dataSurvey = false;
		e.surveyData.hideOnDevice = true;
		e.surveyData.timing_data = true;
		e.surveyData.audit_location_data = true;
		e.surveyData.instanceNameDefn = "old name";
		e.surveyData.surveyClass = "theme-grid";
		e.surveyData.publicLink = "link";
		e.surveyData.languages.add(new org.smap.sdal.model.Language(0, "language", "en", false));
		StyleList sl = new StyleList();
		e.surveyData.styleLists.put("s1", sl);
		Form ef = new Form("main", -1, -1);
		e.surveyData.forms.add(ef);
		Question eq = new Question();
		eq.name = "country";
		eq.type = "select1";
		eq.display_name = "Country of birth";
		eq.pii = "anonymise";
		eq.style_list = "s1";
		ef.questions.add(eq);
		Question en = new Question();
		en.name = "n";
		en.type = "int";
		en.display_name = "Visits";
		en.pii = "pii";
		ef.questions.add(en);
		Question sc = new Question();
		sc.name = "server_total";
		sc.type = "server_calculate";
		sc.columnName = "server_total";
		sc.server_calculation = new ServerCalculation();
		sc.labels = new ArrayList<> ();
		ef.questions.add(sc);
		OptionList eol = new OptionList();
		Option eo = new Option();
		eo.value = "au";
		eo.display_name = "Aus";
		eol.options.add(eo);
		e.surveyData.optionLists.put("countries", eol);
		Form erf = new Form("visits", 0, 0);
		erf.reference = false;
		erf.merge = true;
		e.surveyData.forms.add(erf);

		ArrayList<ApplicationWarning> warnings = new ArrayList<> ();
		XFormTemplateUploadManager xum = new XFormTemplateUploadManager(localisation, warnings);
		Survey s = xum.getSurvey(1, new FileInputStream(dir + "smap.xml"), "Visits", 1, new HashMap<> (), new HashMap<> (), true, 4);
		xum.keepExistingSettings(e);

		equal("version", s.surveyData.version, 5);
		equal("key policy kept", s.surveyData.uk.key_policy, "merge");
		equal("key kept", s.surveyData.uk.key, "${country}");
		check("data survey kept", !s.surveyData.dataSurvey);
		check("hide on device kept", s.surveyData.hideOnDevice);
		check("audit kept when the XForm has none", s.surveyData.timing_data && s.surveyData.audit_location_data);
		equal("instance name from XForm", s.surveyData.instanceNameDefn, "concat(${country}, ' ', ${city})");
		equal("style kept when the XForm has none", s.surveyData.surveyClass, "theme-grid");
		equal("public link kept", s.surveyData.publicLink, "link");
		equal("language code kept", s.surveyData.languages.get(0).code, "en");
		check("style lists kept", s.surveyData.styleLists.get("s1") != null);

		Question country = q(s, "country");
		equal("display name kept", country.display_name, "Country of birth");
		equal("pii from XForm", country.pii, "pii");
		equal("style kept", country.style_list, "s1");
		Question n = q(s, "n");
		equal("pii kept when the XForm has none", n.pii, "pii");
		Question kept = q(s, "server_total");
		check("server calculation kept", kept != null && kept.server_calculation != null);
		if(kept != null) {
			equal("server calculation labels", kept.labels.size(), s.surveyData.languages.size());
		}
		equal("choice display name kept", s.surveyData.optionLists.get("countries").options.get(0).display_name, "Aus");
		check("repeat form setting kept", s.surveyData.forms.get(1).merge);
	}

	/*
	 * ------------------------------------------------------------------------------------
	 * The XLSForm upload now uses the checks it shares with the XForm upload
	 */
	static void xlsChecks() throws Exception {
		xls("valid", new String[][] {{"type", "name", "label"}, {"text", "q1", "Q1"}, {"select_one yn", "q2", "Q2"}},
				new String[][] {{"list_name", "name", "label"}, {"yn", "y", "Yes"}, {"yn", "n", "No"}}, null);
		xls("duplicate", new String[][] {{"type", "name", "label"}, {"text", "q1", "Q1"}, {"text", "Q1", "Q1"}},
				null, "already used");
		xls("missing list", new String[][] {{"type", "name", "label"}, {"select_one zz", "q1", "Q1"}},
				null, "zz");
		xls("missing question", new String[][] {{"type", "name", "label", "relevant"}, {"text", "q1", "Q1", "${q9} = 1"}},
				null, "q9");
		xls("bad name", new String[][] {{"type", "name", "label"}, {"text", "1q", "Q1"}},
				null, "invalid");
		xls("unbalanced", new String[][] {{"type", "name", "label", "relevant"}, {"text", "q1", "Q1", ""}, {"text", "q2", "Q2", "(${q1} = 1"}},
				null, "Row 3");
	}

	static void xls(String test, String[][] survey, String[][] choices, String expectError) throws Exception {
		XSSFWorkbook wb = new XSSFWorkbook();
		fill(wb.createSheet("survey"), survey);
		if(choices != null) {
			fill(wb.createSheet("choices"), choices);
		}
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		wb.write(out);
		wb.close();
		InputStream in = new ByteArrayInputStream(out.toByteArray());
		try {
			XLSTemplateUploadManager tum = new XLSTemplateUploadManager(localisation, new ArrayList<> ());
			Survey s = tum.getSurvey(null, 1, "xlsx", in, "x", 1, null, null, false, 0);
			if(expectError == null) {
				check("xls " + test, s.surveyData.forms.get(0).questions.size() == survey.length - 1);
			} else {
				check("xls " + test + " (no error)", false);
			}
		} catch (Exception e) {
			String msg = String.valueOf(e.getMessage());
			check("xls " + test + " (" + msg + ")", expectError != null && msg.contains(expectError));
		}
	}

	static void fill(Sheet sheet, String[][] rows) {
		for(int i = 0; i < rows.length; i++) {
			Row r = sheet.createRow(i);
			for(int j = 0; j < rows[i].length; j++) {
				if(rows[i][j].length() > 0) {
					r.createCell(j).setCellValue(rows[i][j]);
				}
			}
		}
	}

	/*
	 * ------------------------------------------------------------------------------------
	 */
	static Question q(Survey s, String name) {
		for(Form f : s.surveyData.forms) {
			for(Question q : f.questions) {
				if(name.equals(q.name) && !"end group".equals(q.type)) {
					return q;
				}
			}
		}
		check("question " + name + " found", false);
		return new Question();
	}

	static String names(Form f) {
		StringBuilder sb = new StringBuilder();
		for(Question q : f.questions) {
			if(sb.length() > 0) {
				sb.append(',');
			}
			sb.append(q.name);
		}
		return sb.toString();
	}

	static String param(Question q, String k) {
		if(q.paramArray != null) {
			for(KeyValueSimp kv : q.paramArray) {
				if(kv.k.equals(k)) {
					return kv.v;
				}
			}
		}
		return null;
	}

	static boolean hasMeta(Survey s, String name, String sourceParam) {
		for(MetaItem mi : s.surveyData.meta) {
			if(mi.isPreload && name.equals(mi.name) && sourceParam.equals(mi.sourceParam)) {
				return true;
			}
		}
		return false;
	}

	static int countPreloads(Survey s) {
		int c = 0;
		for(MetaItem mi : s.surveyData.meta) {
			if(mi.isPreload) {
				c++;
			}
		}
		return c;
	}

	static void equal(String test, Object got, Object expected) {
		boolean ok = expected == null ? got == null : expected.equals(got);
		if(!ok) {
			System.out.println("  FAIL " + test + ": got [" + got + "] expected [" + expected + "]");
			failures++;
		} else {
			System.out.println("  ok   " + test);
		}
	}

	static void check(String test, boolean ok) {
		if(!ok) {
			System.out.println("  FAIL " + test);
			failures++;
		} else {
			System.out.println("  ok   " + test);
		}
	}
}
