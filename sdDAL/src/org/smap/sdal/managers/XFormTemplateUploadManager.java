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

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.ApplicationWarning;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.ManifestInfo;
import org.smap.sdal.model.MetaItem;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionList;
import org.smap.sdal.model.Question;
import org.smap.sdal.model.QuestionForm;
import org.smap.sdal.model.SetValue;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.SurveyDAO;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/*
 * Load a survey definition from an XForm
 *
 * The XForm is converted into the same survey model that an XLSForm upload creates, checked
 * with the same rules, and saved in the same way.  An XForm cannot hold many of the settings
 * that a Smap survey has, for example the key policy, roles, styles or the display names of
 * questions.  When an XForm replaces a survey those settings are kept from the survey being
 * replaced (keepExistingSettings).  That is unlike an XLSForm, which is a complete replacement.
 */
public class XFormTemplateUploadManager {

	/*
	 * How each field of the survey, form and question models is handled when an XForm replaces
	 * a survey.  The test in test/xform checks that every field is listed, so that a new setting
	 * cannot be added to the model without deciding whether an XForm replacement keeps it.
	 *
	 *  FROM_XFORM		The value comes from the XForm, or from the upload itself
	 *  XFORM_OR_KEPT	The value comes from the XForm if the XForm has it, else the existing value is kept
	 *  KEPT			The XForm cannot hold the value so the existing value is kept
	 *  KEPT_BY_IDENT	Stored against the survey ident or bundle, not written by the upload, so it stays as it is
	 *  NOT_WRITTEN		Not saved when a survey is written, or only used while it is being edited
	 */
	public static final List<String> SURVEY_FROM_XFORM = Arrays.asList(
			"id", "e_id", "o_id", "p_id", "ident", "displayName", "version", "loadedFromXLS",
			"deleted", "blocked", "forms", "optionLists", "languages", "meta", "manifest");
	public static final List<String> SURVEY_XFORM_OR_KEPT = Arrays.asList(
			"instanceNameDefn", "def_lang", "timing_data", "audit_location_data", "track_changes", "surveyClass");
	public static final List<String> SURVEY_KEPT = Arrays.asList(
			"task_file", "myReferenceData", "hideOnDevice", "searchLocalData", "dataSurvey", "oversightSurvey",
			"readOnlySurvey", "autoTranslate", "default_logo", "compress_pdf", "turnstile", "showFormIndex",
			"maxReferenceRecords", "pulldata", "uk", "publicLink", "groupSurveyIdent", "styleLists");
	public static final List<String> SURVEY_KEPT_BY_IDENT = Arrays.asList(
			"roles", "referenceFilters");
	public static final List<String> SURVEY_NOT_WRITTEN = Arrays.asList(
			"hasManifest", "serverCalculations", "surveyManifest", "filters", "changes", "instance",
			"pdfTemplateName", "basedOn", "created", "exclude_empty", "projectName", "groupSurveyDetails", "links");

	public static final List<String> QUESTION_FROM_XFORM = Arrays.asList(
			"name", "columnName", "type", "list_name", "defaultanswer", "setValues", "appearance", "paramArray",
			"choice_filter", "source", "source_param", "calculation", "constraint", "constraint_msg", "relevant",
			"visible", "readonly", "readonly_expression", "required", "required_expression", "autoplay",
			"accuracy", "intent", "labels", "repeatCount", "trigger", "dataType");
	public static final List<String> QUESTION_XFORM_OR_KEPT = Arrays.asList(
			"pii");
	public static final List<String> QUESTION_KEPT = Arrays.asList(
			"display_name", "compressed", "style_list", "flash", "server_calculation");
	public static final List<String> QUESTION_NOT_WRITTEN = Arrays.asList(
			"id", "fId", "formIndex", "childFormIndex", "text_id", "hint_id", "l_id", "seq", "sourceSeq",
			"sourceFormId", "sourceFormIndex", "app_choices", "parameters", "propertyType", "published",
			"soft_deleted", "inMeta", "width", "linked_target", "nodeset", "external_choices", "external_table",
			"isTableList", "style_id");

	public static final List<String> FORM_FROM_XFORM = Arrays.asList(
			"name", "parentFormIndex", "parentQuestionIndex", "questions");
	public static final List<String> FORM_KEPT = Arrays.asList(
			"referenceName", "reference", "merge", "replace", "append");
	public static final List<String> FORM_NOT_WRITTEN = Arrays.asList(
			"id", "parentform", "parentQuestion", "tableName", "generatedCalculations");

	/*
	 * Question types that are only held on the server, so they are not in an XForm.  When an
	 * XForm replaces a survey these questions are kept from the survey being replaced.
	 */
	private static final List<String> SERVER_ONLY_TYPES = Arrays.asList("server_calculate", "pdf_field");

	/*
	 * Attributes of a body element that are not parameters
	 */
	private static final HashSet<String> BODY_ATTRIBUTES = new HashSet<>(Arrays.asList(
			"ref", "bind", "appearance", "autoplay", "accuracyThreshold", "intent", "mediatype", "initial",
			"flash", "nodeset", "count", "noAddRemove"));

	private static final Pattern ITEXT = Pattern.compile("jr:itext\\(\\s*['\"]([^'\"]*)['\"]\\s*\\)");
	private static final Pattern INSTANCE = Pattern.compile("^\\s*instance\\(\\s*['\"]([^'\"]*)['\"]\\s*\\)(.*)$");

	/*
	 * Error messages for an XForm name the question rather than a row
	 */
	private class XFormErrorFormatter implements SurveyUploadValidator.ErrorFormatter {
		public ApplicationException error(String code, int location, String sheet, String p1, String p2, String p3) {
			String msg = localisation.getString(code);
			msg = msg.replace("%sheet", "XForm");
			msg = msg.replace("%row", "");
			if(p1 != null) {
				msg = msg.replace("%s1", GeneralUtilityMethods.getSafeText(p1, true));
			}
			msg = msg.replace("%s2", "XForm");
			if(p2 != null) {
				msg = msg.replace("%s3", GeneralUtilityMethods.getSafeText(p2, true));
			}
			if(p3 != null) {
				msg = msg.replace("%s4", GeneralUtilityMethods.getSafeText(p3, true));
			}
			if(location >= 0 && location < locations.size()) {
				msg = localisation.getString("tu_xf_loc").replace("%s1",
						GeneralUtilityMethods.getSafeText(locations.get(location), true)) + " " + msg;
			}
			return new ApplicationException(msg);
		}
	}

	/*
	 * A list of choices taken from inline items or a secondary instance
	 */
	private class ChoiceList {
		String name;
		ArrayList<Option> options = new ArrayList<> ();
	}

	private ResourceBundle localisation;
	private ArrayList<ApplicationWarning> warnings;
	private HashSet<String> warningText = new HashSet<> ();
	private SurveyUploadValidator v;

	private Survey survey;
	private HashMap<String, QuestionForm> questionNames;
	private HashMap<String, String> optionNames;
	private boolean merge;
	private int metaId = MetaItem.INITIAL_ID;

	// The parts of the XForm
	private String title;
	private Element primaryRoot;
	private String rootPath;
	private HashMap<String, Element> binds = new HashMap<> ();
	private HashMap<String, Element> bindsById = new HashMap<> ();
	private HashMap<String, Element> controls = new HashMap<> ();			// Body element for a path
	private HashMap<String, Element> repeatBodies = new HashMap<> ();		// Repeat element for a path
	private HashMap<String, Element> repeatGroups = new HashMap<> ();		// Group element around a repeat
	private HashMap<String, Element> secondaryInstances = new HashMap<> ();
	private HashSet<String> externalInstances = new HashSet<> ();
	private HashSet<String> countPaths = new HashSet<> ();				// Nodes that hold a repeat count
	private HashMap<String, String> pathToName = new HashMap<> ();
	private HashMap<String, ArrayList<String[]>> setValues = new HashMap<> ();	// path -> (event, value)
	private HashMap<String, String[]> triggers = new HashMap<> ();				// target path -> (source path, value)
	private HashMap<String, String> startGeopoints = new HashMap<> ();
	private HashMap<String, String> backgroundAudio = new HashMap<> ();		// path -> quality

	// itext: language -> text id -> form -> value element
	private LinkedHashMap<String, HashMap<String, HashMap<String, Element>>> itext = new LinkedHashMap<> ();
	private ArrayList<String> languageNames = new ArrayList<> ();
	private String defaultLanguage;
	private boolean useDefaultLanguage = false;

	private HashMap<String, ChoiceList> instanceLists = new HashMap<> ();
	private HashMap<String, String> listSignatures = new HashMap<> ();	// signature -> list name

	// What the XForm says about settings it can hold, used when it replaces a survey
	private boolean hasAudit = false;
	private boolean hasInstanceName = false;
	private boolean hasSurveyClass = false;
	private boolean hasDefaultLanguage = false;
	private HashSet<String> questionsWithPii = new HashSet<> ();

	// Locations are question paths, the index of a path is used as the location in messages
	private ArrayList<String> locations = new ArrayList<> ();
	private java.util.IdentityHashMap<Question, Integer> questionLocations = new java.util.IdentityHashMap<> ();

	public XFormTemplateUploadManager(ResourceBundle l, ArrayList<ApplicationWarning> w) {
		localisation = l;
		warnings = w;
		v = new SurveyUploadValidator(l, w, new XFormErrorFormatter());
	}

	/*
	 * Get a survey definition from an XForm
	 */
	public Survey getSurvey(
			int oId,
			InputStream inputStream,
			String displayName,
			int p_id,
			HashMap<String, QuestionForm> questionNames,
			HashMap<String, String> optionNames,
			boolean merge,
			int existingVersion) throws Exception {

		this.questionNames = questionNames;
		this.optionNames = optionNames;
		this.merge = merge;

		DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();
		dbf.setNamespaceAware(true);
		DocumentBuilder b = dbf.newDocumentBuilder();
		Document doc = b.parse(inputStream);
		Element html = doc.getDocumentElement();

		Element head = firstChild(html, "head");
		Element model = head == null ? null : firstChild(head, "model");
		if(model == null) {
			model = firstDescendant(html, "model");
		}
		if(model == null) {
			throw new ApplicationException(localisation.getString("tu_xf_nm"));
		}
		Element body = firstChild(html, "body");
		if(body == null) {
			throw new ApplicationException(localisation.getString("tu_xf_nb"));
		}
		if(head != null) {
			Element t = firstChild(head, "title");
			if(t != null) {
				title = t.getTextContent().trim();
			}
		}

		// Create survey and set defaults
		survey = new Survey();
		survey.surveyData.displayName = (displayName != null && displayName.trim().length() > 0) ? displayName : title;
		if(survey.surveyData.displayName == null || survey.surveyData.displayName.trim().length() == 0) {
			throw new ApplicationException(localisation.getString("tu_xf_name"));
		}
		survey.surveyData.o_id = oId;
		survey.surveyData.p_id = p_id;
		survey.surveyData.version = merge ? existingVersion + 1 : 1;
		survey.surveyData.loadedFromXLS = false;
		survey.surveyData.deleted = false;
		survey.surveyData.blocked = false;
		v.setSurvey(survey);
		survey.surveyData.meta.add(new MetaItem(metaId--, "string", "instanceID", null, "instanceid", null, false, null, null));
		survey.surveyData.meta.add(new MetaItem(metaId--, "string", "instanceName", null, "instancename", null, false, null, null));

		/*
		 * 1. The model
		 */
		readModel(model);
		String surveyClass = attr(body, "class");
		if(surveyClass != null && surveyClass.trim().length() > 0) {
			survey.surveyData.surveyClass = surveyClass.trim();
			hasSurveyClass = true;
		}

		/*
		 * 2. The body
		 */
		readBody(body, rootPath, null);

		/*
		 * 3. Languages
		 */
		setLanguages();

		/*
		 * 4. The forms and questions, from the primary instance
		 */
		Form f = new Form("main", -1, -1);
		survey.surveyData.forms.add(f);
		readForm(primaryRoot, rootPath, 0);
		if(f.questions.size() == 0) {
			throw new ApplicationException(localisation.getString("tu_nq"));
		}

		/*
		 * 5. Preloads that every survey has
		 */
		if(!hasMeta("start")) {
			survey.surveyData.meta.add(new MetaItem(metaId--, "dateTime", "_start", "start", "_start", "timestamp", true, "start", null));
		}
		if(!hasMeta("end")) {
			survey.surveyData.meta.add(new MetaItem(metaId--, "dateTime", "_end", "end", "_end", "timestamp", true, "end", null));
		}
		if(!hasMeta("deviceid")) {
			survey.surveyData.meta.add(new MetaItem(metaId--, "string", "_device", "deviceid", "_device", "property", true, "device", null));
		}

		/*
		 * 6. Check the survey using the same rules as an XLSForm
		 */
		for(MetaItem mi : survey.surveyData.meta) {
			v.addName(mi.name, location(rootPath + "/" + mi.name));
		}
		validateForm(0);
		v.validateSurvey();

		return survey;
	}

	/*
	 * Keep the settings of the survey being replaced that the XForm cannot hold, or that it
	 * leaves out.  Roles and reference filters are stored against the survey ident and bundle so
	 * they are kept by the replacement without being written again.
	 */
	public void keepExistingSettings(Survey existing) throws Exception {
		SurveyDAO n = survey.surveyData;
		SurveyDAO e = existing.surveyData;

		// Settings an XForm cannot hold
		for(String name : SURVEY_KEPT) {
			Field fld = SurveyDAO.class.getField(name);
			fld.set(n, fld.get(e));
		}
		if(n.styleLists == null) {
			n.styleLists = new HashMap<> ();
		}

		// Settings the XForm may have left out
		if(!hasAudit) {
			n.timing_data = e.timing_data;
			n.audit_location_data = e.audit_location_data;
			n.track_changes = e.track_changes;
		}
		if(!hasInstanceName) {
			n.instanceNameDefn = e.instanceNameDefn;
		}
		if(!hasSurveyClass) {
			n.surveyClass = e.surveyClass;
		}
		if(!hasDefaultLanguage && e.def_lang != null) {
			for(Language l : n.languages) {
				if(GeneralUtilityMethods.isSameLanguage(l.name, e.def_lang)) {
					n.def_lang = l.name;
					break;
				}
			}
		}

		// Language codes and direction
		for(Language l : n.languages) {
			for(Language el : e.languages) {
				if(el.name != null && el.name.equals(l.name)) {
					if(l.code == null) {
						l.code = el.code;
					}
					l.rtl = el.rtl;
					break;
				}
			}
		}

		// Preload display names
		for(MetaItem mi : n.meta) {
			if(e.meta != null) {
				for(MetaItem emi : e.meta) {
					if(emi.name != null && emi.name.equals(mi.name) && mi.display_name == null) {
						mi.display_name = emi.display_name;
					}
				}
			}
		}

		// Question, form and choice settings, matched by name
		HashMap<String, Question> existingQuestions = new HashMap<> ();
		HashMap<String, Form> existingForms = new HashMap<> ();
		for(Form ef : e.forms) {
			existingForms.put(ef.name, ef);
			for(Question eq : ef.questions) {
				if(eq.name != null) {
					existingQuestions.put(eq.name, eq);
				}
			}
		}
		HashSet<String> newNames = new HashSet<> ();
		for(Form f : n.forms) {
			Form ef = existingForms.get(f.name);
			if(ef != null) {
				for(String name : FORM_KEPT) {
					Field fld = Form.class.getField(name);
					fld.set(f, fld.get(ef));
				}
			}
			for(Question q : f.questions) {
				newNames.add(q.name);
				Question eq = existingQuestions.get(q.name);
				if(eq != null && eq.type != null && !eq.type.equals("end group")) {
					for(String name : QUESTION_KEPT) {
						Field fld = Question.class.getField(name);
						fld.set(q, fld.get(eq));
					}
					if(!questionsWithPii.contains(q.name)) {
						q.pii = eq.pii;
					}
					if(q.style_list != null && n.styleLists.get(q.style_list) == null) {
						q.style_list = null;
					}
				}
			}
		}

		// Questions that are only on the server, add them to the end of their form
		for(Form ef : e.forms) {
			Form f = null;
			for(Form nf : n.forms) {
				if(nf.name.equals(ef.name)) {
					f = nf;
					break;
				}
			}
			if(f == null) {
				continue;
			}
			for(Question eq : ef.questions) {
				if(eq.type != null && SERVER_ONLY_TYPES.contains(eq.type) && !newNames.contains(eq.name)) {
					Question q = new Question();
					for(String name : QUESTION_FROM_XFORM) {
						Field fld = Question.class.getField(name);
						fld.set(q, fld.get(eq));
					}
					for(String name : QUESTION_KEPT) {
						Field fld = Question.class.getField(name);
						fld.set(q, fld.get(eq));
					}
					q.pii = eq.pii;
					if(q.labels == null) {
						q.labels = new ArrayList<> ();
					}
					// One label for each language of the new survey
					while(q.labels.size() < n.languages.size()) {
						q.labels.add(new Label());
					}
					while(q.labels.size() > n.languages.size()) {
						q.labels.remove(q.labels.size() - 1);
					}
					if(q.style_list != null && n.styleLists.get(q.style_list) == null) {
						q.style_list = null;
					}
					f.questions.add(q);
				}
			}
		}

		// Choice display names
		for(String listName : n.optionLists.keySet()) {
			OptionList ol = n.optionLists.get(listName);
			OptionList eol = e.optionLists.get(listName);
			if(eol != null) {
				for(Option o : ol.options) {
					for(Option eo : eol.options) {
						if(eo.value != null && eo.value.equals(o.value)) {
							if(o.display_name == null) {
								o.display_name = eo.display_name;
							}
							break;
						}
					}
				}
			}
		}
	}

	/*
	 * ----------------------------------------------------------------------------------
	 * The model
	 */
	private void readModel(Element model) throws Exception {

		// Instances
		for(Element inst : children(model, "instance")) {
			String id = attr(inst, "id");
			if(primaryRoot == null && (id == null || id.trim().length() == 0)) {
				primaryRoot = firstElement(inst);
			} else if(id != null) {
				String src = attr(inst, "src");
				if(src != null && src.trim().length() > 0) {
					externalInstances.add(id);
				} else {
					secondaryInstances.put(id, inst);
				}
			}
		}
		if(primaryRoot == null) {
			// No instance without an id, use the first
			Element inst = firstChild(model, "instance");
			if(inst != null) {
				primaryRoot = firstElement(inst);
			}
		}
		if(primaryRoot == null) {
			throw new ApplicationException(localisation.getString("tu_xf_ni"));
		}
		rootPath = "/" + localName(primaryRoot);

		// Names of all the nodes in the primary instance, used to convert references to names
		addPaths(primaryRoot, rootPath);

		for(Element el : elementChildren(model)) {
			String ln = localName(el);
			if(ln.equals("bind")) {
				String path = attr(el, "nodeset");
				if(path == null) {
					path = attr(el, "ref");
				}
				if(path != null) {
					path = normalisePath(path, rootPath);
					Element existing = binds.get(path);
					if(existing != null) {
						// Merge binds for the same node
						NamedNodeMap atts = el.getAttributes();
						for(int i = 0; i < atts.getLength(); i++) {
							Node a = atts.item(i);
							existing.setAttributeNS(a.getNamespaceURI(), a.getNodeName(), a.getNodeValue());
						}
					} else {
						binds.put(path, el);
					}
					String id = attr(el, "id");
					if(id != null) {
						bindsById.put(id, binds.get(path));
					}
				}
			} else if(ln.equals("itext")) {
				readItext(el);
			} else if(ln.equals("setvalue") || ln.equals("setgeopoint") || ln.equals("recordaudio")) {
				readAction(el, null);
			} else if(ln.equals("instance") || ln.equals("submission")) {
				// Done or not needed
			} else {
				warn("tu_xf_ign", el.getNodeName(), null);
			}
		}
	}

	private void addPaths(Element node, String path) {
		for(Element c : elementChildren(node)) {
			String cpath = path + "/" + localName(c);
			if(pathToName.get(cpath) == null) {
				pathToName.put(cpath, localName(c));
			}
			addPaths(c, cpath);
		}
	}

	private void readItext(Element el) {
		for(Element tr : children(el, "translation")) {
			String lang = attr(tr, "lang");
			if(lang == null || lang.trim().length() == 0) {
				lang = "default";
			}
			// "English" and "English (en)" are one language, pyxform writes media with no language to the
			// default translation, so the two are merged
			// The merged language keeps a code from either, e.g. en from "English (en)"
			for(int i = 0; i < languageNames.size(); i++) {
				String l = languageNames.get(i);
				if(GeneralUtilityMethods.getLanguageName(l).equals(GeneralUtilityMethods.getLanguageName(lang))) {
					if(GeneralUtilityMethods.getLanguageCode(l) == null && GeneralUtilityMethods.getLanguageCode(lang) != null) {
						languageNames.set(i, lang);
						itext.put(lang, itext.remove(l));
						if(l.equals(defaultLanguage)) {
							defaultLanguage = lang;
						}
					} else {
						lang = l;
					}
					break;
				}
			}
			String def = attr(tr, "default");
			if(def != null && def.trim().startsWith("true")) {
				defaultLanguage = lang;
			}
			HashMap<String, HashMap<String, Element>> texts = itext.get(lang);
			if(texts == null) {
				texts = new HashMap<> ();
				itext.put(lang, texts);
				languageNames.add(lang);
			}
			for(Element text : children(tr, "text")) {
				String id = attr(text, "id");
				if(id == null) {
					continue;
				}
				HashMap<String, Element> forms = texts.get(id);
				if(forms == null) {
					forms = new HashMap<> ();
					texts.put(id, forms);
				}
				for(Element value : children(text, "value")) {
					String form = attr(value, "form");
					if(form == null || form.trim().length() == 0) {
						form = "none";
					}
					// A merged translation keeps the real value over pyxform's "-" placeholder
					Element existing = forms.get(form.trim());
					if(existing == null || "-".equals(existing.getTextContent().trim())) {
						forms.put(form.trim(), value);
					}
				}
			}
		}
	}

	/*
	 * setvalue, odk:setgeopoint and odk:recordaudio
	 * A setvalue inside a body control that runs when that question changes is a trigger
	 */
	private void readAction(Element el, String controlPath) {
		String ln = localName(el);
		String ref = attr(el, "ref");
		if(ref == null) {
			ref = attr(el, "bind") == null ? null : bindPath(attr(el, "bind"));
		}
		if(ref == null) {
			return;
		}
		String path = normalisePath(ref, controlPath == null ? rootPath : controlPath);
		String event = attr(el, "event");
		if(event == null) {
			event = SetValue.START;
		}

		if(ln.equals("setgeopoint")) {
			startGeopoints.put(path, event);
		} else if(ln.equals("recordaudio")) {
			String quality = attr(el, "quality");
			backgroundAudio.put(path, quality == null ? "" : quality);
		} else {
			String value = attr(el, "value");
			if(value == null) {
				value = el.getTextContent();
			}
			if(controlPath != null && event.contains(SetValue.TRIGGER)) {
				triggers.put(path, new String[] {controlPath, value});
			} else {
				ArrayList<String[]> list = setValues.get(path);
				if(list == null) {
					list = new ArrayList<> ();
					setValues.put(path, list);
				}
				list.add(new String[] {event, value});
			}
		}
	}

	/*
	 * ----------------------------------------------------------------------------------
	 * The body, record the body element for each path
	 */
	private void readBody(Element el, String context, Element enclosingGroup) {
		for(Element c : elementChildren(el)) {
			String ln = localName(c);
			if(ln.equals("group")) {
				String path = controlPath(c, context);
				if(path != null) {
					controls.put(path, c);
				}
				readBody(c, path == null ? context : path, c);
			} else if(ln.equals("repeat")) {
				String path = attr(c, "nodeset");
				if(path != null) {
					path = normalisePath(path, context);
					repeatBodies.put(path, c);
					if(enclosingGroup != null) {
						String gPath = controlPath(enclosingGroup, context);
						if(gPath == null || gPath.equals(path)) {
							repeatGroups.put(path, enclosingGroup);
						}
					}
					String count = attr(c, "count");
					if(count != null && count.trim().length() > 0) {
						String cp = normalisePath(count.trim(), path);
						if(pathToName.get(cp) != null) {
							countPaths.add(cp);
						}
					}
					readBody(c, path, null);
				}
			} else if(ln.equals("input") || ln.equals("select") || ln.equals("select1") || ln.equals("rank")
					|| ln.equals("upload") || ln.equals("trigger") || ln.equals("range")) {
				String path = controlPath(c, context);
				if(path != null) {
					controls.put(path, c);
					for(Element a : elementChildren(c)) {
						String aln = localName(a);
						if(aln.equals("setvalue") || aln.equals("setgeopoint") || aln.equals("recordaudio")) {
							readAction(a, path);
						}
					}
				}
			} else if(ln.equals("setvalue") || ln.equals("setgeopoint") || ln.equals("recordaudio")) {
				readAction(c, null);
			} else if(ln.equals("label") || ln.equals("hint")) {
				// Labels of groups, handled with the group
			} else {
				warn("tu_xf_ign", c.getNodeName(), null);
				readBody(c, context, enclosingGroup);
			}
		}
	}

	private String controlPath(Element c, String context) {
		String ref = attr(c, "ref");
		if(ref == null) {
			String bind = attr(c, "bind");
			if(bind != null) {
				return bindPath(bind);
			}
			return null;
		}
		return normalisePath(ref, context);
	}

	private String bindPath(String id) {
		Element b = bindsById.get(id);
		if(b != null) {
			String path = attr(b, "nodeset");
			if(path == null) {
				path = attr(b, "ref");
			}
			return path == null ? null : normalisePath(path, rootPath);
		}
		return null;
	}

	/*
	 * ----------------------------------------------------------------------------------
	 * Languages
	 */
	private void setLanguages() {
		if(languageNames.size() == 0) {
			survey.surveyData.languages.add(new Language(0, "language", null, false));
			useDefaultLanguage = true;
		} else {
			for(String lang : languageNames) {
				survey.surveyData.languages.add(new Language(0, GeneralUtilityMethods.getLanguageName(lang),
						GeneralUtilityMethods.getLanguageCode(lang),
						GeneralUtilityMethods.isRtl(lang)));
			}
			if(defaultLanguage != null) {
				survey.surveyData.def_lang = GeneralUtilityMethods.getLanguageName(defaultLanguage);
				hasDefaultLanguage = true;
			}
		}
	}

	/*
	 * ----------------------------------------------------------------------------------
	 * Forms and questions
	 */
	private void readForm(Element node, String path, int formIndex) throws Exception {

		Form f = survey.surveyData.forms.get(formIndex);
		HashSet<String> done = new HashSet<> ();

		for(Element c : elementChildren(node)) {
			String name = localName(c);
			String cpath = path + "/" + name;

			if(done.contains(cpath)) {
				continue;		// The real copy of a repeat that follows its template
			}
			done.add(cpath);

			if(formIndex == 0 && path.equals(rootPath) && name.equals("meta")) {
				readMeta(c, cpath);
				continue;
			}
			if(countPaths.contains(cpath) || name.endsWith("_table_list_labels")) {
				continue;
			}
			if(addPreload(c, cpath)) {
				continue;
			}

			boolean isRepeat = repeatBodies.get(cpath) != null || attr(c, "template") != null;
			if(isRepeat) {

				Question q = newQuestion(name, "begin repeat", cpath);
				Element repeat = repeatBodies.get(cpath);
				Element group = repeatGroups.get(cpath);
				if(group == null) {
					group = controls.get(cpath);
				}
				q.labels = getLabels(group, cpath, true);
				if(repeat != null) {
					q.appearance = clean(attr(repeat, "appearance"), cpath);
					String count = attr(repeat, "count");
					if(count != null && count.trim().length() > 0) {
						String cp = normalisePath(count.trim(), cpath);
						Element countBind = binds.get(cp);
						if(countPaths.contains(cp) && countBind != null && attr(countBind, "calculate") != null) {
							q.repeatCount = convertExpr(attr(countBind, "calculate"), cp, false, "repeat_count");
						} else {
							q.repeatCount = convertExpr(count, cpath, false, "repeat_count");
						}
					}
				}
				setBindProperties(q, cpath);
				q.source = null;
				q.visible = true;
				if(name.startsWith("geopolygon") || name.startsWith("geolinestring")) {
					warn("tu_xf_geo", name, null);
				}
				f.questions.add(q);

				Form subForm = new Form(name, formIndex, f.questions.size() - 1);
				survey.surveyData.forms.add(subForm);
				int subFormIndex = survey.surveyData.forms.size() - 1;
				readForm(c, cpath, subFormIndex);

			} else if(elementChildren(c).size() > 0) {

				// A group
				Question q = newQuestion(name, "begin group", cpath);
				Element group = controls.get(cpath);
				q.labels = getLabels(group, cpath, true);
				if(group != null) {
					q.appearance = clean(attr(group, "appearance"), cpath);
					q.intent = attr(group, "intent");
				}
				setBindProperties(q, cpath);
				q.source = null;
				q.visible = true;
				f.questions.add(q);

				readForm(c, cpath, formIndex);

				Question qe = newQuestion(name, "end group", cpath);
				qe.labels = emptyLabels();
				qe.source = null;
				qe.visible = false;
				f.questions.add(qe);

			} else {
				Question q = getQuestion(c, name, cpath);
				if(q != null) {
					f.questions.add(q);
				}
			}
		}
	}

	private void readMeta(Element meta, String path) throws Exception {
		for(Element c : elementChildren(meta)) {
			String name = localName(c);
			String cpath = path + "/" + name;
			Element bind = binds.get(cpath);
			if(name.equals("instanceID") || name.equals("deprecatedID")) {
				// Added by the server
			} else if(name.equals("instanceName")) {
				if(bind != null && attr(bind, "calculate") != null) {
					survey.surveyData.instanceNameDefn = convertExpr(attr(bind, "calculate"), cpath, false, "instance_name");
					hasInstanceName = true;
				}
			} else if(name.equals("audit")) {
				hasAudit = true;
				survey.surveyData.timing_data = true;
				if(bind != null) {
					survey.surveyData.audit_location_data = attr(bind, "location-priority") != null;
					String tc = attr(bind, "track-changes");
					survey.surveyData.track_changes = tc != null && tc.trim().startsWith("true");
				}
			} else if(!addPreload(c, cpath)) {
				warn("tu_xf_ign", "meta/" + name, null);
			}
		}
	}

	/*
	 * Add the node as a preload if it is one
	 */
	private boolean addPreload(Element c, String cpath) throws Exception {
		String name = localName(c);
		String type = null;
		String settings = "";
		ArrayList<KeyValueSimp> params = null;
		Element bind = binds.get(cpath);

		if(startGeopoints.get(cpath) != null) {
			type = "start-geopoint";
		} else if(backgroundAudio.get(cpath) != null) {
			type = "background-audio";
			String quality = backgroundAudio.get(cpath);
			if(quality != null && quality.length() > 0) {
				params = new ArrayList<> ();
				params.add(new KeyValueSimp("quality", quality));
			}
		} else if(bind != null && attr(bind, "preload") != null) {
			String preload = attr(bind, "preload").trim();
			String param = attr(bind, "preloadParams");
			param = param == null ? "" : param.trim();
			if(preload.equals("timestamp") && (param.equals("start") || param.equals("end"))) {
				type = param;
			} else if(preload.equals("date") && param.equals("today")) {
				type = "today";
			} else if(preload.equals("property") && (param.equals("deviceid") || param.equals("subscriberid")
					|| param.equals("simserial") || param.equals("phonenumber") || param.equals("username")
					|| param.equals("email"))) {
				type = param;
			} else if(preload.equals("uid")) {
				return true;		// instance id, added by the server
			} else {
				warn("tu_xf_ign", "jr:preload=\"" + preload + "\" " + name, null);
				return true;
			}
		}

		if(type == null) {
			return false;
		}

		MetaItem item = GeneralUtilityMethods.getPreloadItem(type, name, null, metaId, settings, params);
		if(item != null) {
			metaId--;
			survey.surveyData.meta.add(item);
			return true;
		}
		return false;
	}

	private boolean hasMeta(String sourceParam) {
		for(MetaItem mi : survey.surveyData.meta) {
			if(mi.isPreload && mi.sourceParam.equals(sourceParam)) {
				return true;
			}
		}
		return false;
	}

	private Question newQuestion(String name, String type, String path) {
		Question q = new Question();
		q.name = name;
		q.type = type;
		if(merge && questionNames != null) {
			QuestionForm qt = questionNames.get(name);
			if(qt != null) {
				q.columnName = qt.columnName;
			} else {
				q.columnName = GeneralUtilityMethods.cleanName(name, true, true, true);
			}
		} else {
			q.columnName = GeneralUtilityMethods.cleanName(name, true, true, true);
		}
		q.compressed = true;
		q.labels = new ArrayList<> ();
		questionLocations.put(q, location(path));
		return q;
	}

	/*
	 * A question that holds data
	 */
	private Question getQuestion(Element node, String name, String path) throws Exception {

		Element bind = binds.get(path);
		Element control = controls.get(path);
		String bindType = bind == null ? null : attr(bind, "type");
		if(bindType != null && bindType.indexOf(':') >= 0) {
			bindType = bindType.substring(bindType.indexOf(':') + 1);
		}
		String calculate = bind == null ? null : attr(bind, "calculate");

		String type = getType(control, bindType, calculate, bind, name);
		Question q = newQuestion(name, type, path);

		// Default
		String def = node.getTextContent();
		if(def != null && def.trim().length() > 0) {
			def = def.trim();
			if(def.startsWith("jr://")) {
				def = def.substring(def.lastIndexOf('/') + 1);
			}
			q.defaultanswer = def;
		}

		// Labels
		q.labels = getLabels(control, path, false);

		// Body attributes
		if(control != null) {
			q.appearance = clean(attr(control, "appearance"), path);
			q.autoplay = attr(control, "autoplay");
			q.accuracy = attr(control, "accuracyThreshold");
			q.intent = attr(control, "intent");
			String flash = attr(control, "flash");
			if(flash != null) {
				try {
					q.flash = Integer.valueOf(flash.trim());
				} catch (Exception e) {
					// ignore
				}
			}
			q.paramArray = new ArrayList<> ();
			NamedNodeMap atts = control.getAttributes();
			for(int i = 0; i < atts.getLength(); i++) {
				Node a = atts.item(i);
				String an = a.getLocalName() == null ? a.getNodeName() : a.getLocalName();
				String nodeName = a.getNodeName();
				if(nodeName.equals("xmlns") || nodeName.startsWith("xmlns:") || BODY_ATTRIBUTES.contains(an)) {
					continue;
				}
				q.paramArray.add(new KeyValueSimp(an, a.getNodeValue()));
			}
		}

		// Bind attributes
		setBindProperties(q, path);
		if(bind != null) {
			String pixels = attr(bind, "max-pixels");
			if(pixels != null) {
				addParam(q, "max-pixels", pixels);
			}
			String quality = attr(bind, "quality");
			if(quality != null && !(type.equals("audio") && quality.equals("normal"))) {
				addParam(q, "quality", quality);
			}
			String pii = attr(bind, "pii");
			if(pii != null) {
				q.pii = GeneralUtilityMethods.validPii(pii);
				questionsWithPii.add(name);
			}
			if(calculate != null && calculate.trim().length() > 0) {
				q.calculation = convertExpr(calculate, path, false, "calculation");
			}
		}

		// Set values
		ArrayList<String[]> svs = setValues.get(path);
		if(svs != null) {
			for(String[] sv : svs) {
				q.addSetValue(sv[0], convertExpr(sv[1], path, false, "default"), null);
			}
		}

		// Trigger
		String[] trigger = triggers.get(path);
		if(trigger != null) {
			String source = pathToName.get(trigger[0]);
			if(source != null) {
				q.trigger = "${" + source + "}";
				if(q.calculation == null) {
					q.calculation = convertExpr(trigger[1], path, false, "trigger");
				}
			}
		}

		// Choices
		if(type.startsWith("select") || type.equals("rank")) {
			setChoices(q, control, path);
		}

		// Derived values, as for an XLSForm
		if(type.equals("conversation")) {
			q.source = "sms";
		} else {
			q.source = "user";
		}
		q.visible = !type.equals("calculate");

		// Update the survey manifest if csv files are referenced from the appearance and/or the calculation
		ManifestInfo mi = GeneralUtilityMethods.addManifestFromAppearance(q.appearance, survey.surveyData.manifest);
		mi = GeneralUtilityMethods.addManifestFromCalculate(q.calculation, mi.manifest);
		survey.surveyData.manifest = mi.manifest;

		return q;
	}

	/*
	 * Get the Smap question type from the body element and the bind
	 */
	private String getType(Element control, String bindType, String calculate, Element bind, String name) {

		if(control == null) {
			return "calculate";
		}

		String ln = localName(control);
		String type = null;
		if(ln.equals("select1")) {
			type = "select1";
		} else if(ln.equals("select")) {
			type = "select";
		} else if(ln.equals("rank")) {
			type = "rank";
		} else if(ln.equals("trigger")) {
			type = "acknowledge";
		} else if(ln.equals("range")) {
			type = "range";
		} else if(ln.equals("upload")) {
			String mediatype = attr(control, "mediatype");
			mediatype = mediatype == null ? "" : mediatype.toLowerCase();
			if(mediatype.startsWith("image")) {
				type = "image";
			} else if(mediatype.startsWith("audio")) {
				type = "audio";
			} else if(mediatype.startsWith("video")) {
				type = "video";
			} else {
				type = "file";
			}
		} else {
			// input
			String bt = bindType == null ? "string" : bindType;
			String readonly = bind == null ? null : attr(bind, "readonly");
			String appearance = attr(control, "appearance");
			if(bt.equals("string")) {
				if(appearance != null && appearance.contains("conversation")) {
					type = "conversation";
				} else if(readonly != null && readonly.trim().equals("true()")) {
					type = "note";
				} else {
					type = "string";
				}
			} else if(bt.equals("int") || bt.equals("integer")) {
				type = "int";
			} else if(bt.equals("decimal") || bt.equals("date") || bt.equals("time") || bt.equals("dateTime")
					|| bt.equals("geopoint") || bt.equals("geotrace") || bt.equals("geoshape")
					|| bt.equals("geocompound") || bt.equals("barcode")) {
				type = bt;
			} else {
				warn("tu_xf_type", bt, name);
				type = "string";
			}
		}
		return type;
	}

	/*
	 * Properties held on the bind that apply to every question, including groups and repeats
	 */
	private void setBindProperties(Question q, String path) {
		Element bind = binds.get(path);
		if(bind == null) {
			return;
		}

		String relevant = attr(bind, "relevant");
		if(relevant != null && relevant.trim().length() > 0) {
			q.relevant = convertExpr(relevant, path, false, "relevant");
		}

		String constraint = attr(bind, "constraint");
		if(constraint != null && constraint.trim().length() > 0) {
			q.constraint = convertExpr(constraint, path, false, "constraint");
		}

		String required = attr(bind, "required");
		if(required != null) {
			required = required.trim();
			if(required.equals("true()")) {
				q.required = true;
			} else if(required.length() > 0 && !required.equals("false()")) {
				q.required = true;
				q.required_expression = convertExpr(required, path, false, "required");
			}
		}

		String readonly = attr(bind, "readonly");
		if(readonly != null && !q.type.equals("note")) {
			readonly = readonly.trim();
			if(readonly.equals("true()")) {
				q.readonly = true;
			} else if(readonly.length() > 0 && !readonly.equals("false()")) {
				q.readonly = true;
				q.readonly_expression = convertExpr(readonly, path, false, "readonly");
			}
		}

		// Messages
		String constraintMsg = attr(bind, "constraintMsg");
		if(constraintMsg != null) {
			Matcher m = ITEXT.matcher(constraintMsg);
			if(m.find()) {
				int idx = 0;
				for(String lang : itextLanguages()) {
					String msg = itextValue(lang, m.group(1), "none", path);
					if(idx < q.labels.size() && msg != null) {
						q.labels.get(idx).constraint_msg = msg;
					}
					idx++;
				}
			} else {
				q.constraint_msg = constraintMsg;
				for(Label l : q.labels) {
					l.constraint_msg = constraintMsg;
				}
			}
		}
		String requiredMsg = attr(bind, "requiredMsg");
		if(requiredMsg != null) {
			Matcher m = ITEXT.matcher(requiredMsg);
			int idx = 0;
			for(String lang : itextLanguages()) {
				String msg = m.find(0) ? itextValue(lang, m.group(1), "none", path) : requiredMsg;
				if(idx < q.labels.size() && msg != null) {
					q.labels.get(idx).required_msg = msg;
				}
				idx++;
			}
		}
	}

	/*
	 * ----------------------------------------------------------------------------------
	 * Labels
	 */
	private ArrayList<String> itextLanguages() {
		if(useDefaultLanguage) {
			ArrayList<String> l = new ArrayList<> ();
			l.add(null);
			return l;
		}
		return languageNames;
	}

	private ArrayList<Label> emptyLabels() {
		ArrayList<Label> labels = new ArrayList<> ();
		for(int i = 0; i < survey.surveyData.languages.size(); i++) {
			labels.add(new Label());
		}
		return labels;
	}

	private ArrayList<Label> getLabels(Element control, String path, boolean isGroup) {
		ArrayList<Label> labels = emptyLabels();
		if(control == null) {
			return labels;
		}

		Element labelEl = firstChild(control, "label");
		Element hintEl = firstChild(control, "hint");

		ArrayList<String> langs = itextLanguages();
		for(int i = 0; i < langs.size(); i++) {
			String lang = langs.get(i);
			Label l = labels.get(i);
			if(labelEl != null) {
				String id = itextId(labelEl);
				if(id != null) {
					l.text = itextValue(lang, id, "none", path);
					if(l.text == null) {
						l.text = itextValue(lang, id, "long", path);
					}
					l.image = media(itextValue(lang, id, "image", path));
					l.bigImage = media(itextValue(lang, id, "big-image", path));
					l.audio = media(itextValue(lang, id, "audio", path));
					l.video = media(itextValue(lang, id, "video", path));
				} else {
					l.text = valueText(labelEl, path);
				}
			}
			if(hintEl != null) {
				String id = itextId(hintEl);
				if(id != null) {
					l.hint = itextValue(lang, id, "none", path);
					l.guidance_hint = itextValue(lang, id, "guidance", path);
				} else {
					l.hint = valueText(hintEl, path);
				}
			}
		}

		// If any language has a value make sure every language does
		if(labels.size() > 1) {
			fillMissing(labels, "text");
			fillMissing(labels, "hint");
			fillMissing(labels, "guidance_hint");
		}
		return labels;
	}

	private void fillMissing(ArrayList<Label> labels, String field) {
		try {
			Field f = Label.class.getField(field);
			boolean has = false;
			for(Label l : labels) {
				if(f.get(l) != null) {
					has = true;
					break;
				}
			}
			if(has) {
				for(Label l : labels) {
					if(f.get(l) == null) {
						f.set(l, "-");
					}
				}
			}
		} catch (Exception e) {
			// Field names are constants
		}
	}

	private String itextId(Element el) {
		String ref = attr(el, "ref");
		if(ref != null) {
			Matcher m = ITEXT.matcher(ref);
			if(m.find()) {
				return m.group(1);
			}
		}
		return null;
	}

	private String itextValue(String lang, String id, String form, String context) {
		if(lang == null) {
			// No languages in the XForm, use the first translation if there is one
			if(itext.size() == 0) {
				return null;
			}
			lang = itext.keySet().iterator().next();
		}
		HashMap<String, HashMap<String, Element>> texts = itext.get(lang);
		if(texts == null) {
			return null;
		}
		HashMap<String, Element> forms = texts.get(id);
		if(forms == null) {
			return null;
		}
		Element value = forms.get(form);
		if(value == null) {
			return null;
		}
		return valueText(value, context);
	}

	/*
	 * The text of a label, an output element becomes a reference to the question
	 */
	private String valueText(Element el, String context) {
		StringBuilder sb = new StringBuilder();
		NodeList nl = el.getChildNodes();
		for(int i = 0; i < nl.getLength(); i++) {
			Node n = nl.item(i);
			if(n.getNodeType() == Node.TEXT_NODE || n.getNodeType() == Node.CDATA_SECTION_NODE) {
				sb.append(n.getNodeValue());
			} else if(n.getNodeType() == Node.ELEMENT_NODE) {
				Element c = (Element) n;
				if(localName(c).equals("output")) {
					String value = attr(c, "value");
					if(value == null) {
						value = attr(c, "ref");
					}
					if(value != null) {
						sb.append(convertExpr(value, context, false, "label"));
					}
				} else {
					sb.append(valueText(c, context));
				}
			}
		}
		String s = sb.toString().trim();
		return s.length() == 0 ? null : s;
	}

	private String media(String v) {
		if(v == null) {
			return null;
		}
		int idx = v.lastIndexOf('/');
		return idx >= 0 ? v.substring(idx + 1) : v;
	}

	/*
	 * ----------------------------------------------------------------------------------
	 * Choices
	 */
	private void setChoices(Question q, Element control, String path) throws Exception {

		Element itemset = firstChild(control, "itemset");
		if(itemset == null) {
			// Inline items
			ChoiceList cl = new ChoiceList();
			for(Element item : children(control, "item")) {
				Option o = new Option();
				Element valueEl = firstChild(item, "value");
				o.value = valueEl == null ? null : valueEl.getTextContent().trim();
				o.labels = getLabels(item, path, false);
				for(Label l : o.labels) {
					l.hint = null;
					l.guidance_hint = null;
				}
				cl.options.add(o);
			}
			q.list_name = addList(cl, q.name);
			return;
		}

		String nodeset = attr(itemset, "nodeset");
		if(nodeset == null) {
			nodeset = "";
		}
		nodeset = nodeset.trim();

		// randomize(nodeset[, seed])
		if(nodeset.startsWith("randomize(") && nodeset.endsWith(")")) {
			String inner = nodeset.substring("randomize(".length(), nodeset.length() - 1);
			int comma = topLevelComma(inner);
			addParam(q, "randomize", "true");
			if(comma > 0) {
				String seed = inner.substring(comma + 1).trim();
				addParam(q, "seed", convertExpr(seed, path, false, "seed"));
				inner = inner.substring(0, comma);
			}
			nodeset = inner.trim();
		}

		// Split off a predicate
		String predicate = null;
		int b = nodeset.indexOf('[');
		if(b > 0 && nodeset.endsWith("]")) {
			predicate = nodeset.substring(b + 1, nodeset.length() - 1);
			nodeset = nodeset.substring(0, b);
		}
		if(predicate != null) {
			q.choice_filter = convertExpr(predicate, path, true, "choice_filter");
		}

		Element valueEl = firstChild(itemset, "value");
		Element labelEl = firstChild(itemset, "label");
		String valueRef = valueEl == null ? "name" : attr(valueEl, "ref");
		String labelRef = labelEl == null ? "label" : attr(labelEl, "ref");
		valueRef = valueRef == null ? "name" : stripDot(valueRef);
		labelRef = labelRef == null ? "label" : labelRef.trim();

		Matcher im = INSTANCE.matcher(nodeset);
		if(im.find()) {
			String id = im.group(1);
			if(externalInstances.contains(id) || secondaryInstances.get(id) == null) {
				warn("tu_xf_ext", q.name, "instance('" + id + "')");
				q.list_name = addList(new ChoiceList(), q.name);
				return;
			}
			ChoiceList cl = instanceLists.get(id + "::" + valueRef + "::" + labelRef);
			if(cl == null) {
				cl = readInstanceList(id, valueRef, labelRef, path);
				cl.name = addList(cl, id);
				instanceLists.put(id + "::" + valueRef + "::" + labelRef, cl);
			}
			q.list_name = cl.name;
		} else {
			// Choices from the answers in a repeat
			String repeatPath = resolvePath(nodeset, path);
			String name = repeatPath == null ? null : pathToName.get(repeatPath + "/" + valueRef);
			if(name != null) {
				q.list_name = "${" + name + "}";
			} else {
				warn("tu_xf_ext", q.name, nodeset);
				q.list_name = addList(new ChoiceList(), q.name);
			}
		}
	}

	private ChoiceList readInstanceList(String id, String valueKey, String labelRef, String path) {
		ChoiceList cl = new ChoiceList();
		Element inst = secondaryInstances.get(id);
		Element root = firstElement(inst);
		if(root == null) {
			return cl;
		}
		Matcher lm = Pattern.compile("^jr:itext\\(\\s*([^)\\s]+)\\s*\\)$").matcher(labelRef);
		String itextKey = lm.find() ? stripDot(lm.group(1)) : null;
		String labelKey = itextKey == null ? stripDot(labelRef) : null;

		for(Element item : elementChildren(root)) {
			Option o = new Option();
			o.labels = emptyLabels();
			o.cascade_filters = new HashMap<String, String> ();
			for(Element c : elementChildren(item)) {
				String k = localName(c);
				String val = c.getTextContent();
				val = val == null ? null : val.trim();
				if(k.equals(valueKey)) {
					o.value = val;
				} else if(itextKey != null && k.equals(itextKey)) {
					ArrayList<String> langs = itextLanguages();
					for(int i = 0; i < langs.size(); i++) {
						Label l = o.labels.get(i);
						String lang = langs.get(i);
						l.text = itextValue(lang, val, "none", path);
						l.image = media(itextValue(lang, val, "image", path));
						l.bigImage = media(itextValue(lang, val, "big-image", path));
						l.audio = media(itextValue(lang, val, "audio", path));
						l.video = media(itextValue(lang, val, "video", path));
					}
				} else if(labelKey != null && k.equals(labelKey)) {
					for(Label l : o.labels) {
						l.text = val;
					}
				} else if(val != null && val.length() > 0) {
					o.cascade_filters.put(k, val);
				}
			}
			if(o.labels.size() > 1) {
				fillMissing(o.labels, "text");
			}
			cl.options.add(o);
		}
		return cl;
	}

	/*
	 * Add a list of choices to the survey, a list that is the same as one already added is used again
	 */
	private String addList(ChoiceList cl, String baseName) throws Exception {

		String signature = listSignature(cl);
		String existing = listSignatures.get(signature);
		if(existing != null && cl.options.size() > 0) {
			return existing;
		}

		String name = baseName;
		int i = 2;
		while(survey.surveyData.optionLists.get(name) != null) {
			name = baseName + "_" + i++;
		}

		OptionList ol = new OptionList();
		for(Option o : cl.options) {
			o.optionList = name;
			if(o.cascade_filters == null) {
				o.cascade_filters = new HashMap<String, String> ();
			}
			if(merge) {
				String n = optionNames == null ? null : optionNames.get(name + "__" + o.value);
				o.columnName = n != null ? n : GeneralUtilityMethods.cleanName(o.value, false, false, false);
			} else {
				o.columnName = GeneralUtilityMethods.cleanName(o.value, false, false, false);
			}
			o.published = false;
			v.validateOption(o, location("list " + name));
			ol.options.add(o);
		}
		survey.surveyData.optionLists.put(name, ol);
		listSignatures.put(signature, name);
		return name;
	}

	private String listSignature(ChoiceList cl) {
		StringBuilder sb = new StringBuilder();
		for(Option o : cl.options) {
			sb.append(o.value).append('\u0001');
			for(Label l : o.labels) {
				sb.append(l.text).append('\u0002').append(l.image).append('\u0002').append(l.bigImage)
					.append('\u0002').append(l.audio).append('\u0002').append(l.video).append('\u0003');
			}
			if(o.cascade_filters != null && o.cascade_filters.size() > 0) {
				sb.append(new java.util.TreeMap<String, String>(o.cascade_filters).toString());
			}
			sb.append('\u0004');
		}
		return sb.toString();
	}

	private int topLevelComma(String s) {
		int depth = 0;
		char quote = 0;
		for(int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if(quote != 0) {
				if(c == quote) {
					quote = 0;
				}
			} else if(c == '\'' || c == '"') {
				quote = c;
			} else if(c == '(' || c == '[') {
				depth++;
			} else if(c == ')' || c == ']') {
				depth--;
			} else if(c == ',' && depth == 0) {
				return i;
			}
		}
		return -1;
	}

	private void addParam(Question q, String k, String val) {
		if(q.paramArray == null) {
			q.paramArray = new ArrayList<> ();
		}
		for(KeyValueSimp kv : q.paramArray) {
			if(kv.k.equals(k)) {
				return;
			}
		}
		q.paramArray.add(new KeyValueSimp(k, val));
	}

	/*
	 * ----------------------------------------------------------------------------------
	 * Validation, in the order of the questions as they are in the XForm
	 */
	private void validateForm(int formIndex) throws Exception {
		Form f = survey.surveyData.forms.get(formIndex);
		int formLocation = -1;
		for(int i = 0; i < f.questions.size(); i++) {
			Question q = f.questions.get(i);
			Integer loc = questionLocations.get(q);
			int qLocation = loc == null ? -1 : loc;
			if(formLocation < 0) {
				formLocation = qLocation;
			}

			if(q.type.equals("begin group")) {
				v.getGroupStack(formIndex).push(q);
			} else if(q.type.equals("end group")) {
				Stack<Question> gs = v.getGroupStack(formIndex);
				if(!gs.isEmpty()) {
					gs.pop();
				}
			}
			v.validateQuestion(q, qLocation, formIndex);
			if(merge && questionNames != null) {
				v.validateTypeCompatible(q, questionNames, qLocation);
			}
			if(q.type.equals("begin repeat")) {
				for(int j = 0; j < survey.surveyData.forms.size(); j++) {
					Form sf = survey.surveyData.forms.get(j);
					if(sf.parentFormIndex == formIndex && sf.parentQuestionIndex == i) {
						validateForm(j);
						break;
					}
				}
			}
		}
		v.validateForm(formLocation, f);
	}

	private int location(String path) {
		int idx = locations.indexOf(path);
		if(idx < 0) {
			locations.add(path);
			idx = locations.size() - 1;
		}
		return idx;
	}

	/*
	 * ----------------------------------------------------------------------------------
	 * Expressions
	 *
	 * References to nodes, absolute or relative, become ${name} as in an XLSForm.  In a choice
	 * filter the context is the choice, so only references made through current() or an absolute
	 * path are to questions.
	 */
	String convertExpr(String in, String context, boolean itemContext, String what) {
		if(in == null) {
			return null;
		}
		StringBuilder out = new StringBuilder();
		int n = in.length();
		int i = 0;
		while(i < n) {
			char c = in.charAt(i);
			if(c == '\'' || c == '"') {
				int j = in.indexOf(c, i + 1);
				if(j < 0) {
					j = n - 1;
				}
				// A quoted absolute path to a question, as used by jr:choice-name()
				String quoted = in.substring(i + 1, Math.max(i + 1, j)).trim();
				String qName = quoted.startsWith("/") ? pathToName.get(normalisePath(quoted, "")) : null;
				if(qName != null) {
					out.append(c).append("${").append(qName).append("}").append(c);
				} else {
					out.append(in, i, j + 1);
				}
				i = j + 1;
				continue;
			}
			int end = pathEnd(in, i, itemContext);
			if(end > i) {
				String token = in.substring(i, end);
				String path = resolvePath(token, context);
				String name = path == null ? null : pathToName.get(path);
				if(name != null) {
					out.append("${").append(name).append("}");
				} else {
					out.append(token);
					warn("tu_xf_path", token, what);
				}
				i = end;
				continue;
			}
			out.append(c);
			i++;
		}
		return out.toString().trim();
	}

	/*
	 * If a path starts at position i return the position after it, else i
	 */
	private int pathEnd(String in, int i, boolean itemContext) {
		int n = in.length();
		char prev = 0;
		for(int k = i - 1; k >= 0; k--) {
			char pc = in.charAt(k);
			if(!Character.isWhitespace(pc)) {
				prev = pc;
				break;
			}
		}
		boolean isCurrent = in.startsWith("current()", i);
		if(!isCurrent && prev != 0 && (Character.isLetterOrDigit(prev) || prev == '_' || prev == '.' || prev == ')'
				|| prev == ']' || prev == '}' || prev == '-' || prev == ':' || prev == '@')) {
			// A step of a path that does not start here, or a path that follows a function such as instance()
			return i;
		}

		int start = i;
		if(isCurrent) {
			int j = i + "current()".length();
			while(j < n && Character.isWhitespace(in.charAt(j))) {
				j++;
			}
			if(j < n && in.charAt(j) == '/') {
				start = j;
			} else {
				return i;
			}
		} else if(in.charAt(i) == '/') {
			// absolute
		} else if(!itemContext && (in.startsWith("../", i) || in.startsWith("./", i))) {
			// relative
		} else {
			return i;
		}

		int j = start;
		while(j < n) {
			char c = in.charAt(j);
			if(Character.isLetterOrDigit(c) || c == '_' || c == '.' || c == '-' || c == ':' || c == '/') {
				j++;
			} else {
				break;
			}
		}
		// A path must have a name in it
		String token = in.substring(i, j);
		if(!token.matches(".*[A-Za-z_].*")) {
			return i;
		}
		if(token.endsWith("/")) {
			j--;
		}
		return j;
	}

	/*
	 * Resolve an absolute or relative reference against the context path
	 */
	String resolvePath(String token, String context) {
		token = token.trim();
		String base = context;
		if(token.startsWith("current()")) {
			token = token.substring("current()".length()).trim();
		}
		if(token.startsWith("/")) {
			base = "";
		}
		return normalisePath(token, base);
	}

	private String normalisePath(String path, String context) {
		if(path == null) {
			return null;
		}
		path = path.trim();
		String full = path.startsWith("/") ? path : (context == null ? "" : context) + "/" + path;
		ArrayList<String> steps = new ArrayList<> ();
		for(String step : full.split("/")) {
			step = step.trim();
			if(step.length() == 0 || step.equals(".")) {
				continue;
			} else if(step.equals("..")) {
				if(steps.size() > 0) {
					steps.remove(steps.size() - 1);
				}
			} else {
				int b = step.indexOf('[');
				if(b > 0) {
					step = step.substring(0, b);
				}
				int c = step.indexOf(':');
				if(c >= 0) {
					step = step.substring(c + 1);
				}
				steps.add(step);
			}
		}
		StringBuilder sb = new StringBuilder();
		for(String s : steps) {
			sb.append('/').append(s);
		}
		return sb.toString();
	}

	private String stripDot(String ref) {
		ref = ref.trim();
		if(ref.startsWith("./")) {
			ref = ref.substring(2);
		}
		return ref;
	}

	private String clean(String v, String context) {
		if(v == null) {
			return null;
		}
		v = v.trim();
		return v.length() == 0 ? null : convertExpr(v, context, false, "appearance");
	}

	private void warn(String code, String p1, String p2) {
		String msg = localisation.getString(code);
		if(p1 != null) {
			msg = msg.replace("%s1", GeneralUtilityMethods.getSafeText(p1, true));
		}
		if(p2 != null) {
			msg = msg.replace("%s2", GeneralUtilityMethods.getSafeText(p2, true));
		}
		if(!warningText.contains(msg)) {
			warningText.add(msg);
			warnings.add(new ApplicationWarning(msg));
		}
	}

	/*
	 * ----------------------------------------------------------------------------------
	 * DOM helpers.  Elements are matched on their local name so the prefixes used in the
	 * XForm do not matter
	 */
	private static String localName(Node n) {
		String ln = n.getLocalName();
		if(ln == null) {
			ln = n.getNodeName();
			int idx = ln.indexOf(':');
			if(idx >= 0) {
				ln = ln.substring(idx + 1);
			}
		}
		return ln;
	}

	private static String attr(Element e, String local) {
		if(e == null) {
			return null;
		}
		NamedNodeMap atts = e.getAttributes();
		for(int i = 0; i < atts.getLength(); i++) {
			Node a = atts.item(i);
			String nn = a.getNodeName();
			if(nn.equals("xmlns") || nn.startsWith("xmlns:")) {
				continue;
			}
			if(localName(a).equals(local)) {
				return a.getNodeValue();
			}
		}
		return null;
	}

	private static ArrayList<Element> elementChildren(Node n) {
		ArrayList<Element> list = new ArrayList<> ();
		if(n != null) {
			NodeList nl = n.getChildNodes();
			for(int i = 0; i < nl.getLength(); i++) {
				if(nl.item(i).getNodeType() == Node.ELEMENT_NODE) {
					list.add((Element) nl.item(i));
				}
			}
		}
		return list;
	}

	private static ArrayList<Element> children(Node n, String local) {
		ArrayList<Element> list = new ArrayList<> ();
		for(Element e : elementChildren(n)) {
			if(localName(e).equals(local)) {
				list.add(e);
			}
		}
		return list;
	}

	private static Element firstChild(Node n, String local) {
		for(Element e : elementChildren(n)) {
			if(localName(e).equals(local)) {
				return e;
			}
		}
		return null;
	}

	private static Element firstElement(Node n) {
		ArrayList<Element> l = elementChildren(n);
		return l.size() > 0 ? l.get(0) : null;
	}

	private static Element firstDescendant(Node n, String local) {
		for(Element e : elementChildren(n)) {
			if(localName(e).equals(local)) {
				return e;
			}
			Element d = firstDescendant(e, local);
			if(d != null) {
				return d;
			}
		}
		return null;
	}
}
