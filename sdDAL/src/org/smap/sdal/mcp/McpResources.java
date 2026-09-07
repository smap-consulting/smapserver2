package org.smap.sdal.mcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.smap.sdal.managers.DataManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.Survey;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * Things a client can read by name rather than by calling a tool.
 *
 * A tool is something the model decides to do; a resource is something the person, or the model,
 * decides to look at.  The distinction matters for survey definitions in particular: a client can
 * put one into its context once and refer back to it, instead of calling a tool every time it needs
 * to remember what a question was called.
 *
 * Access is established exactly as it is for tools, by looking for the survey in the caller's own
 * list.  A survey they could not list does not exist here either, so there is no second permission
 * path to keep in step with the first.
 */
public class McpResources {

	private static Logger log = Logger.getLogger(McpResources.class.getName());

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	public static final String SCHEME = "smap://";

	/*
	 * Held so the tool catalogue can be rendered from what is actually registered rather than from
	 * a description of it kept somewhere else
	 */
	private final McpToolRegistry registry;

	public McpResources(McpToolRegistry registry) {
		this.registry = registry;
	}

	/*
	 * What a read produced
	 */
	public static class Content {
		public final String uri;
		public final String mimeType;
		public final String text;

		Content(String uri, String mimeType, String text) {
			this.uri = uri;
			this.mimeType = mimeType;
			this.text = text;
		}

		public Map<String, Object> toMap() {
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("uri", uri);
			m.put("mimeType", mimeType);
			m.put("text", text);
			return m;
		}
	}

	/*
	 * Concrete resources.  Survey definitions are not listed one by one: a server can hold thousands
	 * and a client does not need them all in its context to ask for one.  They are reachable through
	 * the templates below, with completion to find an ident.
	 */
	public List<Map<String, Object>> list(McpToolContext ctx) {

		List<Map<String, Object>> resources = new ArrayList<>();
		resources.add(resource("smap://docs/tools", "Available tools",
				"What this connection can do, and what each tool is for", "text/markdown"));
		return resources;
	}

	public List<Map<String, Object>> templates() {

		List<Map<String, Object>> templates = new ArrayList<>();
		templates.add(template("smap://survey/{ident}/definition", "Survey definition",
				"The questions, options and settings of one survey, as JSON. The ident comes from "
				+ "survey_list.", "application/json"));
		templates.add(template("smap://record/{ident}/{instanceId}", "One submitted record",
				"A single record and its repeating groups, addressed by instance id.",
				"application/json"));
		return templates;
	}

	/*
	 * Argument completion, so a client can offer survey idents rather than making the model guess
	 * one and be told it does not exist.
	 */
	public List<String> complete(McpToolContext ctx, String argumentName, String prefix) {

		List<String> values = new ArrayList<>();
		if(!"ident".equals(argumentName)) {
			return values;
		}
		try {
			for(Survey s : userSurveys(ctx)) {
				if(prefix == null || prefix.isEmpty() || s.getIdent().startsWith(prefix)) {
					values.add(s.getIdent());
				}
				if(values.size() >= 100) {		// The specification caps a completion page at 100
					break;
				}
			}
		} catch (Exception e) {
			log.warning("Completing survey idents: " + e.getMessage());
		}
		return values;
	}

	public Content read(McpToolContext ctx, String uri) throws Exception {

		if(uri == null || !uri.startsWith(SCHEME)) {
			throw new IllegalArgumentException("Not a Smap resource: " + uri);
		}
		String[] parts = uri.substring(SCHEME.length()).split("/");

		if(parts.length == 2 && "docs".equals(parts[0])) {
			return docs(ctx, parts[1]);
		}
		if(parts.length == 3 && "survey".equals(parts[0]) && "definition".equals(parts[2])) {
			return surveyDefinition(ctx, parts[1]);
		}
		if(parts.length == 3 && "record".equals(parts[0])) {
			return record(ctx, parts[1], parts[2]);
		}
		throw new IllegalArgumentException("Unknown resource: " + uri);
	}

	/* ------------------------------------------------------------------ readers */

	private Content surveyDefinition(McpToolContext ctx, String ident) throws Exception {

		Survey listed = findSurvey(ctx, ident);
		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone);

		Survey survey = sm.getById(ctx.sd, ctx.cResults, ctx.user, false, listed.getId(),
				true,			// full definition
				null,			// basePath
				null,			// instanceId
				false,			// getResults
				false,			// generateDummyValues
				false,			// getPropertyTypeQuestions
				false,			// getSoftDeleted
				true,			// getHrk
				"real",			// external options if they exist
				false,			// getChangeHistory
				false,			// getRoles
				false,			// superUser - the caller's own rights, not an administrator's
				"geojson",
				false,			// referenceSurveys
				false,			// onlyGetLaunched
				false);			// mergeDefaultSetValue

		return new Content("smap://survey/" + ident + "/definition", "application/json",
				gson.toJson(survey));
	}

	private Content record(McpToolContext ctx, String ident, String instanceId) throws Exception {

		Survey survey = findSurvey(ctx, ident);

		DataManager dm = new DataManager(ctx.localisation, ctx.timezone);
		jakarta.ws.rs.core.Response response = dm.getRecordHierarchy(ctx.sd, ctx.cResults, ctx.user,
				survey.getIdent(), survey.getId(), instanceId, "no", ctx.localisation, ctx.timezone,
				false, null, null, false);

		Object entity = response.getEntity();
		return new Content("smap://record/" + ident + "/" + instanceId, "application/json",
				entity == null ? "[]" : entity.toString());
	}

	/*
	 * A description of what this connection can do, written for a model to read.  Generated from the
	 * registry rather than maintained by hand, so it cannot drift from what is actually offered, and
	 * filtered the same way tools/list is, so it describes this caller's server rather than someone
	 * else's.
	 */
	private Content docs(McpToolContext ctx, String topic) {

		StringBuilder md = new StringBuilder();
		md.append("# Smap tools available to you\n\n");
		md.append("Acting as **").append(ctx.user).append("**");
		md.append(" with permissions: ").append(ctx.scope).append("\n\n");

		for(McpTool tool : registry.visibleTo(ctx.sd, ctx)) {
			md.append("## ").append(tool.getName()).append("\n\n");
			md.append(tool.getDescription()).append("\n\n");
			if(tool.isMutating()) {
				md.append("Changes data. Reversed by: ").append(tool.getReversal()).append("\n\n");
			}
		}
		return new Content("smap://docs/" + topic, "text/markdown", md.toString());
	}

	/* ------------------------------------------------------------------ helpers */

	private Survey findSurvey(McpToolContext ctx, String ident) throws Exception {
		for(Survey s : userSurveys(ctx)) {
			if(ident.equals(s.getIdent())) {
				return s;
			}
		}
		throw new IllegalArgumentException("No such survey, or you do not have access to it");
	}

	private ArrayList<Survey> userSurveys(McpToolContext ctx) throws Exception {
		SurveyManager sm = new SurveyManager(ctx.localisation, ctx.timezone);
		return sm.getSurveys(ctx.sd, ctx.user, false, false, 0, false, false, false, false, false, null);
	}

	private Map<String, Object> resource(String uri, String name, String description, String mimeType) {
		Map<String, Object> r = new LinkedHashMap<>();
		r.put("uri", uri);
		r.put("name", name);
		r.put("description", description);
		r.put("mimeType", mimeType);
		return r;
	}

	private Map<String, Object> template(String uriTemplate, String name, String description,
			String mimeType) {
		Map<String, Object> t = new LinkedHashMap<>();
		t.put("uriTemplate", uriTemplate);
		t.put("name", name);
		t.put("description", description);
		t.put("mimeType", mimeType);
		return t;
	}
}
