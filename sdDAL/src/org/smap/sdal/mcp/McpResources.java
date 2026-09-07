package org.smap.sdal.mcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
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
	 * Base64 costs about a third again on top of the file, and the whole thing has to sit in one
	 * JSON-RPC response and then in a model's context. A survey photo is usually well under this;
	 * anything over it is reported with the URL to fetch instead.
	 */
	private static final long MAX_ATTACHMENT_BYTES = 5 * 1024 * 1024;

	/*
	 * How many surveys are worth naming one by one in resources/list.
	 *
	 * Below this the list is exhaustive and useful: a client shows the surveys by name, and nothing
	 * has to expand a template to find them.  Above it the same list stops being a menu and becomes
	 * noise - a model choosing from a thousand near-identical entries is worse off than one that
	 * calls survey_list and reads a filtered answer, and some clients put the whole list in front of
	 * the model, which at that size costs more context than the work does.
	 *
	 * So it is a clean cut rather than a truncated list.  A half list with no nextCursor would be a
	 * lie, and paginating a thousand entries is the case being avoided.  Above the threshold the
	 * templates and completion do the work, which is what they are for.
	 */
	private static final int MAX_LISTED_SURVEYS = 100;

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
		public final String blob;		// base64, for anything that is not text

		Content(String uri, String mimeType, String text) {
			this(uri, mimeType, text, null);
		}

		Content(String uri, String mimeType, String text, String blob) {
			this.uri = uri;
			this.mimeType = mimeType;
			this.text = text;
			this.blob = blob;
		}

		public Map<String, Object> toMap() {
			Map<String, Object> m = new LinkedHashMap<>();
			m.put("uri", uri);
			m.put("mimeType", mimeType);
			if(blob != null) {
				m.put("blob", blob);
			} else {
				m.put("text", text);
			}
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

		try {
			ArrayList<Survey> surveys = userSurveys(ctx);
			if(surveys.size() <= MAX_LISTED_SURVEYS) {
				for(Survey s : surveys) {
					resources.add(resource("smap://survey/" + s.getIdent() + "/definition",
							s.getDisplayName(),
							"The questions, options and settings of " + s.getDisplayName()
							+ " (project " + s.getProjectName() + ")",
							"application/json"));
				}
			} else {
				log.info("Not listing " + surveys.size() + " surveys as resources for " + ctx.user
						+ "; over the threshold, so the template and completion are the way in");
			}
		} catch (Exception e) {
			// A resource list that cannot be built is an empty one, not a failed request
			log.warning("Listing survey resources for " + ctx.user + ": " + e.getMessage());
		}

		return resources;
	}

	/*
	 * Whether this caller's surveys are named individually in resources/list
	 */
	public boolean listsSurveys(McpToolContext ctx) {
		try {
			return userSurveys(ctx).size() <= MAX_LISTED_SURVEYS;
		} catch (Exception e) {
			return false;
		}
	}

	public List<Map<String, Object>> templates() {

		List<Map<String, Object>> templates = new ArrayList<>();
		templates.add(template("smap://survey/{ident}/definition", "Survey definition",
				"The questions, options and settings of one survey, as JSON. The ident comes from "
				+ "survey_list.", "application/json"));
		templates.add(template("smap://record/{ident}/{instanceId}", "One submitted record",
				"A single record and its repeating groups, addressed by instance id.",
				"application/json"));
		templates.add(template("smap://attachment/{ident}/{file}",
				"An attachment on a record",
				"A photo, audio or other file submitted with a record. Take the part of the "
				+ "attachment URL in survey data after /attachments/ and put smap://attachment/ "
				+ "in front of it.", null));
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
		if(parts.length >= 3 && "attachment".equals(parts[0])) {
			String file = String.join("/", Arrays.copyOfRange(parts, 2, parts.length));
			return attachment(ctx, parts[1], file);
		}
		throw new IllegalArgumentException("Not a resource this server serves: " + uri
				+ ". Expected smap://docs/tools, smap://survey/{ident}/definition, "
				+ "smap://record/{ident}/{instanceId} or smap://attachment/{ident}/{file}");
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
				false,
				org.smap.sdal.Utilities.GeneralUtilityMethods.getUrlPrefix(ctx.request),
				org.smap.sdal.Utilities.GeneralUtilityMethods.getAttachmentPrefix(ctx.request, false),
				false);

		Object entity = response.getEntity();
		return new Content("smap://record/" + ident + "/" + instanceId, "application/json",
				entity == null ? "[]" : entity.toString());
	}

	/*
	 * One attachment, as bytes the client can actually look at.
	 *
	 * The alternative, which is what the data tool returns, is a URL under /app/attachments. That is
	 * behind form authentication, so a client holding a bearer token cannot fetch it: it can see
	 * that a record has a photo and never see the photo. Serving it here, through the endpoint the
	 * client is already authenticated to, is the only way a model gets to look at one.
	 *
	 * Addressed the way the data presents it. Attachments are stored at attachments/<ident>/<file>,
	 * and that is what appears in the URL survey data returns, so the resource uri is the same thing
	 * with smap://attachment/ in front. An earlier version asked for the instance id and the
	 * question name instead, which was a worse fit for what a model has in its hands after reading
	 * data - it holds the URL, not the column the answer went into - and it collided with the
	 * thumbs subdirectory.
	 *
	 * The survey ident is the first segment, which is what authorises the read: the survey has to be
	 * one this caller could have listed. That makes this stricter than /app/attachments, which
	 * authenticates a caller but does not check they may see the survey the file belongs to.
	 */
	private Content attachment(McpToolContext ctx, String ident, String file) throws Exception {

		findSurvey(ctx, ident);		// Throws unless the caller can see this survey

		/*
		 * The file part comes from the caller, so it is checked rather than trusted. The character
		 * set excludes a backslash and the segment check excludes any way back up the tree; the
		 * canonical path is then confirmed to be inside this survey's own directory, so even a
		 * traversal that got past both would land nowhere useful.
		 */
		if(file.isEmpty() || !file.matches("[A-Za-z0-9._/-]{1,255}")) {
			throw new IllegalArgumentException("Not an attachment name: " + file);
		}
		for(String segment : file.split("/")) {
			if(segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
				throw new IllegalArgumentException("Not an attachment name: " + file);
			}
		}

		java.io.File base = new java.io.File(GeneralUtilityMethods.getBasePath(ctx.request));
		java.io.File surveyDir = new java.io.File(base, "attachments/" + ident);
		java.io.File target = new java.io.File(surveyDir, file);

		if(!target.getCanonicalPath().startsWith(surveyDir.getCanonicalPath() + java.io.File.separator)) {
			log.warning("Attachment path escaped its survey directory: " + ident + "/" + file);
			throw new IllegalArgumentException("That attachment cannot be read");
		}
		if(!target.exists() || !target.isFile()) {
			throw new IllegalArgumentException("No such attachment: " + file);
		}
		if(target.length() > MAX_ATTACHMENT_BYTES) {
			throw new IllegalArgumentException("That attachment is "
					+ (target.length() / (1024 * 1024)) + "MB, too large to return. Fetch it from "
					+ GeneralUtilityMethods.getAttachmentPrefix(ctx.request, false)
					+ "attachments/" + ident + "/" + file);
		}

		byte[] bytes = java.nio.file.Files.readAllBytes(target.toPath());
		return new Content("smap://attachment/" + ident + "/" + file, mimeType(file), null,
				java.util.Base64.getEncoder().encodeToString(bytes));
	}

	/*
	 * From the file name. The stored value is what the device uploaded, so the extension is the
	 * only thing there is to go on, and an unknown one is reported as bytes rather than guessed at.
	 */
	private String mimeType(String path) {
		String lower = path.toLowerCase();
		if(lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
		if(lower.endsWith(".png")) return "image/png";
		if(lower.endsWith(".gif")) return "image/gif";
		if(lower.endsWith(".webp")) return "image/webp";
		if(lower.endsWith(".mp4")) return "video/mp4";
		if(lower.endsWith(".mp3")) return "audio/mpeg";
		if(lower.endsWith(".m4a")) return "audio/mp4";
		if(lower.endsWith(".3gp")) return "audio/3gpp";
		if(lower.endsWith(".pdf")) return "application/pdf";
		if(lower.endsWith(".txt")) return "text/plain";
		return "application/octet-stream";
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

		if(listsSurveys(ctx)) {
			md.append("Your surveys are listed individually as resources, so you can read a "
					+ "definition without looking its ident up first.\n\n");
		} else {
			md.append("You have more surveys than are worth listing one by one, so they are not "
					+ "named in the resource list. Use `survey_list` to find one, then read "
					+ "`smap://survey/{ident}/definition`.\n\n");
		}

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
