package surveyKPI.oauth;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.OAuthManager;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.model.OAuthClient;
import org.smap.sdal.model.ServerData;

/*
 * The authorization endpoint, and the consent screen that goes with it.
 *
 * This is the real gate.  Registration is open and a client id is not access; nothing happens until
 * a person who holds the mcp access group reads this page and approves it.  So the page is written
 * to be read rather than clicked past:
 *
 *  - it names the host the user will be sent back to, because that is the part an attacker cannot
 *    fake, whereas the application name is whatever the client wrote about itself
 *  - it says out loud when a client registered itself
 *  - it describes each permission in words, not scope strings
 *  - nothing is ticked that the client did not ask for, and there are no "grant everything" presets
 *
 * The endpoint sits behind Apache form authentication, so whoever is signed in to the console is
 * who consents.  That also means it works unchanged if a server later authenticates its console
 * against an external identity provider.
 */
@Path("/oauth/authorize")
public class OAuthAuthorize extends Application {

	private static Logger log = Logger.getLogger(OAuthAuthorize.class.getName());

	/*
	 * The consent form's CSRF token is signed rather than held in an HttpSession.
	 *
	 * A session would not survive the journey.  Tomcat scopes JSESSIONID to the webapp context
	 * path, /surveyKPI, but Apache presents this endpoint at /oauth/authorize, so the browser never
	 * sends the cookie back and every POST arrives sessionless.  Rewriting the cookie path in the
	 * vhost would work, but it makes the consent screen depend on a proxy directive that is easy to
	 * lose, for state that does not need to exist.
	 *
	 * Instead the token carries its own proof: an expiry and a MAC over the user, the client and
	 * that expiry.  Another site cannot forge one without the key, cannot reuse one issued to a
	 * different user or for a different client, and cannot use a stale one.  The key is per JVM, so
	 * a restart invalidates forms that are still open, which is what the expiry message already
	 * tells the user to do something about.
	 */
	private static final byte[] CSRF_KEY = new byte[32];
	static {
		new SecureRandom().nextBytes(CSRF_KEY);
	}
	private static final long CSRF_TTL_MS = 10 * 60 * 1000L;

	@GET
	@Produces(MediaType.TEXT_HTML)
	public Response authorize(@Context HttpServletRequest request,
			@QueryParam("response_type") String responseType,
			@QueryParam("client_id") String clientId,
			@QueryParam("redirect_uri") String redirectUri,
			@QueryParam("scope") String scope,
			@QueryParam("state") String state,
			@QueryParam("code_challenge") String codeChallenge,
			@QueryParam("code_challenge_method") String codeChallengeMethod,
			@QueryParam("resource") String resource) {

		String connectionString = "surveyKPI-OAuthAuthorize";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {
			ServerData server = new ServerManager().getServer(sd, null);
			if(!server.mcp_enabled) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}

			String user = request.getRemoteUser();
			if(user == null) {
				// Apache should have dealt with this; if it has not, do not fall through to consent
				return page("Not signed in", "<p>Please sign in and try again.</p>");
			}
			if(!GeneralUtilityMethods.hasSecurityGroup(sd, user, Authorise.MCP_ACCESS_ID)) {
				return page("MCP access not granted", "<p>Your account does not have MCP access. "
						+ "A server owner can grant it from user management.</p>");
			}

			OAuthClient client;
			try {
				client = new OAuthManager().resolveClient(sd, clientId, server.mcp_client_registration);
			} catch (ApplicationException e) {
				return page("This application cannot be authorised", "<p>" + esc(e.getMessage()) + "</p>");
			}

			/*
			 * Everything below is checked before anything is displayed, and a failure renders here
			 * rather than redirecting.  Redirecting an error to an address that has not been proved
			 * to belong to the client is how an open redirect starts.
			 */
			OAuthManager om = new OAuthManager();
			if(!om.isRegisteredRedirectUri(client, redirectUri)) {
				return page("This application cannot be authorised",
						"<p>The address it asked to be returned to is not one it has registered.</p>");
			}
			if(!"code".equals(responseType)) {
				return redirectError(redirectUri, state, "unsupported_response_type",
						"Only the authorization code flow is supported", request);
			}
			if(codeChallenge == null || !"S256".equals(codeChallengeMethod)) {
				return redirectError(redirectUri, state, "invalid_request",
						"PKCE with S256 is required", request);
			}
			if(!OAuthUrls.isThisResource(request, resource)) {
				return redirectError(redirectUri, state, "invalid_target",
						"The resource parameter must name this server's MCP endpoint", request);
			}

			List<String> requested = MCPScope.parse(scope);
			if(requested.isEmpty()) {
				requested = new ArrayList<>(MCPScope.SUPPORTED);
			}

			return page("Authorise " + esc(displayName(client)),
					consentBody(request, client, redirectUri, requested, scope, state, codeChallenge,
							codeChallengeMethod, resource, clientId));

		} catch (Exception e) {
			log.log(Level.SEVERE, "Authorize", e);
			return page("Something went wrong", "<p>The request could not be processed.</p>");
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.TEXT_HTML)
	public Response decide(@Context HttpServletRequest request, MultivaluedMap<String, String> form) {

		/*
		 * The whole form is taken as one entity rather than field by field.
		 *
		 * Reading some fields with @FormParam and the rest with request.getParameter() does not
		 * work: Jersey consumes the request body to populate the annotated parameters, and the
		 * servlet container then has nothing left to parse, so every getParameter() comes back
		 * null.  That is silent - the checkboxes simply read as unticked and the user is told they
		 * granted no permissions after they plainly ticked them.
		 */
		String clientId = form.getFirst("client_id");
		String redirectUri = form.getFirst("redirect_uri");
		String scope = form.getFirst("scope");
		String state = form.getFirst("state");
		String codeChallenge = form.getFirst("code_challenge");
		String codeChallengeMethod = form.getFirst("code_challenge_method");
		String resource = form.getFirst("resource");
		String csrf = form.getFirst("csrf");
		String approve = form.getFirst("approve");

		String connectionString = "surveyKPI-OAuthDecide";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {
			ServerData server = new ServerManager().getServer(sd, null);
			if(!server.mcp_enabled) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}

			String user = request.getRemoteUser();
			if(user == null) {
				return page("Not signed in", "<p>Please sign in and try again.</p>");
			}

			/*
			 * The consent form is a state changing POST inside an authenticated browser session, so
			 * without this a page on another site could submit it and collect the code.
			 */
			if(!checkCsrf(csrf, user, clientId)) {
				log.warning("Consent form rejected for " + user
						+ ", csrf " + (csrf == null ? "missing" : "not valid")
						+ ", client_id " + (clientId == null ? "missing" : "present"));
				return page("This form has expired", "<p>Please start the authorisation again.</p>");
			}

			if(!GeneralUtilityMethods.hasSecurityGroup(sd, user, Authorise.MCP_ACCESS_ID)) {
				return page("MCP access not granted", "<p>Your account does not have MCP access.</p>");
			}

			OAuthManager om = new OAuthManager();
			OAuthClient client = om.resolveClient(sd, clientId, server.mcp_client_registration);
			if(!om.isRegisteredRedirectUri(client, redirectUri)) {
				return page("This application cannot be authorised",
						"<p>The address it asked to be returned to is not one it has registered.</p>");
			}
			if(!OAuthUrls.isThisResource(request, resource)) {
				return redirectError(redirectUri, state, "invalid_target",
						"The resource parameter must name this server's MCP endpoint", request);
			}

			if(approve == null) {
				return redirectError(redirectUri, state, "access_denied",
						"The request was declined", request);
			}

			/*
			 * Only what the client asked for and the user left ticked.  Anything the user cleared
			 * simply is not granted; the client will ask again if it turns out to need it.
			 */
			List<String> granted = new ArrayList<>();
			for(String s : MCPScope.parse(scope)) {
				if(form.getFirst("scope_" + s.replace(':', '_')) != null) {
					granted.add(s);
				}
			}
			if(granted.isEmpty()) {
				return redirectError(redirectUri, state, "access_denied",
						"No permissions were granted", request);
			}

			int uId = GeneralUtilityMethods.getUserId(sd, user);
			int oId = GeneralUtilityMethods.getOrganisationId(sd, user);
			String grantedScope = MCPScope.join(granted);

			om.recordConsent(sd, uId, clientId, grantedScope);
			String code = om.issueCode(sd, clientId, uId, oId, grantedScope, resource, redirectUri,
					codeChallenge, codeChallengeMethod);

			log.info("MCP consent granted by " + user + " to " + clientId + " for " + grantedScope);

			StringBuilder url = new StringBuilder(redirectUri);
			url.append(redirectUri.contains("?") ? "&" : "?");
			url.append("code=").append(enc(code));
			if(state != null) {
				url.append("&state=").append(enc(state));
			}
			// RFC 9207, so the client can tell which authorization server answered
			url.append("&iss=").append(enc(OAuthUrls.base(request)));

			return Response.seeOther(java.net.URI.create(url.toString())).build();

		} catch (ApplicationException e) {
			return page("This application cannot be authorised", "<p>" + esc(e.getMessage()) + "</p>");
		} catch (Exception e) {
			log.log(Level.SEVERE, "Authorize decision", e);
			return page("Something went wrong", "<p>The request could not be processed.</p>");
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	/* ---------------------------------------------------------------- the page */

	private String consentBody(HttpServletRequest request, OAuthClient client, String redirectUri,
			List<String> requested, String rawScope, String state, String codeChallenge,
			String codeChallengeMethod, String resource, String clientId) {

		String csrf = newCsrf(request.getRemoteUser(), clientId);
		String host = hostOf(redirectUri);

		StringBuilder h = new StringBuilder();
		h.append("<p class=\"lead\">");
		h.append(esc(displayName(client)));
		h.append(" wants to use Smap as <strong>").append(esc(request.getRemoteUser())).append("</strong>.</p>");

		/*
		 * The return address, prominently.  A client picks its own name, so the name proves nothing;
		 * where the authorization code will be sent is the part that matters.
		 */
		h.append("<p class=\"where\">If you continue you will be sent to <strong>")
				.append(esc(host)).append("</strong>.</p>");

		if(client.isSelfRegistered()) {
			h.append("<p class=\"warn\">This application registered itself with your server. "
					+ "Anyone can do that, so only continue if you started this yourself.</p>");
		}

		h.append("<form method=\"post\">");
		h.append("<p>It is asking to:</p><ul class=\"scopes\">");
		for(String s : requested) {
			String id = "scope_" + s.replace(':', '_');
			h.append("<li><label><input type=\"checkbox\" name=\"").append(id)
					.append("\" id=\"").append(id).append("\" checked> ")
					.append(esc(describe(s)));
			if(MCPScope.isAlwaysReconsented(s)) {
				h.append(" <span class=\"warn\">You will be asked about this every time.</span>");
			}
			h.append("</label></li>");
		}
		h.append("</ul>");
		h.append("<p class=\"note\">It can never do more than your own account is allowed to do.</p>");

		h.append(hidden("client_id", clientId));
		h.append(hidden("redirect_uri", redirectUri));
		h.append(hidden("scope", MCPScope.join(requested)));
		h.append(hidden("state", state));
		h.append(hidden("code_challenge", codeChallenge));
		h.append(hidden("code_challenge_method", codeChallengeMethod));
		h.append(hidden("resource", resource));
		h.append(hidden("csrf", csrf));

		h.append("<div class=\"buttons\">");
		h.append("<button type=\"submit\" name=\"approve\" value=\"1\" class=\"primary\">Allow</button>");
		h.append("<button type=\"submit\" name=\"deny\" value=\"1\">Cancel</button>");
		h.append("</div></form>");

		return h.toString();
	}

	/*
	 * Plain language, because a scope string is not something a person should have to decode in
	 * order to decide whether to hand over their account.
	 */
	private String describe(String scope) {
		if(MCPScope.READ.equals(scope)) {
			return "Read your surveys, data, tasks and reports";
		}
		if(MCPScope.WRITE.equals(scope)) {
			return "Change surveys, data, tasks and cases, and prepare mailouts";
		}
		if(MCPScope.ADMIN.equals(scope)) {
			return "Manage users, projects and organisation details";
		}
		if(MCPScope.ACCESS.equals(scope)) {
			return "Change who can access what, including other people's permissions";
		}
		if(MCPScope.SERVER.equals(scope)) {
			return "Change server settings";
		}
		if(MCPScope.PRIVACY.equals(scope)) {
			return "Export personal data for subject access requests";
		}
		return scope;
	}

	/*
	 * A client chooses its own name, and a self registered one is not vouched for by anybody, so it
	 * is escaped and shown as a label rather than treated as a fact.  logo_uri is deliberately not
	 * rendered: it would let a stranger place an image of their choosing on an authenticated page
	 * of this server, and load it from an address that then learns who is looking.
	 */
	private String displayName(OAuthClient client) {
		if(client.client_name != null && client.client_name.trim().length() > 0) {
			return client.client_name.trim();
		}
		return client.client_id;
	}

	private String hostOf(String uri) {
		try {
			java.net.URI u = new java.net.URI(uri);
			return u.getHost() == null ? uri : u.getHost();
		} catch (Exception e) {
			return uri;
		}
	}

	private String newCsrf(String user, String clientId) {
		long expires = System.currentTimeMillis() + CSRF_TTL_MS;
		return expires + "." + mac(user, clientId, expires);
	}

	private boolean checkCsrf(String value, String user, String clientId) {

		if(value == null) {
			return false;
		}
		int dot = value.indexOf('.');
		if(dot < 1) {
			return false;
		}
		long expires;
		try {
			expires = Long.parseLong(value.substring(0, dot));
		} catch (NumberFormatException e) {
			return false;
		}
		if(expires < System.currentTimeMillis()) {
			return false;
		}

		String expected = mac(user, clientId, expires);
		// Constant time, so the comparison does not report how much of a guess was right
		return MessageDigest.isEqual(
				expected.getBytes(StandardCharsets.UTF_8),
				value.substring(dot + 1).getBytes(StandardCharsets.UTF_8));
	}

	private String mac(String user, String clientId, long expires) {
		try {
			Mac mac = Mac.getInstance("HmacSHA256");
			mac.init(new SecretKeySpec(CSRF_KEY, "HmacSHA256"));
			String message = (user == null ? "" : user) + "|"
					+ (clientId == null ? "" : clientId) + "|" + expires;
			return Base64.getUrlEncoder().withoutPadding()
					.encodeToString(mac.doFinal(message.getBytes(StandardCharsets.UTF_8)));
		} catch (Exception e) {
			// HmacSHA256 is required of every JVM, so this cannot happen
			throw new IllegalStateException("HmacSHA256 unavailable", e);
		}
	}

	private String hidden(String name, String value) {
		return value == null ? "" : "<input type=\"hidden\" name=\"" + esc(name)
				+ "\" value=\"" + esc(value) + "\">";
	}

	private Response redirectError(String redirectUri, String state, String code,
			String description, HttpServletRequest request) {
		try {
			StringBuilder url = new StringBuilder(redirectUri);
			url.append(redirectUri.contains("?") ? "&" : "?");
			url.append("error=").append(enc(code));
			url.append("&error_description=").append(enc(description));
			if(state != null) {
				url.append("&state=").append(enc(state));
			}
			// RFC 9207 asks for iss on error responses too
			url.append("&iss=").append(enc(OAuthUrls.base(request)));
			return Response.seeOther(java.net.URI.create(url.toString())).build();
		} catch (Exception e) {
			return page("This application cannot be authorised", "<p>" + esc(description) + "</p>");
		}
	}

	private Response page(String title, String body) {
		String html = "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">"
				+ "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
				+ "<title>" + esc(title) + "</title><style>"
				+ "body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;"
				+ "background:#f4f5f7;margin:0;padding:2rem;color:#1c1e21}"
				+ ".card{max-width:34rem;margin:2rem auto;background:#fff;border-radius:10px;"
				+ "padding:2rem;box-shadow:0 1px 3px rgba(0,0,0,.12)}"
				+ "h1{font-size:1.3rem;margin:0 0 1rem}"
				+ ".lead{font-size:1.05rem}"
				+ ".where{background:#eef3fb;border-left:3px solid #3d6fb5;padding:.7rem .9rem;border-radius:4px}"
				+ ".warn{color:#8a5300}"
				+ "ul.scopes{list-style:none;padding:0}"
				+ "ul.scopes li{padding:.45rem 0}"
				+ ".note{color:#555;font-size:.9rem}"
				+ ".buttons{margin-top:1.5rem;display:flex;gap:.75rem}"
				+ "button{font-size:1rem;padding:.6rem 1.4rem;border-radius:6px;border:1px solid #ccc;"
				+ "background:#fff;cursor:pointer}"
				+ "button.primary{background:#2f6fb5;border-color:#2f6fb5;color:#fff}"
				+ "</style></head><body><div class=\"card\"><h1>" + esc(title) + "</h1>"
				+ body + "</div></body></html>";
		return Response.ok(html).type(MediaType.TEXT_HTML).build();
	}

	/*
	 * Escape rather than sanitise.  Everything shown here is text supplied by a stranger and
	 * displayed as text; escaping is total, whereas a sanitiser is a filter that has to keep being
	 * right about what is safe to let through.
	 */
	private static String esc(String s) {
		if(s == null) {
			return "";
		}
		return s.replace("&", "&amp;")
				.replace("<", "&lt;")
				.replace(">", "&gt;")
				.replace("\"", "&quot;")
				.replace("'", "&#39;");
	}

	private static String enc(String s) throws UnsupportedEncodingException {
		return URLEncoder.encode(s == null ? "" : s, "UTF-8");
	}
}
