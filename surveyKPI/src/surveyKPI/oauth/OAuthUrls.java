package surveyKPI.oauth;

import jakarta.servlet.http.HttpServletRequest;

/*
 * Where this server thinks it is.
 *
 * Every URL in the discovery documents, and the audience a token is bound to, has to agree with
 * what the client typed or the client will refuse the token as being for somebody else.  A Smap
 * server can answer to several host names, so the host is taken from the request rather than from
 * configuration, and https is assumed because Apache terminates TLS in front of Tomcat and the
 * request arriving over AJP looks like plain http from here.
 */
public class OAuthUrls {

	public static String base(HttpServletRequest request) {
		return "https://" + host(request);
	}

	/*
	 * The canonical URI of the MCP endpoint, which is what a client puts in the resource parameter
	 * and what a token is audience bound to.  No trailing slash, per RFC 8707.
	 */
	public static String canonicalResource(HttpServletRequest request) {
		return "https://" + host(request) + "/mcp";
	}

	/*
	 * A token is for this server or it is for nothing.  Compared case insensitively on scheme and
	 * host, and tolerant of a trailing slash, because those are the two ways a client can spell the
	 * same resource without meaning anything different by it.
	 */
	public static boolean isThisResource(HttpServletRequest request, String resource) {
		if(resource == null) {
			return false;
		}
		String normalised = resource.trim();
		while(normalised.endsWith("/")) {
			normalised = normalised.substring(0, normalised.length() - 1);
		}
		return normalised.equalsIgnoreCase(canonicalResource(request));
	}

	private static String host(HttpServletRequest request) {
		String host = request.getHeader("X-Forwarded-Host");
		if(host == null || host.trim().length() == 0) {
			host = request.getServerName();
		}
		// Only the first, if a chain of proxies has appended to it
		int comma = host.indexOf(',');
		if(comma > 0) {
			host = host.substring(0, comma);
		}
		return host.trim();
	}
}
