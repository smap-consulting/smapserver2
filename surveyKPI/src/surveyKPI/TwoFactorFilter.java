package surveyKPI;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.smap.sdal.Utilities.TwoFactorSession;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/*
 * Refuses console services to a user who has two factor authentication turned on but has
 * not yet supplied a code.
 *
 * This sits in front of everything rather than inside Authorise so that it fails closed: a
 * service added later is protected without anyone having to remember to protect it.  The
 * exemptions below are the services that either have to work before a code has been given,
 * or that authenticate some other way and never see a browser.
 *
 * A servlet filter rather than a Jersey provider, so that the request it inspects is
 * unambiguously the container's own - the whole control depends on getRemoteUser() and the
 * cookies being the real ones.
 *
 * Only the console is gated.  Password authenticated API and device access is unaffected.
 */
public class TwoFactorFilter implements Filter {

	/*
	 * First path segment of the services that are never gated
	 */
	private static final Set<String> EXEMPT_ROOTS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
			"authorise",		// Page level access check, only reports the caller's own groups
			"onetimelogon",		// Password reset, no session yet
			"register",			// Self registration, no user yet
			"subscriptions",	// Unsubscribe links, no user yet
			"action",			// Key authenticated, used by anonymous form links
			"myassignments"		// fieldTask, authenticated by Apache basic auth not a session
			)));

	/*
	 * Longer paths that are exempt.  Matched as prefixes, so sub paths are exempt too.
	 *
	 * The two factor services are listed one by one rather than exempting the whole path.
	 * twofactor/user/{ident} clears someone's two factor, and an administrator is allowed
	 * to name themselves - exempting it would let anyone holding an administrator's
	 * password switch the second factor off instead of passing it.  Of the rest, the
	 * challenge and removal need a current code anyway, and enrolment refuses to run for a
	 * user who already has two factor set up.
	 */
	private static final String[] EXEMPT_PATHS = {
			"twofactor/status",		// Is a code needed, and is enrolment half finished
			"twofactor/enrol",		// Refuses if two factor is already set up
			"twofactor/confirm",	// Needs a code
			"twofactor/verify",		// The challenge itself
			"twofactor/remove",		// Needs a code
			"utility/timezones",	// Static reference data, fetched before login completes
			"reports/view",			// Public report links
			"reports/oembed.json",	// Public report links
			"file/id",				// Key authenticated
			"log/key",				// Key authenticated
			"login/basic"			// Checks whether a basic password exists
	};

	private static final String BODY =
			"{\"code\": 403, \"twoFactorRequired\": true, "
			+ "\"message\": \"Two factor authentication code required\"}";

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
			throws IOException, ServletException {

		HttpServletRequest request = (HttpServletRequest) servletRequest;

		if(request.getRemoteUser() != null
				&& !isExempt(request.getMethod(), path(request))
				&& TwoFactorSession.isRequired(request)) {

			HttpServletResponse response = (HttpServletResponse) servletResponse;
			response.setStatus(HttpServletResponse.SC_FORBIDDEN);
			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			response.getWriter().write(BODY);
			return;
		}

		chain.doFilter(servletRequest, servletResponse);
	}

	private boolean isExempt(String method, String path) {

		/*
		 * GET /user is the call every console page makes on load, and is how the console
		 * finds out that a code is needed.  Only that one - the rest of /user includes
		 * saving the account and changing its password, which must stay behind the code.
		 */
		if("user".equals(path) && "GET".equals(method)) {
			return true;
		}

		int slash = path.indexOf('/');
		String root = slash < 0 ? path : path.substring(0, slash);
		if(EXEMPT_ROOTS.contains(root)) {
			return true;
		}

		for(String exempt : EXEMPT_PATHS) {
			if(path.equals(exempt) || path.startsWith(exempt + "/")) {
				return true;
			}
		}
		return false;
	}

	/*
	 * The service path with no leading slash, matching how the services name themselves -
	 * the filter is mapped on /rest/*, so pathInfo is everything after that
	 */
	private String path(HttpServletRequest request) {
		String p = request.getPathInfo();
		if(p == null) {
			return "";
		}
		while(p.startsWith("/")) {
			p = p.substring(1);
		}
		return p;
	}
}
