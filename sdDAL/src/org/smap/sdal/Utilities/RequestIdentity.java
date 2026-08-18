/*****************************************************************************

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

 ******************************************************************************/

package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;

import org.smap.sdal.managers.ApiTokenManager;
import org.smap.sdal.managers.PasswordMigrationManager;

/*
 * Who is making this request, resolved once.
 *
 * Smap authenticates callers three different ways and the code used to work that out
 * inline, over and over:
 *
 *    String user = request.getRemoteUser();
 *    if(user == null) { user = ...look the x-api-key header up in users...; }
 *    if(user == null) { throw new AuthorisationException("Unknown User"); }
 *
 * repeated in about fifteen places, each with its own idea of what to do when it fails.
 * Worse, several of those places then went back to calling request.getRemoteUser() for
 * the next lookup, which is null whenever the caller came in on a token - so the request
 * carried on under a different, empty identity and quietly lost its language and its
 * super user status.  It also defeated the request scoped cache in
 * GeneralUtilityMethods.getUserContext(), which keys on the ident.
 *
 * Resolve once, here, and pass the result down.  The answer is cached on the request, so
 * asking again costs nothing.
 */
public class RequestIdentity {

	private static Logger log = Logger.getLogger(RequestIdentity.class.getName());

	private static final String ATTRIBUTE = "org.smap.sdal.requestIdentity";

	public static final String TOKEN_HEADER = "x-api-key";

	public static final String SCOPE_API = ApiTokenManager.SCOPE_API;	// Server to server, the /api endpoints
	public static final String SCOPE_APP = ApiTokenManager.SCOPE_APP;	// fieldTask and other devices

	private static final ApiTokenManager tokenManager = new ApiTokenManager();
	private static final PasswordMigrationManager passwordMigration = new PasswordMigrationManager();

	public enum Source {
		APACHE,			// Apache authenticated the password and set REMOTE_USER
		TOKEN,			// x-api-key header
		DYNAMIC_KEY		// A time limited key in the URL, used by webform links and task assignments
	}

	public final String ident;
	public final Source source;
	public final String scope;			// Null unless source is TOKEN

	/*
	 * The URL key this identity was resolved from, so a cached answer is not reused for a
	 * request that presents a different one
	 */
	private final String dynamicKey;

	private RequestIdentity(String ident, Source source, String scope, String dynamicKey) {
		this.ident = ident;
		this.source = source;
		this.scope = scope;
		this.dynamicKey = dynamicKey;
	}

	/*
	 * True when the caller was authenticated by a key rather than a password.  Callers
	 * that grant a temporary user reduced rights use this instead of tracking a separate
	 * boolean alongside the ident.
	 */
	public boolean isKeyAuthenticated() {
		return source != Source.APACHE;
	}

	/*
	 * Resolve the caller, or return null if none of the three mechanisms identifies them.
	 *
	 * dynamicKey may be null for the endpoints that do not take one.  tokenScope names
	 * which kind of token is acceptable here; pass null to accept any.
	 */
	public static RequestIdentity resolve(Connection sd, HttpServletRequest request,
			String dynamicKey, String tokenScope) {

		/*
		 * Guard the cache on the key.  A service that takes a URL key must not be handed an
		 * answer worked out earlier in the same request without one - the key names who the
		 * link was issued to, and that is not necessarily whoever the browser is signed in as.
		 */
		if(request != null) {
			RequestIdentity cached = (RequestIdentity) request.getAttribute(ATTRIBUTE);
			if(cached != null && Objects.equals(cached.dynamicKey, dynamicKey)) {
				return cached;
			}
		}

		RequestIdentity identity = null;

		/*
		 * A key in the URL wins.  It names the user the link was issued to, which is the
		 * point of the link even when a different user happens to be logged in to the
		 * same browser.
		 */
		if(dynamicKey != null) {
			try {
				String ident = GeneralUtilityMethods.getDynamicUser(sd, dynamicKey);
				if(ident != null) {
					identity = new RequestIdentity(ident, Source.DYNAMIC_KEY, null, dynamicKey);
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Resolving dynamic user key", e);
			}
		}

		if(identity == null && request != null && request.getRemoteUser() != null) {
			identity = new RequestIdentity(request.getRemoteUser(), Source.APACHE, null, dynamicKey);

			/*
			 * A device request carries the password Apache just checked against the old
			 * MD5 digest.  If this user has no bcrypt yet, take it - it is the only way
			 * they will ever get one without being asked to reset, and the enumerators
			 * this affects are the least able to arrange that.  Costs nothing once the
			 * last account has moved across.
			 */
			passwordMigration.migrateIfNeeded(sd, request, identity.ident);
		}

		if(identity == null && request != null) {
			identity = fromToken(sd, request, tokenScope, dynamicKey);
		}

		if(identity != null && request != null) {
			request.setAttribute(ATTRIBUTE, identity);
		}

		return identity;
	}

	/*
	 * Identify the holder of the x-api-key header.
	 *
	 * The scope is not enforced.  A single key has served both the API and fieldTask since
	 * before scopes existed, and some integrations use one for both; refusing the wrong
	 * scope here would break them silently.  The scope is recorded on the token and
	 * reported in the console so that it can be tightened later.
	 */
	private static RequestIdentity fromToken(Connection sd, HttpServletRequest request,
			String tokenScope, String dynamicKey) {

		String value = request.getHeader(TOKEN_HEADER);
		if(value == null || value.trim().length() == 0) {
			return null;
		}

		if(!TokenThrottle.isPermitted(request)) {
			log.warning("Token attempts throttled for " + request.getRemoteAddr());
			return null;
		}

		try {
			ApiTokenManager.Resolved resolved = tokenManager.resolve(sd, value.trim());
			if(resolved == null) {
				TokenThrottle.failed(request);
				return null;
			}
			tokenManager.touch(sd, resolved.tokenId, request.getRemoteAddr());
			return new RequestIdentity(resolved.ident, Source.TOKEN, resolved.scope, dynamicKey);
		} catch (Exception e) {
			log.log(Level.SEVERE, "Resolving request token", e);
			return null;
		}
	}

	/*
	 * As resolve(), but refuses the request with a 401 when nobody can be identified.
	 * This is the one to use at a service entry point - it fails closed.
	 */
	public static RequestIdentity require(Connection sd, HttpServletRequest request,
			String dynamicKey, String tokenScope) {

		RequestIdentity identity = resolve(sd, request, dynamicKey, tokenScope);
		if(identity == null) {
			throw new AuthenticationException("Unknown User");
		}
		return identity;
	}

	/*
	 * Convenience for the many call sites that only want the ident
	 */
	public static String requireIdent(Connection sd, HttpServletRequest request,
			String dynamicKey, String tokenScope) {
		return require(sd, request, dynamicKey, tokenScope).ident;
	}

	/*
	 * The ident already resolved for this request, or REMOTE_USER if nothing has resolved
	 * one yet.  For code deep inside a request that needs the effective user and cannot
	 * easily be given it as a parameter.  Prefer passing the ident down.
	 */
	public static String effectiveIdent(HttpServletRequest request) {
		if(request == null) {
			return null;
		}
		RequestIdentity cached = (RequestIdentity) request.getAttribute(ATTRIBUTE);
		return cached != null ? cached.ident : request.getRemoteUser();
	}

	/*
	 * Discard the resolved identity.  Needed only by the services that deliberately act
	 * as a different user part way through a request.
	 */
	public static void clear(HttpServletRequest request) {
		if(request != null) {
			request.removeAttribute(ATTRIBUTE);
		}
	}
}
