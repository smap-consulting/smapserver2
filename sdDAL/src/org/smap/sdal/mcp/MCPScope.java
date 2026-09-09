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

package org.smap.sdal.mcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/*
 * The scopes an MCP token can carry.
 *
 * These attenuate what the user can already do; they never add to it.  Effective permission is
 * always the user's security groups intersected with the token's scopes, so a token naming
 * SMAP_ADMIN in the hands of an enumerator grants nothing.
 *
 * They are deliberately not hierarchical.  A broader scope does not imply a narrower one, so
 * enforcement stays a set membership test and there is no implication chain to get wrong.
 *
 * A tool needing more than the token carries answers 403 with an insufficient_scope challenge
 * naming what is required; the client unions that with what it holds and re-authorises.  See
 * ADVERTISED for what a client is told it may ask for, which is a different question from what it
 * asks for first.
 */
public class MCPScope {

	public static final String READ = "smap:read";			// Everything the user can see, read only
	public static final String WRITE = "smap:write";		// Surveys, data, tasks, cases, notifications
	public static final String ADMIN = "smap:admin";		// Users, projects, organisations as objects
	public static final String ACCESS = "smap:access";		// Who can reach what.  Never remembered
	public static final String SERVER = "smap:server";		// Server settings
	public static final String PRIVACY = "smap:privacy";	// DSAR export

	private static final List<String> ALL = Arrays.asList(READ, WRITE, ADMIN, ACCESS, SERVER, PRIVACY);

	/*
	 * What a client is granted when it has nothing else to go on: an authorisation request naming
	 * no scope, or a console minted token with none chosen.  The minimal set that makes the server
	 * useful.
	 */
	public static final List<String> SUPPORTED = Arrays.asList(READ);

	/*
	 * What a client is told it may ask for, in the protected resource metadata.
	 *
	 * This has to name every scope a tool can challenge for.  A 403 naming smap:write sends the
	 * client to that document to learn how to ask for it (RFC 9728); if the scope is not listed
	 * there the client asks again for what it already held, is refused again, and gives up.
	 * Advertising READ alone made every write tool unreachable from any client that discovers
	 * scopes rather than being handed an authorisation URL by hand - the challenge named a
	 * permission the metadata said could not be requested.
	 *
	 * Least privilege still holds, in the two places it can be enforced rather than assumed: the
	 * consent form lists each scope with its own checkbox, so a person grants only what they mean
	 * to, and the token is intersected with the user's groups on every call.
	 *
	 * Scopes are added here as their increments land, so a client is never offered a permission no
	 * tool yet uses.
	 */
	public static final List<String> ADVERTISED = Arrays.asList(READ, WRITE);

	/*
	 * Consent for this scope is never remembered, so granting it is always a deliberate act rather
	 * than something a returning client picks up silently.
	 */
	public static boolean isAlwaysReconsented(String scope) {
		return ACCESS.equals(scope);
	}

	public static boolean isKnown(String scope) {
		return ALL.contains(scope);
	}

	public static List<String> all() {
		return ALL;
	}

	/*
	 * Split a space delimited scope string, dropping anything unrecognised.  An unknown scope is
	 * ignored rather than refused: a client that asks for something we have never heard of should
	 * get what it can legitimately have, not an error.
	 */
	public static List<String> parse(String scope) {
		List<String> out = new ArrayList<>();
		if(scope != null) {
			for(String s : scope.trim().split("\\s+")) {
				if(isKnown(s) && !out.contains(s)) {
					out.add(s);
				}
			}
		}
		return out;
	}

	public static String join(List<String> scopes) {
		return scopes == null ? "" : String.join(" ", scopes);
	}

	/*
	 * The union of two scope strings, used when a client steps up: it must keep what it already had
	 * or an upgrade for one operation would silently break another.
	 */
	public static String union(String a, String b) {
		Set<String> merged = new LinkedHashSet<>(parse(a));
		merged.addAll(parse(b));
		return String.join(" ", merged);
	}

	public static boolean has(String tokenScope, String required) {
		return required == null || parse(tokenScope).contains(required);
	}
}
