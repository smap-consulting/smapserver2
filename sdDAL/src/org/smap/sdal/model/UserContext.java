package org.smap.sdal.model;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/*
 * The facts about the requesting user that nearly every service needs: identity,
 * organisation, language and security groups.  These used to be fetched one query
 * each - GeneralUtilityMethods.getUserContext() loads them together and caches the
 * result for the life of the request.
 *
 * Immutable, so it can be handed around without any risk of a service changing what
 * a later authorisation check sees.
 */
public class UserContext {

	public final int id;					// users.id, -1 if the user does not exist
	public final String ident;
	public final int oId;					// users.o_id
	public final int eId;					// organisation.e_id, -1 if the user is in no organisation
	public final String language;			// Never null - falls back to the request locale then "en"
	public final Set<Integer> groupIds;
	public final Set<String> groupNames;

	public UserContext(int id, String ident, int oId, int eId, String language,
			Set<Integer> groupIds, Set<String> groupNames) {
		this.id = id;
		this.ident = ident;
		this.oId = oId;
		this.eId = eId;
		this.language = language;
		this.groupIds = Collections.unmodifiableSet(new HashSet<>(groupIds));
		this.groupNames = Collections.unmodifiableSet(new HashSet<>(groupNames));
	}

	/*
	 * True if the user is in any of the named groups.  Replaces the count(*) over
	 * users/groups/user_group that Authorise used to run.
	 */
	public boolean inAnyGroup(Iterable<String> names) {
		for (String name : names) {
			if (groupNames.contains(name)) {
				return true;
			}
		}
		return false;
	}

	public boolean inGroup(int gId) {
		return groupIds.contains(gId);
	}
}
