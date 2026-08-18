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

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

/*
 * No usable credentials were presented - 401, as distinct from AuthorisationException,
 * which is 403 and means we know who the caller is but they may not do this.
 *
 * Used by the services Apache grants unauthenticated (/api/v2 and /token), where the
 * caller identifies itself with an x-api-key header and a missing or unknown key is an
 * authentication failure rather than an authorisation one.
 */
public class AuthenticationException extends WebApplicationException {

	private static final long serialVersionUID = 1L;

	public AuthenticationException() {
		this("Authentication Error");
	}

	public AuthenticationException(String msg) {
		super(Response.status(Status.UNAUTHORIZED)
				.entity("{\"code\": 401, \"message\": \"" + msg + "\"}")
				.type("application/json")
				.build());
	}
}
