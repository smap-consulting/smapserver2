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

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ApiTokenManager;
import org.smap.sdal.model.ApiToken;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/*
 * API and device tokens.
 *
 * Replaces the single api_key on the user account and the single app_key used by the
 * fieldTask QR login.  A user may now hold several, each with its own name, scope and
 * expiry, and each can be revoked without disturbing the others.
 *
 * The value is returned only by the call that creates it.  Only its hash is stored, so
 * there is no endpoint that reads a token back - the QR code has to be scanned while it
 * is on screen, and a token that has been lost is replaced rather than recovered.
 *
 * Deliberately not exempt from TwoFactorFilter: a user with two factor turned on has to
 * pass it before they can mint a credential that does not need it.
 */
@Path("/token")
public class Token extends Application {

	private static Logger log = Logger.getLogger(Token.class.getName());

	Authorise aAdmin = null;		// Managing somebody else's tokens

	public Token() {
		ArrayList<String> authorisations = new ArrayList<String> ();
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ORG);
		authorisations.add(Authorise.SECURITY);
		aAdmin = new Authorise(authorisations, null);
	}

	/*
	 * What the client sends to create a token
	 */
	private static class TokenRequest {
		String scope;			// api || app
		String name;
		Integer expiryDays;		// Null or zero for no expiry
	}

	/*
	 * The response to a create.  Carries the extra fields fieldTask expects in its QR
	 * payload, so the console can render the code straight from this.
	 */
	private static class NewToken {
		ApiToken token;
		String auth_token;		// The value, named as fieldTask reads it
		String server_url;
		String username;
	}

	private final Gson gson = new GsonBuilder().disableHtmlEscaping()
			.setDateFormat("yyyy-MM-dd HH:mm:ss").create();

	/*
	 * The caller's own tokens
	 */
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getMyTokens(@Context HttpServletRequest request,
			@QueryParam("scope") String scope) {

		// Authorisation - not required, a user may always see their own tokens
		return list(request, request.getRemoteUser(), scope, false);
	}

	/*
	 * Another user's tokens.  Administrators issue the fieldTask device tokens on behalf
	 * of enumerators, who often never sign in to the console at all.
	 */
	@GET
	@Path("/user/{ident}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getUserTokens(@Context HttpServletRequest request,
			@PathParam("ident") String ident,
			@QueryParam("scope") String scope) {

		return list(request, ident, scope, true);
	}

	/*
	 * Issue a token to the caller
	 */
	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createMyToken(@Context HttpServletRequest request,
			@FormParam("tokenDetails") String tokenDetails) {

		return create(request, request.getRemoteUser(), tokenDetails, false);
	}

	/*
	 * Issue a token to another user
	 */
	@POST
	@Path("/user/{ident}")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createUserToken(@Context HttpServletRequest request,
			@PathParam("ident") String ident,
			@FormParam("tokenDetails") String tokenDetails) {

		return create(request, ident, tokenDetails, true);
	}

	/*
	 * Revoke one of the caller's own tokens
	 */
	@DELETE
	@Path("/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response revokeMyToken(@Context HttpServletRequest request,
			@PathParam("id") int id) {

		return revoke(request, request.getRemoteUser(), id, false);
	}

	/*
	 * Revoke a token belonging to another user
	 */
	@DELETE
	@Path("/user/{ident}/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response revokeUserToken(@Context HttpServletRequest request,
			@PathParam("ident") String ident,
			@PathParam("id") int id) {

		return revoke(request, ident, id, true);
	}

	private Response list(HttpServletRequest request, String ident, String scope, boolean otherUser) {

		String connectionString = "surveyKPI-Token-list";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			authorise(sd, request, ident, otherUser);

			ApiTokenManager tm = new ApiTokenManager();
			ArrayList<ApiToken> tokens = tm.getTokens(sd, ident, scope);
			return Response.ok(gson.toJson(tokens)).build();

		} catch (AuthorisationException ae) {
			throw ae;
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	private Response create(HttpServletRequest request, String ident, String tokenDetails, boolean otherUser) {

		// Check for Ajax and reject if not.  Same guard the api_key and app_key endpoints
		// this replaces already had - it stops a token being minted by a cross site form post.
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With"))) {
			log.info("Error: Non ajax request");
			throw new AuthorisationException();
		}

		String connectionString = "surveyKPI-Token-create";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			authorise(sd, request, ident, otherUser);

			Type type = new TypeToken<TokenRequest>(){}.getType();
			TokenRequest tr = new Gson().fromJson(tokenDetails, type);
			if(tr == null) {
				tr = new TokenRequest();
			}
			if(tr.scope == null) {
				tr.scope = ApiTokenManager.SCOPE_API;
			}

			Timestamp expires = null;
			if(tr.expiryDays != null && tr.expiryDays > 0) {
				expires = new Timestamp(System.currentTimeMillis()
						+ tr.expiryDays.longValue() * 24 * 60 * 60 * 1000L);
			}

			ApiTokenManager tm = new ApiTokenManager();
			ApiToken token = tm.create(sd, ident, tr.scope, tr.name, expires,
					request.getRemoteUser(), request.getServerName());

			NewToken nt = new NewToken();
			nt.token = token;
			nt.auth_token = token.value;
			nt.server_url = request.getScheme() + "://" + request.getServerName();
			nt.username = ident;

			return Response.ok(gson.toJson(nt)).build();

		} catch (AuthorisationException ae) {
			throw ae;
		} catch (ApplicationException ae) {
			return Response.status(Status.BAD_REQUEST).entity(ae.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	private Response revoke(HttpServletRequest request, String ident, int id, boolean otherUser) {

		String connectionString = "surveyKPI-Token-revoke";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			authorise(sd, request, ident, otherUser);

			ApiTokenManager tm = new ApiTokenManager();
			if(!tm.revoke(sd, id, ident, request.getRemoteUser(), request.getServerName())) {
				return Response.status(Status.NOT_FOUND).build();
			}
			return Response.ok("{}").build();

		} catch (AuthorisationException ae) {
			throw ae;
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	/*
	 * Acting on somebody else's tokens needs administrator rights and the target has to be
	 * in the caller's own organisation.  Acting on your own needs nothing beyond being
	 * signed in, which is how the api_key endpoints already worked.
	 */
	private void authorise(Connection sd, HttpServletRequest request, String ident, boolean otherUser) {

		if(request.getRemoteUser() == null) {
			throw new AuthorisationException();
		}
		if(otherUser && !request.getRemoteUser().equals(ident)) {
			aAdmin.isAuthorised(sd, request, request.getRemoteUser());
			aAdmin.isValidUserIdent(sd, request.getRemoteUser(), ident);
		}
	}
}
