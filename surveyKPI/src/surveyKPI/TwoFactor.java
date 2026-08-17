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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.TwoFactorSession;
import org.smap.sdal.managers.TwoFactorManager;
import org.smap.sdal.model.TwoFactorStatus;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/*
 * Two factor authentication.
 *
 * Apache checks the password before any of this is reached, so every method here already
 * knows who the user is.  What it does not know is whether they have proved possession of
 * their authenticator, which is what these services establish.
 *
 * TwoFactorFilter exempts this whole path - the challenge has to be reachable by a user who
 * has not yet passed it.
 */
@Path("/twofactor")
public class TwoFactor extends Application {

	private static Logger log = Logger.getLogger(TwoFactor.class.getName());

	// Only a full administrator may clear someone else's two factor
	private Authorise aAdmin = null;

	public TwoFactor() {
		ArrayList<String> authorisations = new ArrayList<String> ();
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.ORG);
		aAdmin = new Authorise(authorisations, null);
	}

	/*
	 * Whether the logged in user has two factor set up
	 */
	@GET
	@Path("/status")
	@Produces("application/json")
	public Response getStatus(@Context HttpServletRequest request) {

		String connectionString = "surveyKPI-TwoFactor-status";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			TwoFactorManager tfm = new TwoFactorManager(getLocalisation(sd, request));
			TwoFactorStatus status = tfm.getStatus(sd, request.getRemoteUser());

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			return Response.ok(gson.toJson(status)).build();

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	/*
	 * Start enrolment.  Returns the secret, the otpauth URL and a QR code to scan.
	 */
	@POST
	@Path("/enrol")
	@Produces("application/json")
	public Response enrol(@Context HttpServletRequest request) {

		rejectNonAjax(request);

		String connectionString = "surveyKPI-TwoFactor-enrol";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			TwoFactorManager tfm = new TwoFactorManager(getLocalisation(sd, request));

			// The issuer is what the authenticator app shows as the account heading, so it
			// has to identify this server for a user with accounts on more than one
			TwoFactorStatus status = tfm.enrol(sd, request.getRemoteUser(), request.getServerName());

			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			return Response.ok(gson.toJson(status)).build();

		} catch (ApplicationException e) {
			return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	/*
	 * Finish enrolment by proving the secret was added to the authenticator app correctly
	 */
	@POST
	@Path("/confirm")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces("application/json")
	public Response confirm(@Context HttpServletRequest request, @FormParam("code") String code) {

		rejectNonAjax(request);

		String connectionString = "surveyKPI-TwoFactor-confirm";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			String ident = request.getRemoteUser();
			TwoFactorManager tfm = new TwoFactorManager(getLocalisation(sd, request));
			tfm.confirm(sd, ident, code, GeneralUtilityMethods.getOrganisationId(sd, request, ident));

			// Step the session up now, so enrolling does not immediately lock the user out
			// of the page they enrolled from
			return Response.ok("{}")
					.header(HttpHeaders.SET_COOKIE,
							TwoFactorSession.cookieHeader(TwoFactorSession.issue(sd, ident)))
					.build();

		} catch (ApplicationException e) {
			return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	/*
	 * The login challenge
	 */
	@POST
	@Path("/verify")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces("application/json")
	public Response verify(@Context HttpServletRequest request, @FormParam("code") String code) {

		rejectNonAjax(request);

		String connectionString = "surveyKPI-TwoFactor-verify";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			String ident = request.getRemoteUser();
			TwoFactorManager tfm = new TwoFactorManager(getLocalisation(sd, request));
			tfm.verify(sd, ident, code, GeneralUtilityMethods.getOrganisationId(sd, request, ident));

			return Response.ok("{}")
					.header(HttpHeaders.SET_COOKIE,
							TwoFactorSession.cookieHeader(TwoFactorSession.issue(sd, ident)))
					.build();

		} catch (ApplicationException e) {
			return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	/*
	 * A user turning off their own two factor.  A current code is required so that a
	 * hijacked session cannot be used to remove the second factor.
	 */
	@POST
	@Path("/remove")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces("application/json")
	public Response remove(@Context HttpServletRequest request, @FormParam("code") String code) {

		rejectNonAjax(request);

		String connectionString = "surveyKPI-TwoFactor-remove";
		Connection sd = SDDataSource.getConnection(connectionString);
		try {
			String ident = request.getRemoteUser();
			TwoFactorManager tfm = new TwoFactorManager(getLocalisation(sd, request));
			tfm.remove(sd, ident, code, GeneralUtilityMethods.getOrganisationId(sd, request, ident));

			return Response.ok("{}")
					.header(HttpHeaders.SET_COOKIE, TwoFactorSession.clearCookieHeader())
					.build();

		} catch (ApplicationException e) {
			return Response.status(Response.Status.BAD_REQUEST).entity(e.getMessage()).build();
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	/*
	 * An administrator clearing another user's two factor.  This is the only way back in for
	 * a user who has lost their phone.
	 */
	@DELETE
	@Path("/user/{ident}")
	@Produces("application/json")
	public Response reset(@Context HttpServletRequest request, @PathParam("ident") String ident) {

		rejectNonAjax(request);

		String connectionString = "surveyKPI-TwoFactor-reset";
		Connection sd = SDDataSource.getConnection(connectionString);

		// Authorisation - the administrator, and only over users in their own organisation
		aAdmin.isAuthorised(sd, request, request.getRemoteUser());
		aAdmin.isValidUserIdent(sd, request.getRemoteUser(), ident);
		// End Authorisation

		try {
			String adminIdent = request.getRemoteUser();
			TwoFactorManager tfm = new TwoFactorManager(getLocalisation(sd, request));
			tfm.reset(sd, adminIdent, ident,
					GeneralUtilityMethods.getOrganisationId(sd, request, adminIdent));

			return Response.ok("{}").build();

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			return Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	private ResourceBundle getLocalisation(Connection sd, HttpServletRequest request) throws Exception {
		Locale locale = new Locale(
				GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
		return ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
	}

	/*
	 * These are all state changing calls made by the console, so requiring the header the
	 * console always sends stops a cross site form post reaching them.  Same guard as the
	 * API key services use.
	 */
	private void rejectNonAjax(HttpServletRequest request) {
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With"))) {
			log.info("Error: Non ajax request");
			throw new AuthorisationException();
		}
	}
}
