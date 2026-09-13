package surveyKPI.oauth;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.TokenThrottle;
import org.smap.sdal.managers.OAuthManager;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.model.OAuthClient;
import org.smap.sdal.model.ServerData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/*
 * RFC 7591 dynamic client registration.
 *
 * Deprecated by the 2026-07-28 MCP specification in favour of Client ID Metadata Documents, and
 * kept only for clients that cannot present one yet.  A server owner can turn it off entirely.
 *
 * Registration is open by default and that is not the hole it first appears to be: a client id is
 * not access.  Nothing at all happens until a user who holds the mcp access group approves the
 * client on the consent screen.  What registration can do is fill a table, so it is rate limited,
 * capped, and clients that never complete a grant are reaped after a week.
 */
@Path("/oauth/register")
public class OAuthRegistration extends Application {

	private static Logger log = Logger.getLogger(OAuthRegistration.class.getName());

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response register(@Context HttpServletRequest request, String body) {

		String connectionString = "surveyKPI-OAuthRegister";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {
			ServerManager sm = new ServerManager();
			ServerData server = sm.getServer(sd, null);

			if(!server.mcp_enabled) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}

			/*
			 * Throttled on the same bucket as failed token attempts.  Registration is the one
			 * unauthenticated endpoint here that writes a row.
			 */
			if(!TokenThrottle.isPermitted(request)) {
				log.warning("Client registration throttled for " + request.getRemoteAddr());
				return error(Response.Status.TOO_MANY_REQUESTS, "temporarily_unavailable",
						"Too many registration attempts");
			}

			OAuthManager om = new OAuthManager();
			OAuthClient client = om.register(sd, body, server.mcp_client_registration,
					request.getRemoteAddr());

			Map<String, Object> out = new HashMap<>();
			out.put("client_id", client.client_id);
			out.put("client_id_issued_at", System.currentTimeMillis() / 1000);
			if(client.client_secret != null) {
				out.put("client_secret", client.client_secret);
				out.put("client_secret_expires_at", 0);		// Does not expire
			}
			out.put("client_name", client.client_name);
			out.put("redirect_uris", client.redirect_uris);
			out.put("grant_types", client.grant_types);
			out.put("token_endpoint_auth_method",
					client.token_endpoint_auth_method == null ? "none" : client.token_endpoint_auth_method);
			if(client.application_type != null) {
				out.put("application_type", client.application_type);
			}

			return Response.status(Response.Status.CREATED).entity(gson.toJson(out)).build();

		} catch (ApplicationException e) {
			TokenThrottle.failed(request);
			return error(Response.Status.BAD_REQUEST, "invalid_client_metadata", e.getMessage());
		} catch (Exception e) {
			log.log(Level.SEVERE, "Registering oauth client", e);
			return error(Response.Status.INTERNAL_SERVER_ERROR, "server_error",
					"The client could not be registered");
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
	}

	private Response error(Response.Status status, String code, String description) {
		Map<String, String> body = new HashMap<>();
		body.put("error", code);
		body.put("error_description", description);
		return Response.status(status).entity(gson.toJson(body)).build();
	}
}
