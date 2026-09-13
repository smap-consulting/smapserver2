package surveyKPI.oauth;

import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.OAuthTokenManager;
import org.smap.sdal.managers.ServerManager;

/*
 * RFC 7009 token revocation.
 *
 * Always answers 200, whether or not the value was a real token.  That is what the RFC asks for,
 * and the reason is that a revocation endpoint which distinguished between "revoked" and "never
 * existed" would be an oracle for checking whether a stolen value is worth anything.
 */
@Path("/oauth/revoke")
public class OAuthRevoke extends Application {

	private static Logger log = Logger.getLogger(OAuthRevoke.class.getName());

	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public Response revoke(@Context HttpServletRequest request,
			@FormParam("token") String token,
			@FormParam("client_id") String clientId) {

		String connectionString = "surveyKPI-OAuthRevoke";
		Connection sd = SDDataSource.getConnection(connectionString);

		try {
			if(!ServerManager.isMcpEnabled(sd)) {
				return Response.status(Response.Status.NOT_FOUND).build();
			}
			if(token != null && token.trim().length() > 0) {
				new OAuthTokenManager().revokeByValue(sd, token, "client");
			}
		} catch (Exception e) {
			// Logged, but still answered 200.  See above
			log.log(Level.SEVERE, "Revoking oauth token", e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return Response.ok().build();
	}
}
