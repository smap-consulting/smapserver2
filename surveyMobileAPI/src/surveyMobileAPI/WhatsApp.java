package surveyMobileAPI;

import jakarta.servlet.http.*;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.SMSManager;
import org.smap.sdal.model.ConversationItemDetails;

import com.google.gson.Gson;

import model.MessageWhatsApp;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/*
 * Receive WhatsApp messages directly from the Meta WhatsApp Cloud API
 */
@Path("/sms/whatsapp")
public class WhatsApp extends Application {

	private static Logger log =
			 Logger.getLogger(WhatsApp.class.getName());

	private Gson gson = new Gson();

	/*
	 * Meta calls this when the webhook URL is set up, to check that it belongs to us
	 */
	@GET
	@Path("/inbound")
	@Produces("text/plain")
	public Response verify(@Context HttpServletRequest request,
			@QueryParam("hub.mode") String mode,
			@QueryParam("hub.verify_token") String verifyToken,
			@QueryParam("hub.challenge") String challenge) {

		String connectionString = "whatsapp-verify";
		Connection sd = null;

		try {
			sd = SDDataSource.getConnection(connectionString);
			String expected = getSetting(sd, "wa_verify_token");

			if("subscribe".equals(mode) && expected != null && expected.trim().length() > 0
					&& verifyToken != null && constantTimeEquals(expected.trim(), verifyToken.trim())) {
				return Response.ok(challenge).build();
			}
			log.info("Error: WhatsApp webhook verification failed");

		} catch(Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		return Response.status(Status.FORBIDDEN).build();
	}

	/*
	 * Receive messages
	 * The body is read as bytes, as the signature is over the body exactly as it was sent
	 */
	@POST
	@Path("/inbound")
	@Consumes("application/json")
	public Response inbound(@Context HttpServletRequest request, byte[] body) {

		String connectionString = "whatsapp-inbound";
		Connection sd = null;

		try {
			sd = SDDataSource.getConnection(connectionString);

			/*
			 * Authenticate request
			 */
			String appSecret = getSetting(sd, "wa_app_secret");
			if(!validSignature(appSecret, request.getHeader("X-Hub-Signature-256"), body)) {
				log.info("Error: WhatsApp message signature check failed");
				return Response.status(Status.FORBIDDEN).build();
			}

			String json = new String(body, StandardCharsets.UTF_8);
			log.info("WhatsApp: " + json);
			MessageWhatsApp inbound = gson.fromJson(json, MessageWhatsApp.class);

			/*
			 * Save each text message for further processing
			 * Anything else, such as status updates, is ignored
			 */
			SMSManager sim = new SMSManager(null, null);
			if(inbound != null && inbound.entry != null) {
				for(MessageWhatsApp.Entry entry : inbound.entry) {
					if(entry.changes == null) {
						continue;
					}
					for(MessageWhatsApp.Change change : entry.changes) {
						if(change.value == null || change.value.messages == null) {
							continue;
						}
						String ourNumber = getOurNumber(sd, sim, change.value.metadata);
						if(ourNumber == null) {
							log.info("Error: WhatsApp message for a number that is not set up");
							continue;
						}
						for(MessageWhatsApp.Message m : change.value.messages) {
							saveMessage(sd, sim, request.getServerName(), ourNumber, m);
						}
					}
				}
			}

		} catch(Exception e) {
			// Still acknowledge the message, otherwise Meta keeps resending one that cannot be processed
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return Response.ok().build();
	}

	private void saveMessage(Connection sd, SMSManager sim, String serverName, String ourNumber,
			MessageWhatsApp.Message m) throws SQLException {

		if(!"text".equals(m.type) || m.text == null || m.text.body == null) {
			log.info("WhatsApp message of type " + m.type + " ignored");
			return;
		}
		/*
		 * Meta's message id is too long for an instanceid, which is limited to 41 characters
		 * Derive a uuid from it instead, the same id always gives the same uuid so repeats are still found
		 */
		String instanceId = null;
		if(m.id != null) {
			instanceId = "uuid:" + UUID.nameUUIDFromBytes(m.id.getBytes(StandardCharsets.UTF_8));
			if(sim.messageExists(sd, ourNumber, instanceId)) {
				log.info("WhatsApp message " + m.id + " already received");
				return;
			}
		}

		Timestamp ts = new Timestamp(System.currentTimeMillis());
		try {
			ts = new Timestamp(Long.parseLong(m.timestamp) * 1000);
		} catch (Exception e) {
			// Use the time received
		}

		ConversationItemDetails msg = new ConversationItemDetails(m.from,
				ourNumber, m.text.body, true,
				ConversationItemDetails.WHATSAPP_CHANNEL,
				ts);
		sim.saveMessage(sd, msg, serverName, instanceId, SMSManager.SMS_TYPE, null);
	}

	/*
	 * Find our number from Meta's id for it, or failing that from the number Meta shows
	 */
	private String getOurNumber(Connection sd, SMSManager sim, MessageWhatsApp.Metadata metadata) throws SQLException {
		String ourNumber = null;
		if(metadata != null) {
			if(metadata.phone_number_id != null) {
				ourNumber = sim.getOurNumberForWhatsAppId(sd, metadata.phone_number_id);
			}
			if(ourNumber == null && metadata.display_phone_number != null) {
				ourNumber = metadata.display_phone_number.replaceAll("[^0-9]", "");
			}
		}
		return ourNumber;
	}

	/*
	 * The signature header is "sha256=" followed by the hex HMAC-SHA256 of the body keyed with the app secret
	 */
	private boolean validSignature(String appSecret, String header, byte[] body) throws Exception {
		if(appSecret == null || appSecret.trim().length() == 0 || header == null
				|| !header.startsWith("sha256=") || body == null) {
			return false;
		}
		Mac mac = Mac.getInstance("HmacSHA256");
		mac.init(new SecretKeySpec(appSecret.trim().getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
		StringBuilder expected = new StringBuilder();
		for(byte b : mac.doFinal(body)) {
			expected.append(String.format("%02x", b));
		}
		return constantTimeEquals(expected.toString(), header.substring(7).toLowerCase());
	}

	private boolean constantTimeEquals(String a, String b) {
		return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
	}

	private String getSetting(Connection sd, String column) throws SQLException {
		String value = null;
		String sql = "select " + column + " from server";	// column is never user input
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				value = rs.getString(1);
			}
		} finally {
			if (pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return value;
	}
}
