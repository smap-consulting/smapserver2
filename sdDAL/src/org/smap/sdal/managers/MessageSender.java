package org.smap.sdal.managers;

import java.io.File;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.ServerSettings;
import org.smap.sdal.model.ConversationItemDetails;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.vonage.client.VonageClient;
import com.vonage.client.messages.MessageRequest;
import com.vonage.client.messages.MessageResponse;
import com.vonage.client.messages.MessagesClient;
import com.vonage.client.messages.sms.SmsTextRequest;
import com.vonage.client.messages.whatsapp.WhatsappTextRequest;

/*
 * Send SMS and WhatsApp messages
 * WhatsApp messages go directly to the Meta WhatsApp Cloud API if it is set up, otherwise through Vonage
 * SMS messages go through Vonage
 */
public class MessageSender {

	private static Logger log =
			 Logger.getLogger(MessageSender.class.getName());

	private static final String DEFAULT_WA_API_VERSION = "v21.0";

	private static final HttpClient httpClient = HttpClient.newBuilder()
			.connectTimeout(Duration.ofSeconds(20))
			.build();

	private Gson gson = new Gson();

	private VonageClient vonageClient;

	private MessageSender(VonageClient vonageClient) {
		this.vonageClient = vonageClient;
	}

	/*
	 * Get a sender, or null if no way of sending messages has been set up
	 */
	public static MessageSender getMessageSender(Connection sd) throws SQLException, ApplicationException {
		VonageClient vonageClient = getVonageClient(sd);
		if(vonageClient == null && getWhatsAppSettings(sd) == null) {
			return null;
		}
		return new MessageSender(vonageClient);
	}

	/*
	 * Send a message and return the id the provider gave it
	 */
	public String send(Connection sd,
			String channel,
			String ourNumber,
			String toNumber,
			String msgText) throws Exception {

		boolean whatsApp = ConversationItemDetails.WHATSAPP_CHANNEL.equals(channel);

		if(whatsApp) {
			/*
			 * The Meta settings are read for each message so that a new token is used without a restart
			 */
			WhatsAppSettings wa = getWhatsAppSettings(sd);
			if(wa != null) {
				return sendWhatsAppCloud(sd, wa, ourNumber, toNumber, msgText);
			}
		}

		if(vonageClient == null) {
			throw new ApplicationException(whatsApp
					? "WhatsApp has not been set up.  Add the WhatsApp access token or a Vonage client"
					: "Vonage client has not been configured");
		}
		return sendVonage(whatsApp, ourNumber, toNumber, msgText);
	}

	private String sendVonage(boolean whatsApp,
			String ourNumber,
			String toNumber,
			String msgText) {

		MessagesClient messagesClient = vonageClient.getMessagesClient();
		MessageResponse response;

		if(whatsApp) {

			WhatsappTextRequest message = WhatsappTextRequest.builder()
					.from(ourNumber)
					.to(toNumber)
					.text(msgText)
					.build();

			response = messagesClient.sendMessage(message);
		} else {

			MessageRequest message = SmsTextRequest.builder()
					.from(ourNumber)
					.to(toNumber)
					.text(msgText)
					.build();

			response = messagesClient.sendMessage(message);
		}

		return response.getMessageUuid() == null ? null : response.getMessageUuid().toString();
	}

	/*
	 * Send a text message through the Meta WhatsApp Cloud API
	 * Free text is only delivered within 24 hours of the last message from the recipient
	 */
	private String sendWhatsAppCloud(Connection sd,
			WhatsAppSettings wa,
			String ourNumber,
			String toNumber,
			String msgText) throws Exception {

		String phoneNumberId = getWhatsAppPhoneNumberId(sd, ourNumber);
		if(phoneNumberId == null) {
			throw new ApplicationException("The WhatsApp phone number id has not been set for " + ourNumber);
		}

		JsonObject text = new JsonObject();
		text.addProperty("body", msgText);
		JsonObject body = new JsonObject();
		body.addProperty("messaging_product", "whatsapp");
		body.addProperty("to", toNumber.startsWith("+") ? toNumber.substring(1) : toNumber);
		body.addProperty("type", "text");
		body.add("text", text);

		String url = "https://graph.facebook.com/" + wa.apiVersion + "/"
				+ URLEncoder.encode(phoneNumberId, StandardCharsets.UTF_8) + "/messages";
		HttpRequest request = HttpRequest.newBuilder(URI.create(url))
				.timeout(Duration.ofSeconds(30))
				.header("Authorization", "Bearer " + wa.accessToken)
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(gson.toJson(body)))
				.build();

		HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

		if(response.statusCode() / 100 != 2) {
			log.info("Error: WhatsApp send failed: " + response.statusCode() + " " + response.body());
			throw new ApplicationException("WhatsApp send failed: " + getErrorMessage(response.body()));
		}

		String id = null;
		try {
			JsonObject result = gson.fromJson(response.body(), JsonObject.class);
			JsonArray messages = result.getAsJsonArray("messages");
			if(messages != null && messages.size() > 0) {
				id = messages.get(0).getAsJsonObject().get("id").getAsString();
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Unexpected WhatsApp response: " + response.body(), e);
		}
		return id;
	}

	/*
	 * Meta returns {"error": {"message": "..."}}
	 */
	private String getErrorMessage(String responseBody) {
		try {
			JsonObject result = gson.fromJson(responseBody, JsonObject.class);
			return result.getAsJsonObject("error").get("message").getAsString();
		} catch (Exception e) {
			return responseBody;
		}
	}

	private String getWhatsAppPhoneNumberId(Connection sd, String ourNumber) throws SQLException {

		String sql = "select wa_phone_number_id from sms_number where our_number = ?";
		PreparedStatement pstmt = null;
		String id = null;

		if(ourNumber != null && ourNumber.startsWith("+")) {
			ourNumber = ourNumber.substring(1);
		}
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ourNumber);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				id = rs.getString(1);
			}
		} finally {
			if (pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return (id == null || id.trim().length() == 0) ? null : id.trim();
	}

	private static class WhatsAppSettings {
		String accessToken;
		String apiVersion;
	}

	/*
	 * Return the Meta settings, or null if there is no access token
	 */
	private static WhatsAppSettings getWhatsAppSettings(Connection sd) throws SQLException {

		String sql = "select wa_access_token, wa_api_version from server";
		PreparedStatement pstmt = null;
		WhatsAppSettings wa = null;

		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String token = rs.getString(1);
				if(token != null && token.trim().length() > 0) {
					wa = new WhatsAppSettings();
					wa.accessToken = token.trim();
					String version = rs.getString(2);
					wa.apiVersion = (version == null || version.trim().length() == 0)
							? DEFAULT_WA_API_VERSION : version.trim();
				}
			}
		} finally {
			if (pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return wa;
	}

	/*
	 * Create a Vonage client object if the private key exists and application id is specified
	 */
	private static VonageClient getVonageClient(Connection sd) throws SQLException, ApplicationException {

		LogManager lm = new LogManager();
		VonageClient vonageClient = null;
		String privateKeyFile = ServerSettings.getBasePath() + "_bin/resources/properties/vonage_private.key";
		File vonagePrivateKey = new File(privateKeyFile);
		String vonageApplicationId = getVonageApplicationId(sd);

		if(vonagePrivateKey.exists() && vonageApplicationId != null && vonageApplicationId.trim().length() > 0) {
			log.fine("Getting vonage client with application Id: " + vonageApplicationId + " file at: " + privateKeyFile);
			try {
				vonageClient = VonageClient.builder()
						.applicationId(vonageApplicationId)
						.privateKeyPath(vonagePrivateKey.getAbsolutePath())
						.build();
				log.fine("Got vonage client");
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(),e);
				lm.writeLogOrganisation(sd, -1, null, LogManager.SMS,
							"Cannot create vonage client" + " " + e.getMessage(), 0);
			}
		} else {
			String msg = "Cannot create vonage client. "
					+ (!vonagePrivateKey.exists() ? " vonage_private.key was not found." : "")
					+ (vonageApplicationId == null ? " The vonage application Id was not found in settings." : "");

			if(vonageApplicationId != null && vonageApplicationId.trim().length() > 0) {
				// Set organisation id to -1 as this is an issue not related to an organisation
				lm.writeLogOrganisation(sd, -1, null, LogManager.SMS, msg, 0);
				log.fine("Error setting up vonage client: " + msg);
			}

		}

		return vonageClient;
	}

	private static String getVonageApplicationId(Connection sd) throws SQLException {
		String id = null;
		String sql = "select vonage_application_id from server";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				id = rs.getString(1);
			}
		} finally {
			if (pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return id;
	}
}
