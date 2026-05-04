import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Part;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.RecordEventManager;
import org.smap.sdal.model.DatabaseConnections;
import org.smap.sdal.model.EmailReplyData;

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

/*
 * Polls an S3 bucket for inbound emails that are replies to Smap notifications.
 * Decodes the reply+ address, strips quoted content, and writes to record_event.
 */
public class EmailResponseProcessor {

	String confFilePath;
	DocumentBuilderFactory dbf = GeneralUtilityMethods.getDocumentBuilderFactory();

	private static Logger log = Logger.getLogger(EmailResponseProcessor.class.getName());

	private static final int POLL_INTERVAL_SECS = 60;
	private static final String INBOUND_PREFIX = "inbound/";
	private static final String ERROR_PREFIX = "inbound/error/";

	private class ResponseLoop implements Runnable {
		DatabaseConnections dbc = new DatabaseConnections();
		String basePath;
		String hostname;
		long pid;
		AmazonS3 s3 = null;
		String lastRegion = null;

		public ResponseLoop(String basePath, String hostname, long pid) {
			this.basePath = basePath;
			this.hostname = hostname;
			this.pid = pid;
		}

		public void run() {
			boolean loop = true;
			while(loop) {
				String subscriberControl = GeneralUtilityMethods.getSettingFromFile("/smap/settings/subscriber");
				if(subscriberControl != null && subscriberControl.equals("stop")) {
					GeneralUtilityMethods.log(log, "---------- Email Response Processor Stopped", "email-response", null);
					loop = false;
				} else {
					try {
						GeneralUtilityMethods.getDatabaseConnections(dbf, dbc, confFilePath);

						String[] cfg = getResponseConfig(dbc.sd);
						if(cfg != null) {
							String region = cfg[0];
							String bucket = cfg[1];

							// Reinitialise S3 client if region changed
							if(s3 == null || !region.equals(lastRegion)) {
								s3 = AmazonS3Client.builder()
										.withRegion(region)
										.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
										.build();
								lastRegion = region;
							}

							processInbound(dbc, s3, bucket);
						}
					} catch(Exception e) {
						log.log(Level.SEVERE, "Email response processor error", e);
					}

					try { Thread.sleep(POLL_INTERVAL_SECS * 1000L); } catch(InterruptedException e) {}
				}
			}

			try {if(dbc.sd != null) dbc.sd.close();} catch(SQLException e) {}
			try {if(dbc.results != null) dbc.results.close();} catch(SQLException e) {}
		}
	}

	// Returns [region, bucket, domain] if awssdk + bucket + domain are all configured
	private String[] getResponseConfig(Connection sd) throws SQLException {
		String sql = "select aws_region, email_type, email_response_bucket, email_response_domain from server";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				String type   = rs.getString("email_type");
				String region = rs.getString("aws_region");
				String bucket = rs.getString("email_response_bucket");
				String domain = rs.getString("email_response_domain");
				if("awssdk".equals(type) && region != null && bucket != null && !bucket.isEmpty()
						&& domain != null && !domain.isEmpty()) {
					return new String[]{region, bucket, domain};
				}
			}
		} finally {
			try {if(pstmt != null) pstmt.close();} catch(Exception e) {}
		}
		return null;
	}

	private void processInbound(DatabaseConnections dbc, AmazonS3 s3, String bucket) {
		ListObjectsV2Request req = new ListObjectsV2Request()
				.withBucketName(bucket)
				.withPrefix(INBOUND_PREFIX)
				.withDelimiter("/");   // skip error/ subfolder
		ListObjectsV2Result listing = s3.listObjectsV2(req);

		for(S3ObjectSummary obj : listing.getObjectSummaries()) {
			String key = obj.getKey();
			if(key.equals(INBOUND_PREFIX)) continue;  // skip the prefix directory entry
			processObject(dbc, s3, bucket, key);
		}
	}

	private void processObject(DatabaseConnections dbc, AmazonS3 s3, String bucket, String key) {
		try {
			S3Object s3obj = s3.getObject(bucket, key);
			S3ObjectInputStream content = s3obj.getObjectContent();

			Session session = Session.getDefaultInstance(new Properties());
			MimeMessage mime = new MimeMessage(session, content);

			// Extract the reply+{encoded} address from To/Delivered-To headers
			String encodedPayload = findEncodedPayload(mime);
			if(encodedPayload == null) {
				log.warning("No reply+ address found in: " + key);
				moveToError(s3, bucket, key);
				return;
			}

			// Decode JSON payload
			JsonObject payload;
			try {
				String json = new String(Base64.getUrlDecoder().decode(encodedPayload), StandardCharsets.UTF_8);
				payload = JsonParser.parseString(json).getAsJsonObject();
			} catch(Exception e) {
				log.warning("Failed to decode payload in: " + key + " error: " + e.getMessage());
				moveToError(s3, bucket, key);
				return;
			}

			String instanceId  = payload.has("ii") ? payload.get("ii").getAsString() : null;
			String surveyIdent = payload.has("si") ? payload.get("si").getAsString() : null;
			int    messageId   = payload.has("mi") ? payload.get("mi").getAsInt()    : 0;

			if(instanceId == null || surveyIdent == null) {
				log.warning("Payload missing fields in: " + key);
				moveToError(s3, bucket, key);
				return;
			}

			// Build reply data
			EmailReplyData reply = new EmailReplyData();
			reply.fromAddress    = extractFrom(mime);
			reply.subject        = mime.getSubject();
			reply.body           = extractBody(mime);
			reply.sentAt         = extractSentAt(mime);
			reply.replyMessageId = extractHeader(mime, "Message-ID");
			reply.sesInReplyTo   = extractHeader(mime, "In-Reply-To");
			reply.messageId      = messageId;

			// Idempotency: use this reply's own Message-ID, not In-Reply-To
			if(reply.replyMessageId != null && isDuplicate(dbc.sd, reply.replyMessageId)) {
				log.fine("Duplicate reply, skipping: " + reply.replyMessageId);
				s3.deleteObject(bucket, key);
				return;
			}

			RecordEventManager rem = new RecordEventManager();
			rem.writeEmailReply(dbc.sd, dbc.results, instanceId, surveyIdent, messageId, reply);

			s3.deleteObject(bucket, key);
			log.info("Processed email reply from " + reply.fromAddress + " for instance " + instanceId);

		} catch(Exception e) {
			log.log(Level.SEVERE, "Error processing S3 object " + key, e);
			try { moveToError(s3, bucket, key); } catch(Exception ex) {}
		}
	}

	// Find the reply+{encoded} local part in To or Delivered-To headers
	private String findEncodedPayload(MimeMessage mime) throws MessagingException {
		// Check To recipients
		Address[] toAddrs = mime.getRecipients(Message.RecipientType.TO);
		if(toAddrs != null) {
			for(Address a : toAddrs) {
				String addr = a.toString();
				String enc = extractReplyPlusPayload(addr);
				if(enc != null) return enc;
			}
		}
		// Check Delivered-To header (SES sometimes uses this)
		String[] delivered = mime.getHeader("Delivered-To");
		if(delivered != null) {
			for(String h : delivered) {
				String enc = extractReplyPlusPayload(h);
				if(enc != null) return enc;
			}
		}
		return null;
	}

	private String extractReplyPlusPayload(String address) {
		if(address == null) return null;
		// Match reply+{payload}@
		int plusIdx = address.indexOf("reply+");
		if(plusIdx < 0) return null;
		int atIdx = address.indexOf('@', plusIdx);
		if(atIdx < 0) return null;
		return address.substring(plusIdx + 6, atIdx);
	}

	private String extractFrom(MimeMessage mime) throws MessagingException {
		Address[] froms = mime.getFrom();
		if(froms != null && froms.length > 0) return froms[0].toString();
		return "";
	}

	private String extractHeader(MimeMessage mime, String name) {
		try {
			String[] vals = mime.getHeader(name);
			return (vals != null && vals.length > 0) ? vals[0] : null;
		} catch(MessagingException e) {
			return null;
		}
	}

	private String extractSentAt(MimeMessage mime) {
		try {
			java.util.Date sent = mime.getSentDate();
			if(sent != null) {
				return DateTimeFormatter.ISO_INSTANT.format(sent.toInstant());
			}
		} catch(MessagingException e) { /* ignored */ }
		return java.time.Instant.now().toString();
	}

	// Extract plain-text body and strip quoted content
	private String extractBody(MimeMessage mime) throws Exception {
		String text = getTextContent(mime);
		if(text == null) return "";
		return stripQuotedContent(text);
	}

	private String getTextContent(Part part) throws Exception {
		if(part.isMimeType("text/plain")) {
			return (String) part.getContent();
		}
		if(part.isMimeType("multipart/*")) {
			Multipart mp = (Multipart) part.getContent();
			// Prefer plain text part
			for(int i = 0; i < mp.getCount(); i++) {
				Part subPart = mp.getBodyPart(i);
				if(subPart.isMimeType("text/plain")) {
					return (String) subPart.getContent();
				}
			}
			// Fall back to HTML part
			for(int i = 0; i < mp.getCount(); i++) {
				Part subPart = mp.getBodyPart(i);
				if(subPart.isMimeType("text/html")) {
					String html = (String) subPart.getContent();
					return html.replaceAll("<[^>]+>", "");
				}
			}
		}
		if(part.isMimeType("text/html")) {
			String html = (String) part.getContent();
			return html.replaceAll("<[^>]+>", "");
		}
		return null;
	}

	/*
	 * Strip quoted content from reply body.
	 * Scans line by line and truncates at the first recognised separator.
	 */
	private String stripQuotedContent(String text) {
		String[] lines = text.split("\r?\n", -1);
		StringBuilder result = new StringBuilder();
		String prevNonBlank = "";

		for(int i = 0; i < lines.length; i++) {
			String line = lines[i];
			String trimmed = line.trim();

			// Priority 1: "On ... wrote:" — handles single-line and two-line wrapped form
			if(trimmed.endsWith("wrote:") && (trimmed.startsWith("On ") || prevNonBlank.startsWith("On "))) {
				// Remove the "On ..." prefix line from result if it was the previous non-blank
				if(!trimmed.startsWith("On ") && prevNonBlank.startsWith("On ")) {
					// Strip the previously appended "On ..." line
					String built = result.toString();
					int lastOnIdx = built.lastIndexOf("On ");
					if(lastOnIdx >= 0) {
						result = new StringBuilder(built.substring(0, lastOnIdx));
					}
				}
				break;
			}

			// Priority 2: line starting with > after a blank line (standard quote)
			if(trimmed.startsWith(">") && result.toString().endsWith("\n\n")) {
				break;
			}

			// Priority 3: Outlook "-----Original Message-----"
			if(trimmed.matches("-{5,}\\s*[Oo]riginal [Mm]essage\\s*-{5,}")) {
				break;
			}

			// Priority 4: "From:" at start of body line after a blank line (old clients)
			if(trimmed.matches("(?i)From:\\s*.+") && result.length() > 0
					&& result.toString().endsWith("\n\n")) {
				break;
			}

			// Priority 5: long underline (some Outlook variants)
			if(trimmed.matches("_{10,}")) {
				break;
			}

			result.append(line).append("\n");
			if(!trimmed.isEmpty()) prevNonBlank = trimmed;
		}

		// Trim trailing blank lines
		String out = result.toString();
		while(out.endsWith("\n\n")) {
			out = out.substring(0, out.length() - 1);
		}
		return out.trim();
	}

	private boolean isDuplicate(Connection sd, String sesInReplyTo) throws SQLException {
		String sql = "select 1 from record_event where ses_message_id = ? limit 1";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sesInReplyTo);
			ResultSet rs = pstmt.executeQuery();
			return rs.next();
		} finally {
			try {if(pstmt != null) pstmt.close();} catch(Exception e) {}
		}
	}

	private void moveToError(AmazonS3 s3, String bucket, String key) {
		try {
			String destKey = ERROR_PREFIX + key.substring(INBOUND_PREFIX.length());
			s3.copyObject(bucket, key, bucket, destKey);
			s3.deleteObject(bucket, key);
		} catch(Exception e) {
			log.log(Level.WARNING, "Could not move to error prefix: " + key, e);
		}
	}

	public void go(String smapId, String basePath, String hostname, long pid) {
		confFilePath = "./" + smapId;
		Thread t = new Thread(new ResponseLoop(basePath, hostname, pid), "email-response");
		t.start();
	}
}
