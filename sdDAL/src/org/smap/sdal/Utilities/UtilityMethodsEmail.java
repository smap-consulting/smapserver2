package org.smap.sdal.Utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

import org.smap.sdal.managers.RecordEventManager;
import org.smap.sdal.model.AwsSdkEmailServer;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.Label;
import org.smap.sdal.model.Language;
import org.smap.sdal.model.ManifestValue;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.SmtpEmailServer;
import org.smap.sdal.model.Survey;

import com.google.common.io.Files;


public class UtilityMethodsEmail {

	private static Logger log =
			Logger.getLogger(UtilityMethodsEmail.class.getName());

	/*
	 * Mark a record and all its children as either bad or good
	 */
	static public void markRecord(
			Connection cResults, 
			Connection sd, 
			ResourceBundle localisation,
			String tName, 
			boolean value, 
			String reason, 
			int key, 
			int sId, 
			int fId,
			boolean modified,
			boolean isChild,
			String user,
			boolean updateChildren,
			String tz,
			boolean overideModifiedFlag		// Set if this is a manual request
			) throws Exception {

		String sql = "update " 
				+ tName 
				+ " set _bad = ?, _bad_reason = ?, _modified = ? " 
				+ " where prikey = ?";
		
		if(!overideModifiedFlag) {
			 sql += " and _modified = 'false'";
		}
		
		String sqlChild = "update " + tName + " set _bad = ?, _bad_reason = ? " + 
				" where prikey = ?;";

		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;

		try {

			if(isChild) {
				pstmt = cResults.prepareStatement(sqlChild);
			} else {
				pstmt = cResults.prepareStatement(sql);
			}
			pstmt.setBoolean(1, value);
			pstmt.setString(2, reason);
			if(isChild) {
				pstmt.setInt(3, key);
			} else {
				pstmt.setBoolean(3, modified);
				pstmt.setInt(4, key);
			}

			log.info("Mark record: " + pstmt.toString());
			int count = pstmt.executeUpdate();

			if(count != 1) {
				log.info("Expecting 1 record to be updated.  Number of records updated: " + count);
			}
			
			if(updateChildren) {
				// Get the child tables
				sql = "SELECT DISTINCT f.table_name, f_id FROM form f " +
						" where f.s_id = ? " + 
						" and f.parentform = ?;";
	
				if (pstmt != null) try {pstmt.close();} catch(Exception e) {};
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, sId);
				pstmt.setInt(2, fId);
	
				log.info(pstmt.toString());
				ResultSet tableSet = pstmt.executeQuery();
				while(tableSet.next()) {
					String childTable = tableSet.getString(1);
					int childFormId = tableSet.getInt(2);
	
					// Get the child records to be updated
					sql = "select prikey from " + childTable + 
							" where parkey = ?;";
	
					if (pstmt2 != null) try {pstmt2.close();} catch(Exception e) {};
					pstmt2 = cResults.prepareStatement(sql);	
					pstmt2.setInt(1, key);
					log.info(pstmt2.toString());
	
					ResultSet childRecs = pstmt2.executeQuery();
					while(childRecs.next()) {
						int childKey = childRecs.getInt(1);
						markRecord(cResults, sd, localisation, childTable, value, reason, childKey, 
								sId, childFormId, modified, true, user, updateChildren, tz, overideModifiedFlag);
					}
				}
			}
			
			/*
			 * Write to event table if this is the top level table
			 */
			if(!isChild) {
				String instanceId = GeneralUtilityMethods.getInstanceId(cResults, tName, key);
				RecordEventManager rem = new RecordEventManager();
				rem.writeEvent(sd, cResults, 
						value ? RecordEventManager.DELETED : RecordEventManager.RESTORED, 
						RecordEventManager.STATUS_SUCCESS,
						user, 
						tName, 
						instanceId, 
						null, 					// Change object
						null, 					// Task object
						null,					// Notification object
						reason, 				// Description
						sId, 
						null,
						0,
						0);	
			}
		} catch (SQLException e) {
			log.log(Level.SEVERE,"Error", e);
			throw e;
		} finally {
			if (pstmt != null) try {pstmt.close();} catch(Exception e) {};
			if (pstmt2 != null) try {pstmt2.close();} catch(Exception e) {};
		}


	}

	/*
	 * Get applicable user idents from an email
	 */
	static public ArrayList<String> getIdentsFromEmail(
			Connection connectionSD, 
			PreparedStatement pstmt, 
			String email) throws SQLException {

		ArrayList<String> idents = new ArrayList<String> ();

		/*
		 * Get the table name and column name containing the text data
		 * Do a case insensitive check
		 */
		String sql = "select ident from users where email ilike ?";

		pstmt = connectionSD.prepareStatement(sql);	
		pstmt.setString(1, email);
		log.info(pstmt.toString());
		ResultSet rs = pstmt.executeQuery();
		while (rs.next()) {
			idents.add(rs.getString(1));
		}

		return idents;
	}

	/*
	 * Get the email address for an ident
	 */
	static public String getEmailFromIdent(
			Connection connectionSD, 
			PreparedStatement pstmt, 
			String ident) throws SQLException {

		String email = null;;

		/*
		 * Get the table name and column name containing the text data
		 * Do a case insensitive check
		 */
		String sql = "select email from users where ident = ?";

		pstmt = connectionSD.prepareStatement(sql);	
		pstmt.setString(1, ident);
		log.info(pstmt.toString());
		ResultSet rs = pstmt.executeQuery();
		if (rs.next()) {
			email = rs.getString(1);
		}

		return email;
	}

	/*
	 * Get the administrator email for the organisation that the user belongs to
	 */
	static public Organisation getOrganisationDefaults(
			Connection sd, 
			String email,
			String user) throws SQLException {

		Organisation o = new Organisation();

		String sqlOrganisation = "select o.id, o.name, o.company_name, o.admin_email, o.smtp_host, "
				+ "o.email_domain, o.default_email_content,"
				+ "o.locale, o.company_email, o.timezone,"
				+ "o.server_description,"
				+ "o.can_notify,"
				+ "o.can_use_api,"
				+ "o.can_submit,"
				+ "o.can_sms, "
				+ "o.send_optin "
				+ "from organisation o, users u "
				+ "where u.o_id = o.id ";
		String sqlUser = " and u.ident = ?;";
		String sqlEmail = " and u.email ilike ?;";
		String sql = null;

		if(user == null) {
			sql = sqlOrganisation + sqlEmail;
		} else {
			sql = sqlOrganisation + sqlUser;
		}

		PreparedStatement pstmt = null;

		try {

			pstmt = sd.prepareStatement(sql);
			if(user == null) {
				pstmt.setString(1, email);
			} else {
				pstmt.setString(1, user);
			}
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				o.id = rs.getInt(1);
				o.name = rs.getString(2);
				o.company_name = rs.getString(3);
				o.admin_email = rs.getString(4);
				o.smtp_host = rs.getString(5);
				o.email_domain = rs.getString(6);
				o.default_email_content = rs.getString(7);
				o.locale = rs.getString(8);
				o.company_email = rs.getString(9);
				o.timeZone = rs.getString(10);
				o.server_description = rs.getString(11);
				o.can_notify = rs.getBoolean(12);
				o.can_use_api = rs.getBoolean(13);
				o.can_submit = rs.getBoolean(14);
				o.can_sms = rs.getBoolean(15);
				o.send_optin = rs.getBoolean(16);
						
			}
		} catch (SQLException e) {
			log.log(Level.SEVERE,"Error", e);
			throw e;
		} finally {
			try {if (pstmt != null) { pstmt.close();}} catch (Exception e) {}
		}
		
		if(o.locale == null) {
			o.locale = "en";
		}
		
		return o;
	}

	/*
	 * Get the smtp host for the organisation that the user belongs to
	 */
	static public EmailServer getEmailServer(
			Connection sd,
			ResourceBundle localisation,
			String email,
			String user,
			int o_id) throws SQLException, ApplicationException {

		EmailServer emailServer = null;
		
		String sqlOrg = "select email_type, smtp_host, email_domain, email_user, email_password, email_port " +
				" from organisation o " +
				" where id = ?";
		
		String sqlIdent = "select o.email_type, o.smtp_host, o.email_domain, o.email_user, o.email_password, o.email_port " +
				" from organisation o, users u " +
				" where u.o_id = o.id " +
				" and u.ident = ?";

		String sqlEmail = "select o.email_type, o.smtp_host, o.email_domain, o.email_user, o.email_password, o.email_port " +
				" from organisation o, users u " +
				" where u.o_id = o.id " +
				" and u.email ilike ?";

		String sqlServer = "select email_type, smtp_host, email_domain, email_user, email_password, email_port " +
				" from server ";

		PreparedStatement pstmt = null;
		ResultSet rs = null;

		String basePath = ServerSettings.getBasePath();
		String region = "ap-southeast-2";	// TODO Need a file for email region
		try {

			if(o_id > 0) {
				pstmt = sd.prepareStatement(sqlOrg);
				pstmt.setInt(1, o_id);
			} else if(user != null) {
				pstmt = sd.prepareStatement(sqlIdent);
				pstmt.setString(1, user);
			} else if(email != null) {
				/*
				 * This will be for a forgotten password
				 * Use the email server for the matching email
				 */
				pstmt = sd.prepareStatement(sqlEmail);
				pstmt.setString(1, email);
			} 
			
			if(pstmt != null) {
				log.info("Get smtp_host SQL:" + pstmt.toString());
				rs = pstmt.executeQuery();
				if(rs.next()) {
					emailServer = getEmailServerDetails(rs, localisation, region, basePath);		
				}
			}

			/*
			 * If the email details were not set at the organisation level try the server level defaults
			 */
			if(emailServer == null) {

				try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sqlServer);
				rs = pstmt.executeQuery();
				if(rs.next()) {
					emailServer = getEmailServerDetails(rs, localisation, region, basePath);
				}
			}

		} catch (SQLException e) {
			log.log(Level.SEVERE,"Error", e);
			throw e;
		} finally {
			try {if (pstmt != null) { pstmt.close();}} catch (SQLException e) {}
		}
		return emailServer;
	}

	static private EmailServer getEmailServerDetails(ResultSet rs, 
			ResourceBundle localisation,
			String region,
			String basePath) throws SQLException {
		
		EmailServer emailServer = null;	
		
		String type = rs.getString("email_type");
		String host = rs.getString("smtp_host");
		String domain = rs.getString("email_domain");
		String emailuser = rs.getString("email_user");
		String emailpassword = rs.getString("email_password");
		int emailport = rs.getInt("email_port");

		/*
		 * Create the SMTP or AWSSDK email server object
		 */
		if(type != null && type.equals("awssdk")) {
			emailServer = new AwsSdkEmailServer(localisation, region, basePath);
		} else if(host != null && domain != null && host.trim().length() > 0 && domain.trim().length() > 0) {
			emailServer = new SmtpEmailServer(localisation);		// Default to smtp
		}
		
		/*
		 * Fill in the details
		 */
		if(emailServer != null) {
			if(host != null) {
				if(host.trim().length() > 0) {
					emailServer.smtpHost = host;
				}
			}
			if(domain != null) {		
				if(domain.trim().length() > 0) {
					emailServer.emailDomain = domain;
				}
			}
			if(emailuser != null) {		
				if(emailuser.trim().length() > 0) {
					emailServer.emailUser = emailuser;
					if(emailServer.emailUser.indexOf('@') > 0) {  // Remove domain if it has been included
						emailServer.emailUser = emailServer.emailUser.substring(0, emailServer.emailUser.indexOf('@'));
					}
				}
			}
			if(emailpassword != null) {		
				if(emailpassword.trim().length() > 0) {
					emailServer.emailPassword = emailpassword;
				}
			}
			if(emailport > 0) {		
				emailServer.emailPort = emailport;	
			}
		}
		
		return emailServer;
	}
	
	// Set a one time password
	static public String setOnetimePassword(
			Connection connectionSD, 
			PreparedStatement pstmt, 
			String email, 
			String interval) throws SQLException {

		String uuid = String.valueOf(UUID.randomUUID());
		interval = interval.replace("'", "''");	// Escape apostrophes

		/*
		 * Update the users table by adding the UUID and expiry time
		 * Do a case insensitive test against email
		 */
		String sql = "update users set "
				+ "one_time_password = ?,"
				+ "one_time_password_expiry = timestamp 'now' + interval '" + interval + "' "
				+ "where email ilike ?";

		pstmt = connectionSD.prepareStatement(sql);	
		pstmt.setString(1, uuid);
		pstmt.setString(2, email);
		log.info("SQL set oneTimePassword: " + pstmt.toString());
		int count = pstmt.executeUpdate();

		if(count > 0) {
			return uuid;
		} else {
			return null;
		}
	}
	
	/*
	 * Check to see if a password has been sent within the last 'interval'
	 * The interval for password reset is 1 hour, however if the expiry time was set by account creation it will be 48 hours.
	 * Hence allow a resend if the expiry time is greater than 1 hour
	 * This means there is an anomoly where resend will not be allowed between 50 and 60 minutes before an account registration expires
	 * This is a security measure to prevent spamming
	 * 
	 * Returns the number of minutes since an email has been sent
	 */
	public static int hasOnetimePasswordBeenSent(
			Connection connectionSD, 
			PreparedStatement pstmt, 
			String email, 
			String interval) throws SQLException {

		int seconds = 0;
		interval = interval.replace("'", "''");	// Escape apostrophes

		String sql = "select EXTRACT(EPOCH FROM (now() - one_time_password_sent)) from users "
				+ "where one_time_password_sent > (timestamp 'now' - interval '" + interval + "') "
				+ "and email ilike ?";

		pstmt = connectionSD.prepareStatement(sql);	
		pstmt.setString(1, email);
		log.info("SQL checking for already sent email: " + pstmt.toString());
		ResultSet rs = pstmt.executeQuery();
		if(rs.next()) {
			seconds = rs.getInt(1);
			if(seconds < 60) {
				seconds  = 60;  // Set the elapsed time to a minimym of 1 minute		
			}
		}

		return seconds / 60;
	}
	
	/*
	 * Record in the database that a one time password email has been sent
	 */
	public static void reportOnetimePasswordSent(
			Connection connectionSD,  
			String email) throws SQLException {

		String sql = "update users "
				+ "set one_time_password_sent = now() "
				+ "where email ilike ?";

		PreparedStatement pstmt = null;
		try {
			pstmt = connectionSD.prepareStatement(sql);	
			pstmt.setString(1, email);
			log.info("SQL record one time password sent: " + pstmt.toString());
			pstmt.executeUpdate();
			
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}

		return;
	}


	/*
	 * Get a substring of a date that is in ISO 8601 format
	 */
	public static String getPartDate(String fullDate, String format) throws ParseException {

		String partDate = null;

		// parse the input date
		SimpleDateFormat inFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");  // ISO8601 date formats - add timezone after upgrade of java rosa libraries
		SimpleDateFormat outFormat = new SimpleDateFormat(format);
		Date theDate = null;

		theDate = inFormat.parse(fullDate);
		partDate = outFormat.format(theDate).toString();

		return partDate;
	}

	public static String getPartLocation(String location, String dimension) {

		String partLocation= "0.0";

		String vals[] = location.split(" ");
		if(vals.length > 2) {
			if(dimension.equals("lat")) {
				partLocation = vals[0];
			} else if(dimension.equals("lon")) {
				partLocation = vals[1];
			}
		}	

		return partLocation;
	}

	/*
	 * Get a files extension or return an empty string
	 */
	public static String getExtension(String filename) {
		String extension = "";
		int idx = filename.lastIndexOf('.');
		if(idx >= 0) {
			extension = filename.substring(idx+1).toLowerCase();
		}
		return extension;
	}
	
	/*
	 * Get the content type from the filename
	 */
	public static String getContentType(String filename) {

		String ct = null;
		String extension = getExtension(filename);

		if (extension.equals("xml")) {
			ct = "text/xml";
		} else if (extension.equals("jpg") || extension.equals("jpeg") || extension.equals("jpe")) {
			ct = "image/jpeg";
		} else if (extension.equals("png")) {
			ct = "image/png";
		} else if (extension.equals("svg")) {
			ct = "image/svg+xml";
		} else if (extension.equals("gif")) {
			ct = "image/gif";
		} else if (extension.equals("3gp")) {
			ct = "video/3gp";
		} else if (extension.equals("3ga")) {
			ct = "audio/3ga";
		} else if (extension.equals("mp2") || extension.equals("mp3") || extension.equals("mpga")) {
			ct = "audio/mpeg";
		} else if (extension.equals("mpeg") || extension.equals("mpg") || extension.equals("mpe")) {
			ct = "video/mpeg";
		} else if (extension.equals("qt") || extension.equals("mov")) {
			ct = "video/quicktime";
		} else if (extension.equals("mp4") || extension.equals("m4p")) {
			ct = "video/mp4";
		} else if (extension.equals("avi")) {
			ct = "video/x-msvideo";
		} else if (extension.equals("movie")) {
			ct = "video/x-sgi-movie";
		} else if (extension.equals("m4a")) {
			ct = "audio/m4a";
		} else if (extension.equals("csv")) {
			ct = "text/csv";
		} else if (extension.equals("css")) {
			ct = "text/css";
		} else if (extension.equals("xlsx")) {
			ct = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
		} else if (extension.equals("xls")) {
			ct = "application/vnd.ms-excel";
		} else if (extension.equals("amr")) {
			ct = "audio/amr";
		} else if (extension.equals("xls")) {
			ct = "application/vnd.ms-excel";
		} else if (extension.equals("geojson")) {
			ct = "application/geojson";
		}  else if (extension.equals("zip")) {
			ct = "application/octet-stream; charset=UTF-8";
		} else {
			ct = "application/octet-stream";
			log.info("	Info: unrecognised content type for extension " + extension);           
		}

		return ct;
	}

	/*
	 * Create a thumbnail for a file
	 */
	public static void createThumbnail(String name, String path, File file) {

		String contentType = getContentType(name);
		String dest = path + "/thumbs/" + name;

		/*
		 * Ensure that the thumbs directory exists
		 */
		try {
			Files.createParentDirs(new File(dest));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		int idx = dest.lastIndexOf('.');
		String destRoot = dest;
		if(idx > 0) {
			destRoot = dest.substring(0, idx + 1);
		}

		String os = System.getProperty("os.name");
		String binDir = "/usr/bin/";
		if(os.startsWith("Mac")) {
			binDir = "/usr/local/bin/";
		} 
		String cmd = null;
		if(contentType.startsWith("image")) {
			cmd = binDir + "convert -thumbnail 100x100 \"" + file.getAbsolutePath() + "\" \"" + dest + "\"";
		} else if(contentType.startsWith("video")) {
			cmd = binDir + "ffmpeg -i \"" + file.getAbsolutePath() + "\" -vf scale=-1:100 -vframes 1 \"" + destRoot + "jpg\" -y";
		} 

		log.info("Exec: " + cmd);

		if(cmd != null) {
			try {

				Process proc = Runtime.getRuntime().exec(new String [] {"/bin/sh", "-c", cmd});

				int code = proc.waitFor();
				log.info("Attachment processing finished with status:" + code);
				if(code != 0) {
					log.info("Error: Attachment processing failed");
					InputStream stderr = proc.getErrorStream();
					InputStreamReader isr = new InputStreamReader(stderr);
					BufferedReader br = new BufferedReader(isr);
					String line = null;
					while ( (line = br.readLine()) != null) {
						log.info("** " + line);
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * Get labels for an option or question
	 */
	public static void getLabels(PreparedStatement pstmt,
			Survey s, 
			String text_id, 
			ArrayList<Label> labels,
			String basePath,
			int oId) throws Exception {

		try {

			for(int i = 0; i < s.surveyData.languages.size(); i++) {

				Label l = new Label();
				ResultSet resultSet;

				// Get label and media
				if(text_id != null) {
					
					int idx = text_id.indexOf(':');
					if(idx > 0) {
						String root = text_id.substring(0, idx + 1) + "%";
						
						pstmt.setString(2, s.surveyData.languages.get(i).name);
						pstmt.setString(3, root);
						//log.info("Get labels: " + pstmt.toString());
	
						resultSet = pstmt.executeQuery();		
						while(resultSet.next()) {
	
							String t = resultSet.getString(1).trim();
							String v = resultSet.getString(2);
							String id = resultSet.getString(3);
	
							if(t.equals("none") && id.endsWith("label")) {
								l.text = GeneralUtilityMethods.convertAllEmbeddedOutput(v, true);
							} else if(t.equals("image") || t.equals("big-image") || t.equals("audio") || t.equals("video")) {							
								if(basePath != null && oId > 0) {							
									ManifestValue manifest = new ManifestValue();
									// Assume this is only called by UI hence do not get the device form of the URLs
									getFileUrl(manifest, s.surveyData.ident, v, basePath, oId, s.surveyData.id, false);
									if(t.equals("image")) {
										l.image = v;
										l.imageUrl = manifest.url;
										l.imageThumb = manifest.thumbsUrl;
									} if(t.equals("big-image")) {
										l.bigImage = v;
										l.bigImageUrl = manifest.url;
									} else if(t.equals("audio")) {
										l.audio = v;
										l.audioUrl = manifest.url;
										l.audioThumb = null;
									} else if(t.equals("video")) {
										l.video = v;
										l.videoUrl = manifest.url;
										l.videoThumb = manifest.thumbsUrl;
									}
								}
							}  else if(t.equals("none") && id.endsWith("hint")) {
								l.hint = v;
							} else if(t.equals("guidance") || t.equals("guidance_hint")) {
								l.guidance_hint = v;
							} else if(t.equals("constraint_msg")) {
								l.constraint_msg = v;
							} else if(t.equals("required_msg")) {
								l.required_msg = v;
							} else {
								log.info("Error: Invalid label type: " + t);
							}
						}
						
					}
				}

				labels.add(l);		
			}
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
			throw e;
		} 
	}
	
	/*
	 * Get labels for an option or question
	 */
	public static PreparedStatement getLabelsStatement(Connection sd,
			int sId)  throws Exception {

		String sql = "select t.type, t.value, t.text_id from translation t where t.s_id = ? and t.language = ? and t.text_id like ?";
		PreparedStatement pstmt = sd.prepareStatement(sql);
		pstmt.setInt(1,  sId);
		return pstmt;	
	}
	
	/*
	 * Get labels for an option or question
	 */
	public static String getSingleLabel(
			Connection connectionSD,
			int sId,
			String languageName, 
			int l_id,
			String value,
			boolean getImage) throws Exception {

		PreparedStatement pstmt = null;
		String label =  null;
		
		try {

			// Note - there is a limit 1 on the sub query as ovalues need not be unique
			String sqlDisplayName = "select display_name from option where l_id = ? and ovalue = ? limit 1";
			
			String sql = "select t.type, t.value "
					+ "from translation t "
					+ "where t.s_id = ? "
					+ "and t.language = ? "
					+ "and t.text_id = "
						+ "(select label_id from option where l_id = ? and ovalue = ? limit 1)";
			
			// Display name takes precedence
			pstmt = connectionSD.prepareStatement(sqlDisplayName);
			pstmt.setInt(1, l_id);
			pstmt.setString(2, value);
			ResultSet resultSet = pstmt.executeQuery();	
			if(resultSet.next()) {
				label = resultSet.getString(1);
			}
			
			if(label == null || label.trim().length() == 0) {
				if(pstmt != null) try{pstmt.close();}catch(Exception e){}
				pstmt = connectionSD.prepareStatement(sql);
	
				pstmt.setInt(1, sId);
				pstmt.setString(2, languageName);
				pstmt.setInt(3,  l_id);
				pstmt.setString(4, value);
				
				resultSet = pstmt.executeQuery();		
				while(resultSet.next()) {
	
					String t = resultSet.getString(1).trim();
					String v = resultSet.getString("value");
					
					if(getImage && t.equals("image")) {
						label = v;
					} else if(!getImage && t.equals("none")) {
						label = GeneralUtilityMethods.convertAllEmbeddedOutput(v, true);
					}
	
					
				}
			}
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
			throw e;
		} finally {
			if(pstmt != null) try{pstmt.close();}catch(Exception e){}
		}
		return label;
	}

	/*
	 * Set labels for an option or question
	 */
	public static void setLabels(Connection sd,
			int sId, 
			String textId, 
			ArrayList<Label> labels,
			PreparedStatement pstmt,
			boolean external,
			HtmlSanitise sanitise) throws SQLException {

		ArrayList<Language> languages = new ArrayList<Language>();

		/*
		 * Get the languages
		 */
		languages = GeneralUtilityMethods.getLanguages(sd, sId);

		for(int i = 0; i < languages.size(); i++) {

			Label l = labels.get(i);

			// Set common values
			pstmt.setString(2, languages.get(i).name);
			pstmt.setBoolean(6, external);

			// Update text
			if(l.text != null ) {
				pstmt.setString(3, textId + ":label");
				pstmt.setString(4, "none");
				pstmt.setString(5, sanitise.sanitiseHtml(l.text));
				pstmt.executeUpdate();
			}

			// Update hint
			if(l.hint != null) {
				pstmt.setString(3, textId + ":hint");
				pstmt.setString(4, "none");
				pstmt.setString(5, sanitise.sanitiseHtml(l.hint));
				pstmt.executeUpdate();
			}
			
			// Update guidance
			if(l.guidance_hint != null) {
				pstmt.setString(3, textId + ":guidance_hint");
				pstmt.setString(4, "guidance");
				pstmt.setString(5, sanitise.sanitiseHtml(l.guidance_hint));
				pstmt.executeUpdate();
			}
			
			// Update constraint_msg
			if(l.constraint_msg != null) {
				pstmt.setString(3, textId + ":constraint");
				pstmt.setString(4, "constraint_msg");
				pstmt.setString(5, sanitise.sanitiseHtml(l.constraint_msg));
				pstmt.executeUpdate();
			}
			
			// Update required_msg
			if(l.required_msg != null) {
				pstmt.setString(3, textId + ":required");
				pstmt.setString(4, "required_msg");
				pstmt.setString(5, sanitise.sanitiseHtml(l.required_msg));
				pstmt.executeUpdate();
			}

			// Update image
			if(l.image != null) {
				pstmt.setString(3, textId + ":label");
				pstmt.setString(4, "image");
				pstmt.setString(5, sanitise.sanitiseHtml(l.image));
				pstmt.executeUpdate();
			}
			
			// Update big-image
			if(l.bigImage != null) {
				pstmt.setString(3, textId + ":label");
				pstmt.setString(4, "big-image");
				pstmt.setString(5, sanitise.sanitiseHtml(l.bigImage));
				pstmt.executeUpdate();
			}

			// Update video
			if(l.video != null) {
				pstmt.setString(3, textId + ":label");
				pstmt.setString(4, "video");
				pstmt.setString(5, sanitise.sanitiseHtml(l.video));
				pstmt.executeUpdate();
			}

			// Update audio
			if(l.audio != null) {
				pstmt.setString(3, textId + ":label");
				pstmt.setString(4, "audio");
				pstmt.setString(5, sanitise.sanitiseHtml(l.audio));
				pstmt.executeUpdate();
			}


		}

	}

	/*
	 * Get the partial (URL) of the file and its file path or null if the file does not exist
	 */
	public static void getFileUrl(ManifestValue manifest, String sIdent, String fileName, 
			String basePath, 
			int oId, 
			int sId,
			boolean forDevice) {

		String path = null;
		String thumbsPath = null;
		File file = null;
		File thumb = null;
		String urlBase = forDevice ? "/resource/" : "/surveyKPI/file/";
		

		// First try the survey level
		path = basePath + "/media/" + sIdent + "/" + fileName;	
		thumbsPath = basePath + "/media/" + sIdent + "/thumbs/" + fileName;	

		file = new File(path);
		if(file.exists()) {
			manifest.url = urlBase + fileName + "/survey/" + sId;
			manifest.filePath = path;

			thumb = new File(thumbsPath);
			if(thumb.exists()) {
				manifest.thumbsUrl = manifest.url + "?thumbs=true";
			}
		} else {

			// Second try the organisation level
			path = basePath + "/media/organisation/" + oId + "/" + fileName;
			thumbsPath = basePath + "/media/organisation/" + oId + "/thumbs/" + fileName;
			file = new File(path);
			if(file.exists()) {
				manifest.url = urlBase + fileName + "/organisation";
				manifest.filePath = path;

				thumb = new File(thumbsPath);
				if(thumb.exists()) {
					manifest.thumbsUrl = manifest.url + "?thumbs=true";
				}
			}		
		}
	}
	
	/*
	 * Get the path to the media file
	 */
	public static String getMediaPath(String sIdent, String fileName, String basePath, int oId, int sId) {

		String path = null;
		File file = null;

		// First try the survey level
		path = basePath + "/media/" + sIdent + "/" + fileName;	

		file = new File(path);
		if(!file.exists()) {
			
			// Second try the organisation level
			path = basePath + "/media/organisation/" + oId + "/" + fileName;
			file = new File(path);
			if(!file.exists()) {
				path = null;
			}		
		}
		return path;
	}
	
	/*
	 * Validate an email
	 */
	public static boolean isValidEmail(String email) {
		boolean isValid = true;
		try {
		      InternetAddress emailAddr = new InternetAddress(email);
		      emailAddr.validate();
		   } catch (AddressException ex) {
		      isValid = false;
		   }
		return isValid;
	}

}
