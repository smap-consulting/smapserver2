package org.smap.sdal.managers;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.fileupload.FileItem;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.MediaInfo;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.AppearanceOptions;
import org.smap.sdal.model.DashboardDetails;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.MySensitiveData;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.OtherOrgData;
import org.smap.sdal.model.SensitiveData;
import org.smap.sdal.model.SubscriptionStatus;
import org.smap.sdal.model.WebformOptions;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

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
 * This class supports access to User and Organsiation information in the database
 */
public class OrganisationManager {
	
	LogManager lm = new LogManager();		// Application log
	
	private static Logger log =
			 Logger.getLogger(OrganisationManager.class.getName());
	
	private HtmlSanitise sanitise = new HtmlSanitise();
	
	private ResourceBundle localisation;
	
	public OrganisationManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Update a new organisation
	 */
	public void updateOrganisation(
			Connection sd,
			Organisation o,
			String userIdent,
			String bannerFileName,
			String mainFileName,
			String requestUrl,
			String basePath,
			FileItem bannerLogoItem,
			FileItem mainLogoItem,
			String serverName,
			String scheme
			) throws SQLException, ApplicationException {
		
		String sql = "update organisation set "
				+ "name = ?, "
				+ "company_name = ?, "
				+ "company_address = ?, "
				+ "company_phone = ?, " 
				+ "company_email = ?, " 
				+ "allow_email = ?, "
				+ "allow_facebook = ?, "
				+ "allow_twitter = ?, "
				+ "can_edit = ?, "
				+ "email_task = ?, "
				+ "admin_email = ?, "
				+ "smtp_host = ?, "
				+ "email_domain = ?, "
				+ "email_user = ?, "
				+ "email_password = ?, "
				+ "email_port = ?, "
				+ "default_email_content = ?, "
				+ "website = ?, "
				+ "locale = ?, "
				+ "timezone = ?, "
				+ "server_description = ?, "
				+ "changed_by = ?, "
				+ "can_notify = ?, "
				+ "can_use_api = ?, "
				+ "can_submit = ?, "
				+ "set_as_theme = ?, "
				+ "navbar_color = ?, "
				+ "can_sms = ?, "
				+ "send_optin = ?, "
				+ "limits = ?," 
				+ "refresh_rate = ?,"
				+ "password_strength = ?,"
				+ "map_source = ?,"
				+ "changed_ts = now() " 
				+ "where "
				+ "id = ?";
	
		PreparedStatement pstmt = null;
		
		try {
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();

			// Get the current settings in case we need to notify the administrator of a change
			Organisation originalOrg = GeneralUtilityMethods.getOrganisation(sd, o.id);
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, HtmlSanitise.checkCleanName(o.name, localisation));
			pstmt.setString(2, HtmlSanitise.checkCleanName(o.company_name, localisation));
			pstmt.setString(3, HtmlSanitise.checkCleanName(o.company_address, localisation));
			pstmt.setString(4, HtmlSanitise.checkCleanName(o.company_phone, localisation));
			pstmt.setString(5, HtmlSanitise.checkCleanName(o.company_email, localisation));
			pstmt.setBoolean(6, o.allow_email);
			pstmt.setBoolean(7, o.allow_facebook);
			pstmt.setBoolean(8, o.allow_twitter);
			pstmt.setBoolean(9, o.can_edit);
			pstmt.setBoolean(10, o.email_task);
			pstmt.setString(11, HtmlSanitise.checkCleanName(o.admin_email, localisation));
			pstmt.setString(12, HtmlSanitise.checkCleanName(o.smtp_host, localisation));
			pstmt.setString(13, HtmlSanitise.checkCleanName(o.email_domain, localisation));
			pstmt.setString(14, HtmlSanitise.checkCleanName(o.email_user, localisation));
			pstmt.setString(15, o.email_password);
			pstmt.setInt(16, o.email_port);
			pstmt.setString(17, sanitise.sanitiseHtml(o.default_email_content));
			pstmt.setString(18, o.website);
			pstmt.setString(19, HtmlSanitise.checkCleanName(o.locale, localisation));
			pstmt.setString(20, HtmlSanitise.checkCleanName(o.timeZone, localisation));
			pstmt.setString(21, HtmlSanitise.checkCleanName(o.server_description, localisation));
			pstmt.setString(22, userIdent);
			pstmt.setBoolean(23, o.can_notify);
			pstmt.setBoolean(24, o.can_use_api);
			pstmt.setBoolean(25, o.can_submit);
			pstmt.setBoolean(26, o.appearance.set_as_theme);
			pstmt.setString(27, HtmlSanitise.checkCleanName(o.appearance.navbar_color, localisation));
			pstmt.setBoolean(28, o.can_sms);
			pstmt.setBoolean(29, o.send_optin);
			pstmt.setString(30, o.limits == null ? null : gson.toJson(o.limits));
			pstmt.setInt(31, o.refresh_rate);
			pstmt.setDouble(32, o.password_strength);
			pstmt.setString(33, HtmlSanitise.checkCleanName(o.map_source, localisation));
			pstmt.setInt(34, o.id);
					
			log.info("Update organisation: " + pstmt.toString());
			pstmt.executeUpdate();
	
			// Save the banner logo, if it has been passed
			if(bannerFileName != null) {
				writeLogo(bannerFileName, bannerLogoItem, o.id, basePath, userIdent, requestUrl, "bannerLogo");
			}
			// Save the main logo, if it has been passed
			if(mainFileName != null) {
				writeLogo(mainFileName, mainLogoItem, o.id, basePath, userIdent, requestUrl, "mainLogo");
			}
			
			/*
			 * Notify the administrator if access permissions to the organisation have been changed
			 */
			if(originalOrg.can_notify != o.can_notify 
					|| originalOrg.can_use_api != o.can_use_api 
					|| originalOrg.can_submit != o.can_submit
					|| originalOrg.email_task != o.email_task
					|| originalOrg.can_sms != o.can_sms) {
				
				EmailManager em = new EmailManager(localisation);			
				EmailServer emailServer = null;
				SubscriptionStatus subStatus = null;
				
				if(originalOrg.admin_email != null) {
					emailServer = UtilityMethodsEmail.getSmtpHost(sd, null, userIdent, 0);
					if(emailServer.smtpHost != null) {
						
						PeopleManager pm = new PeopleManager(localisation);
						subStatus = pm.getEmailKey(sd, o.id, originalOrg.getAdminEmail());
						if(subStatus.unsubscribed) {
							// Person has unsubscribed
							String msg = localisation.getString("email_us");
							msg = msg.replaceFirst("%s1", originalOrg.getAdminEmail());
							log.info(msg);
						} else {
							String subject = localisation.getString("email_org_change");
							subject = subject.replaceAll("%s1", serverName);
							subject = subject.replaceAll("%s2", originalOrg.name);
							log.info("Sending email confirmation: Header = " + subject);
							
							String content = localisation.getString("org_change");
							content = content.replaceAll("%s1", originalOrg.name);
							StringBuilder contentBuilder = new StringBuilder(content);
							if(originalOrg.can_notify != o.can_notify) {
								contentBuilder.append("<br/>    ").append(o.can_notify ? localisation.getString("en_notify") : localisation.getString("susp_notify"));
							}
							if(originalOrg.can_use_api != o.can_use_api) {
								contentBuilder.append("<br/>    ").append(o.can_use_api ? localisation.getString("en_api") : localisation.getString("susp_api"));
							}
							if(originalOrg.can_submit != o.can_submit) {
								contentBuilder.append("<br/>    ").append(o.can_submit ? localisation.getString("en_submit") : localisation.getString("susp_submit"));
							}
							if(originalOrg.can_sms != o.can_sms) {
								contentBuilder.append("<br/>    ").append(o.can_sms ? localisation.getString("en_sms") : localisation.getString("susp_sms"));
							}
							if(originalOrg.email_task != o.email_task) {
								contentBuilder.append("<br/>    ").append(o.email_task ? localisation.getString("en_email_tasks") : localisation.getString("susp_email_tasks"));
							}
							
							String sender = "";
							// Catch and log exceptions
							try {
								em.sendEmailHtml(
										originalOrg.getAdminEmail(), 
										"bcc", 
										subject, 
										contentBuilder, 
										null, 
										null, 
										emailServer,
										serverName,
										subStatus.emailKey,
										localisation,
										null,
										null,
										null,
										GeneralUtilityMethods.getNextEmailId(sd));
							} catch(Exception e) {
								lm.writeLogOrganisation(sd, o.id, userIdent, LogManager.ORGANISATION_UPDATE, e.getMessage(), 0);
							}
						}
					}
				}
				
			}
			
		} finally {
			
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}
		
	}
	
	/*
	 * Create a new organisation
	 */
	public int createOrganisation(
			Connection sd,
			Organisation o,
			String userIdent,
			String bannerFileName,
			String mainFileName,
			String requestUrl,
			String basePath,
			FileItem bannerLogoItem,
			FileItem mainLogoItem,
			String email
			) throws SQLException, ApplicationException {
		
		int o_id = 0;
		
		String sqlAddOrgList = "insert into user_organisation (u_id, o_id) values (?,?)";
		PreparedStatement pstmtAddOrgList = null;
		
		String sqlCheckExists = "select id from organisation "
				+ "where lower(name) = lower(?) ";		
		PreparedStatement pstmtCheckExists = null;
		
		String sql = "insert into organisation (name, company_name, "
				+ "company_address, "
				+ "company_phone, "
				+ "company_email, "
				+ "allow_email, allow_facebook, allow_twitter, can_edit, email_task, "
				+ "changed_by, admin_email, smtp_host, email_domain, email_user, email_password, "
				+ "email_port, default_email_content, website, locale, timezone, "
				+ "can_notify, can_use_api, can_submit, set_as_theme, e_id, ft_backward_navigation, ft_navigation, "
				+ "ft_guidance, ft_image_size, ft_send, ft_delete, "
				+ "ft_send_location, ft_pw_policy, navbar_color, can_sms, send_optin, limits, "
				+ "ft_high_res_video, refresh_rate, password_strength, map_source, "
				+ "ft_input_method, ft_im_ri, ft_im_acc, changed_ts, owner) "
				+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ "?, ?, ?, ?, ?, ?, "
				+ "?, ?, ?, ?, ?, ?, "
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
				+ "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)";
		PreparedStatement pstmt = null;
		
		try {
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			o.name = o.name.trim();
			/*
			 * Check to see if this organisation name is already taken
			 */
			pstmtCheckExists = sd.prepareStatement(sqlCheckExists);
			pstmtCheckExists.setString(1,  o.name);
			log.info("Check for existing organisations with same name " + pstmtCheckExists.toString());
			ResultSet rs = pstmtCheckExists.executeQuery();			
			if(rs.next()) {
				throw new ApplicationException(localisation.getString("msg_org_exists"));
			} 
			
			// Initialise limits
			if(o.limits == null) {
				o.limits = new HashMap<String, Integer> ();
				o.limits.put(LogManager.TRANSCRIBE, Organisation.DEFAULT_TRANSCRIBE_LIMIT);
				o.limits.put(LogManager.TRANSLATE, Organisation.DEFAULT_TRANSLATE_LIMIT);
				o.limits.put(LogManager.REKOGNITION, Organisation.DEFAULT_REKOGNITION_LIMIT);
				o.limits.put(LogManager.SENTIMENT, Organisation.DEFAULT_SENTIMENT_LIMIT);
			}
			
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1, HtmlSanitise.checkCleanName(o.name, localisation));
			pstmt.setString(2, HtmlSanitise.checkCleanName(o.company_name, localisation));
			pstmt.setString(3, HtmlSanitise.checkCleanName(o.company_address, localisation));
			pstmt.setString(4, HtmlSanitise.checkCleanName(o.company_phone, localisation));
			pstmt.setString(5, HtmlSanitise.checkCleanName(o.company_email, localisation));
			pstmt.setBoolean(6, o.allow_email);
			pstmt.setBoolean(7, o.allow_facebook);
			pstmt.setBoolean(8, o.allow_twitter);
			pstmt.setBoolean(9, o.can_edit);
			pstmt.setBoolean(10, o.email_task);
			pstmt.setString(11, HtmlSanitise.checkCleanName(userIdent, localisation));
			pstmt.setString(12, HtmlSanitise.checkCleanName(o.admin_email, localisation));
			pstmt.setString(13, HtmlSanitise.checkCleanName(o.smtp_host, localisation));
			pstmt.setString(14, HtmlSanitise.checkCleanName(o.email_domain, localisation));
			pstmt.setString(15, HtmlSanitise.checkCleanName(o.email_user, localisation));
			pstmt.setString(16, o.email_password);
			pstmt.setInt(17, o.email_port);
			pstmt.setString(18, sanitise.sanitiseHtml(o.default_email_content));
			pstmt.setString(19, o.website);			// Allowed to have https: - leave unchecked
			pstmt.setString(20, HtmlSanitise.checkCleanName(o.locale, localisation));
			
			if(o.timeZone == null || o.timeZone.trim().length() == 0) {
				o.timeZone = "UTC";			// Default time zone for organisation
			}
			pstmt.setString(21, HtmlSanitise.checkCleanName(o.timeZone, localisation));
			
			pstmt.setBoolean(22, o.can_notify);
			pstmt.setBoolean(23, o.can_use_api);
			pstmt.setBoolean(24, o.can_submit);
			pstmt.setBoolean(25, o.appearance.set_as_theme);
			pstmt.setInt(26, o.e_id);
			pstmt.setString(27, "not set");		// backward navigation
			pstmt.setString(28, "not set");		// screen navigation
			pstmt.setString(29, "not set");		// Guidance
			pstmt.setString(30, "not set");		// image size
			pstmt.setString(31, "not set");		// send automatically
			pstmt.setString(32, "not set");		// FT delete after sending
			pstmt.setString(33, "not set");		// Send location
			pstmt.setInt(34, -1);				// Never require re-entry of FT password
			String navBarColor = o.appearance.navbar_color;
			if(navBarColor == null) {
				navBarColor =  Organisation.DEFAULT_NAVBAR_COLOR;
			}
			pstmt.setString(35,navBarColor);
			pstmt.setBoolean(36, o.can_sms);
			pstmt.setBoolean(37, o.send_optin);
			pstmt.setString(38, o.limits == null ? null : gson.toJson(o.limits));
			pstmt.setString(39, "not set");		// High Resolution Video
			pstmt.setInt(40, o.refresh_rate);
			pstmt.setDouble(41, o.password_strength);
			pstmt.setString(42, HtmlSanitise.checkCleanName(o.map_source, localisation));
			pstmt.setString(43, "not set");		// send automatically
			pstmt.setInt(44, 20);		// FT Geo Recording interval
			pstmt.setInt(45, 10);		// FT Geo Accuracy distance
			
			/*
			 * Set the owner only if this is a personal organisation.
			 * If it is being created by an organisational administrator then they do
			 * not get to keep ownership of the organisations if they lose org admin privilege, hence
			 * the owner would be set to zero.  In other words they are creating community organisations that
			 * will need to be maintained by whichever user has organisational admin privilege
			 */
			pstmt.setInt(46, GeneralUtilityMethods.hasSecurityGroup(sd, userIdent, Authorise.ORG_ID) ? 0 : GeneralUtilityMethods.getUserId(sd, userIdent));
			log.info("Insert organisation: " + pstmt.toString());
			pstmt.executeUpdate();
			
			rs = pstmt.getGeneratedKeys();
            if (rs.next()) {
            	o_id = rs.getInt(1);
            }
            rs.close();
            
			// Save the banner logo, if it has been passed
			if(bannerFileName != null) {			
				writeLogo(bannerFileName, bannerLogoItem, o_id, basePath, userIdent, requestUrl, "bannerLogo");
	        } 
			// Save the main logo, if it has been passed
			if(mainFileName != null) {			
				writeLogo(mainFileName, mainLogoItem, o_id, basePath, userIdent, requestUrl, "mainLogo");
			} 
			// Add this new organisation to the requesting users list of organisations
			if(userIdent != null) {
				int u_id = GeneralUtilityMethods.getUserId(sd, userIdent);
				if(u_id > 0) {
					pstmtAddOrgList = sd.prepareStatement(sqlAddOrgList);
					pstmtAddOrgList.setInt(1, u_id);
					pstmtAddOrgList.setInt(2, o_id);
					pstmtAddOrgList.executeUpdate();	
				}
			}
	            
		} finally {
			
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {}
			try {if (pstmtCheckExists != null) {pstmtCheckExists.close();} } catch (SQLException e) {}
			try {if (pstmtAddOrgList != null) {pstmtAddOrgList.close();} } catch (SQLException e) {}	
		}
		
		return o_id;
		
	}
	
	public void writeLogo( 
			String fileName, 
			FileItem logoItem,
			int oId,
			String basePath,
			String userIdent,
			String requestUrl,
			String storageName) {
		
		MediaInfo mediaInfo = new MediaInfo();
		mediaInfo.setFolder(basePath, userIdent, oId, true);				 
		mediaInfo.setServer(requestUrl);
		
		String folderPath = mediaInfo.getPath();
		fileName = mediaInfo.getFileName(fileName);
		if(folderPath != null) {						
			String filePath = folderPath + "/" + storageName;
		    File savedFile = new File(filePath);
		    log.info("Saving file to: " + filePath);
		    try {
				logoItem.write(savedFile);
				savedFile.setReadable(true);
				savedFile.setWritable(true);
				
				// Set access writes to the new file
				Process proc = Runtime.getRuntime().exec(new String [] {"/bin/sh", "-c", "chmod -R 777 " + filePath});
				int code = proc.waitFor();					        	
				if(code > 0) {
					int len;
					if ((len = proc.getErrorStream().available()) > 0) {
						byte[] buf = new byte[len];
						proc.getErrorStream().read(buf);
						log.info("Command error:\t\"" + new String(buf) + "\"");
					}
				} else {
					int len;
					if ((len = proc.getInputStream().available()) > 0) {
						byte[] buf = new byte[len];
						proc.getInputStream().read(buf);
						log.info("Completed setting access rights to new file process:\t\"" + new String(buf) + "\"");
					}
				}
	            
			} catch (Exception e) {
				e.printStackTrace();
			}
            
		} else {
			log.log(Level.SEVERE, "Media folder not found");
		}
	}
	
	public void deleteLogo( 		
			int oId,
			String basePath,
			String userIdent,
			String requestUrl,
			String storageName) {
		
		MediaInfo mediaInfo = new MediaInfo();
		mediaInfo.setFolder(basePath, userIdent, oId, true);				 
		mediaInfo.setServer(requestUrl);
		
		String folderPath = mediaInfo.getPath();

		if(folderPath != null) {						
			String filePath = folderPath + "/" + storageName;
		    File savedFile = new File(filePath);
		    log.info("Deleting file: " + filePath);
		    savedFile.delete();
		            
		} else {
			log.log(Level.SEVERE, "Logo to delete not found");
		}
	}

	public void updateSensitiveData( 
			Connection sd, 
			int oId,
			SensitiveData sensitiveData) throws SQLException {
		
		String sql = "update organisation set "
				+ "sensitive_data = ? "
				+ "where id = ?";
		PreparedStatement pstmt = null;
		
		try {
			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String data = gson.toJson(sensitiveData);
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, data);
			pstmt.setInt(2, oId);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
	}
	
	public void updateOtherOrgData( 
			Connection sd, 
			int oId,
			OtherOrgData otherData) throws SQLException {
		
		String sql = "update organisation set "
				+ "password_strength = ? "
				+ "where id = ?";
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, otherData.password_strength);
			pstmt.setInt(2, oId);
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
	}

	
	public MySensitiveData getMySensitiveData( 
			Connection sd, 
			String user) throws SQLException {
		
		MySensitiveData msd = new MySensitiveData();
		
		String sql = "select sensitive_data "
				+ "from organisation "
				+ "where "
				+ "id = (select o_id from users where ident = ?)";	
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, user);
			ResultSet rs = pstmt.executeQuery();
			
			Gson gson = new GsonBuilder().disableHtmlEscaping().create();
			if(rs.next()) {
				SensitiveData sensData = gson.fromJson(rs.getString(1), SensitiveData.class);
				if(sensData != null && sensData.signature != null && !sensData.signature.equals("none")) {
					if(sensData.signature.equals("admin_only")) {
						boolean isAdmin = GeneralUtilityMethods.hasSecurityGroup(sd, user, Authorise.ADMIN_ID);
						if(!isAdmin) {
							msd.signature = true;
						}
					}
				}
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return msd;
	}
	
	/*
	 * Get webform options
	 */
	public WebformOptions getWebform(Connection sd, String user) throws SQLException {
		
		WebformOptions webform = null;
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		
		String sql = "select webform "
				+ "from organisation "
				+ "where "
				+ "id = (select o_id from users where ident = ?)";
	
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, user);
					
			log.info("Get organisation webform options: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			if(rs.next()) {
				
				String wfString =  rs.getString("webform");
				if(wfString != null && wfString.trim().startsWith("{")) {
					webform = gson.fromJson(rs.getString("webform"), WebformOptions.class);
				}									
			} 
			if(webform == null) {
				webform = new WebformOptions();
			}
			
		} finally {			
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}	
		}
		
		return webform;
	}
	
	/*
	 * Get appearance options
	 */
	public AppearanceOptions getAppearance(Connection sd, String user) throws SQLException {
		
		String sql = "select set_as_theme, navbar_color, navbar_text_color, css "
				+ "from organisation "
				+ "where "
				+ "id = (select o_id from users where ident = ?)";
	
		PreparedStatement pstmt = null;
		
		AppearanceOptions ao = new AppearanceOptions();
		try {
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, user);
					
			log.info("Get organisation appearance options: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			if(rs.next()) {
				
				ao.set_as_theme = rs.getBoolean(1);
				ao.navbar_color = rs.getString(2);
				ao.navbar_text_color = rs.getString(3);
				ao.css = rs.getString(4);
			} 
			if(ao.navbar_color == null) {
				ao.navbar_color = Organisation.DEFAULT_NAVBAR_COLOR;
			}
			if(ao.navbar_text_color == null) {
				ao.navbar_text_color = Organisation.DEFAULT_NAVBAR_TEXT_COLOR;
			}
			
		} finally {			
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}	
		}
		
		return ao;
	}
	
	public DashboardDetails getDashboardDetails(Connection sd, String user) throws SQLException {
		DashboardDetails dbd = null;
		
		String sql = "select dashboard_region, dashboard_arn, dashboard_session_name "
				+ "from organisation "
				+ "where "
				+ "id = (select o_id from users where ident = ?)";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, user);
			
			log.info("Get organisation dashboard options: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			if(rs.next()) {
				dbd = new DashboardDetails();
				dbd.region = rs.getString("dashboard_region");
				dbd.roleArn = rs.getString("dashboard_arn");
				dbd.roleSessionName = rs.getString("dashboard_session_name");
			}
			
			
		} finally {			
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}	
		}
		return dbd;
	}
	
}
