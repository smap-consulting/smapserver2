package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.Alert;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.GroupSurvey;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.PasswordDetails;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.SubscriptionStatus;
import org.smap.sdal.model.User;
import org.smap.sdal.model.UserGroup;
import org.smap.sdal.model.UserSimple;

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
public class UserManager {

	private static Logger log =
			Logger.getLogger(UserManager.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	private ResourceBundle localisation;
	
	public static String STATUS_COMPLETE = "complete";
	public static String STATUS_EXPIRED = "expired";
	
	public UserManager(ResourceBundle l) {
		this.localisation = l;
	}
	
	/*
	 * Get the user details
	 */
	public User getByIdent(
			Connection sd,
			String ident
			) throws Exception {

		PreparedStatement pstmt = null;

		User user = new User ();
		int passwordAge = 0;
		int orgPasswordExpiry = 0;
		int serverPasswordExpiry = 0;
		int passwordExpiry = 0;

		try {
			String sql = null;
			ResultSet resultSet = null;			

			/*
			 * Get the user details
			 */
			sql = "SELECT u.id as id, "
					+ "u.name as name, "
					+ "u.settings as settings, "
					+ "u.signature as signature, "
					+ "u.language as language, "
					+ "u.email as email, "
					+ "u.current_project_id, "
					+ "u.current_survey_id, "
					+ "u.current_survey_ident, "
					+ "u.current_task_group_id, "
					+ "u.lastalert, "
					+ "u.seen,"
					+ "extract(year from age(now(), u.password_set)) * 12 + extract(month from age(now(), u.password_set)) as password_age,"
					+ "o.id as o_id, "
					+ "o.name as organisation_name, "
					+ "o.company_name as company_name, "
					+ "o.company_address as company_address, "
					+ "o.company_phone as company_phone, "
					+ "o.company_email as company_email, "
					+ "o.allow_email, "
					+ "o.allow_facebook, "
					+ "o.allow_twitter, "
					+ "o.can_edit, "
					+ "o.email_task, "
					+ "o.ft_send_location, "
					+ "o.billing_enabled,"
					+ "o.e_id,"
					+ "o.set_as_theme,"
					+ "o.navbar_color,"
					+ "o.navbar_text_color,"
					+ "o.training,"
					+ "u.timezone as timezone,"
					+ "o.timezone as org_timezone,"
					+ "o.refresh_rate,"
					+ "e.name as enterprise_name,"
					+ "o.ft_input_method, "
					+ "o.ft_im_ri, "
					+ "o.ft_im_acc, "
					+ "o.password_expiry "
					+ "from users u, organisation o, enterprise e "
					+ "where u.ident = ? "
					+ "and u.o_id = o.id "
					+ "and o.e_id = e.id "
					+ "order by u.ident"; 

			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ident);
			log.info("Get user details: " + pstmt.toString());
			resultSet = pstmt.executeQuery();

			while(resultSet.next()) {
				user.id = resultSet.getInt("id");
				user.ident = ident;
				user.name = resultSet.getString("name");
				user.settings = resultSet.getString("settings");
				String sigFile = resultSet.getString("signature");

				if(sigFile != null) {
					sigFile= sigFile.trim();
					if(sigFile.startsWith("/")) {	// Old versions of smap stored a URL rather than the file name, get the file name if this is the case
						int idx = sigFile.lastIndexOf("/");
						sigFile = sigFile.substring(idx + 1);
					}
					user.signature = sigFile;
				}
				user.language = resultSet.getString("language");
				user.email = resultSet.getString("email");
				user.current_project_id = resultSet.getInt("current_project_id");
				user.current_survey_id = resultSet.getInt("current_survey_id");
				user.current_survey_ident = resultSet.getString("current_survey_ident");
				user.current_task_group_id = resultSet.getInt("current_task_group_id");
				user.o_id = resultSet.getInt("o_id");
				user.e_id = resultSet.getInt("e_id");
				user.organisation_name = resultSet.getString("organisation_name");
				user.company_name = resultSet.getString("company_name");
				user.company_address = resultSet.getString("company_address");
				user.company_phone = resultSet.getString("company_phone");
				user.company_email = resultSet.getString("company_email");
				user.allow_email = resultSet.getBoolean("allow_email");
				user.allow_facebook = resultSet.getBoolean("allow_facebook");
				user.allow_twitter = resultSet.getBoolean("allow_twitter");
				user.can_edit = resultSet.getBoolean("can_edit");
				user.email_task = resultSet.getBoolean("email_task");
				user.ft_send_location = resultSet.getString("ft_send_location");
				user.lastalert = resultSet.getString("lastalert");
				user.seen = resultSet.getBoolean("seen");
				user.billing_enabled = resultSet.getBoolean("billing_enabled");
				user.timezone = resultSet.getString("timezone");
				if(user.timezone == null || user.timezone.trim().equals("")) {
					user.timezone = resultSet.getString("org_timezone");
				}
				user.enterprise_name = resultSet.getString("enterprise_name");
				user.set_as_theme = resultSet.getBoolean("set_as_theme");
				user.navbar_color = resultSet.getString("navbar_color");
				user.navbar_text_color = resultSet.getString("navbar_text_color");
				user.training = resultSet.getString("training");
				user.refresh_rate = resultSet.getInt("refresh_rate");
				user.ft_input_method = resultSet.getString("ft_input_method");
				user.ft_im_ri = resultSet.getInt("ft_im_ri");
				user.ft_im_acc = resultSet.getInt("ft_im_acc");		
				
				passwordAge = resultSet.getInt("password_age");
				orgPasswordExpiry = resultSet.getInt("password_expiry");
			}

			/*
			 * Set a flag if email is enabled on the server
			 */
			user.sendEmail = UtilityMethodsEmail.getSmtpHost(sd, null, ident, 0) != null;

			/*
			 * Get the security groups that the user belongs to
			 */
			sql = "SELECT g.id as id, g.name as name " +
					" from groups g, user_group ug " +
					" where g.id = ug.g_id " +
					" and ug.u_id = ? " +
					" order by g.name;";

			if(pstmt != null) try {pstmt.close();} catch(Exception e) {};
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, user.id);
			log.info("SQL: " + pstmt.toString());
			resultSet = pstmt.executeQuery();

			boolean canResetPassword = false;	// Only reset passwords of users with specific groups
			while(resultSet.next()) {
				if(user.groups == null) {
					user.groups = new ArrayList<UserGroup> ();
				}
				UserGroup group = new UserGroup();
				group.id = resultSet.getInt("id");
				group.name = resultSet.getString("name");
				user.groups.add(group);
				if(group.name.equals(Authorise.ADMIN) 
						|| group.name.equals(Authorise.ANALYST)
						|| group.name.equals(Authorise.ORG)
						|| group.name.equals(Authorise.SECURITY)) {
					canResetPassword = true;
				} 
			}

			/*
			 * Get the projects that the user belongs to
			 */
			sql = "SELECT p.id as id, p.name as name " +
					" from project p, user_project up " +
					" where p.id = up.p_id " +
					" and up.u_id = ? " +
					" order by p.name;";

			if(pstmt != null) try {pstmt.close();} catch(Exception e) {};
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, user.id);

			log.info("SQL: " + pstmt.toString());
			resultSet = pstmt.executeQuery();

			while(resultSet.next()) {
				if(user.projects == null) {
					user.projects = new ArrayList<Project> ();
				}
				Project project = new Project();
				project.id = resultSet.getInt("id");
				project.name = resultSet.getString("name");
				user.projects.add(project);
			}		

			/*
			 * Get the roles that the user belongs to
			 */
			user.roles = getUserRoles(sd, user.id);
			
			/*
			 * Get the current survey - group survey relationships
			 */
			sql = "SELECT s_id, group_ident, f_name " +
					" from group_survey " +
					" where u_ident = ?";

			if(pstmt != null) try {pstmt.close();} catch(Exception e) {};
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, ident);

			resultSet = pstmt.executeQuery();

			while(resultSet.next()) {
				if(user.groupSurveys == null) {
					user.groupSurveys = new ArrayList<GroupSurvey> ();
				}
				user.groupSurveys.add(new GroupSurvey(resultSet.getInt(1), resultSet.getString(2), resultSet.getString(3)));
			}
			
			/*
			 * Get the organisations that the user belongs to
			 */
			if(user.orgs == null) {
				user.orgs = new ArrayList<Organisation> ();
			}
			getUserOrganisations(sd, user.orgs, user, user.id);

			/*
			 * Check for password expiry
			 * Only expire admin and analyst passwords (revise this)
			 */
			if(canResetPassword) {
				user.passwordExpired = false;
				if(orgPasswordExpiry > 0 && passwordAge >= orgPasswordExpiry) {
					user.passwordExpired = true;
					passwordExpiry = orgPasswordExpiry;
				} else {
					sql = "select password_expiry from server";
					if(pstmt != null) try {pstmt.close();} catch(Exception e) {};
					pstmt = sd.prepareStatement(sql);
					resultSet = pstmt.executeQuery();
					if(resultSet.next()) {
						serverPasswordExpiry = resultSet.getInt("password_expiry");
						if(serverPasswordExpiry > 0 && passwordAge >= serverPasswordExpiry) {
							user.passwordExpired = true;
							passwordExpiry = serverPasswordExpiry;
						}
					}
				}
				if(user.passwordExpired) {
					String msg = localisation.getString("ar_pwd_expiry");
					msg = msg.replace("%s1", user.name);
					msg = msg.replace("%s2", String.valueOf(passwordAge));
					msg = msg.replace("%s3", String.valueOf(passwordExpiry));
					lm.writeLogOrganisation(sd, user.o_id, ident, LogManager.USER, msg, 0);
					log.info(msg);
				}
			}
			

		} finally {
			try {if (pstmt != null) {pstmt.close();}	} catch (SQLException e) {}
		}

		return user;

	}

	/*
	 * Get the roles a user belongs to
	 */
	public ArrayList<Role> getUserRoles(Connection sd, int uId) throws SQLException {

		ArrayList<Role> roles = null;
		
		String sql = "SELECT r.id as id, r.name as name " +
				" from role r, user_role ur " +
				" where r.id = ur.r_id " +
				" and ur.u_id = ? " +
				" order by r.name asc";
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, uId);
	
			log.info("SQL: " + pstmt.toString());
			ResultSet resultSet = pstmt.executeQuery();
	
			while(resultSet.next()) {
				if(roles == null) {
					roles = new ArrayList<Role> ();
				}
				Role role = new Role();
				role.id = resultSet.getInt("id");
				role.name = resultSet.getString("name");
				roles.add(role);
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}
		return roles;
	}

	/*
	 * Get alerts for a user
	 */
	public ArrayList<Alert> getAlertsByIdent(
			Connection connectionSD,
			String ident
			) throws Exception {

		PreparedStatement pstmt = null;

		ArrayList<Alert> alerts = new ArrayList<Alert> ();

		try {
			String sql = null;
			ResultSet resultSet = null;			

			/*
			 * Get the user details
			 */
			sql = "SELECT "
					+ "a.id as id, "
					+ "a.status as status, "
					+ "a.priority as priority, "
					+ "a.updated_time as updated_time, "
					+ "a.link as link, "
					+ "a.message as message, "
					+ "extract(epoch from (now() - a.updated_time)) as since "
					+ "from alert a, users u "
					+ "where a.u_id = u.id "
					+ "and u.ident = ? "
					+ "order by a.updated_time asc";

			pstmt = connectionSD.prepareStatement(sql);
			pstmt.setString(1, ident);

			log.info("Get alert details: " + pstmt.toString());
			resultSet = pstmt.executeQuery();

			while(resultSet.next()) {
				Alert a = new Alert();
				a.id = resultSet.getInt("id");
				a.userIdent = ident;
				a.status = resultSet.getString("status");
				a.priority = resultSet.getInt("priority");
				a.link = resultSet.getString("link");
				a.message = resultSet.getString("message");
				a.updatedTime = resultSet.getString("updated_time");
				a.since = resultSet.getInt("since");

				alerts.add(a);
			}



		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
			throw new Exception(e);

		} finally {
			try {
				if (pstmt != null) {
					pstmt.close();
				}
			} catch (SQLException e) {

			}

		}

		return alerts;

	}
	
	/*
	 * Get alerts for a user
	 */
	public String getUserEmailByIdent(
			Connection connectionSD,
			String ident
			) throws Exception {

		PreparedStatement pstmt = null;

		String email = null;

		try {
	
			String sql = "select email from users where ident = ?";
				
			pstmt = connectionSD.prepareStatement(sql);
			pstmt.setString(1, ident);

			log.info("Get users email: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();

			if(rs.next()) {
				email = rs.getString(1);
			}



		} catch (Exception e) {
			log.log(Level.SEVERE,"Error", e);
			throw new Exception(e);

		} finally {
			try {
				if (pstmt != null) {
					pstmt.close();
				}
			} catch (Exception e) {

			}

		}

		return email;

	}

	/*
	 * Create a new user Parameters:
	 *   u: Details of the new user
	 *   isOrgUser:  Set to true if this user should be an organisational administrator
	 *   userIdent:  The ident of the user creating this user
	 *   serverName: The name of the server they are being created on
	 *   adminName:  The full name of the user creating this user
	 */
	public int createUser(Connection sd, 
			User u, 
			int o_id, 
			boolean isOrgUser, 
			boolean isSecurityManager,
			boolean isEnterpriseManager,
			boolean isServerOwner,
			String userIdent,
			String scheme,
			String serverName,
			String adminName,
			String adminEmail,
			ResourceBundle localisation) throws Exception {

		// Before creating the user check that email is available if it has been requested
		EmailServer emailServer = null;
		SubscriptionStatus subStatus = null;
		if(u.sendEmail) {
			emailServer = UtilityMethodsEmail.getSmtpHost(sd, null, userIdent, o_id);
			if(emailServer.smtpHost == null) {
				throw new Exception(localisation.getString("email_ne2"));
			}
			PeopleManager pm = new PeopleManager(localisation);
			subStatus = pm.getEmailKey(sd, o_id, u.email);
			if(subStatus.unsubscribed) {
				// Person has unsubscribed
				String msg = localisation.getString("email_us");
				msg = msg.replaceFirst("%s1", u.email);
				throw new ApplicationException(msg);
			}
		}

		int u_id = -1;
		String sql = "insert into users (ident, realm, name, email, o_id, imported, "
				+ "language, password, basic_password, created, password_set) " +
				" values (?, ?, ?, ?, ?, ?, ?, "
				+ "md5(?),"
				+ "'{SHA}'|| encode(digest(?,'sha1'),'base64'),"
				+ " now(), now());";

		PreparedStatement pstmt = null;

		try {
			PasswordManager pwm = new PasswordManager(sd, localisation.getLocale(), localisation, userIdent, serverName);	// For new users use the ident of the user creating this user
			pwm.checkStrength(u.password);
			
			String pwdString = u.ident + ":smap:" + u.password;
			String language = u.language;
			if(language == null || language.trim().length() == 0) {
				/*
				 * Attempt to get the language from the default organisation language
				 */
				Organisation o = GeneralUtilityMethods.getOrganisation(sd, o_id);
				language = o.locale;
			}
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1, HtmlSanitise.checkCleanName(u.ident, localisation));
			pstmt.setString(2, "smap");
			pstmt.setString(3, HtmlSanitise.checkCleanName(u.name, localisation));
			pstmt.setString(4, HtmlSanitise.checkCleanName(u.email, localisation));
			pstmt.setInt(5, o_id);
			pstmt.setBoolean(6,u.imported);
			pstmt.setString(7,  HtmlSanitise.checkCleanName(language, localisation));
			pstmt.setString(8, pwdString);
			pstmt.setString(9, u.password);
			log.info("SQL: " + pstmt.toString());
			pstmt.executeUpdate();

			ResultSet rs = pstmt.getGeneratedKeys();
			if (rs.next()){
				u_id = rs.getInt(1);
				insertUserGroupsProjects(sd, u, u_id, isOrgUser, isSecurityManager, isEnterpriseManager, isServerOwner);
				insertUserOrganisations(sd, u, u_id, o_id, isOrgUser, userIdent);
			}

			// Send a notification email to the user
			if(u.sendEmail) {
				log.info("Checking to see if email enabled: " + u.sendEmail);

				Organisation organisation = UtilityMethodsEmail.getOrganisationDefaults(sd, null, userIdent);

				String subject = localisation.getString("email_ac") + " " + serverName;
				String uuid = UtilityMethodsEmail.setOnetimePassword(sd, pstmt, u.email, "48 hours");
				EmailManager em = new EmailManager(localisation);
				
				HashMap<String, String> customTokens = new HashMap<> ();
				StringBuilder template = new StringBuilder(localisation.getString("email_newuser"));
				template.append(" ").append(localisation.getString("email_un2"));
				
				StringBuilder content = new StringBuilder(template.toString());
				String serverUrl = scheme + "://" + serverName;
				
				// add custom tokens
				customTokens.put("${admin}", adminName);
				if(organisation.server_description == null || organisation.server_description.trim().isEmpty()) {
					organisation.server_description = localisation.getString("email_hga2");
				} 
				customTokens.put("${server_description}", organisation.server_description);
				customTokens.put("${server_url}", serverUrl);
				
				StringBuilder passwordUrl = new StringBuilder("");
				passwordUrl.append("<a href=\"https://")
				.append(serverName)
				.append("/app/resetPassword.html?token=")
				.append(uuid)
				.append("\">")
				.append(localisation.getString("email_link"))
				.append("</a>");
				customTokens.put("${set_password_url}", passwordUrl.toString());
				
				customTokens.put("${user_ident}", u.ident);
				customTokens.put("${valid_for}", "48");

				log.info("Content: " + content);
				log.info("tokens: " + customTokens.toString());
				em.sendEmailHtml(
						u.email, 
						"bcc", 
						subject, 
						content, 
						null, 
						null, 
						emailServer,
						serverName,
						subStatus.emailKey,
						localisation,
						customTokens,
						organisation.getAdminEmail(),
						organisation.getEmailFooter(),
						GeneralUtilityMethods.getNextEmailId(sd));

			}
			
			// Log this event
			String msg = localisation.getString("log_uc");
			if(msg != null && u.ident != null) {
				msg = msg.replace("%s1", u.ident);
			}
			lm.writeLogOrganisation(sd, o_id, userIdent, LogManager.CREATE, msg, 0);
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}

		}
		return u_id;
	}

	/*
	 * Create a new temporary user
	 */
	public int createTemporaryUser(Connection sd, 
			User u, 
			int o_id) throws Exception {

		log.info("############# Creating temporary user");
		
		int u_id = -1;
		String sql = "insert into users "
				+ "(ident, o_id, email, name, temporary, action_details, single_submission, created) "
				+ "values (?, ?, ?, ?, true, ?, ?, now()) ";

		PreparedStatement pstmt = null;

		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

		try {
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1, HtmlSanitise.checkCleanName(u.ident, localisation));
			pstmt.setInt(2, o_id);
			pstmt.setString(3, HtmlSanitise.checkCleanName(u.email, localisation));
			pstmt.setString(4, HtmlSanitise.checkCleanName(u.name, localisation));
			
			// Validate action name before saving action details
			if(u.action_details != null) {
				u.action_details.validateNames(localisation);
			}
			pstmt.setString(5, gson.toJson(u.action_details));
			pstmt.setBoolean(6, u.singleSubmission);
			log.info("SQL: " + pstmt.toString());
			pstmt.executeUpdate();

			ResultSet rs = pstmt.getGeneratedKeys();
			if (rs.next()){
				u_id = rs.getInt(1);
				insertUserGroupsProjects(sd, u, u_id, false, true, false, false);		// The user roles are sourced from the action and have been added by a security manager hence we will act as a security manager here
			}

		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}

		}
		return u_id;
	}

	/*
	 * Get the organisations that a user belongs to
	 */
	public void getUserOrganisations(Connection sd, ArrayList<Organisation> orgs, User user, int uId) throws SQLException {
		
		String sql = "SELECT o.id as id, o.name as name " +
				" from organisation o, user_organisation uo " +
				" where o.id = uo.o_id " +
				" and uo.u_id = ? " +
				" order by o.name asc";

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, uId);
	
			log.info("SQL: " + pstmt.toString());
			ResultSet resultSet = pstmt.executeQuery();
	
			while(resultSet.next()) {			
				Organisation o = new Organisation();
				o.id = resultSet.getInt("id");
				o.name = resultSet.getString("name");
				orgs.add(o);
			}
			if(user != null && orgs.size() == 0) {
				// If not organisation then add the users current organisation
				Organisation o = new Organisation();
				o.id = user.o_id;
				o.name = user.organisation_name;
				user.orgs.add(o);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
	}
	
	/*
	 * Update a users details
	 */
	public void updateUser(Connection sd, 
			User u, 						// New details for user being updated
			int adminUserOrgId, 			// Organisation Id of administrator updating the user
			boolean isOrgUser, 
			boolean isSecurityManager,
			boolean isEnterpriseManager,
			boolean isServerOwner,
			String userIdent,
			String serverName,
			String adminName,
			boolean isSwitch) throws Exception {

		int currentUserOrgId = 0;		// Current logged in organisation of the user to be updated

		// Check the user is in the same organisation as the administrator doing the editing
		String sql = "select u.id "
					+ "from users u " 
					+ "where u.id = ? "
					+ "and u.o_id = ? "
				+ "union "
					+ "select uo.u_id "
					+ "from user_organisation uo "
					+ "where uo.u_id = ? "
					+ "and uo.o_id = ?";				
		PreparedStatement pstmt = null;

		String sqlGetOrgId = "select o_id from users where id = ?";
		PreparedStatement pstmtGetOrgId = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, u.id);
			pstmt.setInt(2, adminUserOrgId);
			pstmt.setInt(3, u.id);
			pstmt.setInt(4, adminUserOrgId);
			log.info("Validate user in correct organisation: " + pstmt.toString());
			ResultSet resultSet = pstmt.executeQuery();

			if(resultSet.next()) {

				// Get the current organisation of the user
				pstmtGetOrgId = sd.prepareStatement(sqlGetOrgId);
				pstmtGetOrgId.setInt(1, u.id);
				ResultSet rs2 = pstmtGetOrgId.executeQuery();
				if(rs2.next()) {
					currentUserOrgId = rs2.getInt(1);
				}
				
				/*
				 * If the organisation id is zero
				 * then it will be for the organisation of the person doing the update
				 * However if the current organisation is not in the list of organisations then the user has access to
				 * then their organisation acess is being removed.
				 */
				if(u.o_id == 0) {
					u.o_id = adminUserOrgId;
				}
				
				// Update the saved settings for this user
				updateSavedSettings(sd, u, u.id, u.o_id, isOrgUser, isSecurityManager);
				
				
				/*
				 * Verify that the password is strong enough
				 */
				PasswordManager pwm = null;;
				if(u.password != null) {
					// Note password rules for the users current organisation will be used
					pwm = new PasswordManager(sd, localisation.getLocale(), localisation, u.ident, serverName);
					pwm.checkStrength(u.password);	
				}
				
				/*
				 * Update the current settings if the organisation to be updated is the same
				 * as the current organisation
				 */
				if((adminUserOrgId == currentUserOrgId) || isSwitch) {
					
					// update the current settings for the user
					String pwdString = null;
					if(u.password == null) {
						// Do not update the password
						sql = "update users set "
								+ "ident = ?, "
								+ "realm = ?, "
								+ "name = ?, " 
								+ "email = ?, "
								+ "o_id = ? "
								+ "where "
								+ "id = ?";
					} else {
						// Update the password
						
						sql = "update users set "
								+ "ident = ?, "
								+ "realm = ?, "
								+ "name = ?, " 
								+ "email = ?, "
								+ "o_id = ?, "
								+ "password = md5(?), "
								+ "basic_password = '{SHA}'|| encode(digest(?,'sha1'),'base64'), "
								+ "password_set = now() "
								+ "where "
								+ "id = ?";
	
						pwdString = u.ident + ":smap:" + u.password;
						
						// Delete any session keys for this user
						GeneralUtilityMethods.deleteAccessKeys(sd, u.ident);
					}
	
					try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
					pstmt = sd.prepareStatement(sql);
					pstmt.setString(1, HtmlSanitise.checkCleanName(u.ident, localisation));
					pstmt.setString(2, "smap");
					pstmt.setString(3, HtmlSanitise.checkCleanName(u.name, localisation));
					pstmt.setString(4, HtmlSanitise.checkCleanName(u.email, localisation));
					pstmt.setInt(5, u.o_id);
					if(u.password == null) {
						pstmt.setInt(6, u.id);
					} else {
						pstmt.setString(6, pwdString);
						pstmt.setString(7, u.password);
						pstmt.setInt(8, u.id);
					}

					log.info("Update user details: " + pstmt.toString());
					pstmt.executeUpdate();

					// Update the groups, projects and roles
					insertUserGroupsProjects(sd, u, u.id, isOrgUser, isSecurityManager, isEnterpriseManager, isServerOwner);
					if(!isSwitch) {
						insertUserOrganisations(sd, u, u.id, u.o_id, isOrgUser, userIdent);
					}
					
					if(pwm != null) {
						pwm.logReset();		// Record the successful password reset
					}
					
				} else {
					// update the list of organisation that the user has access to.  These are always stored as current
					// This is the only change permissable if the user is in another organisation
					if(!isSwitch) {
						insertUserOrganisations(sd, u, u.id, u.o_id, isOrgUser, userIdent);
					}
				}

			} else {
				throw new Exception("Invalid user");
			}
		} finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {}
			try {if (pstmtGetOrgId != null) {pstmtGetOrgId.close();} } catch (SQLException e) {}
		}
	}

	public Response setPassword(Connection sd, Locale locale, ResourceBundle localisation, 
			String ident, String serverName, PasswordDetails pwd) {
		
		Response response = null;
		
		PreparedStatement pstmt = null;
		try {	
			
			/*
			 * Update what can be updated by the user, excluding the current project id, survey id, form id and task group
			 */
			String pwdString = null;
			String sql = null;
			PasswordManager pwm  = null;
			
			/*
			 * Verify that the password is strong enough
			 */
			pwm = new PasswordManager(sd, locale, localisation, ident, serverName);
			pwm.checkStrength(pwd.password);
				
			sql = "update users set "
					+ "password = md5(?), "
					+ "basic_password = '{SHA}'|| encode(digest(?,'sha1'),'base64'), "
					+ "password_set = now() "
					+ "where "
					+ "ident = ?";
				
			pwdString = ident + ":smap:" + pwd.password;
				
			// Delete any session keys for this user
			GeneralUtilityMethods.deleteAccessKeys(sd, ident);
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, pwdString);
			pstmt.setString(2, pwd.password);
			pstmt.setString(3, ident);
		
			log.info("Update password: " + pstmt.toString());
			pstmt.executeUpdate();
			
			// Write logs
			log.info("userevent: " + ident + " updated password : " + ident);
			pwm.logReset();
			lm.writeLog(sd, -1, ident, LogManager.USER_DETAILS, localisation.getString("msg_pwd_changed"), 0, serverName);
		
			response = Response.ok().build();
			
			
		} catch (Exception e) {

			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
			
		} finally {			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return response;
	}

	private void insertUserGroupsProjects(Connection sd, User u, int u_id, 
			boolean isOrgUser, 
			boolean isSecurityManager,
			boolean isEnterpriseManager,
			boolean isServerOwner) throws SQLException {

		String sql;
		PreparedStatement pstmt = null;
		PreparedStatement pstmtInsertUserGroup = null;
		PreparedStatement pstmtInsertUserRole = null;
		PreparedStatement pstmtInsertProjectGroup = null;

		log.info("Update groups and projects user id:" + u_id);

		boolean calledFromTransaction = !sd.getAutoCommit();  // Manage auto commit
		
		// Delete existing user groups
		try {

			String sqlInsertUserGroup = "insert into user_group (u_id, g_id) values (?, ?);";
			pstmtInsertUserGroup = sd.prepareStatement(sqlInsertUserGroup);
			pstmtInsertUserGroup.setInt(1, u_id);

			String sqlInsertUserRole = "insert into user_role (u_id, r_id) values (?, ?);";
			pstmtInsertUserRole = sd.prepareStatement(sqlInsertUserRole);
			pstmtInsertUserRole.setInt(1, u_id);

			String sqlInsertProjectGroup = "insert into user_project (u_id, p_id) values (?, ?);";
			pstmtInsertProjectGroup = sd.prepareStatement(sqlInsertProjectGroup);
			pstmtInsertProjectGroup.setInt(1, u_id);

			/*
			 * Update user groups
			 */
			log.info("Set autocommit false");
			sd.setAutoCommit(false);
			if(isServerOwner || isEnterpriseManager) {	// Cannot remove server owner
				sql = "delete from user_group where u_id = ? "
						+ " and g_id != " + Authorise.OWNER_ID;	
			} else if(isOrgUser) {		// Cannot remove enterprise admin and server owner
				sql = "delete from user_group where u_id = ? "
						+ " and g_id != " + Authorise.ENTERPRISE_ID 
						+ " and g_id != " + Authorise.OWNER_ID;
			} else if(isSecurityManager) {	// Cannot remove org admin, enterprise admin and server owner
				sql = "delete from user_group where u_id = ? "
						+ " and g_id != " + Authorise.ORG_ID 
						+ " and g_id != " + Authorise.ENTERPRISE_ID 
						+ " and g_id != " + Authorise.OWNER_ID;	
			}  else {		// Admin user
				sql = "delete from user_group where u_id = ? "
						+ " and g_id != " + Authorise.ORG_ID 
						+ " and g_id != " + Authorise.SECURITY_ID 
						+ " and g_id != " + Authorise.ENTERPRISE_ID 
						+ " and g_id != " + Authorise.OWNER_ID;	
			}

			if(u.groups != null) {
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, u.id);
				log.info("SQL: " + pstmt.toString());
				pstmt.executeUpdate();

				for(int j = 0; j < u.groups.size(); j++) {
					UserGroup g = u.groups.get(j);
					
					if(g.id == Authorise.OWNER_ID) {
						continue;	// Owner id not managed via this service
					}
					// Only insert security groups that the user is authorised to insert
					if(isServerOwner 
							|| (isOrgUser && g.id != Authorise.ENTERPRISE_ID && g.id != Authorise.OWNER_ID)
							|| (isSecurityManager && g.id != Authorise.ORG_ID && g.id != Authorise.ENTERPRISE_ID && g.id != Authorise.OWNER_ID)
							|| (isEnterpriseManager && g.id != Authorise.OWNER_ID)
							|| (g.id != Authorise.SECURITY_ID && g.id != Authorise.ORG_ID && g.id != Authorise.ENTERPRISE_ID && g.id != Authorise.OWNER_ID)) {
							
						pstmtInsertUserGroup.setInt(2, g.id);
						log.info("Insert user group: " + pstmtInsertUserGroup.toString());
						pstmtInsertUserGroup.executeUpdate();
						
					}
				}
				
			} else {
				log.info("No user groups");
			}

			// Delete existing user projects
			if(u.projects != null) {
				sql = "delete from user_project where u_id = ?;";
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, u.id);
				log.info("SQL: " + pstmt.toString());
				pstmt.executeUpdate();

				for(int j = 0; j < u.projects.size(); j++) {
					Project p = u.projects.get(j);

					pstmtInsertProjectGroup.setInt(2, p.id);
					pstmtInsertProjectGroup.executeUpdate();

				}

			} else {
				log.info("No projects to add");
			}

			/*
			 * Update user roles
			 */
			if((isOrgUser || isSecurityManager) && u.roles != null) {
				sql = "delete from user_role where u_id = ?;";

				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, u.id);
				log.info("SQL add roles: " + pstmt.toString());
				pstmt.executeUpdate();

				for(int j = 0; j < u.roles.size(); j++) {
					Role r = u.roles.get(j);
					pstmtInsertUserRole.setInt(2, r.id);
					log.info("Insert user role: " + pstmtInsertUserRole.toString());
					pstmtInsertUserRole.executeUpdate();
				}
			}
			
			// Commit changes unless we are already in a transaction
			if(!calledFromTransaction) {
				sd.commit();
			}

		} catch (Exception e) {
			if(!calledFromTransaction) {
				try{sd.rollback();} catch(Exception ex) {}
			}
			throw e;
		} finally {
			if(!calledFromTransaction) {
				log.info("Set autocommit true");
				sd.setAutoCommit(true);
			}
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtInsertUserGroup != null) {pstmtInsertUserGroup.close();}} catch (SQLException e) {}
			try {if (pstmtInsertUserRole != null) {pstmtInsertUserRole.close();}} catch (SQLException e) {}
			try {if (pstmtInsertProjectGroup != null) {pstmtInsertProjectGroup.close();}} catch (SQLException e) {}
		}

	}
	
	private void insertUserOrganisations(Connection sd, User u, int u_id, int o_id, boolean isOrgUser, String userIdent) throws SQLException {

		StringBuilder sql;
		PreparedStatement pstmtDelete = null;
		PreparedStatement pstmtInsertOrgUser = null;
		PreparedStatement pstmtValidate = null;

		log.info("Update organisations for user id:" + u_id);
		log.info("Autocommit: " + sd.getAutoCommit());
		
		boolean calledFromTransaction = !sd.getAutoCommit();  // Manage auto commit
		
		try {

			int initiatorUserId = 0;
			if(!isOrgUser) {
				initiatorUserId = GeneralUtilityMethods.getUserId(sd, userIdent);
			}
			/*
			 * Create a statement to insert a relationship between a user and an organisation
			 */
			String sqlInsertOrgUser = "insert into user_organisation (u_id, o_id) values (?, ?)";
			pstmtInsertOrgUser = sd.prepareStatement(sqlInsertOrgUser);
			pstmtInsertOrgUser.setInt(1, u_id);
			
			/*
			 * First delete all links in the users current enterprise that are not in the list of 
			 * organisation ids that have been provided
			 * If it is not an admin user only delete personal organisation links
			 */
			int e_id = GeneralUtilityMethods.getEnterpriseId(sd, u.ident);
			sql = new StringBuilder("delete from user_organisation "
					+ "where u_id = ? "
					+ "and o_id != all (?) "
					+ "and o_id in (select id from organisation o where e_id = ?");
			if(isOrgUser) {
				sql.append(")");
			} else {
				sql.append(" and owner = ?)");
			}
			
			if(u.orgs != null) {
				log.info("Set autocommit false");
				sd.setAutoCommit(false);
				
				ArrayList<Integer> orgList = new ArrayList<Integer> ();
				for(int j = 0; j < u.orgs.size(); j++) {
					orgList.add(u.orgs.get(j).id);
				}
				
				int idx = 1;
				pstmtDelete = sd.prepareStatement(sql.toString());
				pstmtDelete.setInt(idx++, u.id);
				pstmtDelete.setArray(idx++, sd.createArrayOf("int", orgList.toArray(new Integer[orgList.size()])));
				pstmtDelete.setInt(idx++, e_id);
				if(!isOrgUser) {
					pstmtDelete.setInt(idx++, initiatorUserId);
				}
				log.info("Delete remove unset org links: " + pstmtDelete.toString());
				pstmtDelete.executeUpdate();

				/*
				 * Create statement to check if the initiating user has permission for this action
				 */
				sql = new StringBuilder("select count(*) from organisation where e_id = ? and id = ? "
						+ "and id not in (select o_id from user_organisation where u_id = ?)");
				if(!isOrgUser) {
					sql.append(" and owner = ?");
				}
				pstmtValidate = sd.prepareStatement(sql.toString());
				pstmtValidate.setInt(1,  e_id);
				if(!isOrgUser) {
					pstmtValidate.setInt(4, initiatorUserId);
				}
				
				// Create an entry for the users current organisation
				if(isValidInsert(pstmtValidate, o_id, u_id)) {
					pstmtInsertOrgUser.setInt(2, o_id);
					log.info("Inserting org link: " + pstmtInsertOrgUser.toString());
					try {
						pstmtInsertOrgUser.executeUpdate();
					} catch (SQLException e) {
						if(!e.getSQLState().equals("23505")) {
							log.log(Level.SEVERE, e.getMessage(), e);
						}
					}
				}
				for(int j = 0; j < u.orgs.size(); j++) {
					if(u.orgs.get(j).id != o_id) {
						if(isValidInsert(pstmtValidate, u.orgs.get(j).id, u.id)) {

							pstmtInsertOrgUser.setInt(2, u.orgs.get(j).id);
							log.info("Inserting org link: " + pstmtInsertOrgUser.toString());
							try {
								pstmtInsertOrgUser.executeUpdate();
							} catch (SQLException e) {
								if(!e.getSQLState().equals("23505")) {
									log.log(Level.SEVERE, e.getMessage(), e);
								}
							}
						}

					}
				}
				
				if(!calledFromTransaction) {
					sd.commit();
				}
				
			} else {
				log.info("No user groups");
			}
		
			
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			if(!calledFromTransaction) {
				try{sd.rollback();} catch(Exception ex) {}
			}
		} finally {
			if(!calledFromTransaction) {
				log.info("Set autocommit true");
				sd.setAutoCommit(true);
			}
			
			try {if (pstmtDelete != null) {pstmtDelete.close();}} catch (SQLException e) {}
			try {if (pstmtInsertOrgUser != null) {pstmtInsertOrgUser.close();}} catch (SQLException e) {}
			try {if (pstmtValidate != null) {pstmtValidate.close();}} catch (SQLException e) {}
		}

	}
	
	private boolean isValidInsert(PreparedStatement pstmtValidate, int o_id, int u_id) throws SQLException {
		boolean valid = false;
		pstmtValidate.setInt(2, o_id);
		pstmtValidate.setInt(3, u_id);
		ResultSet rs = pstmtValidate.executeQuery();
		log.info("Validate update to organisation link: " + pstmtValidate.toString() );
		if(rs.next()) {
			if(rs.getInt(1) > 0) {
				valid = true;
			}
		}
		rs.close();
		return valid;
	}
	
	public void deleteSingleSubmissionTemporaryUser(Connection sd, String userIdent, String status) throws SQLException {
		
		String sql = "delete from users where ident = ? "
				+ "and temporary "
				+ "and single_submission ";
		PreparedStatement pstmt = null;
		
		String sqlArchive = "insert into temp_users_final "
				+ "(ident, status, created) values (?, ?, now()) ";			
		PreparedStatement pstmtArchive = null;
		
		String sqlArchiveUpdate = "update temp_users_final "
				+ " set status = ?, "
				+ "created = now() "
				+ "where ident = ? ";			
		PreparedStatement pstmtArchiveUpdate = null;
		
		try {
			// No need for consistency between these
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  userIdent);	
			log.info("Deleting single submisison user: " + pstmt.toString());
			int count = pstmt.executeUpdate();
			
			if(count > 0) {
				/*
				 * Write the archive record
				 * First try updating an existing record
				 */
				pstmtArchiveUpdate = sd.prepareStatement(sqlArchiveUpdate);
				pstmtArchiveUpdate.setString(1, status);
				pstmtArchiveUpdate.setString(2,  userIdent);
				
				count = pstmtArchiveUpdate.executeUpdate();
				
				// Insert a new entry if none were updated
				if(count == 0) {		
					pstmtArchive = sd.prepareStatement(sqlArchive);
					pstmtArchive.setString(1,  userIdent);	
					pstmtArchive.setString(2, status);
					pstmtArchive.executeUpdate();
				}
			} else {
				log.info("Deletion did not happen.");
			}
			
		} finally {	
			try {pstmt.close();} catch(Exception e) {}
			try {pstmtArchive.close();} catch(Exception e) {}
			try {pstmtArchiveUpdate.close();} catch(Exception e) {}
		}
	}
	
	

	/*
	 * Move a user from their current organisation to another one in the list
	 */
	public void moveFromCurrent(Connection sd, String userIdent, int currentOrgId, int uId) throws Exception {
		
		String sqlGetAvailableOrgs = "select o_id "
				+ "from user_organisation uo "
				+ "where u_id = ? "
				+ "and o_id != ?";
		PreparedStatement pstmtGetOrgs = null;
		
		try {
			// 2. Get the organisation to switch to
			pstmtGetOrgs = sd.prepareStatement(sqlGetAvailableOrgs);
			pstmtGetOrgs.setInt(1, uId);
			pstmtGetOrgs.setInt(2, currentOrgId);
			ResultSet rs2 = pstmtGetOrgs.executeQuery();
			if(rs2.next()) {
				int newOrgId = rs2.getInt(1);
				switchUsersOrganisation(sd, newOrgId, userIdent, true);
			}
		} finally {
			try {if (pstmtGetOrgs != null) {pstmtGetOrgs.close();	}} catch (Exception e) {}
		}
	}
	
	/*
	 * Switch a user to a new organisation
	 */
	public void switchUsersOrganisation(Connection sd, int newOrgId, String userIdent, boolean validateOrgAccess) throws Exception {

		String sql = "select id, o_id from users where ident = ?";
		PreparedStatement pstmt = null;
		
		String sqlValidate = "select settings "
				+ "from user_organisation uo "
				+ "where o_id = ? "
				+ "and u_id = ?";
		PreparedStatement pstmtValidate = null;

		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		try {
			// 1. Get the users current organisation
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  userIdent);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				int uId = rs.getInt(1);
				int currentOrgId = rs.getInt(2);
				if(currentOrgId != newOrgId) {
					
					// 2.  Verify that the user has access to the new organisation
					pstmtValidate = sd.prepareStatement(sqlValidate);
					pstmtValidate.setInt(1, newOrgId);
					pstmtValidate.setInt(2, uId);
					log.info("Validate user organisation switch: " + pstmtValidate.toString());
					rs = pstmtValidate.executeQuery();
					
					String targetSettings = null;
					if(rs.next()) {				
						targetSettings = rs.getString(1);
					} else if (validateOrgAccess) {
						throw new ApplicationException(localisation.getString("u_org_nf"));
					}
						
					User u = null;
						
					// 3. Save the user settings for the current org
					User uCurrent = getByIdent(sd, userIdent);
					updateSavedSettings(sd, uCurrent, uId, currentOrgId, true, true);		// Can pretend to be super user as just saving what is already specified
						
					// 4. Set the current settings to the settings for the new organisation 
					// Use default values from the current organisation if the new settings are null
					if(targetSettings != null) {
						u = gson.fromJson(targetSettings, User.class);
						u.orgs = uCurrent.orgs;		// There is only one true set of organisations the user has access to and these are the current ones
						
						/*
						 * A user cannot lose organisational administration, enterprise administration or server owner privileges just by switching organisation
						 */
						for(UserGroup ug: uCurrent.groups) {
							if(ug.id == Authorise.ORG_ID || ug.id == Authorise.ENTERPRISE_ID || ug.id == Authorise.OWNER_ID) {
								boolean alreadyThere = false;
								for(UserGroup ugNew : u.groups) {
									if(ugNew.id == ug.id) {
										alreadyThere = true;
										break;
									}
								}
								if(!alreadyThere) {
									u.groups.add(ug);
								}
							}
						}
						
					} else {
						u = uCurrent;
						// Clear settings from the current org that we do not want to add as the default to a new org
						u.current_task_group_id = 0;
						u.current_project_id = 0;
						u.current_survey_id = 0;
						u.current_survey_ident = null;
						u.roles = null;
						u.projects = null;
						u.o_id = newOrgId;
					}
					
					updateUser(sd, u, currentOrgId, true, true, true, true, userIdent, null, null, true);
					
				} 
			}			
			
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (Exception e) {}
			try {if (pstmtValidate != null) {pstmtValidate.close();	}} catch (Exception e) {}
		}
	}
	
	/*
	 * Validate a user ident
	 */
	public boolean isValiduserIdent(String ident) {
		boolean isValid = true;
		String regexIdent = "^[a-z0-9_-]+$";
		Pattern patternIdent = Pattern.compile(regexIdent);
		if(!patternIdent.matcher(ident).matches() && !UtilityMethodsEmail.isValidEmail(ident)) {
			isValid = false;
		}
		return isValid;
	}
	
	/*
	 * Save the User Settings, specified in u, into the user_oganisation table
	 * Saved for organisation oId and user uId
	 * 
	 * This may be called by an administrator who does not have full rights to modify the user settings
	 * hence some of the user groups may not be populated in u.  for this reason if the user is not an
	 * organisational administrator then the currently saved settings are checked for the presence of these groups.
	 * Also if the updating user is an administrator then the roles will not be set and must be sourced from currently
	 * saved values.
	 */
	private void updateSavedSettings(Connection sd, User u, int uId, 
			int oId,
			boolean isOrgAdmin,
			boolean isSecurityAdmin) throws SQLException {
		
		String sqlUpdateSettings = "update user_organisation "
				+ "set settings = ? "
				+ "where o_id = ? "
				+ "and u_id = ?";
		PreparedStatement pstmtUpdateSettings = null;
		
		String sqlInsertSettings = "insert into user_organisation (u_id, o_id, settings) values(?, ?, ?) ";				
		PreparedStatement pstmtInsertSettings = null;
		
		String sqlGetSettings = "select settings from user_organisation where u_id = ? and o_id = ?";				
		PreparedStatement pstmtGetSettings = null;
		
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		
		ArrayList<Organisation> buOrgs = null; 
		String buPassword = null;
		int buOrgId = 0;
		
		try {
			// If the user is not a super user then get the existing settings and merge them
			if(!isOrgAdmin) {	
				pstmtGetSettings = sd.prepareStatement(sqlGetSettings);
				pstmtGetSettings.setInt(1, uId);
				pstmtGetSettings.setInt(2,oId);
				ResultSet rs = pstmtGetSettings.executeQuery();
				if(rs.next()) {
					String currentUserString = rs.getString(1);
					if(currentUserString != null) {
						User uCurrent = gson.fromJson(currentUserString, User.class);
						if(isSecurityAdmin) {
							// Set org admin group value from current
							for(UserGroup ug : uCurrent.groups) {
								if(ug.id == Authorise.ORG_ID || ug.id == Authorise.OWNER_ID || ug.id == Authorise.ENTERPRISE_ID) {
									u.groups.add(ug);
									break;
								}
							}
						} else {
							// Administrator
							for(UserGroup ug : uCurrent.groups) {
								if(ug.id == Authorise.ORG_ID || ug.id == Authorise.SECURITY_ID || 
										ug.id == Authorise.OWNER_ID || ug.id == Authorise.ENTERPRISE_ID) {
									u.groups.add(ug);
									break;
								}
							}
							// Set roles from current, this user cannot change their roles
							u.roles = uCurrent.roles;						
						}
					
					}
				}
			}
			
			/*
			 * Don't save the password or user organisation list
			 * Also set the user organisation to the organisation that is being saved 
			 * Restore the original settings after saving
			 */
			buPassword = u.password;
			buOrgs = u.orgs;
			buOrgId = u.o_id;
			
			u.password = null;		// Don't save the password
			u.orgs = null;
			u.o_id = oId;
			
			pstmtUpdateSettings = sd.prepareStatement(sqlUpdateSettings);
			pstmtUpdateSettings.setString(1, gson.toJson(u));
			pstmtUpdateSettings.setInt(2, oId);
			pstmtUpdateSettings.setInt(3, uId);
			log.info("Update current org settings: " + pstmtUpdateSettings.toString());
			int count = pstmtUpdateSettings.executeUpdate();
			if(count == 0) {
				// Need to insert the new settings
				pstmtInsertSettings = sd.prepareStatement(sqlInsertSettings);
				pstmtInsertSettings.setInt(1, uId);
				pstmtInsertSettings.setInt(2, oId);
				pstmtInsertSettings.setString(3,  gson.toJson(u));
				log.info("Insert settings: " + pstmtInsertSettings.toString());
				pstmtInsertSettings.executeUpdate();
			}
		} finally {
			// Restore modified values
			u.password = buPassword;
			u.orgs = buOrgs;
			u.o_id = buOrgId;
			
			try {if (pstmtUpdateSettings != null) {pstmtUpdateSettings.close();	}} catch (Exception e) {}
			try {if (pstmtInsertSettings != null) {pstmtInsertSettings.close();	}} catch (Exception e) {}
			try {if (pstmtGetSettings != null) {pstmtInsertSettings.close();	}} catch (Exception e) {}
		}
	}
	
	/*
	 * Get a list of users 
	 */
	public ArrayList<User> getUserList(Connection sd, int oId, boolean isOrgUser, boolean isSecurityManager, 
			boolean isAdminUser, String adminUserIdent) throws SQLException {
		
		ArrayList<User> users = new ArrayList<User> ();
		Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
		int adminUserId = GeneralUtilityMethods.getUserId(sd, adminUserIdent);
		
		String sql = "select u.id as u_id,"
				+ "u.ident as u_ident, "
				+ "u.name as u_name, "
				+ "u.email as u_email, "
				+ "u.o_id as u_o_id, "
				+ "o.name as o_name, "
				+ "u.language "
				+ "from users u, organisation o "
				+ "where (u.o_id = ? or u.id in (select uo.u_id from user_organisation uo where uo.o_id = ?)) "
				+ "and not u.temporary "
				+ "and u.o_id = o.id "
				+ "order by u.ident asc";
		PreparedStatement pstmt = null;
		
		String sqlGroups = "select g.id,"
				+ "g.name "
				+ "from groups g,"
				+ "user_group ug "
				+ "where ug.u_id = ? "
				+ "and ug.g_id = g.id "
				+ "order by g.id asc";
		PreparedStatement pstmtGroups = null;
		
		String sqlProjects = "select p.id,"
				+ "p.name "
				+ "from project p,"
				+ "user_project up "
				+ "where up.u_id = ? "
				+ "and up.p_id = p.id "
				+ "order by p.name asc";
		PreparedStatement pstmtProjects = null;
		
		String sqlRoles = "select r.id,"
				+ "r.name "
				+ "from role r,"
				+ "user_role ur "
				+ "where ur.u_id = ? "
				+ "and ur.r_id = r.id "
				+ "order by r.name asc";
		PreparedStatement pstmtRoles = null;
		
		StringBuilder sqlOrgs = new StringBuilder("select o.id,"
				+ "o.name as oname "
				+ "from organisation o,"
				+ "user_organisation uo "
				+ "where uo.u_id = ? "
				+ "and uo.o_id = o.id "
				+ "union "
				+ "select o.id,"
				+ "o.name as oname "
				+ "from organisation o,"
				+ "users u "
				+ "where u.o_id = o.id "
				+ "and u.id = ? ");
		if(!isOrgUser && isAdminUser) {
			sqlOrgs.append(" and o.owner = ?");
		}
		sqlOrgs.append("order by oname asc");
		PreparedStatement pstmtOrgs = null;
		
		String sqlGetSavedUser = "select settings "
				+ "from user_organisation uo "
				+ "where o_id = ? "
				+ "and u_id = ?";
		PreparedStatement pstmtGetSavedUser = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = null;

			pstmtGroups = sd.prepareStatement(sqlGroups);
			ResultSet rsGroups = null;
			
			pstmtProjects = sd.prepareStatement(sqlProjects);
			ResultSet rsProjects = null;
			
			pstmtRoles = sd.prepareStatement(sqlRoles);
			ResultSet rsRoles = null;
			
			pstmtOrgs = sd.prepareStatement(sqlOrgs.toString());
			ResultSet rsOrgs = null;
			
			pstmt.setInt(1, oId);
			pstmt.setInt(2, oId);
			log.info("Get user list: " + pstmt.toString());
			rs = pstmt.executeQuery();
			while(rs.next()) {
				int usersOrgId = rs.getInt("u_o_id");
				String current_organisation_name = rs.getString("o_name");
				User user = null;
				int uId = rs.getInt("u_id");

				boolean userFound = false;
				if(usersOrgId != oId) {
					// User is not currently in this organisation
					pstmtGetSavedUser = sd.prepareStatement(sqlGetSavedUser);
					pstmtGetSavedUser.setInt(1, oId);
					pstmtGetSavedUser.setInt(2, uId);
					log.info("Get saved user details: " + pstmtGetSavedUser.toString());
					ResultSet rs2 = pstmtGetSavedUser.executeQuery();
					if(rs2.next()) {
						user = gson.fromJson(rs2.getString(1), User.class);
						if(user != null) {
							userFound = true;
							user.current_org_name = current_organisation_name;
							user.current_org_id = usersOrgId;
						}
					}
				}
				/*
				 * Get the current user.
				 * If there was no archived user and the user is not in the current organisation then populate default
				 * settings
				 */
				if(!userFound) {
					// Current user in the same organisation as the administrator
					user = new User();
				
					user.id = uId;
					user.ident = rs.getString("u_ident");
					user.name = rs.getString("u_name");
					user.email = rs.getString("u_email");
					user.language = rs.getString("language");
					if(usersOrgId == oId) {
						user.current_org_name = rs.getString("o_name");
						user.current_org_id = usersOrgId;
					} else {
						user.current_org_name = current_organisation_name;
						user.current_org_id = usersOrgId;
					}
					user.o_id = usersOrgId;
					
					// Groups
					if(rsGroups != null) try {rsGroups.close();} catch(Exception e) {};
					pstmtGroups.setInt(1, user.id);
					rsGroups = pstmtGroups.executeQuery();
					user.groups = new ArrayList<UserGroup> ();
					while(rsGroups.next()) {
						UserGroup ug = new UserGroup();
						ug.id = rsGroups.getInt("id");
						ug.name = rsGroups.getString("name");
						user.groups.add(ug);
					}
					
					// Projects
					if(usersOrgId == oId) {
						if(rsProjects != null) try {rsProjects.close();} catch(Exception e) {};
						pstmtProjects.setInt(1, user.id);
						rsProjects = pstmtProjects.executeQuery();
						user.projects = new ArrayList<Project> ();
						while(rsProjects.next()) {
							Project p = new Project();
							p.id = rsProjects.getInt("id");
							p.name = rsProjects.getString("name");
							user.projects.add(p);
						}
						
						// Roles
						if(isOrgUser || isSecurityManager) {
							if(rsRoles != null) try {rsRoles.close();} catch(Exception e) {};
							pstmtRoles.setInt(1, user.id);
							rsRoles = pstmtRoles.executeQuery();
							user.roles = new ArrayList<Role> ();
							while(rsRoles.next()) {
								Role r = new Role();
								r.id = rsRoles.getInt("id");
								r.name = rsRoles.getString("name");
								user.roles.add(r);
							}
						}
					}
					
				} 
				
				// Always get Organisation list from the current settings
				if((isOrgUser || isAdminUser) && user != null) {
					if(rsOrgs != null) try {rsOrgs.close();} catch(Exception e) {};
					pstmtOrgs.setInt(1, uId);
					pstmtOrgs.setInt(2, uId);
					if(!isOrgUser) {
						pstmtOrgs.setInt(3,  adminUserId);
					}
					rsOrgs = pstmtOrgs.executeQuery();
					user.orgs = new ArrayList<Organisation> ();
					while(rsOrgs.next()) {
						Organisation o = new Organisation();
						o.id = rsOrgs.getInt("id");
						o.name = rsOrgs.getString("oname");
						user.orgs.add(o);
					}
					if(user.orgs.size() == 0) {
						/*
						 * Add a default organisation equal to the users current organisation
						 * This is only needed for users who were created before organisation linking was added
						 */
						Organisation o = new Organisation();
						o.id = usersOrgId;
						user.orgs.add(o);
					}
				}
				
				if(user != null) {
					users.add(user);
				}
			}
			
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (Exception e) {	}
			try {if (pstmtGroups != null) {pstmtGroups.close();	}} catch (Exception e) {	}
			try {if (pstmtProjects != null) {pstmtProjects.close();	}} catch (Exception e) {	}
			try {if (pstmtRoles != null) {pstmtRoles.close();	}} catch (Exception e) {	}
			try {if (pstmtOrgs != null) {pstmtOrgs.close();	}} catch (Exception e) {}
			try {if (pstmtGetSavedUser != null) {pstmtGetSavedUser.close();	}} catch (Exception e) {}
		}
		return users;
	}
	
	/*
	 * Get a list of users with just basic information
	 */
	public ArrayList<UserSimple> getUserListSimple(Connection sd, 
			int oId, 
			boolean orderByName,
			boolean isOnlyViewData,
			String ident, 
			boolean importedOnly) throws SQLException {
		
		ArrayList<UserSimple> users = new ArrayList<> ();
		
		StringBuffer sql = new StringBuffer("select u.id as id,"
				+ "u.ident as ident, "
				+ "u.name as name "			
				+ "from users u "
				+ "where (u.o_id = ? or u.id in (select uo.u_id from user_organisation uo where uo.o_id = ?)) "
				+ "and not u.temporary ");
		
		if(isOnlyViewData) {
			sql.append("and u.ident = ? ");
		}
		if(importedOnly) {
			sql.append("and u.imported ");
		}
		if(orderByName) {
			sql.append("order by u.name asc");
		} else {
			sql.append("order by u.ident asc");
		}
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql.toString());
			ResultSet rs = null;
			
			pstmt.setInt(1, oId);
			pstmt.setInt(2, oId);
			if(isOnlyViewData) {
				pstmt.setString(3, ident);
			}
			
			log.info("Get user list: " + pstmt.toString());
			rs = pstmt.executeQuery();
			while(rs.next()) {
				UserSimple u = new UserSimple();
				u.id = rs.getInt("id");
				u.ident = rs.getString("ident");
				u.name = rs.getString("name");

				users.add(u);
			}
			
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (Exception e) {	}
		}
		return users;
	}
	
	/*
	 * Delete a user
	 */
	public void deleteUser(Connection sd, String requestingUser, 
			String basePath, int uId, int oId, boolean deleteAll) throws Exception {
		
		String ident = null;
		
		PreparedStatement pstmtHardDelete = null;
		PreparedStatement pstmtGetIdent = null;
		PreparedStatement pstmtCountOrgs = null;
		PreparedStatement pstmtHardDeleteAll = null;
		PreparedStatement pstmtSoftDelete = null;
		PreparedStatement pstmtMove = null;
		
		try {
			// Perform a hard delete of the user
			String sqlHardDelete = "delete from users u "  
					+ "where u.id = ? "		// Ensure the user is in the same organisation as the administrator doing the editing
					+ "and u.o_id = ?";					
			pstmtHardDelete = sd.prepareStatement(sqlHardDelete);	
			
			// Perform a hard delete of the user from all organisations. only organisational administrators should be able to call this
			String sqlHardDeleteAll = "delete from users u "  
					+ "where u.id = ? ";				
			pstmtHardDeleteAll = sd.prepareStatement(sqlHardDeleteAll);	
			
			// Get the user ident to use in deleting dependent records
			String sqlGetIdent = "select u.ident "
					+ "from users u "
					+ "where u.id = ? ";							
			pstmtGetIdent = sd.prepareStatement(sqlGetIdent);
	
			pstmtGetIdent.setInt(1, uId);
			ResultSet rs = pstmtGetIdent.executeQuery();
			if(rs.next()) {
				ident = rs.getString(1);
			}
			
			// Get the organisations that this user is a member of
			ArrayList<Integer> organisationList = new ArrayList<> ();
			String sqlCountOrgs = "select o_id from user_organisation where u_id = ?";
			pstmtCountOrgs = sd.prepareStatement(sqlCountOrgs);
			
			pstmtCountOrgs.setInt(1, uId);
			rs = pstmtCountOrgs.executeQuery();
			while(rs.next()) {
				organisationList.add(rs.getInt(1));
			}
			
			/*
			 * Do a hard delete if 
			 *    1) the user is a member of only one organisation
			 *    2) All has been specified and he requesting user is an org admin
			 */
			if(organisationList.size() <= 1) {
				// Only one organisation so perform a Hard delete
				hardDelete(sd, 
						pstmtHardDelete, 
						localisation,
						uId, 
						ident, 
						oId, 
						basePath, 
						false,
						null,
						requestingUser);
			} if(deleteAll && GeneralUtilityMethods.hasSecurityGroup(sd, requestingUser, Authorise.ORG_ID)) {
				// Multiple organisations but delete all has been requested
				hardDelete(sd, 
						pstmtHardDeleteAll, 
						localisation,
						uId, 
						ident, oId, basePath, true, organisationList, requestingUser);
			} else {
				
				// Perform a soft delete of the user
				String sqlSoftDelete = "delete from user_organisation "  
						+ "where u_id = ? "		// Ensure the user is in the same organisation as the administrator doing the editing
						+ "and o_id = ?";					
				pstmtSoftDelete = sd.prepareStatement(sqlSoftDelete);		
				
				pstmtSoftDelete.setInt(1, uId);
				pstmtSoftDelete.setInt(2, oId);
				log.info("Soft Delete user: " + pstmtSoftDelete.toString());						
				pstmtSoftDelete.executeUpdate();
				
				// Move the user to another organisation if they are currently in the organisation from which they have been soft deleted
				String sqlMove = "update users "  
						+ "set o_id = (select o_id from user_organisation where u_id = ? limit 1) "
						+ "where id = ? "
						+ "and o_id = ?";				
				pstmtMove = sd.prepareStatement(sqlMove);	
				
				pstmtMove.setInt(1, uId);
				pstmtMove.setInt(2, uId);
				pstmtMove.setInt(3, oId);
				log.info("Move user: " + pstmtMove.toString());						
				pstmtMove.executeUpdate();
				 
				String msg = localisation.getString("u_soft_del");
				msg = msg.replace("%s1", ident);
				lm.writeLogOrganisation(sd, 
							oId, 
							requestingUser, 
							LogManager.DELETE, 
							msg, 0);
			}
		} finally {
			try {if (pstmtHardDelete != null) {pstmtHardDelete.close();}} catch (SQLException e) {}
			try {if (pstmtGetIdent != null) {pstmtGetIdent.close();}} catch (SQLException e) {}
			try {if (pstmtCountOrgs != null) {pstmtCountOrgs.close();}} catch (SQLException e) {}
			try {if (pstmtHardDeleteAll != null) {pstmtHardDeleteAll.close();}} catch (SQLException e) {}
			try {if (pstmtSoftDelete != null) {pstmtSoftDelete.close();}} catch (SQLException e) {}
			try {if (pstmtMove != null) {pstmtMove.close();}} catch (SQLException e) {}
		}
	}
	
	/*
	 * Perform a hard delete of a user
	 * Remove them from the users table
	 */
	private void hardDelete(Connection sd, 
			PreparedStatement pstmt, 
			ResourceBundle localisation,
			int uId, 
			String ident, 
			int o_id, 
			String basePath,
			boolean delete_all,
			ArrayList<Integer> organisationList,
			String requestingUser) throws Exception {
		
		pstmt.setInt(1, uId);
		if(!delete_all) {
			pstmt.setInt(2, o_id);
		}
		log.info("Hard Delete user: " + pstmt.toString());
		
		int count = pstmt.executeUpdate();
		
		if(count > 0) {	
			// If a user was deleted then delete their directories
			GeneralUtilityMethods.deleteDirectory(basePath + "/media/users/" + uId);

			String msg = localisation.getString("u_del");
			msg = msg.replace("%s1", ident);
			if(delete_all) {
				// Write logs for other organisations
				for(int ox : organisationList) {
					lm.writeLogOrganisation(sd, 
							ox, requestingUser, LogManager.DELETE, msg, 0);
				}
			} else {
				lm.writeLogOrganisation(sd, 
						o_id, requestingUser, LogManager.DELETE, msg, 0);
			}
		}	
	}
}
