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

package org.smap.sdal.Utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.MetaItem;

public class Authorise {
	
	private static Logger log =
			 Logger.getLogger(Authorise.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	public static String ENUM = "enum";
	public static String ANALYST = "analyst";
	public static String ADMIN = "admin";
	public static String ORG = "org admin";
	public static String MANAGE = "manage";
	public static String SECURITY = "security";
	public static String VIEW_DATA = "view data";
	public static String ENTERPRISE = "enterprise admin";
	public static String OWNER = "server owner";
	public static String VIEW_OWN_DATA = "view own data";
	public static String MANAGE_TASKS = "manage tasks";
	public static String DASHBOARD = "dashboard";
	public static String LINKS = "links";
	public static String CONSOLE_ADMIN = "console admin";
	public static String MCP_ACCESS = "mcp access";
	
	public static int ADMIN_ID = 1;
	public static int ANALYST_ID = 2;
	public static int ENUM_ID = 3;
	public static int ORG_ID = 4;
	public static int MANAGE_ID = 5;
	public static int SECURITY_ID = 6;
	public static int VIEW_DATA_ID = 7;
	public static final int ENTERPRISE_ID = 8;
	public static final int OWNER_ID = 9;
	public static final int VIEW_OWN_DATA_ID = 10;
	public static final int MANAGE_TASKS_ID = 11;
	public static final int DASHBOARD_ID = 12;
	//public static final int LINKS_ID = 13;
	public static final int CONSOLE_ADMIN_ID = 14;
	public static final int MCP_ACCESS_ID = 15;
	
	//private String requiredGroup;
	ArrayList<String> permittedGroups; 
	
	/*
	 * Create authorise object with required security policies
	 */
	public Authorise(ArrayList<String> groups, String group) {
		
		if(groups == null) {
			permittedGroups = new ArrayList<String> ();
			permittedGroups.add(group);
		} else {
			permittedGroups = groups;
		}
	}
	
	/*
	 * Check to see if the user has the rights to perform the requested action
	 */
	public boolean isAuthorised(Connection sd, String user) {
		
		authoriseUser(sd, user, true);
 		
		return true;
	}
	
	/*
	 * Check to see if the user has the rights to perform the requested action
	 */
	public boolean isAuthorisedNoClose(Connection sd, String user) {
		
		authoriseUser(sd, user, false);
 		
		return true;
	}
	
	
	private void authoriseUser(Connection sd, String user, boolean closeConnectionOnError) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		String sql = "select count(*) from users u, groups g, user_group ug " +
				" where u.id = ug.u_id " +
				" and g.id = ug.g_id " +
				" and u.ident = ? " +
				" and (" +
				" g.name = ? ";
				
		if(permittedGroups.size() > 1) {
			for(int i = 1; i < permittedGroups.size(); i++)
				sql += " or g.name = ? ";
		}
		sql += ");";
		
		try {
			pstmt = sd.prepareStatement(sql); 	
			pstmt.setString(1, user);
			for(int i = 0; i < permittedGroups.size(); i++) {
				pstmt.setString(i + 2, permittedGroups.get(i));
			}

			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.info("Authorisation failed for: " + user + " groups required were one of: " );
			if(pstmt!=null) {
				log.info("isAuthorised: " + pstmt.toString());
			}
			for(int i = 0; i < permittedGroups.size(); i++) {
				log.info("  ==== " + permittedGroups.get(i));
			}
			log.log(Level.SEVERE,"SQL Error during authorisation", e);
			sqlError = true;
		} finally {		
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
		// Check to see if the user was authorised to access this service
 		if(count == 0 || sqlError) {
 			StringBuffer msg = new StringBuffer("");
 			msg.append("Authorisation failed for " + user + " group required was one of: ");
 			for(int i = 0; i < permittedGroups.size(); i++) {
 				if(i > 0) {
 					msg.append(", ");
 				}
				msg.append(permittedGroups.get(i));
			}
 			log.info(msg.toString());
 			
 			
 			lm.writeLog(sd, 0, user, LogManager.ERROR, msg.toString(), 0, null);		// Write the application log
 			
 			// Close the connection as throwing an exception will end the service call			
 			if(closeConnectionOnError) {
 				SDDataSource.closeConnection("isAuthorised", sd);
 			}
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException(msg.toString());
			}
		} 
	}
	
	/*
	 * Check to make sure the user is a valid temporary user
	 */
	public boolean isValidTemporaryUser(Connection conn, String user) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		String sql = "select count(*) from users u " +
				" where u.ident = ? " +
				" and u.temporary = true";
		
		try {
			pstmt = conn.prepareStatement(sql); 	
			pstmt.setString(1, user);

			log.info("is temporary user: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"SQL Error during authorisation", e);
			sqlError = true;
		} finally {		
			// Close the result set and prepared statement
			if(resultSet != null) {try{resultSet.close();} catch (Exception ex) {}}
			if(pstmt != null) {try{pstmt.close();} catch (Exception ex) {}}
		}
		
		// Check to see if the user was authorised to access this service
 		if(count == 0 || sqlError) {
 			log.info("Authorisation failed for: " + user + " needs to be a temporary user");
 			SDDataSource.closeConnection("isAuthorised", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Check to make sure the user id is in the organisation of the user making the request
	 */
	public boolean isValidUser(Connection sd, String adminUser, int uId) {
		
		ResultSet resultSet = null;
		int count = 0;
		boolean sqlError = false;
		

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
		
		try {
			
			int adminUserOrgId = GeneralUtilityMethods.getOrganisationId(sd, adminUser);
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, uId);
			pstmt.setInt(2, adminUserOrgId);
			pstmt.setInt(3, uId);
			pstmt.setInt(4, adminUserOrgId);
			log.info("Validate user in correct organisation: " + pstmt.toString());
			resultSet = pstmt.executeQuery();

			if(resultSet.next()) {
				count = 1;
			}
		} catch (Exception e) {
			log.log(Level.SEVERE,"SQL Error during authorisation", e);
			sqlError = true;
		} finally {		
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
		// Check to see if the user was authorised to access this service
 		if(count == 0 || sqlError) {
 			log.info("Authorisation failed for: " + adminUser);
 			SDDataSource.closeConnection("isAuthorised", sd);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Check to make sure the user ident is in the organisation of the user making the request
	 */
	public boolean isValidUserIdent(Connection sd, String adminUser, String userIdent) {
		
		ResultSet resultSet = null;
		int count = 0;
		boolean sqlError = false;
		

		String sql = "select u.ident "
					+ "from users u " 
					+ "where u.ident = ? "
					+ "and u.o_id = (select o_id from users where ident = ?) ";				
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, userIdent);
			pstmt.setString(2, adminUser);
			log.info("Validate user in correct organisation: " + pstmt.toString());
			resultSet = pstmt.executeQuery();

			if(resultSet.next()) {
				count = 1;
			}
		} catch (Exception e) {
			log.log(Level.SEVERE,"SQL Error during authorisation", e);
			sqlError = true;
		} finally {		
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
		// Check to see if the user was authorised to access this service
 		if(count == 0 || sqlError) {
 			log.info("Authorisation failed for: " + adminUser);
 			SDDataSource.closeConnection("isAuthorised", sd);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Check to make sure the billing organisation is valid
	 */
	public boolean isValidBillingOrganisation(Connection conn, int oId) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		String sql = "select count(*) from organisation o " +
				" where o.id = ? " +
				" and o.billing_enabled";
		
		try {
			pstmt = conn.prepareStatement(sql); 	
			pstmt.setInt(1, oId);

			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"SQL Error during authorisation", e);
			sqlError = true;
		} finally {		
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
		// Check to see if the org has billing enabled
 		if(count == 0 || sqlError) {
 			log.info("Authorisation failed for: " + oId + " billing needs to be enabled for this organisation");
 			SDDataSource.closeConnection("isAuthorised", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Check to make sure the billing enterprise is valid
	 */
	public boolean isValidBillingEnterprise(Connection conn, int eId) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		String sql = "select count(*) from enterprise e " +
				" where e.id = ? " +
				" and e.billing_enabled";
		
		try {
			pstmt = conn.prepareStatement(sql); 	
			pstmt.setInt(1, eId);

			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"SQL Error during authorisation", e);
			sqlError = true;
		} finally {		
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
		// Check to see if the ent has billing enabled
 		if(count == 0 || sqlError) {
 			log.info("Authorisation failed for: " + eId + " billing needs to be enabled for this enterprise");
 			SDDataSource.closeConnection("isAuthorised", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this particular survey
	 */
	public boolean isValidSurvey(Connection conn, String user, int sId, boolean isDeleted, boolean superUser)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Make sure the survey has not been soft deleted and exists or alternately 
		 *    that it has been soft deleted and exists
		 * 2) Make sure survey is in a project that the user has access to
		 */

		StringBuffer sql = new StringBuffer("select count(*) from survey s, users u, user_project up, project p "
				+ "where u.id = up.u_id "
				+ "and p.id = up.p_id "
				+ "and s.p_id = up.p_id "
				+ "and s.s_id = ? "
				+ "and u.ident = ? "
				+ "and s.deleted = ? ");
		
		try {		
			
			if(!superUser) {
				// Add RBAC
				sql.append(GeneralUtilityMethods.getSurveyRBAC());
			}
			
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setInt(1, sId);
			pstmt.setString(2, user);
			pstmt.setBoolean(3, isDeleted);
			
			if(!superUser) {
				pstmt.setString(4, user);
			}
			
			resultSet = pstmt.executeQuery();
			if(resultSet.next()) {
				count = resultSet.getInt(1);
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			if(resultSet != null) {try{resultSet.close();}catch(Exception e) {}};
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}};
		}
		
 		if(count == 0) {
			log.info("IsValidSurvey: " + pstmt.toString());
 			log.info("Survey validation failed for: " + user + " survey was: " + sId);
 			
 			SDDataSource.closeConnection("isValidSurvey", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();	 
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this particular case management setting
	 */
	public boolean isValidCaseManagementAlert(Connection conn, String user, int id)
			throws ServerException, AuthorisationException, NotFoundException {
		
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * Make sure the alert is for a survey that the user has access to
		 */

		StringBuffer sql = new StringBuffer("select count(*) from cms_alert "
				+ "where id = ? "
				+ "and group_survey_ident in "
				+ "( select group_survey_ident from survey s, users u, user_project up, project p "
				+ "	where u.id = up.u_id "
				+ " and p.id = up.p_id "
				+ "	and s.p_id = up.p_id "
				+ "	and u.ident = ? )");
		
		try {		
			
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setInt(1, id);
			pstmt.setString(2, user);
			
			resultSet = pstmt.executeQuery();
			if(resultSet.next()) {
				count = resultSet.getInt(1);
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			if(resultSet != null) {try{resultSet.close();}catch(Exception e) {}};
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}};
		}
		
 		if(count == 0) {
			log.info("IsValidCaseManagementAlert: " + pstmt.toString());
 			log.info("Case management setting validation failed for: " + user + " case management alert id was: " + id);
 			
 			SDDataSource.closeConnection("isValidAlert", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();	 
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this particular survey passing a survey ident
	 */
	public boolean isValidSurveyIdent(Connection conn, String user, String sIdent, boolean isDeleted, boolean superUser)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		String sName = null;
		
		/*
		 * 1) Make sure the survey has not been soft deleted and exists or alternately 
		 *    that it has been soft deleted and exists
		 * 2) Make sure survey is in a project that the user has access to
		 */

		StringBuffer sql = new StringBuffer("select count(*) from survey s, users u, user_project up, project p "
				+ "where u.id = up.u_id "
				+ "and p.id = up.p_id "
				+ "and s.p_id = up.p_id "
				+ "and s.ident = ? "
				+ "and u.ident = ? "
				+ "and s.deleted = ? ");
		
		try {		
			
			if(!superUser) {
				// Add RBAC
				sql.append(GeneralUtilityMethods.getSurveyRBAC());
			}
			
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setString(1, sIdent);
			pstmt.setString(2, user);
			pstmt.setBoolean(3, isDeleted);
			
			if(!superUser) {
				pstmt.setString(4, user);
			}
			
			resultSet = pstmt.executeQuery();
			if(resultSet.next()) {
				count = resultSet.getInt(1);
			}
			
			// If count is zero get the survey name to report the error
			if(count == 0) {
				sName = GeneralUtilityMethods.getSurveyNameFromIdent(conn, sIdent);
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			if(resultSet != null) {try{resultSet.close();}catch(Exception e) {}};
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}};
		}
		
 		if(count == 0) {
			log.info("IsValidSurvey: " + pstmt.toString());
 			log.info("Survey validation failed for: " + user + " survey was: " + sIdent);
 			
 			SDDataSource.closeConnection("isValidSurvey", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				
				throw new AuthorisationException("Survey validation failed for: " + user 
						+ " survey was: " + sName);	 
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this particular bundle
	 * That is if they have access to any survey in the bundle
	 */
	public boolean isValidBundle(Connection conn, String user, String sIdent, boolean isDeleted, boolean superUser)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		String sName = null;
		
		/*
		 * 1) Make sure the survey has not been soft deleted and exists or alternately 
		 *    that it has been soft deleted and exists
		 * 2) Make sure survey is in a project that the user has access to
		 */

		StringBuffer sql = new StringBuffer("select count(*) "
				+ "from survey s, users u, user_project up, project p "
				+ "where u.id = up.u_id "
				+ "and p.id = up.p_id "
				+ "and s.p_id = up.p_id "
				+ "and s.group_survey_ident = ? "
				+ "and u.ident = ? "
				+ "and s.deleted = ? ");
		
		try {		
			
			if(!superUser) {
				// Add RBAC
				sql.append(GeneralUtilityMethods.getSurveyRBAC());
			}
			
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setString(1, sIdent);
			pstmt.setString(2, user);
			pstmt.setBoolean(3, isDeleted);
			
			if(!superUser) {
				pstmt.setString(4, user);
			}
			
			resultSet = pstmt.executeQuery();
			if(resultSet.next()) {
				count = resultSet.getInt(1);
			}
			
			// If count is zero get the survey name to report the error
			if(count == 0) {
				sName = GeneralUtilityMethods.getSurveyNameFromIdent(conn, sIdent);
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			if(resultSet != null) {try{resultSet.close();}catch(Exception e) {}};
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}};
		}
		
 		if(count == 0) {
			log.info("IsValidBundle: " + pstmt.toString());
 			log.info("Bundle validation failed for: " + user + " survey was: " + sIdent);
 			
 			SDDataSource.closeConnection("isValidSurvey", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				
				throw new AuthorisationException("Survey validation failed for: " + user 
						+ " survey was: " + sName);	 
			}
		} 
 		
		return true;
	}
	
	
	/*
	 * Verify that the user is entitled to access this particular pdf templates
	 */
	public boolean isValidPdfTemplate(Connection conn, String user, int id)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Make sure the survey has not been soft deleted and exists or alternately 
		 *    that it has been soft deleted and exists
		 * 2) Make sure survey is in a project that the user has access to
		 */

		StringBuffer sql = new StringBuffer("select count(*) from survey s, users u, user_project up, project p, survey_template st "
				+ "where u.id = up.u_id "
				+ "and p.id = up.p_id "
				+ "and s.p_id = up.p_id "
				+ "and s.ident = st.ident "
				+ "and st.t_id = ? "
				+ "and u.ident = ? ");
		
		try {		
			
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setInt(1, id);
			pstmt.setString(2, user);
			
			resultSet = pstmt.executeQuery();
			if(resultSet.next()) {
				count = resultSet.getInt(1);
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			if(resultSet != null) {try{resultSet.close();}catch(Exception e) {}};
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}};
		}
		
 		if(count == 0) {
			log.info("IsValidPdfTemplate: " + pstmt.toString());
 			log.info("Pdf validation failed for: " + user + "  template was: " + id);
 			
 			SDDataSource.closeConnection("isValidPdfTemplate", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException("PdfTemplate validation failed for: " + user + " template was: " + id);	 
			}
		} 
 		
		return true;
	}
	
	/*
	 * Use with anonymous requests to verify that the survey exists only
	 */
	public boolean surveyExists(Connection conn, String sIdent)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Make sure the survey has not been soft deleted and exists or alternately 
		 *    that it has been soft deleted and exists
		 * 2) Make sure survey is in a project that the user has access to
		 */

		StringBuffer sql = new StringBuffer("select count(*) from survey s "
				+ "where s.ident = ? "
				+ "and s.deleted = false ");
		
		try {		
			
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setString(1, sIdent);
			
			resultSet = pstmt.executeQuery();
			if(resultSet.next()) {
				count = resultSet.getInt(1);
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			if(resultSet != null) {try{resultSet.close();}catch(Exception e) {}};
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}};
		}
		
 		if(count == 0) {
			log.info("IsValidSurvey: " + pstmt.toString());
 			log.info("Survey validation failed for survey: " + sIdent);
 			
 			SDDataSource.closeConnection("isValidSurvey", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException("Survey validation failed for survey: " + sIdent);	 
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is in the same organisation as the message
	 */
	public boolean isValidMessage(Connection conn, String user, int messageId)
			throws ServerException, AuthorisationException, NotFoundException {
		
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		StringBuffer sql = new StringBuffer("select count(*) from message where id = ? and o_id in "
				+ "( select o.id from organisation o, users u "
				+ "where o.id = u.o_id "
				+ "and u.ident = ?)");
						
		try {		
					
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setInt(1, messageId);
			pstmt.setString(2, user);
			
			log.info("IsValidMessage: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Message validation failed for: " + user + " message id was: " + messageId);
 			
 			SDDataSource.closeConnection("isValidMessage", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();	 
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is in the same organisation as the optin
	 */
	public boolean isValidOptin(Connection conn, String user, int id)
			throws ServerException, AuthorisationException, NotFoundException {
		
		String connectionString = "Is valid Opt In Id";
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		StringBuffer sql = new StringBuffer("select count(*) from people where id = ? and o_id in "
				+ "( select o.id from organisation o, users u "
				+ "where o.id = u.o_id "
				+ "and u.ident = ?)");
					
		try {		
					
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setInt(1, id);
			pstmt.setString(2, user);
			
			log.info("IsValidOptin: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("optin validation failed for: " + user + " opt in id was: " + id);
 			
 			SDDataSource.closeConnection(connectionString, conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();	 
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this particular survey
	 * Ignore whether or not it is deleted
	 */
	public boolean isValidDelSurvey(Connection conn, String user, int sId, boolean superUser)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Make sure survey is in a project that the user has access to
		 */

		StringBuffer sql = new StringBuffer("select count(*) from survey s, users u, user_project up, project p "
				+ "where u.id = up.u_id "
				+ "and p.id = up.p_id "
				+ "and s.p_id = up.p_id "
				+ "and s.s_id = ? "
				+ "and u.ident = ? ");
		
		try {		
			
			if(!superUser) {
				// Add RBAC
				sql.append(GeneralUtilityMethods.getSurveyRBAC());
			}
			
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setInt(1, sId);
			pstmt.setString(2, user);
			
			if(!superUser) {
				pstmt.setString(3, user);
			}
			log.info("IsValidSurvey: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Survey validation failed for: " + user + " survey was: " + sId);
 			
 			SDDataSource.closeConnection("isValidSurvey", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();	 
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this particular query
	 */
	public boolean isValidQuery(Connection conn, String user, int queryId)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Make sure the survey  exists 
		 * 2) Make sure user has access to the query
		 */

		StringBuffer sql = new StringBuffer("select count(*) from custom_query q, users u "
				+ "where u.id = q.u_id "
				+ "and q.id = ? "
				+ "and u.ident = ? ");
		
		try {		
			
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setInt(1, queryId);
			pstmt.setString(2, user);
			
			log.info("IsValidQuery: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Error: Query validation failed for: " + user + " query was: " + queryId);
 			
 			SDDataSource.closeConnection("isValidQuery", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new NotFoundException();	// Not found rather than not authorised as we could not find a resource that the user had access to
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this particular view
	 */
	public boolean isValidView(Connection conn, String user, int viewId, boolean update)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Make sure the view  exists 
		 * 2) Make sure user has access to the view
		 */

		StringBuffer sql = new StringBuffer("select count(*) from user_view v, users u "
				+ "where u.id = v.u_id "
				+ "and v.v_id = ? "
				+ "and u.ident = ? ");
		
		if(update) {
			sql.append("and (v.access = 'write' or v.access = 'owner')" );
		}
		try {		
			
			pstmt = conn.prepareStatement(sql.toString());
			pstmt.setInt(1, viewId);
			pstmt.setString(2, user);
			
			log.info("IsValidView: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Error: Query validation failed for: " + user + " query was: " + viewId);
 			
 			SDDataSource.closeConnection("isValidView", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new NotFoundException();	// Not found rather than not authorised as we could not find a resource that the user had access to
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this particular role
	 */
	public boolean isValidRole(Connection conn, String user, int rId)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Make sure the role is in the users organisation
		 */

		String sql = "select count(*) from role r, users u "
				+ "where u.o_id = r.o_id "
				+ "and u.ident = ? "
				+ "and r.id = ?";
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, user);
			pstmt.setInt(2, rId);
			
			log.info("IsValidRole: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Survey validation failed for: " + user + " role was: " + rId);
 			
 			SDDataSource.closeConnection("isValidRole", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new NotFoundException();	// Not found rather than not authorised as we could not find a resource that the user had access to
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this managedForm
	 */
	public boolean isValidManagedForm(Connection conn, String user, int crId)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Make sure the survey has not been soft deleted and exists or alternately 
		 *    that it has been soft deleted and exists
		 * 2) Make sure survey is in a project that the user has access to
		 */

		String sql = "select count(*) from custom_report cr, users u "
				+ "where cr.o_id = u.o_id "
				+ "and cr.id = ? "
				+ "and u.ident = ? ";
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, crId);
			pstmt.setString(2, user);
			
			log.info("IsValidCustomReport: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Survey validation failed for: " + user + " custom report was: " + crId);
 			
 			SDDataSource.closeConnection("isValidSurvey", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new NotFoundException();	// Not found rather than not authorised as we could not find a resource that the user had access to
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this group survey ident
	 *  
	 */
	public boolean isValidOversightSurvey(Connection conn, String user, int sId, String oversightSurveyIdent)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Ensure group survey matches survey id
		 */

		String sql = "select count(*) "
				+ "from survey s, users u, user_project up "
				+ "where s.p_id = up.p_id "
				+ "and up.u_id = u.id "
				+ "and u.ident = ? "
				+ "and s.ident = ? "
				+ "and s.group_survey_ident is not null "
				+ "and s.group_survey_ident = (select group_survey_ident from survey where s_id = ?) ";

		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, user);
			pstmt.setString(2,  oversightSurveyIdent);
			pstmt.setInt(3,  sId);
			
			log.info("IsValidGroupSurvey: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Survey validation failed for: " + user + " custom survey was: " + oversightSurveyIdent);
 			
 			SDDataSource.closeConnection("isValidSurvey", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new NotFoundException();	// Not found rather than not authorised as we could not find a resource that the user had access to
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this particular task group
	 */
	public boolean isValidTaskGroup(Connection conn, String user, int tgId)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from task_group tg, users u, user_project up, project p " +
				" where u.id = up.u_id" +
				" and p.id = up.p_id" +
				" and tg.p_id = up.p_id" +
				" and tg.tg_id = ? " +
				" and u.ident = ?;";
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, tgId);
			pstmt.setString(2, user);
			
			log.info("IsValidTaskGroup: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Survey validation failed for: " + user + " task group was: " + tgId);
 			
 			SDDataSource.closeConnection("isValidTaskGroup", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new NotFoundException();	// Not found rather than not authorised as we could not find a resource that the user had access to
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the question is in the survey
	 */
	public boolean isValidQuestion(Connection conn, String user, int sId, int qId)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Make sure the survey has not been soft deleted and exists or alternately 
		 *    that it has been soft deleted and exists
		 * 2) Make sure survey is in a project that the user has access to
		 */

		String sql = "select count(*) from question q, form f " +
				" where q.f_id = f.f_id" +
				" and f.s_id = ?" +
				" and q.q_id = ?;"; 
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, sId);
			pstmt.setInt(2, qId);
			
			log.info("IsValidQuestion: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Question validation failed for question: " + qId + " survey was: " + sId);
 			
 			SDDataSource.closeConnection("isValidQuestion", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new NotFoundException();	// Not found rather than not authorised as we could not find a resource that the user had access to
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the question passed by name is in the survey
	 */
	public boolean isValidQuestionName(Connection conn, String user, int sId, 
			String qName, boolean checkPreloads)
			throws ServerException, AuthorisationException, NotFoundException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;
		
		/*
		 * 1) Make sure the survey has not been soft deleted and exists or alternately 
		 *    that it has been soft deleted and exists
		 * 2) Make sure survey is in a project that the user has access to
		 */

		String sql = "select count(*) from question q, form f " +
				" where q.f_id = f.f_id" +
				" and f.s_id = ?" +
				" and q.qname = ?;"; 
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, sId);
			pstmt.setString(2, qName);
			
			log.info("IsValidQuestion: " + pstmt.toString());
			
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
			
			if(count == 0 && checkPreloads) {
				ArrayList<MetaItem> preloads = GeneralUtilityMethods.getPreloads(conn, sId);
				if(preloads != null) {
					for(MetaItem item : preloads) {
						if(item.name != null && item.name.equals(qName)) {
							count++;
							break;
						}
					}
				}
			}
			
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Question name validation failed for question: " + qName + " survey was: " + sId);
 			
 			SDDataSource.closeConnection("isValidQuestion", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new NotFoundException();	// Not found rather than not authorised as we could not find a resource that the user had access to
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the survey is not blocked
	 */
	public boolean isBlocked(Connection conn, int sId, boolean isBlocked)
			throws ServerException, AuthorisationException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select s.blocked, o.can_submit "
				+ "from survey s, project p, organisation o "
				+ "where s.s_id = ? "
				+ "and s.p_id = p.id "
				+ "and p.o_id = o.id ";
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, sId);
			
			log.info("isBlocked: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			boolean surveyBlocked = resultSet.getBoolean("blocked");
			boolean orgCanSubmit = resultSet.getBoolean("can_submit");	
			
			if(isBlocked && (surveyBlocked || !orgCanSubmit)) {
				count = 1;
			} else if (!isBlocked && (!surveyBlocked && orgCanSubmit)) {
				count = 1;
			}

		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation - isBlocked", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Survey validation failed for block check: " + isBlocked + " survey was: " + sId);
 			
 			SDDataSource.closeConnection("isBlocked", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new BlockedException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the survey can load tasks from a file
	 */
	public boolean canLoadTasks(Connection conn, int sId)
			throws ServerException, AuthorisationException {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from survey s " +
				" where s.s_id = ? " +
				" and s.task_file = 'true';"; 
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, sId);
			
			log.info("Verify can load tasks: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation - cannot load tasks", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Survey validation failed for can load tasks: survey was: " + sId);
 			
 			SDDataSource.closeConnection("canLoadTasks", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}

	/*
	 * Verify that the user is entitled to access this project
	 */
	public boolean isValidProject(Connection conn, String user, int pId) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from users u, user_project up, project p " +
				" where u.id = up.u_id" +
				" and p.id = up.p_id" +
				" and p.id = ? " +
				" and u.ident = ?;";
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, pId);
			pstmt.setString(2, user);
			log.info("IsValidProject: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Project validation failed for: " + user + " project was: " + pId);
 			
 			SDDataSource.closeConnection("isValidProject", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException("Invalid Project: " + pId);
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is in the same organisation as the project
	 */
	public boolean projectInUsersOrganisation(Connection conn, String user, int pId) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from project p " +
				" where p.id = ? " +
				" and p.o_id = ?;";
		
		try {
			int oId = GeneralUtilityMethods.getOrganisationId(conn, user);
			
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, pId);
			pstmt.setInt(2, oId);
			log.info("IsProjectInUsersOrganisation: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
			}
		}
		
 		if(count == 0) {
 			log.info("Project in organisation validation failed for: " + user + " project was: " + pId);
 			
 			SDDataSource.closeConnection("isValidProject", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the survey is in the users organisation
	 */
	public boolean surveyInUsersOrganisation(Connection conn, String user, String sIdent) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from survey s, project p "
				+ "where p.id = s.p_id "
				+ "and s.ident = ? "
				+ "and p.o_id = ?;";
		
		try {
			int oId = GeneralUtilityMethods.getOrganisationId(conn, user);
			
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, sIdent);
			pstmt.setInt(2, oId);
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
			}
		}
		
 		if(count == 0) {
 			log.info("Survey in organisation validation failed for: " + user + " survey ident was: " + sIdent);
 			
 			SDDataSource.closeConnection("isValidProject", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is a member of the supplied organisation
	 */
	public boolean isValidOrganisation(Connection conn, String user, int oId) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from users u "
				+ "where (u.o_id = ? and u.ident = ?) "
				+ "or u.id in (select u_id from user_organisation where o_id = ?)";
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, user);
			pstmt.setInt(3, oId);
			log.info("IsValidOrganisation: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Security: Organisation validation failed for: " + user + " organisation was: " + oId);
 			
 			SDDataSource.closeConnection("isValidOrganisation", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is a member of the same enterprise as the organisation
	 */
	public boolean isOrganisationInEnterprise(Connection conn, String user, int oId) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from users u, organisation o "
				+ "where u.o_id = o.id "
				+ "and u.ident = ? "
				+ "and o.e_id = (select e_id from organisation where id = ?)";
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, user);
			pstmt.setInt(2, oId);
			log.info("IsOrganisationInEnterprise: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Security: Enterprise validation failed for: " + user + " organisation was: " + oId);
 			
 			SDDataSource.closeConnection("isOrganisationInEnterprise", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user has update rights to the organisation
	 * They can either be an organisational administrator or
	 * the organisation is owned by the user
	 */
	public boolean canUserUpdateOrganisation(Connection conn, String user, int oId) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;


		String sql = "select count(*) from users u, organisation o "
				+ "where u.ident = ? "
				+ "and o.id = ? "
				+ "and o.owner = u.id";
		
		try {
			// If user is an org administrator then all is good
			if(GeneralUtilityMethods.hasSecurityGroup(conn, user, Authorise.ORG_ID)) {
				return true;
			}
			
			// Else check for ownership - note we already should have verified that they are an admin or org_admin user
			
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, user);
			pstmt.setInt(2, oId);
			log.info("IsOrganisationOwned: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Security: Check for ownership of organisation failed for user: " + user + " organisation was: " + oId);
 			
 			SDDataSource.closeConnection("canUserUpdateOrganisation", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this task
	 */
	public boolean isValidTask(Connection conn, String user, int tId) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from tasks t, users u, user_project up, project p " +
				" where u.id = up.u_id" +
				" and p.id = up.p_id" +
				" and p.id = t.p_id " +
				" and t.id = ? " +
				" and u.ident = ?;";
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, tId);
			pstmt.setString(2, user);
			
			log.info("Is valid task: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
			
			if(count == 0) {
				log.info("Validation of task failed: " + pstmt.toString());
			}
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Task validation failed for: " + user + " task was: " + tId);
 			
 			SDDataSource.closeConnection("isValidTask", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException("Task validation failed for: " + user + " task was: " + tId);
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this assignment
	 */
	public boolean isValidAssignment(Connection conn, String user, int aId) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from assignments a, tasks t, users u, user_project up, project p " +
				" where u.id = up.u_id" +
				" and p.id = up.p_id" +
				" and p.id = t.p_id " +
				" and t.id = a.task_id " +
				" and a.id = ? " +
				" and u.ident = ?";
		
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, aId);
			pstmt.setString(2, user);
			
			log.info("Is valid assignment: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Assignment validation failed for: " + user + " survey was: " + aId);
 			
 			SDDataSource.closeConnection("isValidAssignment", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	
	/*
	 * Verify that the user is entitled to update this SMS number
	 */
	public boolean isValidNumber(Connection conn, String user, String number) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from sms_number n, users u "
				+ "where u.o_id = n.o_id "
				+ "and n.our_number = ? "
				+ "and u.ident = ?";
		
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, number);
			pstmt.setString(2, user);
			
			log.info("Is valid number: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Number validation failed for: " + user + " number was: " + number);
 			
 			SDDataSource.closeConnection("isValidNumber", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException();
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this mailout id
	 */
	public boolean isValidMailout(Connection conn, String user, int mailoutId) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from mailout m, users u, user_project up, project p, survey s "
				+ "where u.id = up.u_id "
				+ "and p.id = up.p_id "
				+ "and p.id = s.p_id "
				+ "and m.survey_ident = s.ident "
				+ "and m.id = ? "
				+ "and u.ident = ? ";
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, mailoutId);
			pstmt.setString(2, user);
			
			log.info("Is valid mailout id: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
			
			if(count == 0) {
				log.info("Validation of mailout failed: " + pstmt.toString());
			}
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Mailout validation failed for: " + user + " mailout id was: " + mailoutId);
 			
 			SDDataSource.closeConnection("isValidTask", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException("Invalid Mailout Id");
			}
		} 
 		
		return true;
	}
	
	/*
	 * Verify that the user is entitled to access this custom report id
	 */
	public boolean isValidCustomReport(Connection conn, String user, int id) {
		ResultSet resultSet = null;
		PreparedStatement pstmt = null;
		int count = 0;
		boolean sqlError = false;

		String sql = "select count(*) from custom_report cr, users u, user_project up, project p, survey s "
				+ "where u.id = up.u_id "
				+ "and p.id = up.p_id "
				+ "and p.id = cr.p_id "
				+ "and cr.id = ? "
				+ "and u.ident = ? ";
		
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, id);
			pstmt.setString(2, user);
			
			log.info("Is valid custom report id: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
			resultSet.next();
			
			count = resultSet.getInt(1);
			
			if(count == 0) {
				log.info("Validation of cusom report failed: " + pstmt.toString());
			}
		} catch (Exception e) {
			log.log(Level.SEVERE,"Error in Authorisation", e);
			sqlError = true;
		} finally {
			// Close the result set and prepared statement
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}
		
 		if(count == 0) {
 			log.info("Custom report validation failed for: " + user + " custom report id was: " + id);
 			
 			SDDataSource.closeConnection("is Valid Custom Report", conn);
			
			if(sqlError) {
				throw new ServerException();
			} else {
				throw new AuthorisationException("Invalid Custom Report Id");
			}
		} 
 		
		return true;
	}
	
}
