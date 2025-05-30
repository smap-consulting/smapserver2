package org.smap.sdal.managers;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.RoleColumnFilter;
import org.smap.sdal.model.RoleColumnFilterOld;
import org.smap.sdal.model.RoleName;
import org.smap.sdal.model.SqlFrag;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

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
 * This class supports access to User and Organisation information in the database
 */
public class RoleManager {
	
	private static Logger log =
			 Logger.getLogger(RoleManager.class.getName());

	LogManager lm = new LogManager(); // Application log
	ResourceBundle localisation = null;
	
	Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
	
	public RoleManager(ResourceBundle l) {
		localisation = l;
	}

	/*
	 * Get available roles
	 */
	public ArrayList<Role> getRoles(Connection sd, int o_id) throws SQLException {
		PreparedStatement pstmt = null;
		ArrayList<Role> roles = new ArrayList<Role> ();
		
		try {
			String sql = null;
			ResultSet resultSet = null;
			
			/*
			 * Get the roles for this organisation
			 */
			sql = "SELECT r.id as id, "
					+ "r.name as name, "
					+ "r.description as desc,"
					+ "r.changed_by as changed_by,"
					+ "r.changed_ts as changed_ts "
					+ "from role r "
					+ "where r.o_id = ? "
					+ "order by r.name asc";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, o_id);
			log.info("Get user roles: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
							
			Role role = null;
			while(resultSet.next()) {
				role = new Role();
				role.oId = o_id;
				role.id = resultSet.getInt("id");
				role.name = resultSet.getString("name");
				role.desc = resultSet.getString("desc");
				role.changed_by = resultSet.getString("changed_by");
				role.changed_ts = resultSet.getString("changed_ts");
				roles.add(role);
			}
			

					    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return roles;
	}
	
	/*
	 * Get the names and idents of roles
	 */
	public ArrayList<RoleName> getRoleNames(Connection sd, int o_id) throws SQLException {
		PreparedStatement pstmt = null;
		ArrayList<RoleName> roles = new ArrayList<> ();
		
		try {
			String sql = null;
			ResultSet resultSet = null;
			
			/*
			 * Get the roles for this organisation
			 */
			sql = "SELECT id, "
					+ "name "
					+ "from role "
					+ "where o_id = ? "
					+ "order by name asc";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, o_id);
			resultSet = pstmt.executeQuery();
							
			while(resultSet.next()) {
				roles.add(new RoleName(resultSet.getInt("id"), resultSet.getString("name")));
			}
						    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return roles;
	}

	
	/*
	 * Create a new Role
	 */
	public int createRole(Connection sd, 
			Role r, 
			int oId, 
			String ident,
			boolean imported) throws Exception {
		
		int rId = 0;
		String sql = "insert into role (o_id, name, description, imported, changed_by, changed_ts) " +
				" values (?, ?, ?, ?, ?, now());";
		
		PreparedStatement pstmt = null;
		
		try {
			
			if(GeneralUtilityMethods.getRoleId(sd, r.name, oId) <= 0) {	// Role name does not exist
				pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
				pstmt.setInt(1, oId);
				pstmt.setString(2, HtmlSanitise.checkCleanName(r.name, localisation));
				pstmt.setString(3, HtmlSanitise.checkCleanName(r.desc, localisation));
				pstmt.setBoolean(4, imported);
				pstmt.setString(5, ident);
	
				log.info("SQL: " + pstmt.toString());
				pstmt.executeUpdate();
				
				ResultSet rs = pstmt.getGeneratedKeys();
				if(rs.next()) {
					rId = rs.getInt(1);
				}
				
				setUsersForRole(sd, rId, r.users);
			}
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}
		
		return rId;

	}
	
	/*
	 * Update a Role
	 */
	public void updateRole(Connection sd, 
			Role r, 
			int o_id, 
			String ident) throws Exception {
		
		String sql = "update role set name = ?, "
				+ "description = ?,"
				+ "changed_by = ?,"
				+ "changed_ts = now() "
				+ "where o_id = ? "
				+ "and id = ?"; 
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);

			pstmt.setString(1, HtmlSanitise.checkCleanName(r.name, localisation));
			pstmt.setString(2, HtmlSanitise.checkCleanName(r.desc, localisation));
			pstmt.setString(3, ident);
			pstmt.setInt(4, o_id);
			pstmt.setInt(5, r.id);

			log.info("SQL: " + pstmt.toString());
			pstmt.executeUpdate();
			
			setUsersForRole(sd, r.id, r.users);
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}

	}
	
	/*
	 * delete roles
	 */
	public void deleteRoles(Connection sd, 
			ArrayList<Role> rArray, 
			int o_id,
			String user) throws Exception {
		
		String sql = "delete from role where o_id = ? and id = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			
			for(int i = 0; i < rArray.size(); i++) {
				
				String roleName = GeneralUtilityMethods.getRoleName(sd, rArray.get(i).id, o_id);
				
				pstmt.setInt(1, o_id);
				pstmt.setInt(2, rArray.get(i).id);
				pstmt.executeUpdate();

				if(roleName != null) {
					String msg = localisation.getString("r_deleted");
					msg = msg.replace("%s1", roleName);
					lm.writeLogOrganisation(sd, o_id, user, LogManager.ROLE, msg, 0);
				}
			}
			
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}

	}
	
	/*
	 * delete imported roles
	 */
	public int deleteImportedRoles(Connection sd, int o_id) throws Exception {
		
		String sql = "delete from role where o_id = ? and imported";		
		PreparedStatement pstmt = null;
		
		int count = 0;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, o_id);		
			count = pstmt.executeUpdate();
			
		}  finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			
		}
		
		return count;

	}
	
	/*
	 * Get roles associated with a survey
	 */
	public ArrayList<Role> getSurveyRoles(Connection sd, String sIdent, int o_id, boolean enabledOnly, 
			String user, boolean isSuperUser) throws Exception {
		PreparedStatement pstmt = null;
		ArrayList<Role> roles = new ArrayList<Role> ();
		
		try {
			ResultSet resultSet = null;
			
			String sql = null;
			String sqlSuperUser = "SELECT r.id as id, "
					+ "r.name as name, "
					+ "sr.role_group, "
					+ "sr.enabled, "
					+ "sr.id as sr_id,"
					+ "sr.row_filter,"
					+ "sr.column_filter "
					+ "from role r "
					+ "left outer join survey_role sr "
					+ "on r.id = sr.r_id "
					+ "and sr.survey_ident = ? "
					+ "where o_id = ? ";
			
			String sqlNormalUser = "SELECT r.id as id, "
					+ "r.name as name, "
					+ "sr.role_group, "
					+ "sr.enabled, "
					+ "sr.id as sr_id,"
					+ "sr.row_filter,"
					+ "sr.column_filter "
					+ "from role r "
					+ "left outer join survey_role sr "
					+ "on r.id = sr.r_id "
					+ "and sr.survey_ident = ? "
					+ "join user_role ur "
					+ "on r.id = ur.r_id "
					+ "join users u "
					+ "on ur.u_id = u.id "
					+ "where r.o_id = ? "
					+ "and u.ident = ? ";
			
			if(isSuperUser) {
				sql = sqlSuperUser;
			} else {
				sql = sqlNormalUser;
			}
			
			if(enabledOnly) {
				sql += "and sr.enabled ";
			}
			sql += "order by r.name asc";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sIdent);
			pstmt.setInt(2, o_id);
			if(!isSuperUser) {
				pstmt.setString(3,  user);
			}
			log.info("Get survey roles: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
							
			Role role = null;
			Type colFilterType = new TypeToken<ArrayList<RoleColumnFilter>>(){}.getType();
			while(resultSet.next()) {		
				role = new Role();
				role.srId = resultSet.getInt("sr_id");
				role.id = resultSet.getInt("id");
				role.name = resultSet.getString("name");
				role.enabled = resultSet.getBoolean("enabled");
				role.row_filter = resultSet.getString("row_filter");
				role.role_group = resultSet.getString("role_group");
				
				if(role.role_group == null) {
					role.role_group = "A";		// Default the role group if not set
				}
				
				// check for legacy question ids as filter columns
				String colFilter = resultSet.getString("column_filter");
				if(colFilter != null) {
					role.column_filter = gson.fromJson(colFilter, colFilterType);
					convertLegacyColumns(sd, sIdent, role.column_filter, colFilter, role);	// deal with legacy columns specified as question ids	
				}
				
				roles.add(role);
			}		
					    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return roles;
	}
	
	/*
	 * Update a survey role or the link between a survey and a role
	 */
	public int updateSurveyRole(Connection sd, String sIdent, int rId, boolean enabled) throws SQLException {
		
		PreparedStatement pstmt = null;
		int newLinkId = 0;
		
		try {
			String sqlNew = "insert into survey_role (survey_ident, r_id, enabled) "
					+ "values (?, ?, ?)";
			
			String sqlExisting = "update survey_role "
					+ "set enabled = ? "
					+ "where r_id = ? "
					+ "and survey_ident = ?";
			
			pstmt = sd.prepareStatement(sqlExisting);
			pstmt.setBoolean(1, enabled);
			pstmt.setInt(2, rId);
			pstmt.setString(3, sIdent);
			log.info("Update survey role: " + pstmt.toString());
			int count = pstmt.executeUpdate();
			
			if(count == 0) {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sqlNew, Statement.RETURN_GENERATED_KEYS);
				pstmt.setString(1, sIdent);
				pstmt.setInt(2, rId);
				pstmt.setBoolean(3, enabled);	
				log.info("Create new survey role: " + pstmt.toString());
				pstmt.executeUpdate();
				
				ResultSet rs = pstmt.getGeneratedKeys();
				if(rs.next()) {
					newLinkId = rs.getInt(1);
				} 
			}
				    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return newLinkId;
	}
	
	/*
	 * Update a survey role group
	 */
	public void updateSurveyRoleFilterGroup(Connection sd, String sIdent, int rId, String group) throws SQLException {
		
		PreparedStatement pstmt = null;
		
		try {
			
			String sql = "update survey_role "
					+ "set role_group = ? "
					+ "where r_id = ? "
					+ "and survey_ident = ?";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, group);
			pstmt.setInt(2, rId);
			pstmt.setString(3, sIdent);
			log.info("Update survey role: " + pstmt.toString());
			pstmt.executeUpdate();
				    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
	}
	
	/*
	 * Update the row filter in a survey link
	 */
	public void updateSurveyRoleRowFilter(Connection sd, String sIdent, 
			Role role, ResourceBundle localisation) throws Exception {
		
		PreparedStatement pstmt = null;
		
		SqlFrag sq = new SqlFrag();
		sq.addSqlFragment(role.row_filter, false, localisation, 0);
		
		// Compile a list of columns not in the survey group and throw an error if there are any
		StringBuilder bad = new StringBuilder();
		String groupSurveyIdent = GeneralUtilityMethods.getGroupSurveyIdentFromIdent(sd, sIdent);
		for(int i = 0; i < sq.columns.size(); i++) {		
			if(GeneralUtilityMethods.getGroupColumnName(sd, groupSurveyIdent, sq.humanNames.get(i)) == null) {
				if(bad.length() > 0) {
					bad.append(", ");
				}
				bad.append(sq.columns.get(i));
			}
		}
		if(bad.length() > 0) {
			throw new Exception(localisation.getString("r_mc") + " " + bad);
		}
		
		// if the row filter is empty set it to null
		if(role.row_filter != null && role.row_filter.trim().length() == 0) {
			role.row_filter = null;
		}
		
		try {
			String sql = "update survey_role "
					+ "set row_filter = ? "
					+ "where r_id = ? "
					+ "and survey_ident = ?";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, role.row_filter);
			pstmt.setInt(2, role.id);
			pstmt.setString(3, sIdent);
			
			log.info("Update survey roles: " + pstmt.toString());
			pstmt.executeUpdate();

			    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
	}
	
	/*
	 * Update the column filter in a role
	 */
	public void updateSurveyRoleColumnFilter(Connection sd, String sIdent, 
			Role role, ResourceBundle localisation,
			int sId) throws Exception {
		
		PreparedStatement pstmt = null;
		
		String configString = gson.toJson(role.column_filter);
		
		if(configString != null && (configString.trim().length() == 0 || configString.trim().equals("[]"))) {
			configString = null;
		}
		try {
			String sql = "update survey_role "
					+ "set column_filter = ? "
					+ "where r_id = ? "
					+ "and survey_ident = ?";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, configString);
			pstmt.setInt(2, role.id);
			pstmt.setString(3, sIdent);
			
			log.info("Update survey roles: " + pstmt.toString());
			pstmt.executeUpdate();

			    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
	}
	
	/*
	 * Throw an exception if the survey has enabled roles but the user has none of these roles 
	 */
	public void validateReportRoles(Connection sd, String sIdent, String user) throws Exception {
		
		PreparedStatement pstmt = null;
		
		String sqlSurveyRoles = "select count(*) from survey_role where survey_ident = ? and enabled";
		
		String sqlSurveyUserRoles = "SELECT count(*) "
				+ "from survey_role sr, user_role ur, users u "
				+ "where sr.survey_ident = ? "
				+ "and sr.r_id = ur.r_id "
				+ "and sr.enabled = true "
				+ "and ur.u_id = u.id "
				+ "and u.ident = ?";
		
		try {
			pstmt = sd.prepareStatement(sqlSurveyRoles);
			pstmt.setString(1, sIdent);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next() && rs.getInt(1) > 0) {
				// Survey has roles
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
				pstmt = sd.prepareStatement(sqlSurveyUserRoles);
				pstmt.setString(1, sIdent);
				pstmt.setString(2, user);
				rs = pstmt.executeQuery();
				if(!rs.next() || rs.getInt(1) == 0) {
					// There are no roles common to the survey and the user
					throw new ApplicationException(localisation.getString("rep_report_role"));
				}
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
	}
	
	/*
	 * Get the sql for a survey role filter for a specific user and survey
	 * A user can have multiple roles as can a survey hence an array of roles is returned
	 */
	public ArrayList<SqlFrag> getSurveyRowFilter(Connection sd, String sIdent, String user) throws Exception {
		
		PreparedStatement pstmt = null;
		ArrayList<SqlFrag> rfArray = new ArrayList<SqlFrag> ();
		
		try {
			ResultSet resultSet = null;
			
			String sql = "SELECT sr.row_filter, sr.role_group "
					+ "from survey_role sr, user_role ur, users u "
					+ "where sr.survey_ident = ? "
					+ "and sr.r_id = ur.r_id "
					+ "and sr.enabled = true "
					+ "and ur.u_id = u.id "
					+ "and u.ident = ?";
							
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sIdent);
			pstmt.setString(2, user);
			log.info("Get surveyRowFilter: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
							
			while(resultSet.next()) {		
				String sqlFragString = resultSet.getString("row_filter");
				String group = resultSet.getString("role_group");
				if(sqlFragString != null && sqlFragString.trim().length() > 0) {
					SqlFrag sf = null;
					if(sqlFragString.trim().startsWith("{")) {
						sf = gson.fromJson(sqlFragString, SqlFrag.class);
					} else {
						sf = new SqlFrag();			// New only the string is stored
						sf.addSqlFragment(sqlFragString, false, localisation, 0);			
					}
					sf.group = group;	
					rfArray.add(sf);
				}		
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return rfArray;
	}
	
	/*
	 * Get the sql for a survey role filter for a specific user and survey
	 * A user can have multiple roles as can a survey hence an array of roles is returned
	 */
	public ArrayList<SqlFrag> getSurveyRowFilterRoleList(Connection sd, String sIdent, ArrayList<Role> roles) throws Exception {
		
		PreparedStatement pstmt = null;
		ArrayList<SqlFrag> rfArray = new ArrayList<SqlFrag> ();
		ArrayList<Integer> roleids = new ArrayList<> ();
		
		if(roles != null && roles.size() > 0) {
			for(Role r : roles) {
				roleids.add(r.id);
			}
			
			try {
				String sql = null;
				ResultSet resultSet = null;
				
				sql = "SELECT sr.row_filter "
						+ "from survey_role sr, user_role ur, users u "
						+ "where sr.survey_ident = ? "
						+ "and sr.enabled = true "
						+ "and sr.r_id = any(?) ";
								
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, sIdent);
				pstmt.setArray(2, sd.createArrayOf("text", roleids.toArray(new Integer[roleids.size()])));
				log.info("Get surveyRowFilter: " + pstmt.toString());
				resultSet = pstmt.executeQuery();
								
				while(resultSet.next()) {		
					String sqlFragString = resultSet.getString("row_filter");
					if(sqlFragString != null) {
						if(sqlFragString.trim().startsWith("{")) {
							rfArray.add(gson.fromJson(sqlFragString, SqlFrag.class));		// legacy json
						} else {
							SqlFrag sf = new SqlFrag();									// New only the string is stored
							sf.addSqlFragment(sqlFragString, false, localisation, 0);
							rfArray.add(sf);
						}
					}		
				}
			} finally {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			}
		}
		
		return rfArray;
	}
	
	/*
	 * Convert an array of sql fragments into raw SQL
	 * Fragments from the same group are combined with "or"
	 * Different groups are combined with "and"
	 */
	public String convertSqlFragsToSql(ArrayList<SqlFrag> rfArray) {

		StringBuilder rowFilter = new StringBuilder("");
		StringBuilder sqlFilter = new StringBuilder("");
		
		
		if(rfArray.size() > 0) {
			// 1. Break the array down into groups
			HashMap<String, ArrayList<SqlFrag>> groups = new HashMap<>();			
			for(SqlFrag rf : rfArray) {				
				ArrayList<SqlFrag> a = groups.get(rf.group);
				if(a == null) {
					a = new ArrayList<>();
				}
				a.add(rf);
				groups.put(rf.group, a);
			}
			
			// 2. The passed in role array may have the order of its elements changed, update it so that allocation of variables is still done in the correct order
			rfArray.clear();
			
			// 2. Loop through the groups and combine with "and"
			for(String group : groups.keySet()) {
				String gf = getGroupFilterSql(groups.get(group), rfArray);
				if(gf.length() > 0) {
					if(rowFilter.length() > 0) {
						rowFilter.append(" and");
					}
					rowFilter.append(" (");
					rowFilter.append(gf);
					rowFilter.append(")");
				}
				
			}
			sqlFilter.append("(");
			sqlFilter.append(rowFilter);
			sqlFilter.append(")");
		}
		return sqlFilter.toString();
	}
	
	/*
	 * Combine SQL fragments in the same group with "OR"
	 * Add each SQL fragment back to rfArray in the order that they are processed
	 */
	private String getGroupFilterSql(ArrayList<SqlFrag> groupArray, ArrayList<SqlFrag> rfArray) {
		StringBuilder gfString = new StringBuilder("");
		
		if(groupArray.size() > 0) {
			for(SqlFrag rf : groupArray) {	
				if(rf.columns.size() > 0) {
					if(gfString.length() > 0) {
						gfString.append(" or");
					}
					gfString.append(" (");
					gfString.append(rf.sql.toString());
					gfString.append(")");
				}
				
				rfArray.add(rf);
			}
		}
		
		return gfString.toString();
	}
	
	/*
	 * Get the columns for a survey role column filter for a specific user and survey
	 * A user can have multiple roles as can a survey hence an array of roles is returned
	 */
	public ArrayList<RoleColumnFilter> getSurveyColumnFilter(Connection sd, String sIdent, String user) throws Exception {
		
		PreparedStatement pstmt = null;
		ArrayList<RoleColumnFilter> cfArray = new ArrayList<RoleColumnFilter> ();
		
		try {
			String sql = null;
			ResultSet resultSet = null;
			
			sql = "SELECT sr.column_filter "
					+ "from survey_role sr, user_role ur, users u "
					+ "where sr.survey_ident = ? "
					+ "and sr.enabled = true "
					+ "and sr.r_id = ur.r_id "
					+ "and ur.u_id = u.id "
					+ "and u.ident = ?";
							
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sIdent);
			pstmt.setString(2, user);
			log.info("Get surveyColumnFilter: " + pstmt.toString());
			resultSet = pstmt.executeQuery();
							
			Type cfArrayType = new TypeToken<ArrayList<RoleColumnFilter>>(){}.getType();
			
			while(resultSet.next()) {		
				String sqlFragString = resultSet.getString("column_filter");
				if(sqlFragString != null) {
					ArrayList<RoleColumnFilter> cols = gson.fromJson(sqlFragString, cfArrayType);
					convertLegacyColumns(sd, sIdent, cols, sqlFragString, null);	// deal with legacy columns specified as question ids	

					cfArray.addAll(cols);
				}		
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return cfArray;
	}
	
	/*
	 * Get the columns for a survey role column filter for an array of roles and a survey
	 * This is used for immediate anonymous requests such as prepopulating a survey where there is no user
	 *  but there is a set of roles
	 */
	public ArrayList<RoleColumnFilter> getSurveyColumnFilterRoleList(Connection sd, String sIdent, ArrayList<Role> roles) throws SQLException {
		
		ArrayList<RoleColumnFilter> cfArray = new ArrayList<RoleColumnFilter> ();
		ArrayList<Integer> roleids = new ArrayList<> ();
		
		if(roles.size() > 0) {
			PreparedStatement pstmt = null;
			
			for(Role r : roles) {
				roleids.add(r.id);
			}
			
			try {
				String sql = null;
				ResultSet resultSet = null;
				
				sql = "SELECT sr.column_filter "
						+ "from survey_role sr "
						+ "where sr.survey_ident = ? "
						+ "and sr.enabled = true "
						+ "and sr.r_id = any(?) ";
								
				pstmt = sd.prepareStatement(sql);
				pstmt.setString(1, sIdent);
				pstmt.setArray(2, sd.createArrayOf("text", roleids.toArray(new Integer[roleids.size()])));
				log.info("Get surveyColumnFilter From Role List: " + pstmt.toString());
				resultSet = pstmt.executeQuery();
								
				Type cfArrayType = new TypeToken<ArrayList<RoleColumnFilter>>(){}.getType();
				
				while(resultSet.next()) {		
					String sqlFragString = resultSet.getString("column_filter");
					if(sqlFragString != null) {
						ArrayList<RoleColumnFilter> cols = gson.fromJson(sqlFragString, cfArrayType);
						cfArray.addAll(cols);
					}		
				}
			} finally {
				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			}
		}
		
		return cfArray;
	}
	
	private void setUsersForRole(Connection sd, int rId, ArrayList<Integer> users) {
		
		String sqlDelete = "delete from user_role where r_id = ?";
		PreparedStatement pstmtDelete = null;
		
		String sql = "insert into user_role (u_id, r_id) values (?, ?)";
		PreparedStatement pstmt = null;
		
		
		try {
			log.info("Set autocommit false");
			sd.setAutoCommit(false);
			
			// delete existing
			pstmtDelete = sd.prepareStatement(sqlDelete);
			pstmtDelete.setInt(1, rId);
			pstmtDelete.executeUpdate();
			
			// add new
			if(users != null && users.size() > 0) {
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(2, rId);
				for(int uId : users) {
					pstmt.setInt(1, uId);
					pstmt.executeUpdate();
				}
			}
				
			sd.commit();
			
		} catch (Exception e) {
			try {sd.rollback();} catch(Exception ex) {}
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			try {sd.setAutoCommit(true);} catch(Exception e) {}
			try {if (pstmtDelete != null) {pstmtDelete.close();}} catch (SQLException e) {}
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
	}
	
	/*
	 * Check for legacy column filters which use question ids to filter on - 27/5/2025 - Remove after a year or so
	 * Question names are now used. This means that column filters are automatically preserved on survey replace
	 */
	private  void convertLegacyColumns(Connection sd, String sIdent, 
			ArrayList<RoleColumnFilter> column_filter, String colFilter,
			Role role) throws Exception {
		if(column_filter!= null && column_filter.size() > 0) {
			if(column_filter.get(0).name == null) {
				// legacy found convert the definition into column names
				log.info("x1x1x1x1x1x1x1x1x1x1: Found legacy column filter: " + colFilter);
				Type legacyColFilterType = new TypeToken<ArrayList<RoleColumnFilterOld>>(){}.getType();
				ArrayList<RoleColumnFilterOld> questions = gson.fromJson(colFilter, legacyColFilterType);
				int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
				column_filter.clear();
				if(questions.size() > 0) {
					for(RoleColumnFilterOld old : questions) {
						column_filter.add(new RoleColumnFilter(GeneralUtilityMethods.getQuestionNameFromId(sd, sId, old.id)));
					}
				}
				
				// write the updated version back if we have the role details
				if(role != null) {
					role.column_filter = column_filter;
					updateSurveyRoleColumnFilter(sd, sIdent, 
							role, localisation,
							sId);
				}
			}
		}
		
		
	}

}
