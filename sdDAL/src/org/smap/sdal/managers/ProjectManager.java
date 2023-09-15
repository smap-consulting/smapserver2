package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.HtmlSanitise;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.ProjectLinks;

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

public class ProjectManager {
	
	private static Logger log =
			 Logger.getLogger(ProjectManager.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	private ResourceBundle localisation;
	
	public ProjectManager(ResourceBundle l) {
		localisation = l;
	}
	
	/*
	 * Get projects
	 */
	public ArrayList<Project> getProjects(
			Connection sd, 
			String user,
			boolean all,			// If true get all projects in user organisation, otherwise just ones they are in
			boolean links,			// If true include links to other data that uses the project id as a key
			String urlprefix,		// Url prefix for links
			boolean emptyOnly,
			boolean importedOnly
			) throws ApplicationException, SQLException {
		
		PreparedStatement pstmt = null;
		ArrayList<Project> projects = new ArrayList<Project> ();
		
		try {
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, user);
			ResultSet resultSet = null;
			
			if(o_id > 0) {
				
				StringBuffer sql = new StringBuffer("select p.id, p.name, p.description, p.tasks_only, "
						+ "p.changed_by, p.changed_ts ");
				
				if(all) {
					sql.append("from project p "
							+ "where p.o_id = ? ");	
				} else {
					sql.append("from project p, user_project up, users u "
							+ "where p.o_id = ? "
							+ "and p.id = up.p_id "
							+ "and up.u_id = u.id "
							+ "and u.ident = ? ");				
				}
				if(emptyOnly) {
					sql.append("and p.id not in "
							+ " (select p_id from survey " 
							+ " where hidden = false) ");
				}
				if(importedOnly) {
					sql.append("and p.imported ");
				}
				sql.append("order by p.name asc");
				
				pstmt = sd.prepareStatement(sql.toString());			
				pstmt.setInt(1, o_id);
				if(!all) {
					pstmt.setString(2, user);
				}
				
				log.info("Get project list: " + pstmt.toString());
				resultSet = pstmt.executeQuery();
				while(resultSet.next()) {
					Project project = new Project();
					project.id = resultSet.getInt("id");
					project.name = resultSet.getString("name");
					project.desc = resultSet.getString("description");
					project.tasks_only = resultSet.getBoolean("tasks_only");
					project.changed_by = resultSet.getString("changed_by");
					project.changed_ts = resultSet.getString("changed_ts");
					
					if(links) {
						project.links = new ProjectLinks();
						project.links.task_groups = urlprefix + "api/v1/tasks/groups/" + project.id + "?links=true";
						project.links.surveys = urlprefix + "api/v1/admin/surveys/" + project.id + "?links=true";
					}
					projects.add(project);
			
				}
						
			} else {
				throw new ApplicationException("Error: No organisation");
			}
				
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {}
		}
		
		return projects;
	}

	/*
	 * Create a new project
	 */
	public int createProject(
			Connection sd, 
			String remoteUser,
			Project p, 
			int o_id, 
			int u_id, 
			String userIdent) throws SQLException, ApplicationException {
		
		String sql = "insert into project (name, description, o_id, changed_by, changed_ts) " +
				" values (?, ?, ?, ?, now());";
		int p_id = 0;
		
		PreparedStatement pstmt = null;
		try {
		
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1, HtmlSanitise.checkCleanName(p.name, localisation));
			pstmt.setString(2, HtmlSanitise.checkCleanName(p.desc, localisation));
			pstmt.setInt(3, o_id);
			pstmt.setString(4, userIdent);
			log.info("Insert project: " + pstmt.toString());
			pstmt.executeUpdate();
			ResultSet rs = pstmt.getGeneratedKeys();
			if(rs.next()) {
				p_id = rs.getInt(1);
			}
			pstmt.close();
		
			if(p_id > 0) {
				// Add the user to the new project by default
				sql = "insert into user_project (u_id, p_id) " +
						" values (?, ?)";
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, u_id);
				pstmt.setInt(2, p_id);
				log.info("Add the user to the project " + pstmt.toString());
				pstmt.executeUpdate();
				
				String msg = localisation.getString("msg_add_proj");
				msg = msg.replace("%s1", p.name);
				lm.writeLogOrganisation(sd, o_id, remoteUser, LogManager.CREATE, 
						msg, 0);
			}
		} finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return p_id;
	}
	
	/*
	 * Add a user to a project
	 */
	public int addUser(
			Connection sd, 
			int p_id, 
			int o_id, 
			int u_id) throws SQLException {
		
		String sqlValid = "select count(*) from users "
				+ "where id = ? "
				+ "and o_id = ? "
				+ "and id not in (select u_id from user_project where p_id = ?)";
		PreparedStatement pstmtValid = null;
	
		String sql = "insert into user_project (u_id, p_id) " +
				" values (?, ?)";
		PreparedStatement pstmt = null;
		try {
		
			pstmt = sd.prepareStatement(sql);
			
			pstmtValid = sd.prepareStatement(sqlValid);
			pstmtValid.setInt(1, u_id);
			pstmtValid.setInt(2, o_id);
			pstmtValid.setInt(3, p_id);
			
			log.info("Is valid: " + pstmtValid.toString());
			ResultSet rs = pstmtValid.executeQuery();
			if(rs.next() && rs.getInt(1) > 0) {
				pstmt.setInt(1, u_id);
				pstmt.setInt(2, p_id);
				log.info("Add user: " + pstmt.toString());
				pstmt.executeUpdate();
			}

			
		} finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			try {if (pstmtValid != null) {pstmtValid.close();} } catch (SQLException e) {	}
		}
		
		return p_id;
	}
	
	
	/*
	 * Delete projects
	 */
	public void deleteProjects(Connection sd, Connection cResults,
			Authorise a, 
			ArrayList<Project> pArray, 
			String remoteUser,
			String basePath) throws Exception {

		PreparedStatement pstmt = null;
		
		try {	
			String sql = null;
			ResultSet resultSet = null;
			
			int o_id = GeneralUtilityMethods.getOrganisationId(sd, remoteUser);
				
			for(int i = 0; i < pArray.size(); i++) {
				Project p = pArray.get(i);
				
				a.projectInUsersOrganisation(sd, remoteUser, p.id);		// Authorise deletion of this project
				
				String project_name = GeneralUtilityMethods.getProjectName(sd, p.id);
				
				/*
				 * Ensure that there are no undeleted surveys in this project
				 * Don't count hidden surveys which have been replaced
				 */
				sql = "select count(*) "
						+ " from survey s " 
						+ " where s.p_id = ? "
						+ "and s.hidden = false";
				
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, p.id);
				log.info("Check for undeleted surveys: " + pstmt.toString());
				resultSet = pstmt.executeQuery();
				if(resultSet.next()) {
					int count = resultSet.getInt(1);
					if(count > 0) {
						String msg = localisation.getString("msg_undel_proj").replace("%s1", String.valueOf(p.id));
						throw new Exception(msg);
					}
				} else {
					throw new Exception("Error getting survey count");
				}			
				
				// Erase any hidden forms 
				ServerManager sm = new ServerManager();
				sql = "select s_id, ident, display_name "
						+ " from survey s " 
						+ " where s.p_id = ? "
						+ "and s.hidden = true";
				try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {}
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, p.id);
				
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					int sId = rs.getInt(1);
					String ident = rs.getString(2);
					String displayName = rs.getString(3);
					sm.deleteSurvey(		// Delete the replaced survey
							sd, 
							cResults,
							remoteUser,
							p.id,
							sId,
							ident,
							displayName,
							basePath,
							true,
							"yes");
				}
				
				/*
				 * Delete the project
				 */
				sql = "delete from project p " 
						+ "where p.id = ? "
						+ "and p.o_id = ?";			// Ensure the project is in the same organisation as the administrator doing the editing
					
				try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {}
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, p.id);
				pstmt.setInt(2, o_id);
				log.info("Delete project: " + pstmt.toString());
				pstmt.executeUpdate();

				/*
				 * Delete any notifications attached to this project
				 */
				sql = "delete from forward " 
						+ "where p_id = ? ";
											
				try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {}
				pstmt = sd.prepareStatement(sql);
				pstmt.setInt(1, p.id);
				log.info("Delete notification: " + pstmt.toString());
				pstmt.executeUpdate();
				
				/*
				 * Write to the log
				 */
				String msg = localisation.getString("msg_del_proj");
				msg = msg.replace("%s1", project_name);
				lm.writeLogOrganisation(sd, o_id, remoteUser, LogManager.DELETE, msg, 0);
			}
			
			sd.commit();
					
		} finally {	
			try {if (pstmt != null) {pstmt.close();}	} catch (SQLException e) {}
		}
		
	}
	
	/*
	 * Write a list of projects
	 */
	public ArrayList<String> writeProjects(
			Connection sd, 
			ArrayList<Project> projects, 
			int o_id, 
			String userIdent,
			boolean imported) throws SQLException, ApplicationException {
		
		ArrayList<String> added = new ArrayList<String> ();
		
		String sql = "insert into project (name, description, o_id, changed_by, changed_ts, imported) " +
				" values (?, ?, ?, ?, now(), ?);";		
		PreparedStatement pstmt = null;
		
		String sqlExists = "select count(*) from project where name = ? and o_id = ?";
		PreparedStatement pstmtExists = null;
		
		ResultSet rs = null;
		
		try {
		
			// Prepare insert
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(3, o_id);
			pstmt.setString(4, userIdent);
			pstmt.setBoolean(5, imported);
			
			// Prepare exists check
			pstmtExists = sd.prepareStatement(sqlExists);
			pstmtExists.setInt(2,  o_id);
			
			for(Project p : projects) {
				pstmtExists.setString(1,  p.name);
				if(rs != null) {
					rs.close();
				}
				rs = pstmtExists.executeQuery();
				if(!rs.next() || rs.getInt(1) == 0) {
					pstmt.setString(1, HtmlSanitise.checkCleanName(p.name, localisation));
					pstmt.setString(2, HtmlSanitise.checkCleanName(p.desc, localisation));
					pstmt.executeUpdate();
					
					added.add(p.name);
				}
			}

		} finally {		
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			try {if (pstmtExists != null) {pstmtExists.close();} } catch (SQLException e) {	}
		}
		return added;
	}
	
}
