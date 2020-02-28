package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.model.Mailout;
import org.smap.sdal.model.MailoutPerson;
import org.smap.sdal.model.OrganisationLite;
import org.smap.sdal.model.People;
import org.smap.sdal.model.SubscriptionStatus;

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
 * Manage the log table
 * Assume emails are case insensitive
 */
public class MailoutManager {
	
	private static Logger log =
			 Logger.getLogger(MailoutManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	ResourceBundle localisation = null;
	
	public MailoutManager(ResourceBundle l) {
		localisation = l;	
	}

	public static String STATUS_NEW = "new";
	public static String STATUS_SENT = "sent";
	public static String STATUS_UNSUBSCRIBED = "unsubscribed";
	public static String STATUS_PENDING = "pending";
	
	/*
	 * Get mailouts for a survey
	 */
	public ArrayList<Mailout> getMailouts(Connection sd, String surveyIdent) throws SQLException {
		
		ArrayList<Mailout> mailouts = new ArrayList<> ();
		
		String sql = "select id, survey_ident, name "
				+ "from mailout "
				+ "where survey_ident = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, surveyIdent);
			log.info("Get mailouts: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				mailouts.add(new Mailout(
						rs.getInt("id"),
						rs.getString("survey_ident"), 
						rs.getString("name")));
				
			}
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return mailouts;
	}
	
	/*
	 * Add a mailout
	 */
	public void addMailout(Connection sd, Mailout mailout) throws SQLException, ApplicationException {
		
		String sql = "insert into mailout "
				+ "(survey_ident, name, created, modified) "
				+ "values(?, ?, now(), now())";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  mailout.survey_ident);
			pstmt.setString(2, mailout.name);
			log.info("Add mailout: " + pstmt.toString());
			pstmt.executeUpdate();
		
		} catch(Exception e) {
			String msg = e.getMessage();
			if(msg != null && msg.contains("duplicate key value violates unique constraint")) {
				throw new ApplicationException(localisation.getString("msg_dup_name"));
			} else {
				throw e;
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	/*
	 * Get mailouts Details
	 */
	public Mailout getMailoutDetails(Connection sd, int mailoutId) throws SQLException {
		
		Mailout mailout = null;
		
		String sql = "select survey_ident, name "
				+ "from mailout "
				+ "where id = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, mailoutId);
			log.info("Get mailout details: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				mailout = new Mailout(
						mailoutId,
						rs.getString("survey_ident"), 
						rs.getString("name"));
				
			}
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return mailout;
	}
	
	/*
	 * Get mailouts Details
	 */
	public ArrayList<MailoutPerson> getMailoutPeople(Connection sd, int mailoutId) throws SQLException {
		
		ArrayList<MailoutPerson> mp = new ArrayList<> ();
		
		String sql = "select mp.id, p.name, p.email, mp.status "
				+ "from mailout_people mp, people p "
				+ "where p.id = mp.p_id "
				+ "and mp.m_id = ? "
				+ "order by p.email asc ";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, mailoutId);
			log.info("Get mailout people: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				mp.add(new MailoutPerson(
						rs.getInt("id"),
						rs.getString("email"), 
						rs.getString("name"),
						rs.getString("status")));			
			}
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return mp;
	}
	
	public void deleteUnsentEmails(Connection sd, int mailoutId) throws SQLException {
		
		String sql = "delete from mailout_people "
				+ "where m_id = ? "
				+ "and status = ? ";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, mailoutId);
			pstmt.setString(2, STATUS_NEW);
			log.info("Delete unsent: " + pstmt.toString());
			pstmt.executeUpdate();	
		
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
public void writeEmails(Connection sd, int oId, ArrayList<MailoutPerson> mop, int mailoutId) throws Exception {
		
		String sqlGetPerson = "select id from people "
				+ "where o_id = ? "
				+ "and email = ? ";		
		PreparedStatement pstmtGetPerson = null;
		
		String sqlAddPerson = "insert into people "
				+ "(o_id, email, name) "
				+ "values(?, ?, ?)";		
		PreparedStatement pstmtAddPerson = null;
		
		String sqlAddMailoutPerson = "insert into mailout_people "
				+ "(p_id, m_id, status) "
				+ "values(?, ?, 'new') ";
		PreparedStatement pstmtAddMailoutPerson = null;
		
		try {
			pstmtGetPerson = sd.prepareStatement(sqlGetPerson);
			pstmtGetPerson.setInt(1, oId);
			
			pstmtAddPerson = sd.prepareStatement(sqlAddPerson, Statement.RETURN_GENERATED_KEYS);
			pstmtAddPerson.setInt(1, oId);
			
			pstmtAddMailoutPerson = sd.prepareStatement(sqlAddMailoutPerson);
			
			for(MailoutPerson person : mop) {
				
				int personId = 0;
				
				// 1. Get person details
				pstmtGetPerson.setString(2, person.email);
				ResultSet rs = pstmtGetPerson.executeQuery();
				if(rs.next()) {
					personId = rs.getInt(1);
				} else {
					
					// 2. Add person to people table if they do not exist
					pstmtAddPerson.setString(2, person.email);
					pstmtAddPerson.setString(3, person.name);
					pstmtAddPerson.executeUpdate();
					ResultSet rsKeys = pstmtAddPerson.getGeneratedKeys();
					if(rsKeys.next()) {
						personId = rsKeys.getInt(1);
					} else {
						throw new Exception("Failed to get id of person");
					}
				}
				
				// Write the entry into the mailout person table
				pstmtAddMailoutPerson.setInt(1, personId);
				pstmtAddMailoutPerson.setInt(2, mailoutId);
				
				pstmtAddMailoutPerson.executeUpdate();
			}
			
	
		} finally {
			try {if (pstmtGetPerson != null) {pstmtGetPerson.close();} } catch (SQLException e) {	}
			try {if (pstmtAddPerson != null) {pstmtAddPerson.close();} } catch (SQLException e) {	}
			try {if (pstmtAddMailoutPerson != null) {pstmtAddMailoutPerson.close();} } catch (SQLException e) {	}
		}
	}

/*
 * Send emails for a mailout
 */
public void sendEmails(Connection sd, int mailoutId) throws SQLException {
	
	String sql = "update mailout_people set status = '" + MailoutManager.STATUS_PENDING +"'  "
			+ "where m_id = ? "
			+ "and status = '" + MailoutManager.STATUS_NEW +"'";
	
	PreparedStatement pstmt = null;
	
	try {
		pstmt = sd.prepareStatement(sql);
		pstmt.setInt(1, mailoutId);
		log.info("Send unsent: " + pstmt.toString());
		pstmt.executeUpdate();
		
	
	} finally {
		try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
	}
	
	return;
}
	
}


