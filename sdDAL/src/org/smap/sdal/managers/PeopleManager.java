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
import org.smap.sdal.Utilities.HtmlSanitise;
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
public class PeopleManager {
	
	private static Logger log =
			 Logger.getLogger(PeopleManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	ResourceBundle localisation = null;
	
	public PeopleManager(ResourceBundle l) {
		localisation = l;	
	}
	
	/*
	 * Get an email key for this user that can be used to unsubscribe
	 * If the person is already unsubscribed then return null
	 */
	public SubscriptionStatus getEmailKey(Connection sd, 
			int oId,
			String email) throws SQLException, ApplicationException {
		
		String sql = "select unsubscribed, uuid, opted_in, opted_in_sent "
				+ "from people "
				+ "where o_id = ? "
				+ "and email = ? ";
		PreparedStatement pstmt = null;
		
		String sqlCreate = "insert into people "
				+ "(o_id, email, unsubscribed, uuid, opted_in) "
				+ "values(?, ?, 'false', ?, 'false')";
		PreparedStatement pstmtCreate = null;
		
		SubscriptionStatus subStatus = new SubscriptionStatus();
		try {
			if(email != null) {
				email = email.toLowerCase();
				pstmt = sd.prepareStatement(sql);	
				
				pstmt.setInt(1, oId);
				pstmt.setString(2, email);
				
				ResultSet rs = pstmt.executeQuery();
				if(rs.next()) {
					subStatus.unsubscribed = rs.getBoolean(1);
					subStatus.emailKey = rs.getString(2);
					subStatus.optedIn = rs.getBoolean(3);
					subStatus.optedInSent = rs.getTimestamp(4);
					
					// If the subscription key was not already set then get one now
					if(subStatus.emailKey == null) {
						subStatus.emailKey = getSubscriptionKey(sd, oId, email);
					}
				} else {
					// Create a key for this email and save it in the people table
					subStatus.emailKey = UUID.randomUUID().toString();
					subStatus.unsubscribed = false;
					subStatus.optedIn = false;
					subStatus.optedInSent = null;
					
					pstmtCreate = sd.prepareStatement(sqlCreate);
					pstmtCreate.setInt(1,  oId);
					pstmtCreate.setString(2, email);
					pstmtCreate.setString(3, subStatus.emailKey);
					log.info(pstmtCreate.toString());
					pstmtCreate.executeUpdate();
				}
				
			}

		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			try {if (pstmtCreate != null) {pstmtCreate.close();} } catch (SQLException e) {	}
		}
		
		return subStatus;

	}
	
	/*
	 * Get key that can be used to subscribe to emails
	 */
	public String getSubscriptionKey(Connection sd, int oId, String email) throws SQLException, ApplicationException {
		
		String sqlRegulate = "select count(*) "
				+ "from people "
				+ "where email = ? "
				+ "and o_id = ? "
				+ "and unsubscribed "
				+ "and (when_requested_subscribe + interval '1 day') > timestamp 'now' ";
		PreparedStatement pstmtRegulate = null;
		
		String sql = "select unsubscribed, uuid "
				+ "from people "
				+ "where email = ? "
				+ "and o_id = ? ";
		PreparedStatement pstmt = null;
		
		// Create an entry with the user initially unsubscribed
		String sqlCreate = "insert into people "
				+ "(o_id, email, unsubscribed, uuid, when_requested_subscribe) "
				+ "values(?, ?, 'true', ?, now())";
		PreparedStatement pstmtCreate = null;
		
		String sqlUpdate = "update people set "
				+ "uuid = ?, "
				+ "when_requested_subscribe = now() "
				+ "where email = ? "
				+ "and o_id = ? ";		
		PreparedStatement pstmtUpdate = null;
		
		String key = null;
		try {			
			
			if(email != null) {
				
				email = email.toLowerCase();
				
				// Make sure no subscribe requests have been made in the last 24 hours
				pstmtRegulate = sd.prepareStatement(sqlRegulate);
				pstmtRegulate.setString(1, email);
				pstmtRegulate.setInt(2, oId);
				log.info("Check for aleady sent subscription request: " + pstmtRegulate.toString());
				ResultSet rs = pstmtRegulate.executeQuery();
				if(rs.next() && rs.getInt(1) > 0) {
					log.info("Email request subscription already sent");
					throw new ApplicationException(localisation.getString("email_subs"));
				}
				
				/*
				 * Get an existing UUID
				 */
				pstmt = sd.prepareStatement(sql);	
				pstmt.setString(1, email);
				pstmt.setInt(2, oId);
				
				rs = pstmt.executeQuery();
				if(rs.next()) {
					// We already have an entry for this email
					// Create a new key and update the people table
					key = UUID.randomUUID().toString();
					pstmtUpdate = sd.prepareStatement(sqlUpdate);				
					pstmtUpdate.setString(1, key);
					pstmtUpdate.setString(2, email);
					pstmtUpdate.setInt(3, oId);
					log.info(pstmtUpdate.toString());
					pstmtUpdate.executeUpdate();
					
				} else {
					// Create a key for this email and save it in the people table
					key = UUID.randomUUID().toString();
					pstmtCreate = sd.prepareStatement(sqlCreate);
					pstmtCreate.setInt(1, oId);
					pstmtCreate.setString(2, email);
					pstmtCreate.setString(3, key);
					log.info(pstmtCreate.toString());
					pstmtCreate.executeUpdate();
				}
			}

		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			try {if (pstmtCreate != null) {pstmtCreate.close();} } catch (SQLException e) {	}
			try {if (pstmtUpdate != null) {pstmtUpdate.close();} } catch (SQLException e) {	}
			try {if (pstmtRegulate != null) {pstmtRegulate.close();} } catch (SQLException e) {	}
		}
		
		return key;

	}
	
	/*
	 * Unsubscribe the user based on the key
	 */
	public String unsubscribe(Connection sd, 
			String key) throws SQLException, ApplicationException {
		
		String sql = "update people "
				+ "set unsubscribed = true,"
				+ "when_unsubscribed = now() "
				+ "where uuid = ? "
				+ "and not unsubscribed";
		PreparedStatement pstmt = null;
		
		String sqlMailout = "update mailout_people "
				+ "set status = '" + MailoutManager.STATUS_UNSUBSCRIBED + "' "
				+ "where p_id = ?";
		PreparedStatement pstmtMailout = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);	
			pstmt.setString(1, key);			
			int count = pstmt.executeUpdate();
			if(count == 0) {
				throw new ApplicationException(localisation.getString("c_ns"));
			}
			
			// Update mailouts
			int personId = 0;
			ResultSet rsKeys = pstmt.getGeneratedKeys();
			if(rsKeys.next()) {
				personId = rsKeys.getInt(1);
			} 
			if(personId > 0) {
				pstmtMailout = sd.prepareStatement(sqlMailout);
				pstmtMailout.setInt(1, personId);
				pstmtMailout.executeUpdate();			
			}
			
			/*
			 * Log the event
			 */
			People person = getPersonFromSubscriberKey(sd,key);
			String note = localisation.getString("optin_unsubscribed");
			note = note.replace("%s1", person.email);
			lm.writeLogOrganisation(sd, person.oId, person.email, LogManager.OPTIN, note, 0);

		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
			try {if (pstmtMailout != null) {pstmtMailout.close();} } catch (SQLException e) {	}
		}
		
		return key;

	}
	
	/*
	 * Subscribe the user based on the key
	 */
	public String subscribeStep2(Connection sd, 
			String key) throws SQLException, ApplicationException {
		
		String sql = "update people "
				+ "set unsubscribed = false,"
				+ "opted_in = true, "
				+ "when_subscribed = now() "
				+ "where uuid = ? ";
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, key);			
			int count = pstmt.executeUpdate();
			if(count == 0) {
				throw new ApplicationException(localisation.getString("c_error"));
			}

			/*
			 * Log the event
			 */
			People person = getPersonFromSubscriberKey(sd,key);
			String note = localisation.getString("optin_subscribed");
			note = note.replace("%s1", person.email);
			lm.writeLogOrganisation(sd, person.oId, person.email, LogManager.OPTIN, note, 0);

		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return key;

	}
	
	/*
	 * Subscribe the user based on their email and organisation
	 * They can only be subscribed if opted in is false
	 */
	public void subscribeEmail(Connection sd, 
			String email, int oId) throws SQLException, ApplicationException {
		
		String sql = "update people "
				+ "set unsubscribed = false,"
				+ "opted_in = true, "
				+ "when_subscribed = now() "
				+ "where email = ? "
				+ "and o_id = ? "
				+ "and (opted_in is null or opted_in = false)";
		PreparedStatement pstmt = null;
		
		try {
			if(email != null) {
				email = email.toLowerCase();
			}
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, email);	
			pstmt.setInt(2,  oId);
			log.info("Subscribe for email: " + pstmt.toString());
			int count = pstmt.executeUpdate();
			
			/*
			 * Log the event
			 */
			if(count > 0) {
				String note = localisation.getString("optin_subscribed");
				note = note.replace("%s1", email);
				lm.writeLogOrganisation(sd, oId, null, LogManager.OPTIN, note, 0);
			}

		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		

	}
	
	/*
	 * Get the Person from the key
	 */
	public People getPersonFromSubscriberKey(Connection sd, 
			String key) throws SQLException, ApplicationException {
		
		People person = new People();
		
		String sql = "select o_id, email from people "
				+ "where uuid = ?";
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(1, key);	
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				person.oId = rs.getInt(1);
				person.email = rs.getString(2);
			}

		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		
		return person;

	}

	/*
	 * Add a person
	 */
	public void addPerson(Connection sd, int oId, People person) throws SQLException, ApplicationException {
		
		String sql = "insert into people "
				+ "(o_id, email, name) "
				+ "values(?, ?, ?)";
		
		PreparedStatement pstmt = null;
		
		try {
			String email = person.email;
			if(email != null) {
				email = email.toLowerCase();
			}
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2,  HtmlSanitise.checkCleanName(email, localisation));
			pstmt.setString(3, HtmlSanitise.checkCleanName(person.name, localisation));
			log.info("Add person: " + pstmt.toString());
			pstmt.executeUpdate();
		
		} catch(Exception e) {
			String msg = e.getMessage();
			if(msg != null && msg.contains("duplicate key value violates unique constraint")) {
				throw new ApplicationException(localisation.getString("subs_dup_email"));
			} else {
				throw e;
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	/*
	 * Update a person details
	 */
	public void updatePerson(Connection sd, People person) throws SQLException, ApplicationException {
		
		String sql = "update people "
				+ "set email = ?,"
				+ "name = ?"
				+ "where id = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			String email = person.email;
			if(email != null) {
				email = email.toLowerCase();
			}
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1,  HtmlSanitise.checkCleanName(email, localisation));
			pstmt.setString(2, HtmlSanitise.checkCleanName(person.name, localisation));	
			pstmt.setInt(3, person.id);
			
			log.info("Update person: " + pstmt.toString());
			pstmt.executeUpdate();
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
	
	/*
	 * Get all organisations where the specified email is unsubscribed
	 */
	public ArrayList<OrganisationLite> getUnsubOrganisationsFromEmail(Connection sd, String email) throws SQLException {
		
		String sql = "select o.id, o.name from people p, organisation o "
				+ "where p.o_id = o.id "
				+ "and p.email = ?"
				+ "and p.unsubscribed";
		
		PreparedStatement pstmt = null;
		
		ArrayList<OrganisationLite> oList = new ArrayList<> ();
		try {
			if(email != null) {
				email = email.toLowerCase();
			}
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, email);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				oList.add(new OrganisationLite(rs.getInt(1), rs.getString(2)));
			}

		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
		return oList;
	}
	
	/*
	 * Remove a contact
	 */
	public void deletePerson(Connection sd, int id) throws SQLException {
		
		String sql = "delete from people "
				+ "where id = ?";
		
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, id);
			
			log.info("Delete contact: " + pstmt.toString());
			pstmt.executeUpdate();
		} finally {
			try {if (pstmt != null) {pstmt.close();} } catch (SQLException e) {	}
		}
	}
}


