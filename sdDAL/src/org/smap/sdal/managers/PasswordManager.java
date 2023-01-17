package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;

import me.gosimple.nbvcxz.Nbvcxz;
import me.gosimple.nbvcxz.resources.Configuration;
import me.gosimple.nbvcxz.resources.ConfigurationBuilder;
import me.gosimple.nbvcxz.resources.Dictionary;
import me.gosimple.nbvcxz.scoring.Result;

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
 * Manage the sending of emails
 */
public class PasswordManager {

	private static Logger log =
			Logger.getLogger(PasswordManager.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	private Connection sd;
	private String userIdent;
	private boolean checkStrength = false;
	private double minStrength = 0.0;
	private ResourceBundle localisation;
	private Locale locale;
	private String hostname;
	Result result;
	
	public PasswordManager(Connection sd, 
			Locale locale, 
			ResourceBundle localisation, 
			String userIdent,
			String hostname) throws SQLException {
		
		this.sd = sd;
		this.locale = locale;
		this.localisation = localisation;
		this.userIdent = userIdent;
		this.hostname = hostname;
		
		minStrength = getMinStrength();
		if(minStrength > 0.0) {
			checkStrength = true;		// Always check password strength irrespective of user privileges
			//checkStrength = validUser();
		}

	}
	
	public void checkStrength(String password) throws ApplicationException {

		if(checkStrength && password != null) {
			// Check for blocked password
			if(password.equals("password") 
					|| password.equals("Passw0rd")
					|| password.equals("b0Gota987")
					|| password.equals("q2@dFgVPx")
					|| password.equals("q2@dFgVPxFvv%67d")) {
				throw new ApplicationException(localisation.getString("msg_bp"));
			}
			// Create a map of excluded words on a per-user basis using a hypothetical "User" object that contains this info
			List<Dictionary> dictionaryList = ConfigurationBuilder.getDefaultDictionaries();
	
			// Create our configuration object and set our custom minimum
			// entropy, and custom dictionary list
			Configuration configuration = new ConfigurationBuilder()
			        .setMinimumEntropy(minStrength)
			        .setLocale(Locale.forLanguageTag(locale.getLanguage()))
			        .setDictionaries(dictionaryList)
			        .createConfiguration();
			        
			// Create our Nbvcxz object with the configuration we built
			Nbvcxz nbvcxz = new Nbvcxz(configuration);
			result = nbvcxz.estimate(password);
			if(!result.isMinimumEntropyMet()) {
				String logMessage = localisation.getString("msg_wpl");
				logMessage = logMessage.replace("%s1", String.format("%.0f", result.getEntropy()));
				logMessage = logMessage.replace("%s2", String.format("%.0f", minStrength));
				lm.writeLog(sd, 0, userIdent, LogManager.USER, logMessage, 0, hostname);
				
				throw new ApplicationException(localisation.getString("msg_wp"));
			}
		}
	}
	
	public void logReset() {
		if(result != null) {
			String logMessage = localisation.getString("msg_pr");
			logMessage = logMessage.replace("%s1", String.format("%.0f", result.getEntropy()));
			logMessage = logMessage.replace("%s2", String.format("%.0f", minStrength));
			lm.writeLog(sd, 0, userIdent, LogManager.USER, logMessage, 0, hostname);
		} else {
			lm.writeLog(sd, 0, userIdent, LogManager.USER, localisation.getString("msg_prns"), 0, hostname);
		}
	}
	
	private double getMinStrength() throws SQLException {
		double minStrength = 0.0;
		
		String sql1 = "select password_strength from server";
		String sql2 = "select o.password_strength "
				+ "from organisation o, users u "
				+ "where u.o_id = o.id "
				+ "and u.ident = ?";
		PreparedStatement pstmt = null;
		try {
			// From server
			pstmt = sd.prepareStatement(sql1);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				double strength = rs.getDouble(1);
				if(strength > minStrength) {
					minStrength = strength;
				}
			}
			
			// From organisation
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
			pstmt = sd.prepareStatement(sql2);
			pstmt.setString(1,  userIdent);
			rs = pstmt.executeQuery();
			if(rs.next()) {
				double strength = rs.getDouble(1);
				if(strength > minStrength) {
					minStrength = strength;
				}
			}
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
		
		return minStrength;	
		
	}
	
	/*
	 * Return true if password strength rules should be enforced for this user
	 */
	private boolean validUser() throws SQLException {
		boolean valid = false;
		
		String sql = "select count(*) "
				+ "from users u, user_group ug "
				+ "where u.id = ug.u_id "
				+ "and u.ident = ? "
				+ "and (ug.g_id = " + Authorise.SECURITY_ID + " "
				+ "or ug.g_id = " + Authorise.ENTERPRISE_ID + " "
				+ "or ug.g_id = " + Authorise.ORG_ID + " "
				+ "or ug.g_id = " + Authorise.ADMIN_ID + " "
				+ "or ug.g_id = " + Authorise.ANALYST_ID + " "
				+ "or ug.g_id = " + Authorise.VIEW_DATA_ID + " "
				+ "or ug.g_id = " + Authorise.DASHBOARD_ID + ")";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, userIdent);
			log.info(pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				if(rs.getInt(1) > 0) {
					valid = true;
				}
			}
		} finally {
			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}}
		}
		
		return valid;
				
	}

}


