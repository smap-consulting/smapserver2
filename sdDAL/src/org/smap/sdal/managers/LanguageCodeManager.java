package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.mail.internet.InternetAddress;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.EmailServer;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.LanguageCode;
import org.smap.sdal.model.Mailout;
import org.smap.sdal.model.MailoutLinks;
import org.smap.sdal.model.MailoutMessage;
import org.smap.sdal.model.MailoutPerson;
import org.smap.sdal.model.MailoutPersonTotals;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.SubscriptionStatus;

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
 * Manage the language codes
 */
public class LanguageCodeManager {

	public static String LT_TRANSLATE = "translate";
	public static String LT_TRANSCRIBE = "transcribe";

	/*
	 * Get language codes
	 */
	public ArrayList<LanguageCode> getCodes(Connection sd, 
			ResourceBundle localisation) throws SQLException {

		ArrayList<LanguageCode> codes = new ArrayList<LanguageCode> ();
		
		String sql = "select code, aws_translate, aws_transcribe "
				+ "from language_codes "
				+ "order by code asc";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				codes.add(
						new LanguageCode(rs.getString(1), 
								localisation.getString(rs.getString(1)),
								rs.getBoolean(2),
								rs.getBoolean(3)
								));
			}
		} finally {			
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		
		return codes;

	}

	/*
	 * Check if the language code is supported
	 */
	public boolean isSupported(Connection sd, String code, String type) throws SQLException {
		boolean supported = false;
		
		StringBuilder sb = new StringBuilder("select count(*) "
				+ "from language_codes "
				+ "where code = ?");
		
		if(type != null) {
			if(type.equals(LT_TRANSLATE)) {
				sb.append(" and aws_translate");
			} else if(type.equals(LT_TRANSCRIBE)) {
				sb.append(" and aws_transcribe");
			}
		}
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sb.toString());
			pstmt.setString(1, code);
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				supported = rs.getInt(1) > 0;
			}
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
		return supported;
	}
	
	/*
	 * Attempt to get the code from the language name
	 */
	public String getCodeFromLanguage(String name) {
		
		String code = null;
		if(name != null) {
			name = name.toLowerCase().trim();
			if(name.equals("english")) {
				code = "en";
			} else if(name.equals("spanish")) {
				code = "es";
			}
		}
		
		return code;
	}
}


