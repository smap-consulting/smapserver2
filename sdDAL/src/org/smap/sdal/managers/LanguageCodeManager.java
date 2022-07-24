package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import org.smap.sdal.model.LanguageCode;

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
	public static String LT_TRANSCRIBE_MEDICAL = "transcribe_medical";

	/*
	 * Get language codes
	 */
	public ArrayList<LanguageCode> getCodes(Connection sd, 
			ResourceBundle localisation) throws SQLException {

		ArrayList<LanguageCode> codes = new ArrayList<LanguageCode> ();
		
		String sql = "select code, aws_translate, aws_transcribe, transcribe_medical "
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
								rs.getBoolean(3),
								rs.getBoolean(4)
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
			} else if(type.equals(LT_TRANSCRIBE_MEDICAL)) {
				sb.append(" and aws_transcribe_medical");
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


