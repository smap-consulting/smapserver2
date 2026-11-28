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

package org.smap.sdal.legacy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;


public class JdbcTranslationManager {

	private static Logger log =
			 Logger.getLogger(JdbcTranslationManager.class.getName());
	
	
	
	PreparedStatement pstmtGetBySurveyId = null;
	PreparedStatement pstmtGetBySurveyIdNoExternal = null;
	String sqlGet = "select "
			+ "t.t_id,"
			+ "t.s_id,"
			+ "t.language,"
			+ "t.text_id,"
			+ "t.type,"
			+ "t.value "
			+ "from translation t, language l "
			+ "where t.s_id = l.s_id "
			+ "and t.language = l.language "
			+ "and t.s_id = ? ";
	String sqlNoExternal = "and external = false ";
	String sqlOrder = "order by language;";
			
	/*
	 * Constructor
	 */
	public JdbcTranslationManager(Connection sd) throws SQLException {
		pstmtGetBySurveyId = sd.prepareStatement(sqlGet + sqlOrder);
		pstmtGetBySurveyIdNoExternal = sd.prepareStatement(sqlGet + sqlNoExternal + sqlOrder);
	}
	
	
	
	/*
	 * Get translations by survey id
	 */
	public List<Translation> getBySurveyId(int sId, boolean getExternal) throws SQLException {
		if(getExternal) {
			pstmtGetBySurveyId.setInt(1, sId);
			return getTranslationList(pstmtGetBySurveyId);
		} else {
			pstmtGetBySurveyIdNoExternal.setInt(1, sId);
			return getTranslationList(pstmtGetBySurveyIdNoExternal);
		}
	}
	/*
	 * Close prepared statements
	 */
	public void close() {
		try {if(pstmtGetBySurveyId != null) {pstmtGetBySurveyId.close();}} catch(Exception e) {};
		try {if(pstmtGetBySurveyIdNoExternal != null) {pstmtGetBySurveyIdNoExternal.close();}} catch(Exception e) {};
	}
	
	private List<Translation> getTranslationList(PreparedStatement pstmt) throws SQLException {
	
		ArrayList <Translation> trans = new ArrayList<Translation> ();
		
		log.fine("Get Translations: " + pstmt.toString());
		ResultSet rs = pstmt.executeQuery();
		while(rs.next()) {
			Translation t = new Translation();
			
			t.setId(rs.getInt(1));
			t.setSurveyId(rs.getInt(2));
			t.setLanguage(rs.getString(3));
			t.setTextId(rs.getString(4));
			t.setType(rs.getString(5));
			t.setValue(rs.getString(6));
			
			trans.add(t);
		}
		return trans;
	}
}
