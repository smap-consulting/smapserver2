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

public class JdbcFormManager {

	private static Logger log =
			 Logger.getLogger(SurveyTemplate.class.getName());
	
	
	
	PreparedStatement pstmtGetBySurveyId = null;
	String sqlGet = "select "
			+ "f_id, "
			+ "s_id, "
			+ "name, "
			+ "table_name, "
			+ "parentform, "
			+ "parentquestion, "
			+ "repeats, "
			+ "reference, "
			+ "append, "
			+ "merge, "
			+ "replace "
			+ "from form where ";
	String sqlGetBySurveyId = "s_id = ?;";
	
	
	/*
	 * Constructor
	 */
	public JdbcFormManager(Connection sd) throws SQLException {
		pstmtGetBySurveyId = sd.prepareStatement(sqlGet + sqlGetBySurveyId);
	}
	
	
	
	/*
	 * Get a list of forms for the specified survey id
	 */
	public List <Form> getBySurveyId(int sId) throws SQLException {
		pstmtGetBySurveyId.setInt(1, sId);
		return getFormList(pstmtGetBySurveyId);
	}
	
	/*
	 * Close statements
	 */
	public void close() {
		try {if(pstmtGetBySurveyId != null) {pstmtGetBySurveyId.close();}} catch(Exception e) {};
	}
	
	/*
	 * Get an array of forms
	 */
	private List <Form> getFormList(PreparedStatement aPstmt) throws SQLException {
		ArrayList <Form> forms = new ArrayList<Form> ();
		
		log.fine("Get form list: " + aPstmt.toString());
		ResultSet rs = aPstmt.executeQuery();
		while(rs.next()) {
			Form f = new Form();
			f.setId(rs.getInt(1));
			f.setSurveyId(rs.getInt(2));
			f.setName(rs.getString(3));
			f.setTableName(rs.getString(4));
			f.setParentForm(rs.getInt(5));
			f.setParentQuestionId(rs.getInt(6));
			f.setRepeats(rs.getString(7));
			f.setReference(rs.getBoolean(8));
			f.setAppend(rs.getBoolean(9));
			f.setMerge(rs.getBoolean(10));
			f.setReplace(rs.getBoolean(11));
			
			forms.add(f);
		}
		
		return forms;
	}
	
}
