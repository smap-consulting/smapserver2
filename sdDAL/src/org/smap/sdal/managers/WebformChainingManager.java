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

package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.model.WebformChainRule;

public class WebformChainingManager {

	private static Logger log =
			Logger.getLogger(WebformChainingManager.class.getName());

	LogManager lm = new LogManager();		// Application log
	String tz;
	ResourceBundle localisation;

	public WebformChainingManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}
	
	/*
	 * Get the webform chain rules for a project
	 */
	public ArrayList<WebformChainRule> getRules(Connection sd, String sIdent) throws SQLException {
		

		String sql = "select id, type, new_survey_ident, instance, rule "
				+ "from wf_chain "
				+ "where survey_ident = ? "
				+ "order by seq asc";
		PreparedStatement pstmt = null;
		
		ArrayList<WebformChainRule> rules = new ArrayList<> ();
		
		try {
	
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sIdent);

			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				WebformChainRule r = new WebformChainRule();
				r.sIdent = sIdent;
				r.id = rs.getInt("id");
				r.type = rs.getString("type");
				r.newSurveyIdent = rs.getString("new_survey_ident");
				r.instance = rs.getBoolean("instance");
				r.rule = rs.getString("rule");
				rules.add(r);
			}
			
		} finally {

			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}};
		}
		return rules;
	}
	

}
