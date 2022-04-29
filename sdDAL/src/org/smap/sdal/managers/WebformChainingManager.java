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
	 * Get the webform chain rules for a survey
	 */
	public ArrayList<WebformChainRule> getRules(Connection sd, String sIdent) throws SQLException {
		

		String sql = "select w.id, w.type, w.new_survey_ident, w.instance, rule, "
				+ "s.display_name as new_survey_name, s.s_id as new_survey_id, w.seq "
				+ "from wf_chain w "
				+ "left outer join survey s on "
				+ "w.new_survey_ident = s.ident "
				+ "where w.survey_ident = ? "
				+ "order by w.seq asc";
		PreparedStatement pstmt = null;
		
		ArrayList<WebformChainRule> rules = new ArrayList<> ();
		
		try {
	
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, sIdent);
			log.info("Get chain rules: " + pstmt.toString());

			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				WebformChainRule r = new WebformChainRule();
				r.sIdent = sIdent;
				r.id = rs.getInt("id");
				r.type = rs.getString("type");
				r.newSurveyIdent = rs.getString("new_survey_ident");
				r.newSurveyId = rs.getInt("new_survey_id");
				r.instance = rs.getBoolean("instance");
				r.rule = rs.getString("rule");
				r.newSurveyName = rs.getString("new_survey_name");
				r.seq = rs.getInt("seq");
				rules.add(r);
			}
			
		} finally {

			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}};
		}
		return rules;
	}
	

	/*
	 * write a rule to the database
	 */
	public void writeRule(Connection sd, String user, String serverName, int sId, WebformChainRule rule) throws SQLException {
		

		String sqlNew = "insert into wf_chain "
				+ "(survey_ident, type, new_survey_ident, instance, rule, seq) "
				+ "values(?, ?, ?, ?, ?, (select count(*) from wf_chain where survey_ident = ?) + 1) ";
		
		String sqlUpdate = "update wf_chain "
				+ "set type = ?,"
				+ "new_survey_ident = ?,"
				+ "instance = ?,"
				+ "rule = ?, "
				+ "where id = ? "
				+ "and survey_ident = ?";

		PreparedStatement pstmt = null;
		
		
		try {
	
			if(rule.rule == null) {
				rule.rule = "";		// Rule can be empty, it indicates a default
			}
			String msg;
			if(rule.id <= 0) {
				pstmt = sd.prepareStatement(sqlNew);
				pstmt.setString(1, rule.sIdent);
				pstmt.setString(2, rule.type);
				pstmt.setString(3, rule.newSurveyIdent);
				pstmt.setBoolean(4, rule.instance);
				pstmt.setString(5, rule.rule);
				pstmt.setString(6, rule.sIdent);
				
				msg = localisation.getString("lm_ncr");				
				
			} else {
				pstmt = sd.prepareStatement(sqlUpdate);	
				pstmt.setString(1, rule.type);
				pstmt.setString(2, rule.newSurveyIdent);
				pstmt.setBoolean(3, rule.instance);
				pstmt.setString(4, rule.rule);
				pstmt.setInt(5,rule.id);
				pstmt.setString(6, rule.sIdent);
				
				msg = localisation.getString("lm_ucr");
			}

			log.info("Webform chain rule: " + pstmt.toString());
			pstmt.executeUpdate();
			
			msg = msg.replace("%s1", rule.rule);
			lm.writeLog(sd, sId, user, LogManager.CHAIN, msg, 0, serverName);
			
		} finally {

			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}};
		}		
		
	}
	/*
	 * write a rule to the database
	 */
	public void resequence(Connection sd, String user, String serverName, String sIdent, ArrayList<Integer> seq) throws SQLException {
		
		
		String sql = "update wf_chain "
				+ "set seq = ? "
				+ "where id = ? "
				+ "and survey_ident = ?";

		PreparedStatement pstmt = null;
		
		try {
	
		
			String msg;
			pstmt = sd.prepareStatement(sql);	
			pstmt.setString(3, sIdent);
			for(int i = 0; i < seq.size(); i++) {
				pstmt.setInt(1, i + 1);		// Start sequencing from 1
				pstmt.setInt(2, seq.get(i));
				
				pstmt.executeUpdate();
			}

			
		} finally {

			if(pstmt != null) {try {pstmt.close();} catch(Exception e) {}};
		}
	}
	
}
