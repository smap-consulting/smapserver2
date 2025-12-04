package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.Bundle;
import org.smap.sdal.model.CMS;
import org.smap.sdal.model.Case;
import org.smap.sdal.model.CaseCount;
import org.smap.sdal.model.CaseManagementAlert;
import org.smap.sdal.model.CaseManagementSettings;

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
 * This class supports bundle level queries that do not fit within CaseManager
 */
public class BundleManager {
	
	private static Logger log =
			 Logger.getLogger(BundleManager.class.getName());

	LogManager lm = new LogManager(); // Application log
	ResourceBundle localisation = null;
	Gson gson;
	
	public BundleManager(ResourceBundle l) {
		localisation = l;
		gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
	}

	/*
	 * Get Bundles that the user has access to
	 */
	public ArrayList<Bundle> getBundles(Connection sd, String user) throws SQLException {
		
		PreparedStatement pstmt = null;
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		ArrayList<Bundle> bundles = new ArrayList<>();	
		
		try {
			/*
			 * Get the Bundles accessible to this user
			 */
			
			StringBuilder sql = new StringBuilder("select settings, "
					+ "group_survey_ident "
					+ "from cms_setting "
					+ "where group_survey_ident in ");		
			sql.append("(select s.group_survey_ident "
					+ "from survey s "
					+ "join user_project up "
					+ "on s.p_id = up.p_id "
					+ "join users u "
					+ "on u.id = up.u_id "
					+ "join project p "
					+ "on p.id = up.p_id "
					+ "and p.o_id = u.o_id "
					+ "join organisation o "
					+ "on u.o_id = o.id "
					+ "where u.ident = ? ");
			sql.append(GeneralUtilityMethods.getSurveyRBAC());
			sql.append(")");
			
			pstmt = sd.prepareStatement(sql.toString());
			pstmt.setString(1,  user);
			pstmt.setString(2, user);	// Second user entry for RBAC
			log.info("Get Bundles: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
							
			while(rs.next()) {
				String sString = rs.getString("settings");
				
				if(sString != null ) {
					CaseManagementSettings settings = gson.fromJson(sString, CaseManagementSettings.class);
					bundles.add(new Bundle(settings.name, settings.description));
				} 
			}	
					    
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
		return bundles;
	}

	
}
