package org.smap.sdal.managers;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.smap.sdal.model.ReportConfig;
import org.smap.sdal.model.TableColumn;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * 
 * SMAP is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * SMAP is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * SMAP. If not, see <http://www.gnu.org/licenses/>.
 * 
 ******************************************************************************/

/*
 * Manage the table that stores details on tasks
 */
public class CustomReportsManager {

	private static Logger log = Logger.getLogger(CustomReportsManager.class.getName());

	/*
	 * A name that can only be a single sql identifier
	 */
	private boolean isIdentifier(String name) {
		return name != null && name.length() <= 63 && name.matches("^[A-Za-z_][A-Za-z0-9_]*$");
	}
	
	/*
	 * Get a report from the database
	 */
	public ReportConfig get(Connection sd, int crId, int oId) throws Exception {

		ReportConfig config = null;
		String sql = null;
		if (oId > 0) {
			sql = "select type, config from custom_report where id = ? and o_id = ?";
		} else {
			sql = "select type, config from custom_report where id = ?"; // trusted organisation
		}
		PreparedStatement pstmt = null;

		try {

			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, crId);
			if (oId > 0) {
				pstmt.setInt(2, oId);
			}

			log.fine(pstmt.toString());
			pstmt.executeQuery();

			String configString = null;
			String reportType = null;
			ResultSet rs = pstmt.executeQuery();
			if (rs.next()) {
				reportType = rs.getString(1);
				configString = rs.getString(2);
				Gson gson = new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

				Type type = null;
				if (reportType.equals("oversight")) { // Legacy report structure containing only columns
					type = new TypeToken<ArrayList<TableColumn>>() {}.getType();
					config = new ReportConfig();
					config.columns = gson.fromJson(configString, type);
					
				} else if (reportType.equals("oversight1")) {
					config = gson.fromJson(configString, ReportConfig.class);
				} else {
					throw new Exception("Invalid report type: " + reportType);
				}
				
				// Temporary fix to deal with old reports that refer to humanName and name instead of column name
				for(TableColumn tc : config.columns) {	// Rename any stored human name values to displayName
					if(tc.displayName == null) {
						tc.displayName = tc.humanName;
						tc.humanName = null;
					}
					if(tc.column_name == null) {
						tc.column_name = tc.name;
						tc.name = null;
					}
				}
				
				/*
				 * The stored names are used in sql so only keep columns whose names are plain identifiers
				 * Stored calculations are not used, they only apply to server calculates which come from the survey
				 */
				ArrayList<TableColumn> validColumns = new ArrayList<> ();
				for(TableColumn tc : config.columns) {
					tc.calculation = null;
					if(isIdentifier(tc.column_name)
							&& (tc.startName == null || isIdentifier(tc.startName))
							&& (tc.endName == null || isIdentifier(tc.endName))
							&& (tc.type == null || tc.type.matches("^[A-Za-z_]+$"))) {
						validColumns.add(tc);
					} else {
						log.info("Report " + crId + ": column ignored as its name or type is not valid: " + tc.column_name);
					}
				}
				config.columns = validColumns;

			}

		} catch (SQLException e) {
			throw (new Exception(e.getMessage()));
		} finally {
			try {
				pstmt.close();
			} catch (Exception e) {
			}
			;
		}

		return config;
	}

}
