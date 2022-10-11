package org.smap.sdal.managers;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.model.ReportConfig;
import org.smap.sdal.model.CustomReportItem;
import org.smap.sdal.model.CustomReportType;
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
	 * Get a list of reports
	 */
	public ArrayList<CustomReportItem> getReports(Connection sd, int pId)
			throws SQLException {

		ArrayList<CustomReportItem> reports = new ArrayList<>();

		String sql = "select cr.id, cr.name, cr.type_id, cr.config, s.display_name "
				+ "from custom_report cr, survey s "
				+ "where cr.survey_ident = s.ident "
				+ "and cr.p_id = ? "
				+ "order by cr.name asc";
		PreparedStatement pstmt = null;

		try {

			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, pId);
			ResultSet rs = pstmt.executeQuery();

			while (rs.next()) {
				CustomReportItem item = new CustomReportItem(
						rs.getInt(1),
						rs.getString(2),
						rs.getInt(3),
						rs.getString(4),
						rs.getString(5)
						);
				
				reports.add(item);
			}

		} finally {
			if(pstmt != null) {try {pstmt.close();} catch (Exception e) {}};
		}

		return reports;
	}
	
	/*
	 * Save a report to the database
	 */
	public void save(Connection sd, String reportName, String config, int oId, 
			int typeId, int pId, String surveyIdent) throws Exception {

		String sql = "insert into custom_report (o_id, name, config, type_id, p_id, survey_ident) "
				+ "values (?, ?, ?, ?, ?, ?);";
		PreparedStatement pstmt = null;

		try {

			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmt.setString(2, reportName);
			pstmt.setString(3, config);
			pstmt.setInt(4, typeId);
			pstmt.setInt(5, pId);
			pstmt.setString(6, surveyIdent);

			log.info(pstmt.toString());
			pstmt.executeUpdate();

		} catch (SQLException e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			throw (new Exception(e.getMessage()));
		} finally {
			try {
				pstmt.close();
			} catch (Exception e) {
			}
			;
		}
	}

	/*
	 * get a list of custom report types
	 */
	public ArrayList<CustomReportType> getTypeList(Connection sd)
			throws SQLException {

		ArrayList<CustomReportType> reportTypes = new ArrayList<>();

		String sql = "select id, name, config "
				+ "from custom_report_type "
				+ "order by name asc";
		PreparedStatement pstmt = null;

		try {

			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();

			while (rs.next()) {
				CustomReportType item = new CustomReportType(rs.getInt(1), rs.getString(2));
				// TODO config
				reportTypes.add(item);
			}

		} finally {
			if(pstmt != null) {try {pstmt.close();} catch (Exception e) {}};
		}

		return reportTypes;
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

			log.info(pstmt.toString());
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

	/*
	 * Delete a report
	 */
	public void delete(Connection sd, int oId, int id, ResourceBundle localisation) throws Exception {
		
		String sql = "delete from custom_report " 
				+ "where id = ? " 
				+ "and o_id = ?";
		PreparedStatement pstmt = null;

		try {

			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, id);
			pstmt.setInt(2, oId);
			log.info("Delete custom report: " + pstmt.toString());
			pstmt.executeUpdate();

		} finally {

			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}

		}
	}

}
