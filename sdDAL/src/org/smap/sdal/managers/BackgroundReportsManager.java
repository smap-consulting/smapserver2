package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.BackgroundReport;

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
 * Manage the creation and completion of background reports
 */
public class BackgroundReportsManager {

	private static Logger log = Logger.getLogger(BackgroundReportsManager.class.getName());

	private ResourceBundle localisation;
	private String tz;
	
	public static String REPORT_STATUS_COMPLETED = "complete";
	public static String REPORT_STATUS_ERROR = "error";
	public static String REPORT_STATUS_PENDING = "pending";
	public static String REPORT_STATUS_NEW = "new";
	
	public static String PARAM_START_DATE = "startDate";
	public static String PARAM_END_DATE = "endDate";
	public static String PARAM_USER_ID = "userId";
	public static String PARAM_MPS = "mps";
	
	public static String PARAM_O_ID = "oId";
	public static String PARAM_DAY = "day";
	public static String PARAM_MONTH = "month";
	public static String PARAM_YEAR = "year";
	public static String PARAM_BY_SURVEY = "bySurvey";
	public static String PARAM_BY_PROJECT = "byProject";
	public static String PARAM_BY_DEVICE = "byDevice";
	public static String PARAM_INC_TEMP = "incTemp";
	
	public BackgroundReportsManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}
	
	/*
	 * Localisation can change during the life of the manager
	 */
	public boolean processNextReport(Connection sd, String basePath) throws SQLException {
		BackgroundReport report = getNextReport(sd);
		if(report == null) {
			return false;	// no more reports
		} else {
			// Process the report
			String filename = null;
			Locale locale = new Locale(report.language);
			try {
				localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			} catch(Exception e) {
				localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", locale);
			}
			
			boolean error = false;
			try {
				if(report.report_type.equals("locations_kml")) {
					UserTrailManager utm = new UserTrailManager(localisation, report.tz);
					filename = utm.generateKML(sd, report.pId, report.params, basePath);
				} else if(report.report_type.equals("locations_distance")) {
					UserTrailManager utm = new UserTrailManager(localisation, report.tz);
					filename = utm.generateDistanceReport(sd, report.pId, report.params, basePath);
				} else if(report.report_type.equals("u_usage")) {
					XLSXAdminReportsManager rm = new XLSXAdminReportsManager(localisation);
					String userIdent = GeneralUtilityMethods.getUserIdent(sd, report.uId);
					filename = rm.writeNewReport(sd, userIdent, report.params, basePath);
				} else if(report.report_type.equals("u_attendance")) {
					XLSXAttendanceReportsManager rm = new XLSXAttendanceReportsManager(localisation);
					String userIdent = GeneralUtilityMethods.getUserIdent(sd, report.uId);
					filename = rm.writeNewAttendanceReport(sd, userIdent, report.params, basePath);
				} else {
					updateReportStatus(sd, report.id, false, null, "Unsupported report type: " + report.report_type);
					error = true;
				}
				if(!error) {
					updateReportStatus(sd, report.id, true, filename, null);
				}
				
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
				updateReportStatus(sd, report.id, false, null, e.getMessage());
			}
		}
		return true;
	}
	
	private void updateReportStatus(Connection sd, int id, boolean success, String filename, String msg) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "update background_report "
					+ "set filename = ?,"
					+ "status = ?,"
					+ "status_msg = ?,"
					+ "end_time = now() "
					+ "where id = ?;";
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, filename);
			pstmt.setString(2,  success ? REPORT_STATUS_COMPLETED : REPORT_STATUS_ERROR);
			pstmt.setString(3,  msg);
			pstmt.setInt(4, id);
			log.info("Update report status: " + pstmt.toString());
			pstmt.executeUpdate();
			
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
	}
	
	public void deleteOldReports(Connection sd) throws SQLException {
		PreparedStatement pstmt = null;
		try {
			String sql = "delete from background_report "
					+ "where end_time < now() - interval '14 days'";
			
			pstmt = sd.prepareStatement(sql);
			log.info("Delete old reports: " + pstmt.toString());
			pstmt.executeUpdate();
			
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
		}
	}
	
	private BackgroundReport getNextReport(Connection sd) throws SQLException {
		
		BackgroundReport br = null;
		
		PreparedStatement pstmtGet = null;
		PreparedStatement pstmtUpdate = null;
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		
		try {
			sd.setAutoCommit(false);
			
			/*
			 * Get the next report to be processed
			 */
			String sql = "select id, u_id, o_id, p_id, report_type, tz, language, params "
					+ "from background_report "
					+ "where status = ? "
					+ "order by id asc limit 1";
			pstmtGet = sd.prepareStatement(sql);
			pstmtGet.setString(1, REPORT_STATUS_NEW);
			ResultSet rs = pstmtGet.executeQuery();
			if(rs.next()) {
				br = new BackgroundReport();
				br.id = rs.getInt("id");
				br.uId = rs.getInt("u_id");
				br.oId = rs.getInt("o_id");
				br.pId = rs.getInt("p_id");
				br.report_type = rs.getString("report_type");
				br.tz = rs.getString("tz");
				br.language = rs.getString("language");
				br.params = gson.fromJson(rs.getString("params"), new TypeToken<HashMap<String, String>>() {}.getType());
			}
			
			/*
			 * Mark the report as pending
			 */
			if(br != null) {
				
				String sqlUpdate = "update background_report set status = ? "
						+ "where id = ?";
				pstmtUpdate = sd.prepareStatement(sqlUpdate);
				pstmtUpdate.setString(1, REPORT_STATUS_PENDING);
				pstmtUpdate.setInt(2,  br.id);
				pstmtUpdate.executeUpdate();
			}
			
			sd.setAutoCommit(true);
			
		} catch (Exception e) {
			sd.rollback();
			if(!sd.getAutoCommit()) {
				sd.setAutoCommit(true);
			}
			throw e;
		} finally {
			if(pstmtGet != null) try {pstmtGet.close();} catch (Exception e) {}
			if(pstmtUpdate != null) try {pstmtUpdate.close();} catch (Exception e) {}
		}
		
		return br;
	}

}
