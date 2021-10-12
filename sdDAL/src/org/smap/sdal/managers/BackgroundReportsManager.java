package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.model.BackgroundReport;

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
	
	private String getNextSql = null;
	private String updateStatus = null;
	
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
			try {
				UserTrailManager utm = new UserTrailManager(localisation, report.tz);
				String filepath = utm.generateKML(sd, report.params, basePath);
				System.out.println("Filepath: " + filepath);
			} catch (Exception e) {
				
			}
		}
		return true;
	}
	
	
	private BackgroundReport getNextReport(Connection sd) throws SQLException {
		
		BackgroundReport br = null;
		
		PreparedStatement pstmtGet = null;
		PreparedStatement pstmtUpdate = null;
		try {
			sd.setAutoCommit(false);
			
			/*
			 * Get the next report to be processed
			 */
			String sql = "select id, report_type, tz, language from background_report where status = 'new' "
					+ "order by id asc limit 1";
			pstmtGet = sd.prepareStatement(sql);
			ResultSet rs = pstmtGet.executeQuery();
			if(rs.next()) {
				br = new BackgroundReport();
				br.id = rs.getInt("id");
				br.report_type = rs.getString("report_type");
				br.tz = rs.getString("tz");
				br.language = rs.getString("language");
			}
			
			/*
			 * Mark the report as pending
			 */
			if(br != null) {
				
				String sqlUpdate = "update background_report set status = 'pending' "
						+ "where id = ?";
				pstmtUpdate = sd.prepareStatement(sqlUpdate);
				pstmtUpdate.setInt(1,  br.id);
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
