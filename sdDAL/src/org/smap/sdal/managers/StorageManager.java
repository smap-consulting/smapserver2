package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.notifications.interfaces.S3AttachmentUpload;

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
 * Apply any outbound messages
 * This rightly belongs in MessagingManager however it also does legacy direct
 *  email sends and the required email classes are not available to all users
 *  of the MessagingManager
 */
public class StorageManager {

	private static Logger log = Logger.getLogger(StorageManager.class.getName());

	LogManager lm = new LogManager(); // Application log
	
	/*
	 * Upload files to s3
	 */
	public void uploadToS3(Connection sd, String basePath, int s3count) throws SQLException {
		
		String sql = "select id, filepath "
				+ "from s3upload "
				+ "where status = 'new' "
				+ "order by id asc "
				+ "limit 1000";	
		PreparedStatement pstmt = null;
		
		String sqlClean = "delete from s3upload "
				+ "where status = 'success' "
				+ "and processed_time < now() - interval '3 day'";
		PreparedStatement pstmtClean = null;
		
		String sqlDone = "update s3upload "
				+ "set status = ?, "
				+ "reason = ?, "
				+ "processed_time = now() "
				+ "where id = ?";
		PreparedStatement pstmtDone = null;
		
		try {
			pstmtDone = sd.prepareStatement(sqlDone);
			
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				
				String status;
				String reason = null;
				try {
					S3AttachmentUpload.put(basePath, rs.getString("filepath"));
					status = "success";
				} catch (Exception e) {
					status = "failed";
					reason = e.getMessage();
					log.log(Level.SEVERE, e.getMessage(), e);
				}	

				pstmtDone.setString(1, status);
				pstmtDone.setString(2, reason);
				pstmtDone.setInt(3, rs.getInt("id"));
				pstmtDone.executeUpdate();
			}
			
			/*
			 * Clean up old data
			 */
			if(s3count == 0) {
				pstmtClean = sd.prepareStatement(sqlClean);
				pstmtClean.executeUpdate();
			}
			
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtDone != null) {pstmtDone.close();}} catch (SQLException e) {}
			try {if (pstmtClean != null) {pstmtClean.close();}} catch (SQLException e) {}
		}
	}
	
}
