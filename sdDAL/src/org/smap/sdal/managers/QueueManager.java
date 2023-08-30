package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.smap.sdal.model.Queue;

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
 * This class supports access to the status of queues such as the submission queue 
 */
public class QueueManager {

	public String SUBMISSIONS = "submissions";
	public String S3UPLOAD = "s3upload";

	/*
	 * Get status of submission queue
	 */
	public Queue getSubmissionQueueData(Connection sd) throws SQLException {

		PreparedStatement pstmtLength = null;
		PreparedStatement pstmtProcessedRate = null;
		PreparedStatement pstmtNewRate = null;

		Queue queue = new Queue();
		try {

			String sqlLength = "select count(*) "
					+ "from upload_event ue "
					+ "where ue.status = 'success' "
					+ "and ue.s_id is not null "
					+ "and not ue.incomplete "
					+ "and not ue.results_db_applied ";
			pstmtLength = sd.prepareStatement(sqlLength);

			ResultSet rs = pstmtLength.executeQuery();
			if(rs.next()) {
				queue.length = rs.getInt(1);
			}

			String sqlProcessedRate = "select db_status, count(*) "
					+ "from upload_event ue "
					+ "where ue.status = 'success' "
					+ "and ue.s_id is not null "
					+ "and not ue.incomplete "
					+ "and ue.processed_time > now() - interval '1 minute' "
					+ "group by db_status";
			pstmtProcessedRate = sd.prepareStatement(sqlProcessedRate);

			rs = pstmtProcessedRate.executeQuery();
			while(rs.next()) {
				String status = rs.getString(1);
				queue.processed_rpm += rs.getInt(2);	// Processed updated for all status values
				if(status != null) {
					if(status.equals("error")) {
						queue.error_rpm = rs.getInt(2);
					}
				}
			}

			String sqlNewRate = "select count(*) "
					+ "from upload_event ue "
					+ "where ue.status = 'success' "
					+ "and ue.s_id is not null "
					+ "and not ue.incomplete "
					+ "and ue.upload_time > now() - interval '1 minute'";
			pstmtNewRate = sd.prepareStatement(sqlNewRate);

			rs = pstmtNewRate.executeQuery();
			if(rs.next()) {
				queue.new_rpm = rs.getInt(1);
			}


		} finally {
			try {if (pstmtLength != null) {pstmtLength.close();}} catch (SQLException e) {}
			try {if (pstmtProcessedRate != null) {pstmtProcessedRate.close();}} catch (SQLException e) {}
			try {if (pstmtNewRate != null) {pstmtNewRate.close();}} catch (SQLException e) {}
		}

		return queue;
	}
	
	/*
	 * Get status of s3upload queue
	 */
	public Queue getS3UploadQueueData(Connection sd) throws SQLException {

		PreparedStatement pstmtLength = null;
		PreparedStatement pstmtProcessedRate = null;
		PreparedStatement pstmtNewRate = null;

		Queue queue = new Queue();
		try {

			String sqlLength = "select count(*) "
					+ "from s3upload "
					+ "where status = 'new' ";
			pstmtLength = sd.prepareStatement(sqlLength);

			ResultSet rs = pstmtLength.executeQuery();
			if(rs.next()) {
				queue.length = rs.getInt(1);
			}

			String sqlProcessedRate = "select status, count(*) "
					+ "from s3upload "
					+ "where processed_time > now() - interval '1 minute' "
					+ "group by status";
			pstmtProcessedRate = sd.prepareStatement(sqlProcessedRate);

			rs = pstmtProcessedRate.executeQuery();
			while(rs.next()) {
				String status = rs.getString(1);
				if(status != null) {
					queue.processed_rpm += rs.getInt(2);	// processed applies for all status values
					if(status.equals("failed")) {
						queue.error_rpm = rs.getInt(2);
					}
				}
			}

			String sqlNewRate = "select count(*) "
					+ "from s3upload "
					+ "where created_time > now() - interval '1 minute' ";

			pstmtNewRate = sd.prepareStatement(sqlNewRate);

			rs = pstmtNewRate.executeQuery();
			if(rs.next()) {
				queue.new_rpm = rs.getInt(1);
			}


		} finally {
			try {if (pstmtLength != null) {pstmtLength.close();}} catch (SQLException e) {}
			try {if (pstmtProcessedRate != null) {pstmtProcessedRate.close();}} catch (SQLException e) {}
			try {if (pstmtNewRate != null) {pstmtNewRate.close();}} catch (SQLException e) {}
		}

		return queue;
	}


}
