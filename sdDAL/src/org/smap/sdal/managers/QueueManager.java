package org.smap.sdal.managers;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.Queue;
import org.smap.sdal.model.QueueItem;
import org.smap.sdal.model.QueueTime;
import org.smap.sdal.model.WorkerInfo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

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
	public String RESTORE = "restore";
	public String S3UPLOAD = "s3upload";
	public String SUBEVENT = "subevent";
	public String MESSAGE = "message";
	public String MESSAGE_DEVICE = "message_device";

	private static Logger log =
			Logger.getLogger(QueueManager.class.getName());
	
	private Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
	/*
	 * Get active workers from subscriber_worker table (heartbeat within last 5 minutes).
	 * workerFilter: optional SQL AND condition appended to the WHERE clause, e.g.
	 *   "subscriber_type = 'upload'" or "queue_name = 'storage'"
	 */
	private ArrayList<WorkerInfo> getActiveWorkers(Connection sd, String workerFilter) throws SQLException {
		ArrayList<WorkerInfo> workers = new ArrayList<>();
		StringBuilder sql = new StringBuilder(
				"select hostname, pid, subscriber_type, queue_name, started_time, heartbeat "
				+ "from subscriber_worker "
				+ "where heartbeat > now() - interval '5 minutes' ");
		if(workerFilter != null) {
			sql.append("and ").append(workerFilter).append(" ");
		}
		sql.append("order by hostname, pid, queue_name");

		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				WorkerInfo w = new WorkerInfo();
				w.hostname = rs.getString("hostname");
				w.pid = rs.getLong("pid");
				w.subscriber_type = rs.getString("subscriber_type");
				w.queue_name = rs.getString("queue_name");
				w.started_time = rs.getTimestamp("started_time");
				w.heartbeat = rs.getTimestamp("heartbeat");
				workers.add(w);
			}
		} finally {
			try {if(pstmt != null) {pstmt.close();}} catch(SQLException e) {}
		}
		return workers;
	}

	/*
	 * Get status of submission queue
	 */
	public Queue getSubmissionQueueData(Connection sd) throws SQLException {

		PreparedStatement pstmtLength = null;
		PreparedStatement pstmtNewRate = null;
		PreparedStatement pstmtStats = null;

		Queue queue = new Queue();
		try {

			// Backlog: received but not yet written to results DB
			String sqlLength = "select count(*) "
					+ "from upload_event ue "
					+ "where ue.status = 'success' "
					+ "and ue.s_id is not null "
					+ "and not ue.incomplete "
					+ "and not ue.restore "
					+ "and not ue.results_db_applied ";
			pstmtLength = sd.prepareStatement(sqlLength);
			ResultSet rs = pstmtLength.executeQuery();
			if(rs.next()) {
				queue.length = rs.getInt(1);
			}

			// Received rate: submissions arriving at the API (pre-worker, queue-level only)
			String sqlNewRate = "select count(*) "
					+ "from upload_event ue "
					+ "where ue.status = 'success' "
					+ "and ue.s_id is not null "
					+ "and not ue.incomplete "
					+ "and not ue.restore "
					+ "and ue.upload_time > now() - interval '1 minute'";
			pstmtNewRate = sd.prepareStatement(sqlNewRate);
			rs = pstmtNewRate.executeQuery();
			if(rs.next()) {
				queue.new_rpm = rs.getInt(1);
			}

			// Single stats query grouped by worker: drives both queue totals and per-worker counts
			String sqlStats = "select worker_host, queue_name, db_status, count(*) "
					+ "from upload_event "
					+ "where processed_time > now() - interval '1 minute' "
					+ "and status = 'success' "
					+ "and s_id is not null "
					+ "and not incomplete "
					+ "and not restore "
					+ "group by worker_host, queue_name, db_status";
			pstmtStats = sd.prepareStatement(sqlStats);

			queue.workers = getActiveWorkers(sd, "subscriber_type = 'upload' and queue_name not like 'qm%'");

			rs = pstmtStats.executeQuery();
			while(rs.next()) {
				String host = rs.getString("worker_host");
				String qName = rs.getString("queue_name");
				String status = rs.getString("db_status");
				int count = rs.getInt(4);

				queue.processed_rpm += count;
				if("error".equals(status)) {
					queue.error_rpm += count;
				}

				if(host != null) {
					for(WorkerInfo w : queue.workers) {
						if(w.hostname.equals(host) && w.queue_name.equals(qName)) {
							w.processed_rpm += count;
							if("error".equals(status)) {
								w.error_rpm += count;
							}
						}
					}
				}
			}

		} finally {
			try {if (pstmtLength != null) {pstmtLength.close();}} catch (SQLException e) {}
			try {if (pstmtNewRate != null) {pstmtNewRate.close();}} catch (SQLException e) {}
			try {if (pstmtStats != null) {pstmtStats.close();}} catch (SQLException e) {}
		}

		return queue;
	}

	/*
	 * Get status of sub event queue
	 * This is the queue that processes all the post submission processing such as sending emails
	 */
	public Queue getSubEventQueueData(Connection sd) throws SQLException {

		PreparedStatement pstmtLength = null;
		PreparedStatement pstmtProcessedRate = null;
		PreparedStatement pstmtNewRate = null;

		Queue queue = new Queue();
		try {

			String sqlLength = "select count(*) "
					+ "from subevent_queue "
					+ "where processed_time is null ";
			pstmtLength = sd.prepareStatement(sqlLength);
			//log.info("Get queue length: " + pstmtLength.toString());
			ResultSet rs = pstmtLength.executeQuery();
			if(rs.next()) {
				queue.length = rs.getInt(1);
			}

			String sqlProcessedRate = "select status, count(*) "
					+ "from subevent_queue "
					+ "where processed_time > now() - interval '1 minute' "
					+ "group by status";
			pstmtProcessedRate = sd.prepareStatement(sqlProcessedRate);

			rs = pstmtProcessedRate.executeQuery();
			while(rs.next()) {
				String status = rs.getString(1);
				queue.processed_rpm += rs.getInt(2);	// Processed updated for all status values
				if(status != null) {
					if(status.equals("failed")) {
						queue.error_rpm = rs.getInt(2);
					}
				}
			}

			String sqlNewRate = "select count(*) "
					+ "from subevent_queue "
					+ "where created_time > now() - interval '1 minute'";
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
	 * Get status of sub event queue
	 * This is the queue that processes all the post submission processing such as sending emails
	 */
	public Queue getMessageQueueData(Connection sd) throws SQLException {

		PreparedStatement pstmtLength = null;
		PreparedStatement pstmtNewRate = null;
		PreparedStatement pstmtStats = null;

		// Topic filter used in multiple queries
		String topicFilter = "and topic != 'task' and topic != 'survey' and topic != 'user' "
				+ "and topic != 'project' and topic != 'resource' ";

		Queue queue = new Queue();
		try {

			String sqlLength = "select count(*) "
					+ "from message "
					+ "where outbound "
					+ "and processed_time is null "
					+ topicFilter;
			pstmtLength = sd.prepareStatement(sqlLength);
			ResultSet rs = pstmtLength.executeQuery();
			if(rs.next()) {
				queue.length = rs.getInt(1);
			}

			String sqlNewRate = "select count(*) "
					+ "from message "
					+ "where created_time > now() - interval '1 minute' "
					+ "and outbound "
					+ topicFilter;
			pstmtNewRate = sd.prepareStatement(sqlNewRate);
			rs = pstmtNewRate.executeQuery();
			if(rs.next()) {
				queue.new_rpm = rs.getInt(1);
			}

			// Single stats query grouped by worker: drives both queue totals and per-worker counts
			String sqlStats = "select worker_host, status, count(*) "
					+ "from message "
					+ "where processed_time > now() - interval '1 minute' "
					+ "and outbound "
					+ topicFilter
					+ "group by worker_host, status";
			pstmtStats = sd.prepareStatement(sqlStats);

			queue.workers = getActiveWorkers(sd, "queue_name like 'qm%'");

			rs = pstmtStats.executeQuery();
			while(rs.next()) {
				String host = rs.getString("worker_host");
				String status = rs.getString("status");
				int count = rs.getInt(3);

				queue.processed_rpm += count;
				if("error".equals(status)) {
					queue.error_rpm += count;
				}

				if(host != null) {
					for(WorkerInfo w : queue.workers) {
						if(w.hostname.equals(host)) {
							w.processed_rpm += count;
							if("error".equals(status)) {
								w.error_rpm += count;
							}
						}
					}
				}
			}

		} finally {
			try {if (pstmtLength != null) {pstmtLength.close();}} catch (SQLException e) {}
			try {if (pstmtNewRate != null) {pstmtNewRate.close();}} catch (SQLException e) {}
			try {if (pstmtStats != null) {pstmtStats.close();}} catch (SQLException e) {}
		}

		return queue;
	}
	
	/*
	 * Get status of message queue
	 * This is the queue that processes all the post submission processing such as sending emails
	 */
	public Queue getMessageDeviceQueueData(Connection sd) throws SQLException {

		PreparedStatement pstmtLength = null;
		PreparedStatement pstmtProcessedRate = null;
		PreparedStatement pstmtNewRate = null;

		Queue queue = new Queue();
		try {

			String sqlLength = "select count(*) "
					+ "from message "
					+ "where outbound "
					+ "and processed_time is null "
					+ "and (topic = 'task' or topic = 'survey' or topic = 'user' or topic = 'project' or topic = 'resource') ";

			pstmtLength = sd.prepareStatement(sqlLength);
			//log.info("Get queue length: " + pstmtLength.toString());
			ResultSet rs = pstmtLength.executeQuery();
			if(rs.next()) {
				queue.length = rs.getInt(1);
			}

			String sqlProcessedRate = "select count(*) "
					+ "from message "
					+ "where processed_time > now() - interval '1 minute' "
					+ "and outbound "
					+ "and (topic = 'task' or topic = 'survey' or topic = 'user' or topic = 'project' or topic = 'resource') ";
			pstmtProcessedRate = sd.prepareStatement(sqlProcessedRate);

			rs = pstmtProcessedRate.executeQuery();
			while(rs.next()) {
				queue.processed_rpm += rs.getInt(1);	// Processed updated for all status values
			}

			String sqlNewRate = "select count(*) "
					+ "from message "
					+ "where created_time > now() - interval '1 minute' "
					+ "and outbound "
					+ "and (topic = 'task' or topic = 'survey' or topic = 'user' or topic = 'project' or topic = 'resource') ";
	
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
	 * Get status of restore queue
	 */
	public Queue getRestoreQueueData(Connection sd) throws SQLException {

		PreparedStatement pstmtLength = null;
		PreparedStatement pstmtNewRate = null;
		PreparedStatement pstmtStats = null;

		Queue queue = new Queue();
		try {

			String sqlLength = "select count(*) "
					+ "from upload_event ue "
					+ "where ue.status = 'success' "
					+ "and ue.s_id is not null "
					+ "and not ue.incomplete "
					+ "and ue.restore "
					+ "and not ue.results_db_applied ";
			pstmtLength = sd.prepareStatement(sqlLength);
			ResultSet rs = pstmtLength.executeQuery();
			if(rs.next()) {
				queue.length = rs.getInt(1);
			}

			String sqlNewRate = "select count(*) "
					+ "from upload_event ue "
					+ "where ue.status = 'success' "
					+ "and ue.s_id is not null "
					+ "and not ue.incomplete "
					+ "and ue.restore "
					+ "and ue.upload_time > now() - interval '1 minute'";
			pstmtNewRate = sd.prepareStatement(sqlNewRate);
			rs = pstmtNewRate.executeQuery();
			if(rs.next()) {
				queue.new_rpm = rs.getInt(1);
			}

			// Single stats query grouped by worker: drives both queue totals and per-worker counts
			String sqlStats = "select worker_host, queue_name, db_status, count(*) "
					+ "from upload_event "
					+ "where processed_time > now() - interval '1 minute' "
					+ "and status = 'success' "
					+ "and s_id is not null "
					+ "and not incomplete "
					+ "and restore "
					+ "group by worker_host, queue_name, db_status";
			pstmtStats = sd.prepareStatement(sqlStats);

			queue.workers = getActiveWorkers(sd, "queue_name = 'qf2_restore'");

			rs = pstmtStats.executeQuery();
			while(rs.next()) {
				String host = rs.getString("worker_host");
				String qName = rs.getString("queue_name");
				String status = rs.getString("db_status");
				int count = rs.getInt(4);

				queue.processed_rpm += count;
				if("error".equals(status)) {
					queue.error_rpm += count;
				}

				if(host != null) {
					for(WorkerInfo w : queue.workers) {
						if(w.hostname.equals(host) && w.queue_name.equals(qName)) {
							w.processed_rpm += count;
							if("error".equals(status)) {
								w.error_rpm += count;
							}
						}
					}
				}
			}

		} finally {
			try {if (pstmtLength != null) {pstmtLength.close();}} catch (SQLException e) {}
			try {if (pstmtNewRate != null) {pstmtNewRate.close();}} catch (SQLException e) {}
			try {if (pstmtStats != null) {pstmtStats.close();}} catch (SQLException e) {}
		}

		return queue;
	}
	
	/*
	 * Get status of s3upload queue
	 */
	public Queue getS3UploadQueueData(Connection sd) throws SQLException {

		PreparedStatement pstmtLength = null;
		PreparedStatement pstmtNewRate = null;
		PreparedStatement pstmtStats = null;

		Queue queue = new Queue();
		try {

			// Backlog: files waiting to be sent to S3
			String sqlLength = "select count(*) "
					+ "from s3upload "
					+ "where status = 'new' ";
			pstmtLength = sd.prepareStatement(sqlLength);
			ResultSet rs = pstmtLength.executeQuery();
			if(rs.next()) {
				queue.length = rs.getInt(1);
			}

			// New items rate: files added to queue (pre-worker, queue-level only)
			String sqlNewRate = "select count(*) "
					+ "from s3upload "
					+ "where created_time > now() - interval '1 minute' ";
			pstmtNewRate = sd.prepareStatement(sqlNewRate);
			rs = pstmtNewRate.executeQuery();
			if(rs.next()) {
				queue.new_rpm = rs.getInt(1);
			}

			// Single stats query grouped by worker: drives both queue totals and per-worker counts
			String sqlStats = "select worker_id, status, count(*) "
					+ "from s3upload "
					+ "where processed_time > now() - interval '1 minute' "
					+ "group by worker_id, status";
			pstmtStats = sd.prepareStatement(sqlStats);

			queue.workers = getActiveWorkers(sd, "queue_name = 'storage'");

			rs = pstmtStats.executeQuery();
			while(rs.next()) {
				String workerId = rs.getString("worker_id");
				String status = rs.getString("status");
				int count = rs.getInt(3);

				queue.processed_rpm += count;
				if("failed".equals(status)) {
					queue.error_rpm += count;
				}

				if(workerId != null) {
					for(WorkerInfo w : queue.workers) {
						if((w.hostname + ":" + w.pid).equals(workerId)) {
							w.processed_rpm += count;
							if("failed".equals(status)) {
								w.error_rpm += count;
							}
						}
					}
				}
			}

		} finally {
			try {if (pstmtLength != null) {pstmtLength.close();}} catch (SQLException e) {}
			try {if (pstmtNewRate != null) {pstmtNewRate.close();}} catch (SQLException e) {}
			try {if (pstmtStats != null) {pstmtStats.close();}} catch (SQLException e) {}
		}

		return queue;
	}

	/*
	 * Get status of s3upload queue
	 */
	public void getS3UploadQueueEvents(Connection sd, 
			ArrayList<QueueItem> items,
			int month,
			int year,
			String status,
			String tz) throws SQLException {

		PreparedStatement pstmt = null;

		try {

			boolean hasStatus = false;
			Timestamp t1 = GeneralUtilityMethods.getTimestampFromParts(year, month, 1);
			Timestamp t2 = GeneralUtilityMethods.getTimestampNextMonth(t1);
			
			String sql1 = "select id, filepath, status, o_id, "
					+ "is_media, created_time, processed_time, status, reason, worker_id "
					+ "from s3upload ";		
			String sql2 = "where timezone(?, created_time) >=  ? "
					+ "and timezone(?, created_time) < ? ";
			String sql3 = "";
			if(status != null && !status.equals("any")) {
				sql3 = "and status = ? "; 
			}
			String sql4 = "order by id desc;";
			StringBuilder sql = new StringBuilder(sql1)
					.append(sql2)
					.append(sql3)
					.append(sql4);

			pstmt = sd.prepareStatement(sql.toString());
			int paramCount = 1;
			pstmt.setString(paramCount++, tz);
			pstmt.setTimestamp(paramCount++, t1);
			pstmt.setString(paramCount++, tz);
			pstmt.setTimestamp(paramCount++, t2);
			if(hasStatus) {
				pstmt.setString(paramCount++, status);
			}
			
			log.info("Queue: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				QueueItem item = new QueueItem();
				items.add(item);
				
				item.id = rs.getInt("id");
				item.filepath = rs.getString("filepath");
				item.oId = rs.getInt("o_id");
				item.media = rs.getBoolean("is_media");
				item.created_time = rs.getTimestamp("created_time");
				item.processed_time = rs.getTimestamp("processed_time");
				item.status = rs.getString("status");
				item.reason = rs.getString("reason");
				item.worker_id = rs.getString("worker_id");
			}


		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}

	}
	
	/*
	 * Get get queue history
	 */
	public ArrayList<QueueTime> getHistory(Connection sd, int interval, String tz, String user) {
		
		String sql = "select "
				+ "to_char(timezone(?, recorded_at), 'YYYY-MM-DD HH24:MI:SS') as recorded_at,"
				+ "payload "
				+ "from monitor_data "
				+ "where recorded_at > now() - interval '" + interval + " days' "
				+ "order by recorded_at asc";
		PreparedStatement pstmt = null;
		
		Type type = new TypeToken<HashMap<String, Queue>>() {}.getType();

		ArrayList<QueueTime> data = new ArrayList<>();
		
		try {			
			
			if(tz == null) {
				tz = GeneralUtilityMethods.getOrganisationTZ(sd, 
						GeneralUtilityMethods.getOrganisationId(sd, user));
			}
			tz = (tz == null) ? "UTC" : tz;
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, tz);
			log.info("Queue history: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				data.add(new QueueTime(rs.getString("recorded_at"), 
						gson.fromJson(rs.getString("payload"), type)));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		
		return data;
	}

}
