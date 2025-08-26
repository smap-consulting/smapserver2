package org.smap.sdal.managers;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.LinkageItem;
import org.smap.sdal.model.Organisation;

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
 * This class supports processing of events triggered by a submission
 * Including notifications and tasks
 */
public class SubmissionEventManager {
	
	private static Logger log =
			 Logger.getLogger(SubmissionEventManager.class.getName());

	LogManager lm = new LogManager(); // Application log

	Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();

	public void writeToQueue(
			Logger log,
			Connection sd, 
			int ue_id,  
			ArrayList<LinkageItem> linkageItems) throws SQLException {
		
		if(linkageItems == null) {
			linkageItems = new ArrayList<LinkageItem> ();
		};
		
		String sql = "insert into subevent_queue (ue_id, linkage_items, status,"
				+ "created_time ) values(?, ?, ?, 'new', now())";
		PreparedStatement pstmt = null;
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1,  ue_id);
			pstmt.setString(2,  gson.toJson(linkageItems));
			log.info("Add submission notification event: " + pstmt.toString());
			pstmt.executeUpdate();
		} finally {
			if(pstmt != null) try {pstmt.close();} catch(Exception e) {}
		}		
	}
	
	/*
	 * Apply any pending submission events
	 */
	public void applyEvents(Connection sd, 
			Connection cResults, 
			String basePath,
			String urlprefix,
			String attachmentPrefix
			) throws SQLException {
		
		String sql = "select id, ue_id, linkage_items "
				+ "from subevent_queue "
				+ "where status = 'new' "
				+ "order by id asc "
				+ "limit 1000";	
		PreparedStatement pstmt = null;
		
		String sqlClean = "delete from subevent_queue "
				+ "where status = 'success' "
				+ "and processed_time < now() - interval '3 day'";
		PreparedStatement pstmtClean = null;
		
		String sqlDone = "update subevent_queue "
				+ "set status = ?, "
				+ "reason = ?, "
				+ "processed_time = now() "
				+ "where id = ?";
		PreparedStatement pstmtDone = null;
		
		try {
			Type linkageItemType = new TypeToken<ArrayList<LinkageItem>>() {}.getType();
			
			pstmtDone = sd.prepareStatement(sqlDone);
			
			pstmt = sd.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				
				String status;
				String reason = null;
				try {
					ArrayList<LinkageItem> linkageItems = gson.fromJson(rs.getString("linkage_items"),linkageItemType );
					applySubmissionEvents(sd, cResults,
							rs.getInt("ue_id"),
							linkageItems,
							basePath,
							urlprefix,
							attachmentPrefix);
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
			pstmtClean = sd.prepareStatement(sqlClean);
			pstmtClean.executeUpdate();
			
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtDone != null) {pstmtDone.close();}} catch (SQLException e) {}
			try {if (pstmtClean != null) {pstmtClean.close();}} catch (SQLException e) {}
		}	
	}
	
	/*
	 * Apply events triggered by a submission
	 */
	private void applySubmissionEvents(Connection sd, 
			Connection cResults, 
			int ueId, 
			ArrayList<LinkageItem> linkageItems,
			String basePath,
			String urlprefix,
			String attachmentPrefix) throws Exception {

		PreparedStatement pstmtGetUploadEvent = null;

		String ident = null;		// The survey ident
		String instanceId = null;	// The submitted instance identifier
		int pId = 0;				// The project containing the survey
		int oId;
		boolean temporaryUser;

		try {

			/*
			 * Get details from the upload event
			 */
			String sqlGetUploadEvent = "select ident, instanceid, p_id, "
					+ "temporary_user, o_id, user_name, server_name " +
					" from upload_event " +
					" where ue_id = ?;";
			pstmtGetUploadEvent = sd.prepareStatement(sqlGetUploadEvent);
			pstmtGetUploadEvent.setInt(1, ueId);
			ResultSet rs = pstmtGetUploadEvent.executeQuery();
			if(rs.next()) {
				ident = rs.getString("ident");
				instanceId = rs.getString("instanceid");
				pId = rs.getInt("p_id");
				oId = rs.getInt("o_id");
				temporaryUser = rs.getBoolean("temporary_user");
				String submittingUser = rs.getString("user_name");
				String server = rs.getString("server_name");
				
				String pName = GeneralUtilityMethods.getProjectName(sd, pId);
				
				Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, oId);
				Locale orgLocale = new Locale(organisation.locale);
				ResourceBundle localisation;
				try {
					localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", orgLocale);
				} catch(Exception e) {
					localisation = ResourceBundle.getBundle("src.org.smap.sdal.resources.SmapResources", orgLocale);
				}
				
				// Apply notifications
				NotificationManager nm = new NotificationManager(localisation);
				nm.notifyForSubmission(
						sd, 
						cResults,
						ueId, 
						submittingUser,
						temporaryUser,
						"https",
						server,
						basePath,
						urlprefix,
						ident,
						instanceId,
						null,		// update survey ident
						null,		// update question
						null		// update value
						);	

				// Apply Tasks
				TaskManager tm = new TaskManager(localisation, "UTC");
				tm.updateTasksForSubmission(
						sd,
						cResults,
						ident,
						server,
						instanceId,
						pId,
						pName,
						submittingUser,
						temporaryUser,
						urlprefix,
						attachmentPrefix
						);
				
				/*
				 * Apply any Linkage items
				 * Disable linkage due to accuracy issues
				if(linkageItems.size() > 0) {
					log.info("----- Applying " + linkageItems.size() + " linkage items");
					LinkageManager linkMgr = new LinkageManager(localisation);
					linkMgr.writeItems(sd, oId, submittingUser, instanceId, linkageItems);
				} else {
					//log.info("----- No linkage items to apply");
				}
				*/
				
			}

		} finally {

			try {if (pstmtGetUploadEvent != null) {pstmtGetUploadEvent.close();}} catch (SQLException e) {}
			
		}
	}
}
