package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import javax.ws.rs.core.Request;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.model.AssignFromSurvey;
import org.smap.sdal.model.BillLineItem;
import org.smap.sdal.model.BillingDetail;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.KeyValueSimp;
import org.smap.sdal.model.KeyValueTask;
import org.smap.sdal.model.Location;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.Survey;
import org.smap.sdal.model.TaskAddressSettings;
import org.smap.sdal.model.TaskBulkAction;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskGroup;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskProperties;
import org.smap.sdal.model.User;
import org.smap.sdal.model.UserGroup;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
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
 * Manage access to billing data
 */
public class BillingManager {

	private static Logger log =
			Logger.getLogger(BillingManager.class.getName());
	
	LogManager lm = new LogManager(); // Application log

	private ResourceBundle localisation = null;

	public BillingManager(ResourceBundle l) {
		localisation = l;
	}
	

	public ArrayList<BillingDetail> getOrganisationBillData(
			Connection sd, 
			int year,
			int month) throws SQLException {
		
		ArrayList<BillingDetail> bills = new ArrayList<BillingDetail>();
		
		// SQL to get submissions for all organisations
		
		String sqlSubmissions = "select  count(*), o.id, o.name from upload_event ue, subscriber_event se, project p, organisation o "
				+ "where ue.ue_id = se.ue_id "
				+ "and se.status = 'success' "
				+ "and subscriber = 'results_db' "
				+ "and extract(month from upload_time) = ? "
				+ "and extract(year from upload_time) = ? "
				+ "and ue.p_id = p.id "
				+ "and o.id = p.o_id "
				+ "group by o.id, o.name "
				+ "order by o.name";
		
		PreparedStatement pstmtSubmissions = null;
		
		String sqlDisk = "select  max(total) as total, max(upload) + max(media) + max(template) + max(attachments) as organisation "
				+ "from disk_usage where o_id = ?  "
				+ "and extract(month from when_measured) = ? "
				+ "and extract(year from when_measured) = ?";
		PreparedStatement pstmtDisk = null;	
		
		try {
			
			pstmtDisk = sd.prepareStatement(sqlDisk);
			pstmtDisk.setInt(2, month);
			pstmtDisk.setInt(3, year);
			
			/*
			 * Get Submission data
			 */
			pstmtSubmissions = sd.prepareStatement(sqlSubmissions);
			pstmtSubmissions.setInt(1, month);
			pstmtSubmissions.setInt(2, year);
			
			ResultSet rs = pstmtSubmissions.executeQuery();
			while(rs.next()) {
				int submissions = rs.getInt(1);
				int oId = rs.getInt(2);
				String name = rs.getString(3);
				
				BillingDetail bill = new BillingDetail();
				bill.oId = oId;
				bill.oName = name;
				bill.year = year;
				bill.month = "month" + month;
				bill.line.add(new BillLineItem("submissions", submissions, 0, 0, 0));
				
				// Get disk usage
				pstmtDisk.setInt(1, oId);
				ResultSet rsDisk = pstmtDisk.executeQuery();				
				if(rsDisk.next()) {
					int diskUsage = (int) (rsDisk.getDouble("organisation") / 1000.0);
					bill.line.add(new BillLineItem("disk", diskUsage, 0, 0, 0));
				}
				bills.add(bill);
				
			}
			

		} finally {
			try {if (pstmtSubmissions != null) {pstmtSubmissions.close();}} catch (SQLException e) {}
			try {if (pstmtDisk != null) {pstmtDisk.close();}} catch (SQLException e) {}		
		}

		
		
		
		
		return bills;
		
	}
}


