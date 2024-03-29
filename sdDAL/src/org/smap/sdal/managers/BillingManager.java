package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.BillLineItem;
import org.smap.sdal.model.BillingDetail;
import org.smap.sdal.model.RateDetail;
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
		
		String sqlSubmissions = "select  count(*), o_id from upload_event ue "
				+ "where ue.db_status = 'success' "
				+ "and upload_time >=  ? "		
				+ "and upload_time < ? "	
				+ "group by o_id "
				+ "order by o_id";
		
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
			Timestamp t1 = GeneralUtilityMethods.getTimestampFromParts(year, month, 1);
			Timestamp t2 = GeneralUtilityMethods.getTimestampNextMonth(t1);
			
			pstmtSubmissions = sd.prepareStatement(sqlSubmissions);
			pstmtSubmissions.setTimestamp(1, t1);
			pstmtSubmissions.setTimestamp(2, t2);
			
			log.info("Get per organisation usage: " + pstmtSubmissions.toString());
			ResultSet rs = pstmtSubmissions.executeQuery();
			while(rs.next()) {
				int submissions = rs.getInt(1);
				int oId = rs.getInt(2);
				String name = GeneralUtilityMethods.getOrganisationName(sd, oId);
				
				BillingDetail bill = new BillingDetail();
				bill.line = new ArrayList<BillLineItem> ();
				bill.oId = oId;
				bill.oName = name;
				bill.year = year;
				bill.month = "month" + month;
				bill.line.add(new BillLineItem("submissions", submissions, 0, 0, 0));
				
				// Get disk usage
				pstmtDisk.setInt(1, oId);
				
				ResultSet rsDisk = pstmtDisk.executeQuery();				
				if(rsDisk.next()) {
					log.info("Disk usage for organisation: " + oId + " = " + rsDisk.getDouble("organisation"));
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
	
	/*
	 * Get the rates for the specified enterprise, organisation, year and month
	 */
	public RateDetail getRates(
			Connection sd, 
			int year,
			int month,
			int eId,
			int oId) throws SQLException, ApplicationException {
		
		RateDetail rd = new RateDetail();
		
		String sql = "select rates, currency from bill_rates "
				+ "where o_id = ? "
				+ "and e_id = ? "
				+ "and ts_applies_from < ? "
				+ "order by ts_applies_from desc "
				+ "limit 1";
		PreparedStatement pstmt = null;
		
		/*
		 * Get the last set of rates that was created prior to the end of the requested month
		 * Set the month to the next month and get the latest rates less than that
		 */
		if(month >= 12) {
			month = 1;
			year++;
		} else {
			month++;
		}
		LocalDateTime d = LocalDateTime.of(year, month, 1, 0, 0);
		
		
		try {
			pstmt = sd.prepareStatement(sql);
			
			// Try with the passed organisation and enterprise
			pstmt.setInt(1, oId);
			pstmt.setInt(2, eId);
			pstmt.setObject(3, d);
			log.info("Get rates: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				getRates(rs, rd);
				rd.oId = oId;
				rd.eId = eId;
			} else {
				// Go up one level
				if(oId > 0) {
					oId = 0;		// Try the enterprise level rates
				} else if(eId > 0) {
					eId = 0;		// Try the server level rates
				} else {
					throw new ApplicationException("Rates not found");
				}
				pstmt.setInt(1, oId);
				pstmt.setInt(2, eId);
				log.info("Get rates2: " + pstmt.toString());
				rs.close();
				rs = pstmt.executeQuery();
				if(rs.next()) {
					getRates(rs, rd);
					rd.oId = oId;
					rd.eId = eId;
				} else {
					// Go up a second level
					if(eId > 0) {
						eId = 0;		// Try the server level rates
					} else {
						throw new ApplicationException("Rates not found");
					}
					pstmt.setInt(1, oId);
					pstmt.setInt(2, eId);
					log.info("Get rates3: " + pstmt.toString());
					rs.close();
					rs = pstmt.executeQuery();
					if(rs.next()) {
						getRates(rs, rd);
						rd.oId = oId;
						rd.eId = eId;
					} else {
						throw new ApplicationException("Rates not found");
					}
				}
			}
		} catch (ApplicationException e) {
			rd.oId = -1;
			rd.eId = -1;
			log.info("Error: rates not found");
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
		
		if(rd.line == null) {
			rd.line = new ArrayList<BillLineItem> ();
		}
		
		return rd;
	
	}
	
	private void getRates(ResultSet rs, RateDetail rd) throws SQLException {
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		String rString = rs.getString(1);
		if(rString != null) {					
			rd.line = gson.fromJson(rString, new TypeToken<ArrayList<BillLineItem>>() {}.getType());
		}
		rd.currency = rs.getString(2);
	}
	
	/*
	 * Get the array of rates for the specified enterprise, organisation
	 */
	public ArrayList<RateDetail> getRatesList(
			Connection sd, 
			String tz,
			int eId,
			int oId) throws SQLException {
		
		ArrayList<RateDetail> rates = new ArrayList<RateDetail> ();
		
		String sql = "select rates, currency, timezone(?, ts_applies_from), created_by,"
				+ "timezone(?, ts_created) "
				+ "from bill_rates "
				+ "where o_id = ? "
				+ "and e_id = ? "
				+ "order by ts_applies_from desc ";
		PreparedStatement pstmt = null;
		
		/*
		 * Get the last set of rates that was created prior to the end of the requested month
		 * Set the month to the next month and get the latest rates less than that
		 */	
		Gson gson = new GsonBuilder().disableHtmlEscaping().create();
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, tz);		// Applies from
			pstmt.setString(2, tz);		// Created time
			pstmt.setInt(3, oId);
			pstmt.setInt(4, eId);
			log.info("Get rates list: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				RateDetail rd = new RateDetail();
				String rString = rs.getString(1);
				if(rString != null) {					
					rd.line = gson.fromJson(rString, new TypeToken<ArrayList<BillLineItem>>() {}.getType());
				}
				
				rd.oId = oId;
				rd.eId = eId;
				rd.currency = rs.getString(2);
				
				java.sql.Date sqlDate = rs.getDate(3);
				rd.appliesFrom = sqlDate.toLocalDate();
				
				rd.modifiedBy = rs.getString(4);
				
				Timestamp sqlTimestamp = rs.getTimestamp(5);
				rd.modified = sqlTimestamp.toLocalDateTime();
				rates.add(rd);
			}
		} catch(Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
		
		return rates;
	
	}
	
	/*
	 * Return true if the bill is enabled for this enterprise / organisation / server
	 */
	public boolean isBillEnabled(
			Connection sd, 
			int eId,
			int oId) throws SQLException {
		
		boolean enabled = true;
		
		String sqlServer = "select billing_enabled from server ";
		String sqlEnterprise = "select billing_enabled from enterprise where id = ? ";
		String sqlOrganisation = "select billing_enabled from organisation where id = ? ";
				
		PreparedStatement pstmt = null;

		try {
			if(oId == 0 && eId == 0) {
				pstmt = sd.prepareStatement(sqlServer);
			} else if(eId > 0) {
				pstmt = sd.prepareStatement(sqlEnterprise);
				pstmt.setInt(1, eId);
			} else if(oId > 0) {
				pstmt = sd.prepareStatement(sqlOrganisation);
				pstmt.setInt(1, oId);
			}
		
			log.info("Is billing enabled: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				enabled = rs.getBoolean(1);
			}
		} finally {
			if(pstmt != null) {try{pstmt.close();} catch(Exception e) {}}
		}
		
		return enabled;
	
	}

} 


