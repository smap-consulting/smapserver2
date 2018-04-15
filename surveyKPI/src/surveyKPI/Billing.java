package surveyKPI;

/*
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

*/

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.model.BillingDetail;
import org.smap.sdal.model.LanguageItem;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.OptionLite;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Locale;
import java.util.ResourceBundle;

/*
 * Returns a list of all options for the specified question
 */
@Path("/billing")
public class Billing extends Application {

	Authorise aOrg = null;
	Authorise aServer = null;
	
	public Billing() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ORG);
		aServer = new Authorise(authorisations, null);
		
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		aOrg = new Authorise(authorisations, null);
	}
	
	@GET
	@Produces("application/json")
	public String getOptions(@Context HttpServletRequest request,
			@QueryParam("date") Date date, 
			@QueryParam("org") int oId) throws Exception { 
	
		if(date == null) {
			throw new ApplicationException("Date must be specified");
		}
		
		// Authorisation - Access
		GeneralUtilityMethods.assertBusinessServer(request.getServerName());
		Connection sd = SDDataSource.getConnection("surveyKPI-Billing");
		if(oId > 0) {
			aOrg.isAuthorised(sd, request.getRemoteUser());
			aOrg.isValidBillingOrganisation(sd, oId);
		} else {
			aServer.isAuthorised(sd, request.getRemoteUser());
		}
		
		// End Authorisation
		
		BillingDetail bill = new BillingDetail();
		
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();

		// SQL to get submissions for all organisations
		String sqlSubmissions = "select  count(*) from upload_event ue, subscriber_event se "
				+ "where ue.ue_id = se.ue_id "
				+ "and se.status = 'success' "
				+ "and subscriber = 'results_db' "
				+ "and extract(month from upload_time) = ? "
				+ "and extract(year from upload_time) = ?";
		
		PreparedStatement pstmtSubmissions = null;
		
		String sqlDisk = "select  max(total) as total, max(upload) + max(media) + max(template) + max(attachments) as organisation "
				+ "from disk_usage where o_id = ?  "
				+ "and extract(month from when_measured) = ? "
				+ "and extract(year from when_measured) = ?";
		PreparedStatement pstmtDisk = null;
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			// Get the month and year for the query
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			int month = cal.get(Calendar.MONTH) + 1;		// Postgres months are 1 - 12
			int day = cal.get(Calendar.DAY_OF_MONTH);
			int year = cal.get(Calendar.YEAR);
			
			System.out.println("Year: " + year);
			System.out.println("Month: "+ month);
			bill.oId = oId;
			bill.year = year;
			bill.month = "month" + month;	
			
			/*
			 * Get Billing parameters
			 */
			if(oId == 0) {
				bill.currency = "USD";
				
				bill.diskUnitCost = 0.25;
				bill.freeDisk = 20;
				
				bill.submissionUnitCost = 1.0;
				bill.freeSubmissions = 100;
			}
			/*
			 * Get submisison data
			 */
			if(oId == 0) {

				pstmtSubmissions = sd.prepareStatement(sqlSubmissions);
				pstmtSubmissions.setInt(1, month);
				pstmtSubmissions.setInt(2, year);
				
				ResultSet rs = pstmtSubmissions.executeQuery();
				if(rs.next()) {
					bill.submissions = rs.getInt(1);
				}
				bill.submissionAmount = (bill.submissions - bill.freeSubmissions) * bill.submissionUnitCost;
				if(bill.submissionAmount < 0) {
					bill.submissionAmount = 0.0;
				}
			}
			
			/*
			 * Get Disk Usage
			 */
			pstmtDisk = sd.prepareStatement(sqlDisk);
			pstmtDisk.setInt(1, oId);
			pstmtDisk.setInt(2, month);
			pstmtDisk.setInt(3, year);
			
			ResultSet rs = pstmtDisk.executeQuery();
			if(rs.next()) {
				if(oId == 0) {
					bill.diskUsage = rs.getDouble("total") / 1000.0;
				} else {
					bill.diskUsage = rs.getDouble("organisation") / 1000.0;
				}
				bill.diskAmount = (bill.diskUsage - bill.freeDisk) * bill.diskUnitCost;
				if(bill.diskAmount < 0) {
					bill.diskAmount = 0.0;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			try {if (pstmtSubmissions != null) {pstmtSubmissions.close();}} catch (SQLException e) {}
			try {if (pstmtDisk != null) {pstmtDisk.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-Billing", sd);
		}

		return gson.toJson(bill);
	}

}

