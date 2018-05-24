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
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.BillingManager;
import org.smap.sdal.managers.TaskManager;
import org.smap.sdal.model.BillLineItem;
import org.smap.sdal.model.BillingDetail;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.TaskGroup;
import org.smap.sdal.model.TaskListGeoJson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import utilities.XLSBillingManager;
import utilities.XLSTaskManager;

import java.sql.*;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Requests for billing data
 */
@Path("/billing")
public class Billing extends Application {

	Authorise aOrg = null;
	Authorise aServer = null;
	
	private static Logger log =
			 Logger.getLogger(Tasks.class.getName());
	
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
	public String getServer(@Context HttpServletRequest request,
			@QueryParam("month") int month,
			@QueryParam("year") int year,
			@QueryParam("org") int oId) throws Exception { 
	
		if(month < 1 || month > 12) {
			throw new ApplicationException("Month must be specified and be between 1 and 12");
		}
		if(year == 0) {
			throw new ApplicationException("Year must be specified");
		}
		
		// Authorisation - Access
		GeneralUtilityMethods.assertBusinessServer(request.getServerName());
		Connection sd = SDDataSource.getConnection("surveyKPI-Billing");
		if(oId > 0) {
			aOrg.isAuthorised(sd, request.getRemoteUser());
			
			boolean superUser = false;
			try {
				superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			} catch (Exception e) {
			}
			if(!superUser) {
				aOrg.isValidBillingOrganisation(sd, oId);
			}
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
		
		String sqlStaticMap = "select  count(*) as total "
				+ "from log "
				+ "where event = 'Mapbox Request' "
				+ "and extract(month from log_time) = ? "
				+ "and extract(year from log_time) = ?";
		
		String sqlRekognition = "select  count(*) as total "
				+ "from log "
				+ "where event = 'Rekognition Request' "
				+ "and extract(month from log_time) = ? "
				+ "and extract(year from log_time) = ?";
		PreparedStatement pstmt = null;
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			// Get the month and year for the query
			//Calendar cal = Calendar.getInstance();
			//cal.setTime(date);
			//int month = cal.get(Calendar.MONTH) + 1;		// Postgres months are 1 - 12
			//int day = cal.get(Calendar.DAY_OF_MONTH);
			//int year = cal.get(Calendar.YEAR);
			
			bill.oId = oId;
			bill.year = year;
			bill.month = "month" + month;	
			bill.currency = "USD";
			
			// Zarkman charges
			double diskUnitCost = 0.25;
			int freeDisk = 20;
			
			double submissionUnitCost = 0.01;
			int freeSubmissions = 100;
			
			double staticMapUnitCost = 0.0;
			int freeStaticMap = 0;
			
			double rekognitionUnitCost = 0.02;
			int freeRekognition = 100;
			
			/*
			 * Server Charge
			 */			
			if(oId == 0) {
				bill.line.add(new BillLineItem("server", 1, 0, 50, 50));
			}
			
			/*
			 * Get submisison data
			 */
			if(oId == 0) {

				pstmtSubmissions = sd.prepareStatement(sqlSubmissions);
				pstmtSubmissions.setInt(1, month);
				pstmtSubmissions.setInt(2, year);
				
				int submissions = 0;
				ResultSet rs = pstmtSubmissions.executeQuery();
				if(rs.next()) {
					submissions = rs.getInt(1);
				}
				double submissionAmount = (submissions - freeSubmissions) * submissionUnitCost;
				if(submissionAmount < 0) {
					submissionAmount = 0.0;
				}
				bill.line.add(new BillLineItem("submissions", submissions, 
						freeSubmissions, submissionUnitCost, submissionAmount));
			}
			
			/*
			 * Get Disk Usage
			 */
			pstmtDisk = sd.prepareStatement(sqlDisk);
			pstmtDisk.setInt(1, oId);
			pstmtDisk.setInt(2, month);
			pstmtDisk.setInt(3, year);
			
			ResultSet rs = pstmtDisk.executeQuery();
			int diskUsage;
			double diskAmount;
			if(rs.next()) {
				if(oId == 0) {
					diskUsage = (int) (rs.getDouble("total") / 1000.0);
				} else {
					diskUsage = (int) (rs.getDouble("organisation") / 1000.0);
				}
				diskAmount = (diskUsage - freeDisk) * diskUnitCost;
				diskAmount = Math.round(diskAmount * 100.0) / 100.0;
				if(diskAmount < 0) {
					diskAmount = 0.0;
				}
				bill.line.add(new BillLineItem("disk", diskUsage, 
						freeDisk, diskUnitCost, diskAmount));
			}
			
			/*
			 * Get Static Map Usage
			 */
			pstmt = sd.prepareStatement(sqlStaticMap);
			pstmt.setInt(1, month);
			pstmt.setInt(2, year);
			
			rs = pstmt.executeQuery();
			int staticMapUsage;
			double staticMapAmount;
			if(rs.next()) {
				staticMapUsage = rs.getInt("total");
				
				staticMapAmount = (staticMapUsage - freeStaticMap) * staticMapUnitCost;
				if(staticMapAmount < 0) {
					staticMapAmount = 0.0;
				}
				
				bill.line.add(new BillLineItem("static map", staticMapUsage, 
						freeStaticMap, staticMapUnitCost, staticMapAmount));
			}
			
			/*
			 * Get Rekognition usage
			 */
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			pstmt = sd.prepareStatement(sqlRekognition);
			pstmt.setInt(1, month);
			pstmt.setInt(2, year);
			
			rs = pstmt.executeQuery();
			int rekognitionUsage;
			double rekognitionAmount;
			if(rs.next()) {
				rekognitionUsage = rs.getInt("total");
				
				rekognitionAmount = (rekognitionUsage - freeRekognition) * rekognitionUnitCost;
				if(rekognitionAmount < 0) {
					rekognitionAmount = 0.0;
				}
				
				bill.line.add(new BillLineItem("rekognition", rekognitionUsage, 
						freeRekognition, rekognitionUnitCost, rekognitionAmount));
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			try {if (pstmtSubmissions != null) {pstmtSubmissions.close();}} catch (SQLException e) {}
			try {if (pstmtDisk != null) {pstmtDisk.close();}} catch (SQLException e) {}
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyKPI-Billing", sd);
		}

		return gson.toJson(bill);
	}
	
	@GET
	@Path("/organisations")
	@Produces("application/json")
	public Response getOrganisations(@Context HttpServletRequest request,
			@QueryParam("month") int month,
			@QueryParam("year") int year,
			@QueryParam("org") int oId) throws Exception { 
	
		Response responseVal = null;
		
		if(month < 1 || month > 12) {
			throw new ApplicationException("Month must be specified and be between 1 and 12");
		}
		if(year == 0) {
			throw new ApplicationException("Year must be specified");
		}
		
		
		// Authorisation - Access
		GeneralUtilityMethods.assertBusinessServer(request.getServerName());
		Connection sd = SDDataSource.getConnection("surveyKPI-Billing");
		if(oId > 0) {
			aOrg.isAuthorised(sd, request.getRemoteUser());
			
			boolean superUser = false;
			try {
				superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			} catch (Exception e) {
			}
			if(!superUser) {
				aOrg.isValidBillingOrganisation(sd, oId);
			}
		} else {
			aServer.isAuthorised(sd, request.getRemoteUser());
		}	
		// End Authorisation
		
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		ArrayList<BillingDetail> bills = null;
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			BillingManager bm = new BillingManager(localisation);
			bills = bm.getOrganisationBillData(sd, year, month);

			responseVal = Response.status(Status.OK).entity(gson.toJson(bills)).build();
		} catch (Exception e) {
			e.printStackTrace();
			responseVal = Response.status(Status.OK).entity(e.getMessage()).build();
		} finally {
			
			SDDataSource.closeConnection("surveyKPI-Billing", sd);
		}

		return responseVal;
	}
	
	/*
	 * Export Tasks for a task group in an XLS file
	 */
	@GET
	@Path ("/organisations/xlsx")
	@Produces("application/x-download")
	public Response getOrganisationReport (
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@QueryParam("month") int month,
			@QueryParam("year") int year,
			@QueryParam("org") int oId) throws Exception { 

		if(month < 1 || month > 12) {
			throw new ApplicationException("Month must be specified and be between 1 and 12");
		}
		if(year == 0) {
			throw new ApplicationException("Year must be specified");
		}
		
		// Authorisation - Access
		GeneralUtilityMethods.assertBusinessServer(request.getServerName());
		Connection sd = SDDataSource.getConnection("surveyKPI-Billing");
		if(oId > 0) {
			aOrg.isAuthorised(sd, request.getRemoteUser());
			
			boolean superUser = false;
			try {
				superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			} catch (Exception e) {
			}
			if(!superUser) {
				aOrg.isValidBillingOrganisation(sd, oId);
			}
		} else {
			aServer.isAuthorised(sd, request.getRemoteUser());
		}	
		// End Authorisation

		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		ArrayList<BillingDetail> bills = null;
		
		try {
			
			// Localisation
			Organisation organisation = UtilityMethodsEmail.getOrganisationDefaults(sd, null, request.getRemoteUser());
			Locale locale = new Locale(organisation.locale);
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			BillingManager bm = new BillingManager(localisation);
			bills = bm.getOrganisationBillData(sd, year, month);
			
			GeneralUtilityMethods.setFilenameInResponse("bill_" + year + "_" + month + "_" + oId + ".xlsx", response); // Set file name
			
			// Create Billing report
			XLSBillingManager xbm = new XLSBillingManager(localisation);
			xbm.createXLSBillFile(response.getOutputStream(), bills, localisation, year, month);
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			throw new Exception("Exception: " + e.getMessage());
		} finally {
			
			SDDataSource.closeConnection("createXLSTasks", sd);	
			
		}
		return Response.ok("").build();
	}

}

