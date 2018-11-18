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
import org.smap.sdal.model.BillLineItem;
import org.smap.sdal.model.BillingDetail;
import org.smap.sdal.model.Organisation;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import utilities.XLSBillingManager;
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
			@QueryParam("org") int oId,
			@QueryParam("ent") int eId) throws Exception { 
	
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
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			bill.oId = oId;
			bill.year = year;
			bill.month = "month" + month;	
			bill.currency = "USD";
			
			BillingManager bm = new BillingManager(localisation);		
			bill.line = bm.getRates(sd, year, month, eId, oId);
			
			/*
			 * Server Charge
			 */			
			if(eId == 0 && oId == 0) {
				bill.line.add(new BillLineItem("server", 1, 0, 50, 50));
			}
			
			populateBill(sd, bill.line, eId, oId, year, month);

			
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			
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
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
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

	private void populateBill(Connection sd, ArrayList<BillLineItem> items, int eId, int oId, int year, int month) throws SQLException {
		
		for(BillLineItem item : items) {
			if(item.item == BillingDetail.SUBMISSIONS) {
				addUsage(sd, item, eId, oId, year, month);
			} else if(item.item == BillingDetail.DISK) {
				addDisk(sd, item, eId, oId, year, month);
			} else if(item.item == BillingDetail.STATIC_MAP) {
				addStaticMap(sd, item, eId, oId, year, month);
			} else if(item.item == BillingDetail.REKOGNITION) {
				addRekognition(sd, item, eId, oId, year, month);
			} else if(item.item == BillingDetail.MONTHLY) {
				item.amount = item.unitCost;
			}
		}

	}
	
	/*
	 * Get submisison data
	 */
	private void addUsage(Connection sd, BillLineItem item, int eId, int oId, int year, int month) throws SQLException {

		// SQL to get submissions for all organisations
		String sql = "select  count(*) from upload_event ue, subscriber_event se "
				+ "where ue.ue_id = se.ue_id "
				+ "and se.status = 'success' "
				+ "and subscriber = 'results_db' "
				+ "and extract(month from upload_time) = ? "
				+ "and extract(year from upload_time) = ?";	
		if(oId > 0) {
			sql += " and o_id = ?";
		}
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, month);
			pstmt.setInt(2, year);
			if(oId > 0) {
				pstmt.setInt(3, oId);
			}
			item.quantity = 0;
			log.info("Get submissions: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				item.quantity = rs.getInt(1);
			}
			item.amount = (item.quantity - item.free) * item.unitCost;
			if(item.amount < 0) {
				item.amount = 0.0;
			}
			
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
	}
	
	/*
	 * Get Disk Usage
	 */
	private void addDisk(Connection sd, BillLineItem item, int eId, int oId, int year, int month) throws SQLException {
		
		String sqlDisk = "select  max(total) as total, max(upload) + max(media) + max(template) + max(attachments) as organisation "
				+ "from disk_usage where o_id = ?  "
				+ "and extract(month from when_measured) = ? "
				+ "and extract(year from when_measured) = ?";
		PreparedStatement pstmtDisk = null;
		
		try {
			pstmtDisk = sd.prepareStatement(sqlDisk);
			pstmtDisk.setInt(1, oId);
			pstmtDisk.setInt(2, month);
			pstmtDisk.setInt(3, year);
			
			ResultSet rs = pstmtDisk.executeQuery();

			if(rs.next()) {
				if(oId == 0) {
					item.quantity = (int) (rs.getDouble("total") / 1000.0);
				} else {
					item.quantity = (int) (rs.getDouble("organisation") / 1000.0);
				}
				item.amount = (item.quantity - item.free) * item.unitCost;
				item.amount = Math.round(item.amount * 100.0) / 100.0;
				if(item.amount < 0) {
					item.amount = 0.0;
				}
			} 
		} finally {
			try {if (pstmtDisk != null) {pstmtDisk.close();}} catch (SQLException e) {}
		}
	}
	
	/*
	 * Get Static Map Usage
	 */
	private void addStaticMap(Connection sd, BillLineItem item, int eId, int oId, int year, int month) throws SQLException {
		String sqlStaticMap = "select  count(*) as total "
				+ "from log "
				+ "where event = 'Mapbox Request' "
				+ "and extract(month from log_time) = ? "
				+ "and extract(year from log_time) = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sqlStaticMap);
			pstmt.setInt(1, month);
			pstmt.setInt(2, year);
			
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				item.quantity = rs.getInt("total");
				
				item.amount = (item.quantity - item.free) * item.unitCost;
				if(item.amount < 0) {
					item.amount = 0.0;
				}
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
		
	}
	
	/*
	 * Get Rekognition usage
	 */
	private void addRekognition(Connection sd, BillLineItem item, int eId, int oId, int year, int month) throws SQLException {
		
		String sqlRekognition = "select  count(*) as total "
				+ "from log "
				+ "where event = 'Rekognition Request' "
				+ "and extract(month from log_time) = ? "
				+ "and extract(year from log_time) = ?";
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sqlRekognition);
			pstmt.setInt(1, month);
			pstmt.setInt(2, year);
			
			ResultSet rs = pstmt.executeQuery();
		
			if(rs.next()) {
				item.quantity = rs.getInt("total");
				
				item.amount = (item.quantity - item.free) * item.unitCost;
				if(item.amount < 0) {
					item.amount = 0.0;
				}
				
			}
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
		}
	}
	
}

