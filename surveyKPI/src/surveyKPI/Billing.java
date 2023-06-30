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
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.managers.BillingManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.ResourceManager;
import org.smap.sdal.model.BillLineItem;
import org.smap.sdal.model.BillingDetail;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.RateDetail;

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

	Authorise aServer = null;
	Authorise aServerManager = null;
	
	Authorise aEnterprise = null;
	Authorise aEnterpriseManager = null;
	
	Authorise aOrg = null;
	Authorise aOrgManager = null;

	
	private static Logger log =
			 Logger.getLogger(Tasks.class.getName());
	
	public Billing() {
		
		// Server Owner
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.OWNER);				// Edit access to server bill
		aServerManager = new Authorise(authorisations, null);
		
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.OWNER);				// Edit access to server bill
		authorisations.add(Authorise.ENTERPRISE);		// View access to server bill
		aServer = new Authorise(authorisations, null);
		
		// Enterprise
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ENTERPRISE);		// Edit access to enterprise bill
		aEnterpriseManager = new Authorise(authorisations, null);
		
		authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ENTERPRISE);		// Edit access to enterprise bill
		authorisations.add(Authorise.ORG);				// View access to enterprise bill
		aEnterprise = new Authorise(authorisations, null);
		
		// Organisation
		authorisations = new ArrayList<String> ();
		authorisations.add(Authorise.ORG);				// Edit access to enterprise bill		
		aOrgManager = new Authorise(authorisations, null);
		
		authorisations = new ArrayList<String> ();
		authorisations.add(Authorise.ORG);				// Edit access to enterprise bill
		authorisations.add(Authorise.ADMIN);				// View access to organisation bill
		aOrg = new Authorise(authorisations, null);
	}
	
	@GET
	@Produces("application/json")
	public String getServer(@Context HttpServletRequest request,
			@QueryParam("month") int month,
			@QueryParam("year") int year,
			@QueryParam("org") int oId,
			@QueryParam("ent") int eId) throws Exception { 
	
		String connectionString = "surveyKPI-Billing-getServer";
		
		if(month < 1 || month > 12) {
			throw new ApplicationException("Month must be specified and be between 1 and 12");
		}
		if(year == 0) {
			throw new ApplicationException("Year must be specified");
		}
		
		// Authorisation - Access
		GeneralUtilityMethods.assertBusinessServer(request.getServerName());
		Connection sd = SDDataSource.getConnection(connectionString);
		if(oId > 0) {
			aOrg.isAuthorised(sd, request.getRemoteUser());
			
			boolean 	orgUser = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ORG_ID);			
			if(!orgUser) {
				aOrg.isValidBillingOrganisation(sd, oId);
			}
		} else if(eId > 0) { 
			aEnterprise.isAuthorised(sd, request.getRemoteUser());
			
			boolean entUser = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ENTERPRISE_ID);			
			if(!entUser) {
				aOrg.isValidBillingEnterprise(sd, eId);
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
			
			BillingManager bm = new BillingManager(localisation);		
			RateDetail rd = bm.getRates(sd, year, month, eId, oId);
			bill.line = rd.line;
			bill.currency = rd.currency;
			bill.enabled = bm.isBillEnabled(sd, eId, oId);
			bill.ratesOid = rd.oId;	// The organisation that these rates were specified for
			bill.ratesEid = rd.eId;	// The enterprise that these rates were specified for
			
			populateBill(sd, bill.line, eId, oId, year, month);

			
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return gson.toJson(bill);
	}
	
	/*
	 * Export Billing details for an organisation
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

		String connectionString = "surveyKPI-Billing-getOrganisationReport";
		
		if(month < 1 || month > 12) {
			throw new ApplicationException("Month must be specified and be between 1 and 12");
		}
		if(year == 0) {
			throw new ApplicationException("Year must be specified");
		}
		
		// Authorisation - Access
		GeneralUtilityMethods.assertBusinessServer(request.getServerName());
		Connection sd = SDDataSource.getConnection(connectionString);
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
			
			SDDataSource.closeConnection(connectionString, sd);	
			
		}
		return Response.ok("").build();
	}

	private void populateBill(Connection sd, ArrayList<BillLineItem> items, int eId, int oId, int year, int month) throws SQLException, ApplicationException {
		
		for(BillLineItem item : items) {
			if(item.item == BillingDetail.SUBMISSIONS) {
				addUsage(sd, item, eId, oId, year, month);
				
			} else if(item.item == BillingDetail.DISK) {
				addDisk(sd, item, eId, oId, year, month);
				
			} else if(item.item == BillingDetail.STATIC_MAP) {
				addUsageLine(sd, item, LogManager.MAPBOX_REQUEST, eId, oId, year, month);
				
			} else if(item.item == BillingDetail.GOOGLE_STATIC_MAP) {
				addUsageLine(sd, item, LogManager.GOOGLE_REQUEST, eId, oId, year, month);
				
			} else if(item.item == BillingDetail.REKOGNITION) {
				addUsageLine(sd, item, LogManager.REKOGNITION, eId, oId, year, month);
				
			} else if(item.item == BillingDetail.TRANSLATE) {
				addTranslate(sd, item, eId, oId, year, month);
				
			} else if(item.item == BillingDetail.TRANSCRIBE) {
				addTranscribe(sd, item, eId, oId, year, month);
			} else if(item.item == BillingDetail.TRANSCRIBE_MEDICAL) {
				addTranscribeMedical(sd, item, eId, oId, year, month);
			} else if(item.item == BillingDetail.MONTHLY) {
				item.amount = item.unitCost;
			}
		}

	}
	
	/*
	 * Get rates data
	 */
	@GET
	@Path ("/rates")
	@Produces("application/json")
	public String getRates(@Context HttpServletRequest request,
			@QueryParam("org") int oId,
			@QueryParam("ent") int eId,
			@QueryParam("tz") String tz					// Timezone
			) throws Exception { 
		
		String connectionString = "surveyKPI-Billing-getRates";
		
		// Authorisation - Access
		GeneralUtilityMethods.assertBusinessServer(request.getServerName());
		Connection sd = SDDataSource.getConnection(connectionString);
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
		
		ArrayList<RateDetail> rates = null;
		tz = (tz == null) ? "UTC" : tz;
		
		Gson gson =  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);		
			
			BillingManager bm = new BillingManager(localisation);		
			rates = bm.getRatesList(sd, tz, eId, oId);

			
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			
			SDDataSource.closeConnection(connectionString, sd);
		}

		return gson.toJson(rates);
	}
	
	/*
	 * Get submission data
	 */
	private void addUsage(Connection sd, BillLineItem item, int eId, int oId, int year, int month) throws SQLException {

		// SQL to get submissions for all organisations
		String sql = "select  count(*) from upload_event ue "
				+ "where ue.db_status = 'success' "
				+ "and upload_time >=  ? "		// current month
				+ "and upload_time < ? ";		// next month
		if(oId > 0) {
			sql += "and o_id = ?";
		} else if(eId > 0) {
			sql += "and e_id = ?";
		}
		PreparedStatement pstmt = null;
		
		try {
			Timestamp t1 = GeneralUtilityMethods.getTimestampFromParts(year, month, 1);
			Timestamp t2 = GeneralUtilityMethods.getTimestampNextMonth(t1);
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setTimestamp(1, t1);
			pstmt.setTimestamp(2, t2);
			if(oId > 0) {
				pstmt.setInt(3, oId);
			} else if(eId > 0) {
				pstmt.setInt(3, eId);
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
	private void addDisk(Connection sd, BillLineItem item, int eId, int oId, int year, int month) throws SQLException, ApplicationException {
		
		String sqlDisk = "select  max(total) as total, max(upload) + max(media) + max(template) + max(attachments) as organisation "
				+ "from disk_usage where o_id = ?  "
				+ "and extract(month from when_measured) = ? "
				+ "and extract(year from when_measured) = ?";
		
		String sqlDiskEnterprise = "select  max(total) as total, max(upload) + max(media) + max(template) + max(attachments) as organisation "
				+ "from disk_usage where e_id = ?  "
				+ "and extract(month from when_measured) = ? "
				+ "and extract(year from when_measured) = ?";
		
		PreparedStatement pstmtDisk = null;
		
		try {
			if(eId == 0) {
				pstmtDisk = sd.prepareStatement(sqlDisk);
				pstmtDisk.setInt(1, oId);
			} else {
				pstmtDisk = sd.prepareStatement(sqlDiskEnterprise);
				pstmtDisk.setInt(1, eId);
			}			
			pstmtDisk.setInt(2, month);
			pstmtDisk.setInt(3, year);
			
			log.info("Get disk usage: " + pstmtDisk.toString());
			ResultSet rs = pstmtDisk.executeQuery();

			int count = 0;
			Double quantity = 0.0;
			while(rs.next()) {
				if(oId == 0 && eId == 0) {
					quantity = rs.getDouble("total");
				} else if(eId == 0) {
					quantity = rs.getDouble("organisation");
				} else {
					quantity += rs.getDouble("organisation");		// Aggregate quantities
				}
				count++;
			}
			if(count > 1 && eId == 0) {
				throw new ApplicationException("Too many billing results");
			}
			
			item.quantity = (int) (quantity / 1000.0);
			item.amount = (item.quantity - item.free) * item.unitCost;
			item.amount = Math.round(item.amount * 100.0) / 100.0;
			if(item.amount < 0) {
				item.amount = 0.0;
			}
			log.info("Disk quantity: " + eId + " : " + oId + " : " + quantity);

		} finally {
			try {if (pstmtDisk != null) {pstmtDisk.close();}} catch (SQLException e) {}
		}
	}
	
	/*
	 * Get usage for a specific line item
	 */
	private void addUsageLine(Connection sd, BillLineItem item, String usageType, int eId, int oId, int year, int month) throws SQLException {
		
		String sql = "select  count(*) as total "
				+ "from log "
				+ "where event = ? "
				+ "and log_time >=  ? "		// current month
				+ "and log_time < ? ";		// next month
		PreparedStatement pstmt = null;
		
		try {
			Timestamp t1 = GeneralUtilityMethods.getTimestampFromParts(year, month, 1);
			Timestamp t2 = GeneralUtilityMethods.getTimestampNextMonth(t1);
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setString(1, usageType);
			pstmt.setTimestamp(2, t1);
			pstmt.setTimestamp(3, t2);
			
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
	 * Get Translate usage
	 */
	private void addTranslate(Connection sd, BillLineItem item, int eId, int oId, int year, int month) throws SQLException {
		
		ResourceManager rm = new ResourceManager();
		item.quantity = rm.getUsageMeasure(sd, oId, month, year, LogManager.TRANSLATE);				
		item.amount = (item.quantity - item.free) * item.unitCost;
		if(item.amount < 0) {
			item.amount = 0.0;
		}
	}
	
	/*
	 * Get Transcribe usage
	 */
	private void addTranscribe(Connection sd, BillLineItem item, int eId, int oId, int year, int month) throws SQLException {
		ResourceManager rm = new ResourceManager();
		item.quantity = rm.getUsageMeasure(sd, oId, month, year, LogManager.TRANSCRIBE);				
		item.amount = (item.quantity - item.free) * item.unitCost;
		if(item.amount < 0) {
			item.amount = 0.0;
		}
	}
	
	/*
	 * Get Transcribe Medical usage
	 */
	private void addTranscribeMedical(Connection sd, BillLineItem item, int eId, int oId, int year, int month) throws SQLException {
		ResourceManager rm = new ResourceManager();
		item.quantity = rm.getUsageMeasure(sd, oId, month, year, LogManager.TRANSCRIBE_MEDICAL);				
		item.amount = (item.quantity - item.free) * item.unitCost;
		if(item.amount < 0) {
			item.amount = 0.0;
		}
	}
	
	/*
	 * Enable / Disable billing
	 */
	@POST
	@Path("/enable")
	public Response enableBilling(
			@Context HttpServletRequest request,
			@FormParam("enabled") String enable,
			@FormParam("level") String level,
			@FormParam("id") String id) throws Exception { 

		if(level == null) {
			throw new ApplicationException("Billing level must be set");
		}
		if(enable == null) {
			throw new ApplicationException("Enable or disable billing must be specified");
		}
		
		String connectionString = "SurveyKPI-Billing-enable";
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);
		
		if(level.equals("owner")) {
			aServerManager.isAuthorised(sd, request.getRemoteUser());
		} else if(level.equals("ent")) {
			aEnterpriseManager.isAuthorised(sd, request.getRemoteUser());
		} else if(level.equals("org")) {
			aOrgManager.isAuthorised(sd, request.getRemoteUser());
		} else {
			SDDataSource.closeConnection(connectionString, sd);
			throw new AuthorisationException();
		}
		// End Authorisation

		Response response = null;

		String sqlOwner = "update server set billing_enabled = ?";
		String sqlEnterprise = "update enterprise set billing_enabled = ? where id = ?";
		String sqlOrganisation = "update organisation set billing_enabled = ? where id = ?";
		
		PreparedStatement pstmt = null;

		try {
				
			int iId = 0;
			if(id != null) {
				iId = Integer.parseInt(id);
			}
			
			boolean bEnable = false;
			if(enable.equals("true")) {
				bEnable = true;
			}
			
			if(level.equals("owner")) {
				pstmt = sd.prepareStatement(sqlOwner);
			} else if(level.equals("ent")) {
				if(iId == 0) {
					throw new ApplicationException("Enterprise ID must be specified");
				}
				pstmt = sd.prepareStatement(sqlEnterprise);
				pstmt.setInt(2,  iId);
			} else {		// Must be organisation
				if(iId == 0) {
					throw new ApplicationException("Organisation ID must be specified");
				}
				pstmt = sd.prepareStatement(sqlOrganisation);
				pstmt.setInt(2,  iId);
			} 
			pstmt.setBoolean(1, bEnable);
			log.info("Toggle enabled: " + pstmt.toString());
			pstmt.executeUpdate();
			
			response = Response.ok().build();

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			response = Response.serverError().entity(e.getMessage()).build();

		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
}

