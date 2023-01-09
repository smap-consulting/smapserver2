package surveyKPI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import utilities.XLSXAdminReportsHashMap;

/*
 * Export a survey in XLSX format
 * This export follows the approach of CSV exports where a single sub form can be selected
 * Get access to a form for each user
 */
@Path("/adminreport/structure")
public class AdminReportStructure extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(AdminReportStructure.class.getName());

	LogManager lm = new LogManager();		// Application log
	boolean includeTemporaryUsers;
	
	public AdminReportStructure() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ADMIN);
		a = new Authorise(authorisations, null);
	}
	
	/*
	 * Get structure of enterprises, organisations, projects and surveys
	 */
	@GET
	public Response exportSurveyXlsx (@Context HttpServletRequest request, 
			@Context HttpServletResponse response) {

		Response responseVal;
		
		// Authorisation - Access
		String connectionString = "surveyKPI - AdminReports - Usage";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			boolean enterpriseLevel = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ENTERPRISE_ID);
			boolean organisationLevel = GeneralUtilityMethods.hasSecurityGroup(sd, request.getRemoteUser(), Authorise.ORG_ID);
			
			int eId = 0;
			int oId = 0;
			if(enterpriseLevel) {
				// No filtering by enterprise or organisation
			} else if(organisationLevel) {
				eId = GeneralUtilityMethods.getEnterpriseId(sd, request.getRemoteUser());
			} else {
				oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			}
			
			String filename = localisation.getString("ar_report_name");
			
			ArrayList<HashMap<String, String>> report = getAdminReportStructure(sd, eId, oId);
			
			// Add header
			ArrayList<String> header = new ArrayList<String> ();
			header.add(localisation.getString("bill_ent"));
			header.add(localisation.getString("bill_org"));
			header.add(localisation.getString("ar_project"));
			
			// Add elements
			ArrayList<String> elements = new ArrayList<String> ();
			elements.add("e");
			elements.add("o");
			elements.add("p");
			
			XLSXAdminReportsHashMap rm = new XLSXAdminReportsHashMap(localisation);
			responseVal = rm.getNewReport(sd, request, response, header, elements, report, filename);
			
		} catch(Exception e) {
			log.log(Level.SEVERE, "Error", e);
			response.setHeader("Content-type",  "text/html; charset=UTF-8");
			responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return responseVal;

	}

	private ArrayList<HashMap<String, String>> getAdminReportStructure(Connection sd, int eId, int oId) throws SQLException {
		ArrayList<HashMap<String, String>> rows = new ArrayList<HashMap<String, String>> ();
		StringBuilder sql = new StringBuilder("select e.name as ent_name, o.name as org_name, p.name as project_name "
				+ "from enterprise e "
				+ "left outer join organisation o "
				+ "on e.id = o.e_id "
				+ "left outer join project p "
				+ "on o.id = p.o_id ");
		
		boolean hasWhere = false;
		if(eId > 0) {
			sql.append("where e.id = ? ");
			hasWhere = true;
		}
		if(oId > 0) {
			if(hasWhere) {
				sql.append("and o.id = ? ");
			} else {
				sql.append("where o.id = ? ");
			}
		}
		sql.append("order by e.name, o.name, p.name");
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql.toString());
			int idx = 1;
			if(eId > 0) {
				pstmt.setInt(idx++, eId);
			}
			if(oId > 0) {
				pstmt.setInt(idx++, oId);
			}
			
			log.info("Admin structure report: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			
			while(rs.next()) {
				HashMap<String, String> ar = new HashMap<String, String>();
				ar.put("e", rs.getString("ent_name"));
				ar.put("o", rs.getString("org_name"));
				ar.put("p", rs.getString("project_name"));
				rows.add(ar);
			}
			
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		return rows;
	}

	

}
