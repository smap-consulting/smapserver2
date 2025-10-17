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
 * Produce reports on user hierarchies
 */
@Path("/adminreport/userstructure")
public class AdminReportUserStructure extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(AdminReportUserStructure.class.getName());

	LogManager lm = new LogManager();		// Application log
	boolean includeTemporaryUsers;
	
	public AdminReportUserStructure() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ENTERPRISE);
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
		String connectionString = "surveyKPI - AdminReports - User Structure";
		Connection sd = SDDataSource.getConnection(connectionString);
		a.isAuthorised(sd, request.getRemoteUser());
		// End Authorisation
		
		
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String filename = localisation.getString("ar_report_name");
			
			ArrayList<HashMap<String, String>> report = getUserReportStructure(sd);
			
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

	private ArrayList<HashMap<String, String>> getUserReportStructure(Connection sd) throws SQLException {
		ArrayList<HashMap<String, String>> rows = new ArrayList<HashMap<String, String>> ();
		StringBuilder sql = new StringBuilder("select e.name as ent_name, o.name as org_name, p.name as project_name "
				+ "from enterprise e "
				+ "left outer join organisation o "
				+ "on e.id = o.e_id "
				+ "left outer join project p "
				+ "on o.id = p.o_id ");
		
		boolean hasWhere = false;

		sql.append("order by e.name, o.name, p.name");
		PreparedStatement pstmt = null;
		
		try {
			pstmt = sd.prepareStatement(sql.toString());
			int idx = 1;
		
			
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
