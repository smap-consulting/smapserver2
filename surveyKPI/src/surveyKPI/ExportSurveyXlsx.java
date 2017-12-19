package surveyKPI;

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.QueryGenerator;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.QueryManager;
import org.smap.sdal.model.ColDesc;
import org.smap.sdal.model.ColValues;
import org.smap.sdal.model.OptionDesc;
import org.smap.sdal.model.QueryForm;
import org.smap.sdal.model.SqlDesc;

import utilities.XLSUtilities;

/*
 * Export a survey in XLSX format
 * This export follows the approach of CSV exports where a single sub form can be selected
 *    
 */
@Path("/exportxlsx/{sId}/{filename}")
public class ExportSurveyXlsx extends Application {

	Authorise a = new Authorise(null, Authorise.ANALYST);

	private static Logger log =
			Logger.getLogger(ExportSurveyXlsx.class.getName());

	LogManager lm = new LogManager();		// Application log

	@GET
	public Response exportSurveyXlsx (@Context HttpServletRequest request, 
			@PathParam("sId") int sId,
			@PathParam("filename") String filename,
			@QueryParam("split_locn") boolean split_locn,
			@QueryParam("merge_select_multiple") boolean merge_select_multiple,
			@QueryParam("language") String language,
			@QueryParam("exp_ro") boolean exp_ro,
			@QueryParam("embedimages") boolean embedImages,
			@QueryParam("excludeparents") boolean excludeParents,
			@QueryParam("hxl") boolean hxl,
			@QueryParam("form") int fId,
			@QueryParam("from") Date startDate,
			@QueryParam("to") Date endDate,
			@QueryParam("dateId") int dateId,
			@QueryParam("filter") String filter,
			@QueryParam("meta") boolean meta,
			
			@Context HttpServletResponse response) {

		ResponseBuilder builder = Response.ok();
		Response responseVal = null;

		HashMap<ArrayList<OptionDesc>, String> labelListMap = new  HashMap<ArrayList<OptionDesc>, String> ();

		log.info("userevent: " + request.getRemoteUser() + " Export " + sId + " as an xlsx file to " + filename + " starting from form " + fId);

		String urlprefix = request.getScheme() + "://" + request.getServerName() + "/";		


		// Authorisation - Access
		Connection connectionSD = SDDataSource.getConnection("surveyKPI-ExportSurveyMisc");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(connectionSD, request.getRemoteUser());
		} catch (Exception e) {
		}

		a.isAuthorised(connectionSD, request.getRemoteUser());
		a.isValidSurvey(connectionSD, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		lm.writeLog(connectionSD, sId, request.getRemoteUser(), "view", "Export as: xlsx");

		String escapedFileName = null;
		try {
			escapedFileName = URLDecoder.decode(filename, "UTF-8");
			escapedFileName = URLEncoder.encode(escapedFileName, "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}

		escapedFileName = escapedFileName.replace("+", " "); // Spaces ok for file name within quotes
		escapedFileName = escapedFileName.replace("%2C", ","); // Commas ok for file name within quotes
		
		if(sId != 0) {

			Connection cResults = null;
			PreparedStatement pstmt = null;

			try {

				// Get the users locale
				Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(connectionSD, request.getRemoteUser()));
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
				
				cResults = ResultsDataSource.getConnection("surveyKPI-ExportSurvey");				

				if(language == null) {	// ensure a language is set
					language = "none";
				}

				/*
				 * Get the list of forms and surveys to be exported
				 */
				ArrayList<QueryForm> queryList = null;
				QueryManager qm = new QueryManager();				
				queryList = qm.getFormList(connectionSD, sId, fId);		// Get a form list for this survey / form combo

				QueryForm startingForm = qm.getQueryTree(connectionSD, queryList);	// Convert the query list into a tree

				// Get the SQL for this query
				SqlDesc sqlDesc = QueryGenerator.gen(connectionSD, 
						cResults,
						localisation,
						sId,
						fId,
						language, 
						"xlsx", 
						urlprefix, 
						true,
						exp_ro,
						excludeParents,
						labelListMap,
						false,
						false,
						null,
						null,
						null,
						request.getRemoteUser(),
						startDate,
						endDate,
						dateId,
						superUser,
						startingForm,
						filter,
						meta);

				String basePath = GeneralUtilityMethods.getBasePath(request);					
				
				/*
				 * Create XLSX File
				 */
				GeneralUtilityMethods.setFilenameInResponse(filename + "." + "xlsx", response); // Set file name
				Workbook wb = null;
				int rowNumber = 0;
				wb = new SXSSFWorkbook(10);		// Serialised output
				Sheet dataSheet = wb.createSheet("data");
				
				Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
				CellStyle headerStyle = styles.get("header");
				
				/*
				 * Write Question Name Header
				 */
				Row headerRow = dataSheet.createRow(rowNumber++);				
				int colNumber = 0;
				int dataColumn = 0;
				while(dataColumn < sqlDesc.colNames.size()) {
					ColValues values = new ColValues();
					dataColumn = GeneralUtilityMethods.getColValues(
							null, 
							values, 
							dataColumn,
							sqlDesc.colNames, 
							merge_select_multiple);	
		
					Cell cell = headerRow.createCell(colNumber++);
					cell.setCellStyle(headerStyle);
					cell.setCellValue(values.name);
				}
				
				/*
				 * Write each row of data
				 */
				pstmt = cResults.prepareStatement(sqlDesc.sql);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					
					Row dataRow = dataSheet.createRow(rowNumber++);	
					
					colNumber = 0;
					dataColumn = 0;
					while(dataColumn < sqlDesc.colNames.size()) {
						ColValues values = new ColValues();
						dataColumn = GeneralUtilityMethods.getColValues(
								rs, 
								values, 
								dataColumn,
								sqlDesc.colNames, 
								merge_select_multiple);						

						Cell cell = dataRow.createCell(colNumber++);
						XLSUtilities.setCellValue(wb, dataSheet, cell, styles, values.value, 
								values.type, embedImages, basePath, rowNumber, colNumber - 1, true);
						cell.setCellValue(values.value);
					}
					
				}
				
				OutputStream outputStream = response.getOutputStream();
				wb.write(outputStream);
				wb.close();
				outputStream.close();
				((SXSSFWorkbook) wb).dispose();		// Dispose of temporary files


			} catch (ApplicationException e) {
				response.setHeader("Content-type",  "text/html; charset=UTF-8");
				// Return an OK status so the message gets added to the web page
				// Prepend the message with "Error: ", this will be removed by the client
				responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
			} catch (Exception e) {
				log.log(Level.SEVERE, "Error", e);
				response.setHeader("Content-type",  "text/html; charset=UTF-8");
				responseVal = Response.status(Status.OK).entity("Error: " + e.getMessage()).build();
			} finally {	

				try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}

				SDDataSource.closeConnection("surveyKPI-ExportSurvey", connectionSD);
				ResultsDataSource.closeConnection("surveyKPI-ExportSurvey", cResults);
			}
		}

		return responseVal;

	}


}
