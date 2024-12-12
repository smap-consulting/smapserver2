package org.smap.sdal.managers;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskProperties;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.Font;
import com.itextpdf.text.FontFactory;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.pdf.AcroFields;
import com.itextpdf.text.pdf.BaseFont;
import com.itextpdf.text.pdf.PdfContentByte;
import com.itextpdf.text.pdf.PdfImportedPage;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfPageEventHelper;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfStamper;
import com.itextpdf.text.pdf.PdfWriter;

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
 * Manage the creation of PDFS on usage
 */
public class MiscPDFManager {
	
	private static Logger log =
			 Logger.getLogger(MiscPDFManager.class.getName());
	
	private ResourceBundle localisation = null;
	private String tz;
	
	int marginLeft = 36;
	int marginRight = 36;
	int marginTop_1 = 300;
	int marginBottom_1 = 200;
	int marginTop_2 = 50;
	int marginBottom_2 = 50;
	
	class PageSizer extends PdfPageEventHelper {
		int pagenumber = 0;
		public void onStartPage(PdfWriter writer, Document document) {
			pagenumber++;
			log.info("Page number: " + pagenumber);

			document.setMargins(marginLeft, marginRight, marginTop_2, marginBottom_2);
			
		}
		public void onEndPage(PdfWriter writer, Document document) {
			
		}
	}
	public static Font Symbols = null;
	public static Font defaultFont = null;
	public static BaseColor VLG = new BaseColor(0xE8,0xE8,0xE8);

	public MiscPDFManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}
	
	/*
	 * Call this function to create a PDF
	 * Return a suggested name for the PDF file derived from the results
	 */
	public void createUsagePdf(
			Connection sd,
			OutputStream outputStream,
			String basePath, 
			HttpServletResponse response,
			int o_id,
			int month,
			int year,
			String period,
			String org_name) {
		
		PreparedStatement pstmt = null;
		
		if(org_name == null) {
			org_name = "None";
		}
		
		try {
			
			String filename;
			
			// Get fonts and embed them
			String os = System.getProperty("os.name");
			log.info("Operating System:" + os);
			
			if(os.startsWith("Mac")) {
				FontFactory.register("/Library/Fonts/fontawesome-webfont.ttf", "Symbols");
				FontFactory.register("/Library/Fonts/Arial Unicode.ttf", "default");
				FontFactory.register("/Library/Fonts/NotoNaskhArabic-Regular.ttf", "arabic");
				FontFactory.register("/Library/Fonts/NotoSans-Regular.ttf", "notosans");
			} else if(os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 || os.indexOf("aix") > 0) {
				// Linux / Unix
				FontFactory.register("/usr/share/fonts/truetype/fontawesome-webfont.ttf", "Symbols");
				FontFactory.register("/usr/share/fonts/truetype/ttf-dejavu/DejaVuSans.ttf", "default");
				FontFactory.register("/usr/share/fonts/truetype/NotoNaskhArabic-Regular.ttf", "arabic");
				FontFactory.register("/usr/share/fonts/truetype/NotoSans-Regular.ttf", "notosans");
			}
			
			Symbols = FontFactory.getFont("Symbols", BaseFont.IDENTITY_H, 
				    BaseFont.EMBEDDED, 12); 
			defaultFont = FontFactory.getFont("default", BaseFont.IDENTITY_H, 
				    BaseFont.EMBEDDED, 10); 
			
			filename = org_name + "_" + year + "_" + month + ".pdf";
			
			/*
			 * Get the usage results
			 */
			String sql = "SELECT users.id as id,"
					+ "users.ident as ident, "
					+ "users.name as name, "
					+ "(select count (*) from upload_event ue "
						+ "where ue.db_status = 'success' "
						+ "and upload_time >=  ? "		// current month
						+ "and upload_time < ? "		// next month
						+ "and ue.user_name = users.ident) as month, "
					+ "(select count (*) from upload_event ue "
						+ "where ue.db_status = 'success' "
						+ "and ue.user_name = users.ident) as all_time "
					+ "from users "	
					+ "where users.o_id = ? "
					+ "and not users.temporary " 
					+ "order by users.ident;";
			
			Timestamp t1 = GeneralUtilityMethods.getTimestampFromParts(year, month, 1);
			Timestamp t2 = GeneralUtilityMethods.getTimestampNextMonth(t1);
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setTimestamp(1, t1);
			pstmt.setTimestamp(2, t2);
			pstmt.setInt(3, o_id);
			log.info("Get Usage Data: " + pstmt.toString());

			
			// If the PDF is to be returned in an http response then set the file name now
			if(response != null) {
				log.info("Setting filename to: " + filename);
				GeneralUtilityMethods.setFilenameInResponse(filename, response);
			}
			
			/*
			 * Get a template for the PDF report if it exists
			 * The template name will be the same as the XLS form name but with an extension of pdf
			 */
			String stationaryName = basePath + File.separator + "misc" + File.separator + "UsageReportTemplate.pdf";
			File stationaryFile = new File(stationaryName);
			
			ByteArrayOutputStream baos = null;
			ByteArrayOutputStream baos_s = null;
			PdfWriter writer = null;			
				
			/*
			 * Create document in two passes, the second pass adds the letter head
			 */
				
			// Create the underlying document as a byte array
			Document document = new Document(PageSize.A4);
			document.setMargins(marginLeft, marginRight, marginTop_1, marginBottom_1);
			
			if(stationaryFile.exists()) {
				baos = new ByteArrayOutputStream();
				baos_s = new ByteArrayOutputStream();
				writer = PdfWriter.getInstance(document, baos);
			} else {
				writer = PdfWriter.getInstance(document, outputStream);
			}
				
			writer.setInitialLeading(12);
			writer.setPageEvent(new PageSizer()); 
			document.open();
			
			// Write the usage data
			ResultSet resultSet = pstmt.executeQuery();
			
			PdfPTable table = new PdfPTable(4);
			
			// Add the header row
			table.getDefaultCell().setBorderColor(BaseColor.LIGHT_GRAY);
			table.getDefaultCell().setBackgroundColor(VLG);
			
			table.addCell("User Id");
			table.addCell("User Name");
			table.addCell("Usage in Period");
			table.addCell("All Time Usage");
			
			table.setHeaderRows(1);
			
			// Add the user data
			int total = 0;
			int totalAllTime = 0;
			
			table.getDefaultCell().setBackgroundColor(null);
			while(resultSet.next()) {
				String ident = resultSet.getString("ident");
				String name = resultSet.getString("name");
				String monthUsage = resultSet.getString("month");
				int monthUsageInt = resultSet.getInt("month");
				String allTime = resultSet.getString("all_time");
				int allTimeInt = resultSet.getInt("all_time");

				table.addCell(ident);
				table.addCell(name);
				table.addCell(monthUsage);
				table.addCell(allTime);
				
				total += monthUsageInt;
				totalAllTime += allTimeInt;
				
			}
			
			// Add the totals
			table.getDefaultCell().setBackgroundColor(VLG);
			
			table.addCell("Totals: ");
			table.addCell(" ");
			table.addCell(String.valueOf(total));
			table.addCell(String.valueOf(totalAllTime));
			
			document.add(table);
			document.close();
				
			if(stationaryFile.exists()) {
					
				// Step 2 - Populate the fields in the stationary
				PdfReader s_reader = new PdfReader(stationaryName);
				PdfStamper s_stamper = new PdfStamper(s_reader, baos_s);
				

				AcroFields pdfForm = s_stamper.getAcroFields();
				Set<String> fields = pdfForm.getFields().keySet();
				for(String key: fields) {
					log.info("Field: " + key);
				}
					
				pdfForm.setField("billing_period", period);
				pdfForm.setField("organisation", org_name);
				
				s_stamper.setFormFlattening(true);
				s_stamper.close();
				
				// Step 3 - Apply the stationary to the underlying document
				PdfReader reader = new PdfReader(baos.toByteArray());		// Underlying document
				PdfReader f_reader = new PdfReader(baos_s.toByteArray());	// Filled in stationary
				PdfStamper stamper = new PdfStamper(reader, outputStream);
				PdfImportedPage letter1 = stamper.getImportedPage(f_reader, 1);
				int n = reader.getNumberOfPages();
				PdfContentByte background;
				for(int i = 0; i < n; i++ ) {
					background = stamper.getUnderContent(i + 1);
					if(i == 0) {
						background.addTemplate(letter1, 0, 0);
					}
				}
		
				stamper.close();
				reader.close();
				
			}
			
			
		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Error", e);
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			
		}  finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
		}

	
	}
	
	
	/*
	 * Call this function to create a PDF with the list of tasks in it
	 */
	public void createTasksPdf(
			Connection sd,
			OutputStream outputStream,
			String basePath, 
			HttpServletRequest request,
			HttpServletResponse response,
			int tgId) {
		
		try {
			
			// Get fonts and embed them
			String os = System.getProperty("os.name");
			log.info("Operating System:" + os);
			
			if(os.startsWith("Mac")) {
				FontFactory.register("/Library/Fonts/fontawesome-webfont.ttf", "Symbols");
				FontFactory.register("/Library/Fonts/Arial Unicode.ttf", "default");
				FontFactory.register("/Library/Fonts/NotoNaskhArabic-Regular.ttf", "arabic");
			} else if(os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 || os.indexOf("aix") > 0) {
				// Linux / Unix
				FontFactory.register("/usr/share/fonts/truetype/fontawesome-webfont.ttf", "Symbols");
				FontFactory.register("/usr/share/fonts/truetype/ttf-dejavu/DejaVuSans.ttf", "default");
				FontFactory.register("/usr/share/fonts/truetype/NotoNaskhArabic-Regular.ttf", "arabic");
				FontFactory.register("/usr/share/fonts/truetype/NotoSans-Regular.ttf", "notosans");
			}
			
			Symbols = FontFactory.getFont("Symbols", BaseFont.IDENTITY_H, 
				    BaseFont.EMBEDDED, 12); 
			defaultFont = FontFactory.getFont("default", BaseFont.IDENTITY_H, 
				    BaseFont.EMBEDDED, 10); 
			
			/*
			 * Get the tasks for this task group
			 */
			String urlprefix = request.getScheme() + "://" + request.getServerName();
			TaskManager tm = new TaskManager(localisation, tz);
			TaskListGeoJson t = tm.getTasks(sd, 
					urlprefix, 
					0, 
					tgId, 
					0, 		// task id
					0,		// assignment id
					false, 
					0, 
					null, 
					"all", 
					0, 0, "scheduled", "desc", false);	
			PdfWriter writer = null;			
				

			String filename = "tasks.pdf";
			// If the PDF is to be returned in an http response then set the file name now
			if(response != null) {
				log.info("Setting filename to: " + filename);
				GeneralUtilityMethods.setFilenameInResponse(filename, response);
			}
			
			Document document = new Document(PageSize.A4);
			document.setMargins(marginLeft, marginRight, marginTop_1, marginBottom_1);
			writer = PdfWriter.getInstance(document, outputStream);
				
			writer.setInitialLeading(12);
			writer.setPageEvent(new PageSizer()); 
			document.open();
			
			PdfPTable table = new PdfPTable(4);
			
			// Add the header row
			table.getDefaultCell().setBorderColor(BaseColor.LIGHT_GRAY);
			table.getDefaultCell().setBackgroundColor(VLG);
			
			table.addCell("Form Name");
			table.addCell("Task Name");
			table.addCell("Status");
			table.addCell("Assigned To");
	
			table.setHeaderRows(1);
			
			// Add the task data
			
			table.getDefaultCell().setBackgroundColor(null);
			for(TaskFeature tf : t.features) {
				TaskProperties p = tf.properties;

				table.addCell(p.survey_name);
				table.addCell(p.name);
				table.addCell(p.status);
				table.addCell(p.assignee_name);
				
			}
			

			document.add(table);
			document.close();		
			
		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Error", e);
			
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			
		}  
	
	}

}


