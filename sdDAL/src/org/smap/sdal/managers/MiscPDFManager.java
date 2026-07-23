package org.smap.sdal.managers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.TaskFeature;
import org.smap.sdal.model.TaskListGeoJson;
import org.smap.sdal.model.TaskProperties;

import com.itextpdf.forms.PdfAcroForm;
import com.itextpdf.io.font.PdfEncodings;
import com.itextpdf.kernel.colors.ColorConstants;
import com.itextpdf.kernel.colors.DeviceRgb;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.kernel.font.PdfFontFactory;
import com.itextpdf.kernel.geom.PageSize;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfReader;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.kernel.pdf.canvas.PdfCanvas;
import com.itextpdf.kernel.pdf.xobject.PdfFormXObject;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.Cell;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.element.Table;
import com.itextpdf.layout.font.FontProvider;
import com.itextpdf.layout.properties.Property;
import com.itextpdf.layout.properties.UnitValue;

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

	public static DeviceRgb VLG = new DeviceRgb(0xE8,0xE8,0xE8);

	public MiscPDFManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}

	/*
	 * Set up a font provider that gives per-glyph fallback across scripts
	 */
	private FontProvider buildFontProvider() {
		String fontDir = System.getProperty("os.name").startsWith("Mac") ? "/Library/Fonts/" : "/usr/share/fonts/truetype/";
		FontProvider fp = new FontProvider();
		try {
			fp.addFont(fontDir + "NotoSans-Regular.ttf");
			fp.addFont(fontDir + "NotoNaskhArabic-Regular.ttf");
			fp.addFont(fontDir + "fontawesome-webfont.ttf");
		} catch (Exception e) {
			log.fine("Failed to register fonts for pdf");
		}
		return fp;
	}

	private PdfFont defaultFont() throws IOException {
		String fontDir = System.getProperty("os.name").startsWith("Mac") ? "/Library/Fonts/" : "/usr/share/fonts/truetype/";
		return PdfFontFactory.createFont(fontDir + "NotoSans-Regular.ttf", PdfEncodings.IDENTITY_H);
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
			FontProvider fontProvider = buildFontProvider();
			PdfFont font = defaultFont();

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
			log.fine("Get Usage Data: " + pstmt.toString());


			// If the PDF is to be returned in an http response then set the file name now
			if(response != null) {
				log.fine("Setting filename to: " + filename);
				GeneralUtilityMethods.setFilenameInResponse(filename, response);
			}

			/*
			 * Get a template for the PDF report if it exists
			 * The template name will be the same as the XLS form name but with an extension of pdf
			 */
			String stationaryName = basePath + File.separator + "misc" + File.separator + "UsageReportTemplate.pdf";
			File stationaryFile = new File(stationaryName);

			/*
			 * Create document in two passes, the second pass adds the letter head
			 */
			ByteArrayOutputStream baos = stationaryFile.exists() ? new ByteArrayOutputStream() : null;

			PdfDocument pdf = new PdfDocument(new PdfWriter(baos != null ? baos : outputStream));
			Document document = new Document(pdf, PageSize.A4);
			// iText 8 margin order is top, right, bottom, left
			document.setMargins(marginTop_1, marginRight, marginBottom_1, marginLeft);
			document.setFontProvider(fontProvider);
			document.setProperty(Property.FONT, new String[]{"Noto Sans"});

			// Write the usage data
			ResultSet resultSet = pstmt.executeQuery();

			Table table = new Table(UnitValue.createPercentArray(4)).useAllAvailableWidth();

			// Add the header row
			table.addHeaderCell(headerCell("User Id", font));
			table.addHeaderCell(headerCell("User Name", font));
			table.addHeaderCell(headerCell("Usage in Period", font));
			table.addHeaderCell(headerCell("All Time Usage", font));

			// Add the user data
			int total = 0;
			int totalAllTime = 0;

			while(resultSet.next()) {
				String ident = resultSet.getString("ident");
				String name = resultSet.getString("name");
				String monthUsage = resultSet.getString("month");
				int monthUsageInt = resultSet.getInt("month");
				String allTime = resultSet.getString("all_time");
				int allTimeInt = resultSet.getInt("all_time");

				table.addCell(dataCell(ident, font));
				table.addCell(dataCell(name, font));
				table.addCell(dataCell(monthUsage, font));
				table.addCell(dataCell(allTime, font));

				total += monthUsageInt;
				totalAllTime += allTimeInt;

			}

			// Add the totals
			table.addCell(headerCell("Totals: ", font));
			table.addCell(headerCell(" ", font));
			table.addCell(headerCell(String.valueOf(total), font));
			table.addCell(headerCell(String.valueOf(totalAllTime), font));

			document.add(table);
			document.close();		// closes pdf and flushes underlying document

			if(stationaryFile.exists()) {

				// Step 2 - Populate the fields in the stationary
				ByteArrayOutputStream baos_s = new ByteArrayOutputStream();
				PdfDocument sPdf = new PdfDocument(new PdfReader(stationaryName), new PdfWriter(baos_s));

				PdfAcroForm pdfForm = PdfAcroForm.getAcroForm(sPdf, true);
				for(String key : pdfForm.getAllFormFields().keySet()) {
					log.fine("Field: " + key);
				}

				if(pdfForm.getField("billing_period") != null) {
					pdfForm.getField("billing_period").setValue(period);
				}
				if(pdfForm.getField("organisation") != null) {
					pdfForm.getField("organisation").setValue(org_name);
				}

				pdfForm.flattenFields();
				sPdf.close();

				// Step 3 - Apply the stationary to the underlying document
				PdfDocument dest = new PdfDocument(new PdfReader(new ByteArrayInputStream(baos.toByteArray())),
						new PdfWriter(outputStream));							// Underlying document
				PdfDocument src = new PdfDocument(new PdfReader(new ByteArrayInputStream(baos_s.toByteArray())));	// Filled in stationary

				PdfFormXObject letter1 = src.getFirstPage().copyAsFormXObject(dest);
				PdfCanvas background = new PdfCanvas(dest.getFirstPage().newContentStreamBefore(),
						dest.getFirstPage().getResources(), dest);
				background.addXObjectAt(letter1, 0, 0);

				src.close();
				dest.close();

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

			FontProvider fontProvider = buildFontProvider();
			PdfFont font = defaultFont();

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

			String filename = "tasks.pdf";
			// If the PDF is to be returned in an http response then set the file name now
			if(response != null) {
				log.fine("Setting filename to: " + filename);
				GeneralUtilityMethods.setFilenameInResponse(filename, response);
			}

			PdfDocument pdf = new PdfDocument(new PdfWriter(outputStream));
			Document document = new Document(pdf, PageSize.A4);
			document.setMargins(marginTop_1, marginRight, marginBottom_1, marginLeft);
			document.setFontProvider(fontProvider);
			document.setProperty(Property.FONT, new String[]{"Noto Sans"});

			Table table = new Table(UnitValue.createPercentArray(4)).useAllAvailableWidth();

			// Add the header row
			table.addHeaderCell(headerCell("Form Name", font));
			table.addHeaderCell(headerCell("Task Name", font));
			table.addHeaderCell(headerCell("Status", font));
			table.addHeaderCell(headerCell("Assigned To", font));

			// Add the task data
			for(TaskFeature tf : t.features) {
				TaskProperties p = tf.properties;

				table.addCell(dataCell(p.survey_name, font));
				table.addCell(dataCell(p.name, font));
				table.addCell(dataCell(p.status, font));
				table.addCell(dataCell(p.assignee_name, font));

			}

			document.add(table);
			document.close();

		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Error", e);

		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);

		}

	}

	private Cell headerCell(String text, PdfFont font) {
		return new Cell()
				.add(new Paragraph(text == null ? "" : text).setFont(font))
				.setBackgroundColor(VLG)
				.setBorder(new com.itextpdf.layout.borders.SolidBorder(ColorConstants.LIGHT_GRAY, 0.5f));
	}

	private Cell dataCell(String text, PdfFont font) {
		return new Cell()
				.add(new Paragraph(text == null ? "" : text).setFont(font))
				.setBorder(new com.itextpdf.layout.borders.SolidBorder(ColorConstants.LIGHT_GRAY, 0.5f));
	}

}
