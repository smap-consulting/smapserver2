package org.smap.sdal.managers;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.smap.sdal.Utilities.AttachmentStore;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.PdfPageSizer;
import org.smap.sdal.Utilities.PdfUtilities;
import org.smap.sdal.Utilities.TableReportUtilities;
import org.smap.sdal.model.DisplayItem;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.SurveyViewDefn;
import org.smap.sdal.model.TableReportsColumn;
import org.smap.sdal.model.User;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.itextpdf.barcodes.BarcodeQRCode;
import com.itextpdf.io.font.PdfEncodings;
import com.itextpdf.kernel.colors.ColorConstants;
import com.itextpdf.kernel.events.PdfDocumentEvent;
import com.itextpdf.kernel.font.PdfFont;
import com.itextpdf.kernel.font.PdfFontFactory;
import com.itextpdf.kernel.geom.PageSize;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.kernel.pdf.xobject.PdfFormXObject;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.borders.SolidBorder;
import com.itextpdf.layout.element.Cell;
import com.itextpdf.layout.element.Image;
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
 * Manage the table that stores details on the forwarding of data onto other systems
 */
public class PDFTableManager {

	private static Logger log =
			 Logger.getLogger(PDFTableManager.class.getName());

	private static final String DEFAULT_CSS = "/resources/css/default_pdf.css";

	// iText 8 fonts and rendering context (set up in createPdf)
	private PdfFont font;
	private PdfFont fontbold;
	private FontProvider fontProvider;
	private PdfDocument pdfDoc;

	int marginLeft = 50;
	int marginRight = 50;
	int marginTop_1 = 130;
	int marginBottom_1 = 100;
	int marginTop_2 = 50;
	int marginBottom_2 = 100;

	/*
	 * Call this function to create a PDF
	 * Return a suggested name for the PDF file derived from the results
	 */
	public void createPdf(
			Connection sd,
			OutputStream outputStream,
			ArrayList<ArrayList<KeyValue>> dArray,
			SurveyViewDefn mfc,
			ResourceBundle localisation,
			String tz,
			boolean landscape,
			String remoteUser,
			String basePath,
			String title,
			String project
			) {

		User user = null;
		UserManager um = new UserManager(localisation);
		Document document = null;

		try {

			user = um.getByIdent(sd, remoteUser);

			// Set up fonts (font provider gives per-glyph fallback across scripts)
			fontProvider = buildFontProvider();
			font = createFont(notoSansPath());
			fontbold = createFont(notoSansBoldPath());

			ArrayList<TableReportsColumn> cols = TableReportUtilities.getTableReportColumnList(mfc, dArray, localisation);
			ArrayList<String> tableHeader = new ArrayList<String> ();
			for(TableReportsColumn col : cols) {
				tableHeader.add(col.displayName);
			}

			/*
			 * Create a PDF without the stationary
			 */
			PdfWriter writer = new PdfWriter(outputStream);
			pdfDoc = new PdfDocument(writer);

			pdfDoc.addEventHandler(PdfDocumentEvent.END_PAGE, new PdfPageSizer(title,
					user, basePath,
					tableHeader,
					marginLeft, marginRight, marginTop_2, marginBottom_2, null, null));

			document = new Document(pdfDoc, landscape ? PageSize.A4.rotate() : PageSize.A4);
			// iText 8 margin order is top, right, bottom, left
			document.setMargins(marginTop_1, marginRight, marginBottom_1, marginLeft);
			document.setFontProvider(fontProvider);
			document.setProperty(Property.FONT, new String[]{"Noto Sans"});

			processResults(document, dArray, cols, basePath);
			document.close();


		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Error", e);

		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);

		} finally {
			if(document != null) try {document.close();} catch (Exception e) {}
		}

	}


	private class UserSettings {
		String title;
		String license;
	}

	/*
	 * Process the results and write to a table
	 */
	private void processResults(
			Document document,
			ArrayList<ArrayList<KeyValue>> dArray,
			ArrayList<TableReportsColumn> cols,
			String basePath) throws IOException {


		for(int index = 0; index < dArray.size(); index++) {

			ArrayList<KeyValue> record = dArray.get(index);
			Table newTable = processRow(record, cols, basePath);
			document.add(newTable);

		}

		return;
	}


	/*
	 * Add the table row to the document
	 */
	Table processRow(
			ArrayList<KeyValue> record,
			ArrayList<TableReportsColumn> cols,
			String basePath) throws MalformedURLException, IOException {

		Table table = new Table(UnitValue.createPercentArray(cols.size())).useAllAvailableWidth();

		for(TableReportsColumn col : cols) {

			if(col.dataIndex >= 0) {
				Cell cell = addDisplayItem(record.get(col.dataIndex), basePath, col.barcode, col.type);
				cell.setBorder(new SolidBorder(ColorConstants.LIGHT_GRAY, 0.5f));

				table.addCell(cell);
			}

		}
		return table;
	}

	/*
	 * Get the number of blank repeats to generate
	 */
	int getBlankRepeats(String appearance) {
		int repeats = 1;

		if(appearance != null) {
			String [] appValues = appearance.split(" ");
			if(appearance != null) {
				for(int i = 0; i < appValues.length; i++) {
					if(appValues[i].startsWith("pdfrepeat")) {
						String [] parts = appValues[i].split("_");
						if(parts.length >= 2) {
							repeats = Integer.valueOf(parts[1]);
						}
						break;
					}
				}
			}
		}

		return repeats;
	}


	/*
	 * Set the widths of the label and the value
	 * Appearance is:  pdflabelw_## where ## is a number from 0 to 10
	 */
	void setWidths(String aValue, DisplayItem di) {

		String [] parts = aValue.split("_");
		if(parts.length >= 2) {
			di.widthLabel = Integer.valueOf(parts[1]);
		}

		// Do bounds checking
		if(di.widthLabel < 0 || di.widthLabel > 10) {
			di.widthLabel = 5;
		}

	}

	/*
	 * Set the height of the value
	 * Appearance is:  pdfheight_## where ## is the height
	 */
	void setHeight(String aValue, DisplayItem di) {

		String [] parts = aValue.split("_");
		if(parts.length >= 2) {
			di.valueHeight = Double.valueOf(parts[1]);
		}

	}

	/*
	 * Set space before this item
	 * Appearance is:  pdfheight_## where ## is the height
	 */
	void setSpace(String aValue, DisplayItem di) {

		String [] parts = aValue.split("_");
		if(parts.length >= 2) {
			di.space = Integer.valueOf(parts[1]);
		}

	}

	/*
	 * Add the question value
	 */
	private Cell addDisplayItem(
			KeyValue kv,
			String basePath,
			boolean barcode,
			String type) throws MalformedURLException, IOException {

		Cell valueCell = new Cell();
		valueCell.setBorder(new SolidBorder(ColorConstants.LIGHT_GRAY, 0.5f));


		// Set the content of the value cell
		try {
			if(type != null && type.equals("image")) {
				/*
				 * Get the image from local disk or, if it has been archived, from S3 using the
				 * credentials of this server.  It cannot be read over HTTP as the server has no
				 * credentials to authenticate with itself
				 */
				byte [] imageBytes = AttachmentStore.getBytes(basePath, kv.v, null);
				if(imageBytes == null) {
					log.info("Error: Missing image file: " + kv.v);
				} else {
					Image img = new Image(PdfUtilities.createImageData(imageBytes, kv.v));
					valueCell.add(img);
				}
			} else if(barcode && kv.v.trim().length() > 0) {
				BarcodeQRCode qrcode = new BarcodeQRCode(kv.v.trim());
				PdfFormXObject xObject = qrcode.createFormXObject(pdfDoc);
				Image qrcodeImage = new Image(xObject);
				valueCell.add(qrcodeImage);
			} else {
				valueCell.add(getPara(kv.v));
			}
		} catch (Exception e) {
			log.fine("Error updating value cell, continuing: " + basePath + " : " + kv.v);
			log.log(Level.SEVERE, "Exception", e);
		}

		return valueCell;
	}

	private Paragraph getPara(String value) {

		Paragraph para = new Paragraph().setFont(font);

		if(value != null && value.trim().length() > 0) {
			para.add(GeneralUtilityMethods.unesc(value));
		}

		return para;
	}

	/*
	 * Fill in user details for the output when their is no template
	 */
	@SuppressWarnings("unused")
	private void fillNonTemplateUserDetails(Document document, User user, String basePath) throws IOException {

		String settings = user.settings;
		Type type = new TypeToken<UserSettings>(){}.getType();
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		UserSettings us = gson.fromJson(settings, type);

		float indent = (float) 20.0;
		addValue(document, "Completed by:", (float) 0.0);
		if(user.signature != null && user.signature.trim().length() > 0) {
			String fileName = null;
			try {
				fileName = basePath + File.separator + user.signature;

					Image img = new Image(PdfUtilities.createImageData(fileName));
					img.scaleToFit(200, 50);
					img.setMarginLeft(indent);

				    document.add(img);

			} catch (Exception e) {
				log.fine("Error: Failed to add image " + fileName + " to pdf");
			}
		}
		addValue(document, user.name, indent);
		addValue(document, user.company_name, indent);
		if(us != null) {
			addValue(document, us.title, indent);
			addValue(document, us.license, indent);
		}

	}

	/*
	 * Format a key value pair into a paragraph
	 */
	@SuppressWarnings("unused")
	private void addKeyValuePair(Document document, String key, String value) {
		Paragraph para = new Paragraph().setFont(font);

		para.add(new com.itextpdf.layout.element.Text(GeneralUtilityMethods.unesc(key)).setFont(fontbold));
		para.add(new com.itextpdf.layout.element.Text(GeneralUtilityMethods.unesc(value)).setFont(font));

		document.add(para);
	}

	/*
	 * Format a single value into a paragraph
	 */
	private void addValue(Document document, String value, float indent) {

		if(value != null && value.trim().length() > 0) {
			Paragraph para = new Paragraph().setFont(font);
			para.setMarginLeft(indent);
			para.add(GeneralUtilityMethods.unesc(value));
			document.add(para);
		}
	}

	/*
	 * Font helpers
	 */
	private boolean isMac() {
		return System.getProperty("os.name").startsWith("Mac");
	}

	private String fontDir() {
		return isMac() ? "/Library/Fonts/" : "/usr/share/fonts/truetype/";
	}

	private String notoSansPath() { return fontDir() + "NotoSans-Regular.ttf"; }
	private String notoSansBoldPath() { return fontDir() + "NotoSans-Bold.ttf"; }
	private String arabicPath() { return fontDir() + "NotoNaskhArabic-Regular.ttf"; }
	private String fontawesomePath() { return fontDir() + "fontawesome-webfont.ttf"; }

	private PdfFont createFont(String path) throws IOException {
		return PdfFontFactory.createFont(path, PdfEncodings.IDENTITY_H);
	}

	private FontProvider buildFontProvider() {
		FontProvider fp = new FontProvider();
		fp.addFont(notoSansPath());
		fp.addFont(notoSansBoldPath());
		fp.addFont(arabicPath());
		fp.addFont(fontawesomePath());
		return fp;
	}
}
