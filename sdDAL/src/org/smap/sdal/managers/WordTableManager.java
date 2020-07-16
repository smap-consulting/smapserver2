package org.smap.sdal.managers;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;

import org.apache.poi.util.Units;
import org.apache.poi.xwpf.usermodel.ParagraphAlignment;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFRun;
import org.apache.poi.xwpf.usermodel.XWPFStyles;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;
import org.smap.sdal.Utilities.TableReportUtilities;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.SurveyViewDefn;
import org.smap.sdal.model.TableReportsColumn;
import com.google.zxing.BarcodeFormat;
import com.google.zxing.client.j2se.MatrixToImageWriter;
import com.google.zxing.common.BitMatrix;
import com.google.zxing.qrcode.QRCodeWriter;

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
public class WordTableManager {
	
	private static Logger log =
			 Logger.getLogger(WordTableManager.class.getName());
	

	/*
	 * Call this function to create a docx
	 */
	public void create(
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
		
		try {
			
			XWPFDocument template = new XWPFDocument(new FileInputStream(new File("/smap_bin/resources/template.docx")));
			XWPFDocument doc = new XWPFDocument();
			XWPFStyles newStyles = doc.createStyles();
			newStyles.setStyles(template.getStyle());
			template.close();
					
			int n = newStyles.getNumberOfStyles();
			
			XWPFParagraph para = doc.createParagraph();
			para.setStyle("Title");			
			XWPFRun run = para.createRun();
			run.setText(title);
	
			// write to a docx file
			try {
				
				// Write table header
				ArrayList<TableReportsColumn> cols = TableReportUtilities.getTableReportColumnList(mfc, dArray, localisation);
				
				XWPFTable table = doc.createTable();
				XWPFTableRow tableRow = table.getRow(0);
				XWPFParagraph paragraph = null;
				table.setWidth("100.00%");				
				int idx = 0;
				for(TableReportsColumn col : cols) {	
					if(col.dataIndex >= 0) {	
						addHeaderCell(tableRow, idx++, paragraph, col.displayName);							
						if(col.includeText && (col.barcode || !col.type.equals("text"))) {
							addHeaderCell(tableRow, idx++, paragraph, col.displayName);
						}
					}
				}
				
				// Add Table Content
				for(ArrayList<KeyValue> record : dArray) {
					
					tableRow = table.createRow();
					idx = 0;
					for(TableReportsColumn col : cols) {					
						if(col.dataIndex >= 0) {		
							updateCell(tableRow, idx++, record.get(col.dataIndex), basePath, col.barcode, col.type);
							if(col.includeText && (col.barcode || !col.type.equals("text"))) {
								updateCell(tableRow, idx++, record.get(col.dataIndex), basePath, false, "text");
							}
						}
					}		
				}
				doc.write(outputStream);
			} finally {
				if (doc != null) {try {doc.close();} catch (IOException e) {}}
			}
				
		}  catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);	
		}
	
	}
	
	private void addHeaderCell(XWPFTableRow tableRow, int idx, XWPFParagraph paragraph, 
			String value) {
		XWPFTableCell cell = tableRow.getCell(idx);
		if(cell == null) {
			cell = tableRow.createCell();
		}
		
		paragraph = cell.getParagraphArray(0);
		if(paragraph == null) {
			paragraph = cell.addParagraph();
		}
		XWPFRun run = paragraph.createRun();
		run.setBold(true);
		paragraph.setAlignment(ParagraphAlignment.CENTER);
		run.setText(value);
	}
	
	/*
	 * Add the question value
	 */
	private void updateCell(XWPFTableRow tableRow, 
			int idx,
			KeyValue kv, 
			String basePath,
			boolean barcode,
			String type) throws MalformedURLException, IOException {
		
		int bcWidth = 100;
		int bcHeight = 100;
		QRCodeWriter qrCodeWriter = new QRCodeWriter();
		
		XWPFTableCell cell = tableRow.getCell(idx);
		if(cell == null) {
			cell = tableRow.createCell();
		}					
		
		XWPFParagraph paragraph = cell.getParagraphArray(0);
		if(paragraph == null) {
			paragraph = cell.addParagraph();
		}
		XWPFRun run = paragraph.createRun();
		paragraph.setAlignment(ParagraphAlignment.LEFT);
		try {
			if(type != null && type.equals("image")) {
				
				URL url = new URL(kv.v);
				BufferedImage image = ImageIO.read(url);
				
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				ImageIO.write(image, "png", os);
				InputStream is = new ByteArrayInputStream(os.toByteArray());
				    
				run.addPicture(is, XWPFDocument.PICTURE_TYPE_PNG, null, Units.toEMU(bcWidth), Units.toEMU(bcHeight)); 
				is.close();
				os.close();
				
			} else if(barcode && kv.v.trim().length() > 0) {
				BitMatrix matrix = qrCodeWriter.encode(kv.v, BarcodeFormat.QR_CODE, 100, 100);
				BufferedImage image = MatrixToImageWriter.toBufferedImage(matrix);

				ByteArrayOutputStream os = new ByteArrayOutputStream();
				ImageIO.write(image, "png", os);
				InputStream is = new ByteArrayInputStream(os.toByteArray());
				    
				run.addPicture(is, XWPFDocument.PICTURE_TYPE_PNG, null, Units.toEMU(bcWidth), Units.toEMU(bcHeight)); 
				is.close();
				os.close();
			} else {
				run.setText(kv.v);
			}
		} catch (Exception e) {
			log.info("Error updating value cell, continuing: " + basePath + " : " + kv.v);
			log.log(Level.SEVERE, "Exception", e);
		}

	}
	
	
	
	
	
	
}


