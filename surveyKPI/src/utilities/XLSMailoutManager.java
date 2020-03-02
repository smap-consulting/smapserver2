package utilities;

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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.model.MailoutPerson;



public class XLSMailoutManager {
	
	private static Logger log =
			 Logger.getLogger(SurveyInfo.class.getName());
	
	Workbook wb = null;
	int rowNumber = 1;		// Heading row is 0
	String scheme = null;
	String serverName = null;
	
	private class Column {
		String name;
		 CellStyle style;
		
		public Column(ResourceBundle localisation, int col, String name, boolean a, CellStyle style) {
			this.name = name;
			this.style = style;
		}
		
		// Return the width of this column
		public int getWidth() {
			int width = 256 * 20;		// 20 characters is default
			return width;
		}
		
		// Get a value for this column from the provided properties object
		public String getValue(MailoutPerson person) {
			String value = null;
			
			if(name.equals("email")) {
				value = person.email;
			} else if(name.equals("name")) {
				value = person.name;
			} else if(name.equals("status")) {
				value = person.status;
			} 
			
			if(value == null) {
				value = "";
			}
			return value;
		}
	}

	public XLSMailoutManager() {

	}
	
	public XLSMailoutManager(String type, String scheme, String serverName) {
		if(type != null && type.equals("xls")) {
			wb = new HSSFWorkbook();
		} else {
			wb = new XSSFWorkbook();
		}
		this.scheme = scheme;
		this.serverName = serverName;
	}
	
	/*
	 * Write a mailout list to an XLS file
	 */
	public void createXLSFile(OutputStream outputStream, ArrayList<MailoutPerson> mop, ResourceBundle localisation, 
			String tz) throws IOException {
		
		Sheet mailoutSheet = wb.createSheet("mailouts");
		Sheet settingsSheet = wb.createSheet("settings");
		mailoutSheet.createFreezePane(5, 1);	// Freeze header row and first 5 columns
		
		Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);

		ArrayList<Column> cols = getColumnList(localisation, styles);
		createHeader(cols, mailoutSheet);	
		processMailoutListForXLS(mop, mailoutSheet, settingsSheet, styles, cols, tz);
		
		wb.write(outputStream);
		outputStream.close();
	}
	
	/*
	 * Get the columns for the Mailout People sheet
	 */
	private ArrayList<Column> getColumnList(ResourceBundle localisation, Map<String, CellStyle> styles) {
		
		ArrayList<Column> cols = new ArrayList<Column> ();
		
		int colNumber = 0;
	
		cols.add(new Column(localisation, colNumber++, "email", false, styles.get("header_tasks")));
		cols.add(new Column(localisation, colNumber++, "name", false, styles.get("header_tasks")));
		cols.add(new Column(localisation, colNumber++, "status", false, styles.get("group")));	// Ignore on upload
		
		return cols;
	}
	
	
	/*
	 * Create a header row and set column widths
	 */
	private void createHeader(ArrayList<Column> cols, Sheet sheet) {
		// Set column widths
		for(int i = 0; i < cols.size(); i++) {
			sheet.setColumnWidth(i, cols.get(i).getWidth());
		}
				
		Row headerRow = sheet.createRow(0);
		for(int i = 0; i < cols.size(); i++) {
			Column col = cols.get(i);
			
            Cell cell = headerRow.createCell(i);
            cell.setCellStyle(col.style);
            cell.setCellValue(col.name);
        }
	}
	
	/*
	 * Convert a task list array to XLS
	 */
	private void processMailoutListForXLS(
			ArrayList<MailoutPerson> mop, 
			Sheet sheet,
			Sheet settingsSheet,
			Map<String, CellStyle> styles,
			ArrayList<Column> cols,
			String tz) throws IOException {
		
		DataFormat format = wb.createDataFormat();
		CellStyle styleTimestamp = wb.createCellStyle();
		
		styleTimestamp.setDataFormat(format.getFormat("yyyy-mm-dd h:mm"));	
		
		for(MailoutPerson person : mop)  {
			
			Row row = sheet.createRow(rowNumber++);
			for(int i = 0; i < cols.size(); i++) {
				Column col = cols.get(i);	
				Cell cell = row.createCell(i);
				cell.setCellValue(col.getValue(person));
				
	        }	
		}
		
		// Populate settings sheet
		Row settingsRow = settingsSheet.createRow(0);
		Cell k = null;
		Cell v = null;
		k = settingsRow.createCell(0);
		k.setCellValue("Time Zone:");
		v = settingsRow.createCell(1);
		v.setCellValue(tz);
	}

	/*
	 * Create a mailout list from an XLS file
	 */
	public ArrayList<MailoutPerson> getXLSMailoutList(String type, InputStream inputStream, ResourceBundle localisation, String tz) throws Exception {

		Sheet sheet = null;
		Sheet settingsSheet = null;
		Row row = null;
		int lastRowNum = 0;
		ArrayList<MailoutPerson> mailouts = new ArrayList<MailoutPerson> ();

		HashMap<String, Integer> header = null;

		if(type != null && type.equals("xls")) {
			wb = new HSSFWorkbook(inputStream);
		} else {
			wb = new XSSFWorkbook(inputStream);
		}

		/*
		 * Get the task sheet settings
		 */
		settingsSheet = wb.getSheet("settings");
		if(settingsSheet.getPhysicalNumberOfRows() > 0) {
			int lastSettingsRow = settingsSheet.getLastRowNum();
			for(int j = 0; j <= lastSettingsRow; j++) {
				row = settingsSheet.getRow(j);

				if(row != null) {         	
					int lastCellNum = row.getLastCellNum();
					if(lastCellNum > 0) {
						Cell c = row.getCell(0);
						String k = c.getStringCellValue();
						if(k != null && k.trim().toLowerCase().equals("time zone:")) {
							c = row.getCell(1);
							tz = c.getStringCellValue();
							break;
						}
					}
				}
			}
		}

		ZoneId timeZoneId = ZoneId.of(tz);
		ZoneId gmtZoneId = ZoneId.of("GMT");

		sheet = wb.getSheet("mailouts");
		if(sheet == null) {
			throw new ApplicationException("No worksheet called mailouts");
		}
		if(sheet.getPhysicalNumberOfRows() > 0) {

			lastRowNum = sheet.getLastRowNum();
			boolean needHeader = true;

			for(int j = 0; j <= lastRowNum; j++) {

				row = sheet.getRow(j);
				if(row != null) {

					int lastCellNum = row.getLastCellNum();

					if(needHeader) {
						header = getHeader(row, lastCellNum);
						needHeader = false;
					} else {
						String email = XLSUtilities.getColumn(row, "email", header, lastCellNum, null);
						String name = XLSUtilities.getColumn(row, "name", header, lastCellNum, null);

						mailouts.add(new MailoutPerson(email, name));
				

					}

				}

			}
		}

		return mailouts;


	}

	/*
	 * Get a hashmap of column name and column index
	 */
	private HashMap<String, Integer> getHeader(Row row, int lastCellNum) {
		HashMap<String, Integer> header = new HashMap<String, Integer> ();
		
		Cell cell = null;
		String name = null;
		
        for(int i = 0; i <= lastCellNum; i++) {
            cell = row.getCell(i);
            if(cell != null) {
                name = cell.getStringCellValue();
                if(name != null && name.trim().length() > 0) {
                	name = name.toLowerCase();
                    header.put(name, i);
                }
            }
        }
            
		return header;
	}
}
