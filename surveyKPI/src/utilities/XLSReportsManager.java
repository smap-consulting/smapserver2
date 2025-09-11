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
import java.io.OutputStream;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.model.ChartColumn;
import org.smap.sdal.model.ChartData;
import org.smap.sdal.model.ChartRow;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.TableColumn;

import org.smap.sdal.model.SurveyViewDefn;


/*
 * Manage exporting of data posted from a data table
 */

public class XLSReportsManager {
	
	private static Logger log =
			 Logger.getLogger(XLSReportsManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	Workbook wb = null;
	boolean isXLSX = false;
	int rowNumber = 1;		// Heading row is 0
	ResourceBundle localisation = null;
	
	private class Column {
		String name;
		String humanName;
		int dataIndex;
		int colIndex;
		String type;
		
		public Column(int dataIndex, String name, String humanName, String type, int colIndex) {
			this.dataIndex = dataIndex;
			this.colIndex = colIndex;
			this.name = name;
			this.humanName = humanName;		// Need to work out how to use translations when the file needs to be imported again
			this.type = type;
		}
		
		// Return the width of this column
		public int getWidth() {
			int width = 256 * 20;		// 20 characters is default
			return width;
		}
	}

	public XLSReportsManager() {

	}
	
	public XLSReportsManager(String type) {
		if(type != null && type.equals("xls")) {
			wb = new HSSFWorkbook();
			isXLSX = false;
		} else {
			wb = new XSSFWorkbook();
			isXLSX = true;
		}
	}
	
	/*
	 * Write data from the dashboard to an XLS file
	 */
	public void createXLSReportsFile(OutputStream outputStream, 
			ArrayList<ArrayList<KeyValue>> dArray, 
			//ArrayList<ChartData> chartDataArray,
			ArrayList<KeyValue> settings,
			SurveyViewDefn mfc,
			String surveyName,
			String formName,
			ResourceBundle l, 
			String tz) throws IOException {
		
		this.localisation = l;
		Sheet dataSheet = wb.createSheet(localisation.getString("rep_data"));
		Sheet taskSettingsSheet = wb.createSheet(localisation.getString("rep_settings"));
		
		Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);

		ArrayList<Column> cols = getColumnList(mfc, dArray);
		
		createHeader(cols, dataSheet, styles);	
		processDataListForXLS(dArray, dataSheet, taskSettingsSheet, styles, cols, tz, settings, surveyName, formName);
		
		wb.write(outputStream);
		outputStream.close();
	}
	
	
	/*
	 * Get the columns for the data sheet
	 */
	private ArrayList<Column> getColumnList(SurveyViewDefn mfc, 
			ArrayList<ArrayList<KeyValue>> dArray) {
		
		ArrayList<Column> cols = new ArrayList<Column> ();
		ArrayList<KeyValue> record = null;
		
		if(dArray.size() > 0) {
			 record = dArray.get(0);
		}
		
		int colIndex = 0;
		for(int i = 0; i < mfc.columns.size(); i++) {
			TableColumn tc = mfc.columns.get(i);
			if(!tc.hide && tc.include) {
				int dataIndex = -1;
				if(record != null) {
					dataIndex = getDataIndex(record, tc.displayName);
				}
				cols.add(new Column(dataIndex, tc.column_name, tc.displayName, tc.type, colIndex++));
			}
		}
	
		
		return cols;
	}
	
	/*
	 * Get the index into the data set for a column
	 */
	private int getDataIndex(ArrayList<KeyValue> record, String name) {
		int idx = -1;
		
		for(int i = 0; i < record.size(); i++) {
			if(record.get(i).k.equals(name)) {
				idx = i;
				break;
			}
		}
		return idx;
	}
    
	/*
	 * Create a header row and set column widths
	 */
	private void createHeader(ArrayList<Column> cols, Sheet sheet, Map<String, CellStyle> styles) {
		
		// Set column widths
		for(int i = 0; i < cols.size(); i++) {
			sheet.setColumnWidth(i, cols.get(i).getWidth());
		}
				
		Row headerRow = sheet.createRow(0);
		CellStyle headerStyle = styles.get("header");
		for(int i = 0; i < cols.size(); i++) {
			Column col = cols.get(i);
			
            Cell cell = headerRow.createCell(i);
            cell.setCellStyle(headerStyle);
            cell.setCellValue(col.humanName);
        }
	}
	
	/*
	 * Convert a data sheet for xls export
	 */
	private void processDataListForXLS(
			ArrayList<ArrayList<KeyValue>> dArray, 
			Sheet sheet,
			Sheet settingsSheet,
			Map<String, CellStyle> styles,
			ArrayList<Column> cols,
			String tz,
			ArrayList<KeyValue> settings,
			String surveyName,
			String formName) throws IOException {
		
		CreationHelper createHelper = wb.getCreationHelper();
		
		for(int index = 0; index < dArray.size(); index++) {

			Row row = sheet.createRow(rowNumber++);
			ArrayList<KeyValue> record = dArray.get(index);
			for(Column col : cols) {
				Cell cell = row.createCell(col.colIndex);
				String value = "error";
				if(col.dataIndex >= 0) {
					value = record.get(col.dataIndex).v;
				}

				cell.setCellStyle(styles.get("default"));	

				if(value != null && (value.startsWith("https://") || value.startsWith("http://"))) {
					cell.setCellStyle(styles.get("link"));
					Hyperlink url = createHelper.createHyperlink(HyperlinkType.URL);
					url.setAddress(value);
					cell.setHyperlink(url);
				}

				boolean cellWritten = false;
				if(col.type.equals("datetime")) {
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					try {
						java.util.Date date = dateFormat.parse(value);
						cell.setCellStyle(styles.get("datetime"));
						cell.setCellValue(date);
						cellWritten = true;
					} catch (Exception e) {
						// Ignore
					}
				} else if(col.type.equals("date")) {
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
					try {
						java.util.Date date = dateFormat.parse(value);
						cell.setCellStyle(styles.get("date"));
						cell.setCellValue(date);
						cellWritten = true;
					} catch (Exception e) {
						// Ignore
					}
				} 

				if(!cellWritten) {

					// Try to write as number by default
					try {
						double vDouble = Double.parseDouble(value);

						cell.setCellStyle(styles.get("default"));
						cell.setCellValue(vDouble);
						cellWritten = true;
					} catch (Exception e) {
						// Ignore
					}

				}

				if(!cellWritten) {
					cell.setCellStyle(styles.get("default"));
					if(value != null) {
						value = GeneralUtilityMethods.unesc(value);
					}
					cell.setCellValue(value);
				}

			}	
		}
		
		// Populate settings sheet
		int settingsRowIdx = 0;
		Row settingsRow = settingsSheet.createRow(settingsRowIdx++);
		Cell k = settingsRow.createCell(0);
		Cell v = settingsRow.createCell(1);
		k.setCellStyle(styles.get("header"));	
		k.setCellValue(localisation.getString("a_tz"));
		v.setCellValue(tz);
		
		settingsRow = settingsSheet.createRow(settingsRowIdx++);
		k = settingsRow.createCell(0);
		v = settingsRow.createCell(1);
		k.setCellStyle(styles.get("header"));
		k.setCellValue(localisation.getString("ar_survey"));
		v.setCellValue(surveyName);
		
		if(formName != null) {
			settingsRow = settingsSheet.createRow(settingsRowIdx++);
			k = settingsRow.createCell(0);
			v = settingsRow.createCell(1);
			k.setCellStyle(styles.get("header"));
			k.setCellValue(localisation.getString("form"));
			v.setCellValue(formName);
		}
		
		// Show filter settings
		settingsRowIdx++;
		settingsRow = settingsSheet.createRow(settingsRowIdx++);
		Cell f = settingsRow.createCell(0);
		f.setCellStyle(styles.get("header2"));	
		f.setCellValue(localisation.getString("filters"));
		
		if(settings != null) {
			for(KeyValue kv : settings) {
				settingsRow = settingsSheet.createRow(settingsRowIdx++);
				k = settingsRow.createCell(1);
				v = settingsRow.createCell(2);
				k.setCellStyle(styles.get("header"));	
				k.setCellValue(kv.k);
				v.setCellValue(kv.v);
			}
		}
	}

}
