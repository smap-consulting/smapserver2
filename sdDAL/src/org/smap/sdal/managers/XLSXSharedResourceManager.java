package org.smap.sdal.managers;

import java.io.File;
import java.io.FileWriter;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.model.Instance;
import org.smap.sdal.model.MailoutPerson;

/*
 * Manage the loading of spreadsheets as shared resource files
 */
public class XLSXSharedResourceManager {
	
	Workbook wb = null;
	int rowNumber = 1;		// Heading row is 0
	
	
	public XLSXSharedResourceManager() {

	}
	
	/*
	 * Write a CSV file from an XLS file
	 */
	public void writeToCSV(String type, InputStream inputStream, File csvFile, ResourceBundle localisation, String tz) throws Exception {

		Sheet sheet = null;
		Row row = null;
		int lastRowNum = 0;


		HashMap<String, Integer> headerMap = null;
		ArrayList<String> headerList = new ArrayList<> ();		// Initial data columns

		if(type != null && type.equals("xls")) {
			wb = new HSSFWorkbook(inputStream);
		} else {
			wb = new XSSFWorkbook(inputStream);
		}

		sheet = wb.getSheetAt(0);	// Get the first sheet
		if(sheet == null) {
			throw new ApplicationException(localisation.getString("fup_nws"));
		}
		
		FileWriter fw = null;
		try {
			fw = new FileWriter(csvFile);		// Get a file writer to write to the CSV file
			
			if(sheet.getPhysicalNumberOfRows() > 0) {
	
				lastRowNum = sheet.getLastRowNum();
				boolean needHeader = true;
	
				for(int j = 0; j <= lastRowNum; j++) {
	
					row = sheet.getRow(j);
					if(row != null) {
	
						int lastCellNum = row.getLastCellNum();
	
						if(needHeader) {
							headerMap = getHeaderMap(row, lastCellNum);
							headerList = getHeaderList(row, lastCellNum);
							boolean first = true;
							for(String colname : headerList) {
								if(!first) {
									fw.write(",");
								}
								fw.write(colname);
								first = false;
							}
							needHeader = false;
						} else {
							fw.write("\r");	// New line
							boolean first = true;
							for(String colname : headerList) {
								String value = XLSUtilities.getColumn(row, colname, headerMap, lastCellNum, null);
								if(!first) {
									fw.write(",");
								}
								if(value != null && value.trim().length() > 0) {
									fw.write(value);
								}
								first = false;
							}
						}
					}
	
				}
			}
		} finally {
			if(fw != null) {try{fw.close();} catch(Exception e) {}}
		}

	}

	/*
	 * Get a hashmap of column name and column index
	 */
	private HashMap<String, Integer> getHeaderMap(Row row, int lastCellNum) {
		HashMap<String, Integer> header = new HashMap<> ();
		
		Cell cell = null;
		String name = null;
		
        for(int i = 0; i <= lastCellNum; i++) {
            cell = row.getCell(i);
            if(cell != null) {
                name = cell.getStringCellValue();
                if(name != null && name.trim().length() > 0) {
            		header.put(name, i);
                }
            }
        }
            
		return header;
	}
	
	/*
	 * Get an array list of initial data columns
	 */
	private ArrayList<String> getHeaderList(Row row, int lastCellNum) {
		
		ArrayList<String> idx = new ArrayList<> ();
		
		Cell cell = null;
		String name = null;
		
        for(int i = 0; i <= lastCellNum; i++) {
            cell = row.getCell(i);
            if(cell != null) {
                name = cell.getStringCellValue();
                if(name != null && name.trim().length() > 0) {
                	name = name.trim();
                	idx.add(name);
                }
            }
        }
            
		return idx;
	}
}
