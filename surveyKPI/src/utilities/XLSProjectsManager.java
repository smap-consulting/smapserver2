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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.model.Project;


public class XLSProjectsManager {
	
	private static Logger log =
			 Logger.getLogger(XLSProjectsManager.class.getName());
	
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
		public String getValue(Project project) {
			String value = null;
			
			if(name.equals("project_name")) {
				value = project.name;
			} else if(name.equals("desc")) {
				value = project.desc;
			} else if(name.equals("changed_by")) {
				value = project.changed_by;
			} else if(name.equals("changed_when")) {
				value = project.changed_ts;
			} 
			
			if(value == null) {
				value = "";
			}
			return value;
		}
	}

	public XLSProjectsManager() {

	}
	
	public XLSProjectsManager(String scheme, String serverName) {
		
		wb = new XSSFWorkbook();
		this.scheme = scheme;
		this.serverName = serverName;
	}
	
	/*
	 * Write a project list to an XLS file
	 */
	public void createXLSFile(OutputStream outputStream, ArrayList<Project> projects, 
			ResourceBundle localisation, String tz) throws IOException {
		
		Sheet projectSheet = wb.createSheet(localisation.getString("ar_project"));
		
		Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);

		ArrayList<Column> cols = getColumnList(localisation, styles);
		addInitialDataColumns(localisation, cols, projects, styles);
		createHeader(cols, projectSheet);	
		processProjectListForXLS(projects, projectSheet, styles, cols, tz);
		
		wb.write(outputStream);
		outputStream.close();
	}
	
	/*
	 * Get the columns for the Project Psheet
	 */
	private ArrayList<Column> getColumnList(ResourceBundle localisation, Map<String, CellStyle> styles) {
		
		ArrayList<Column> cols = new ArrayList<Column> ();
		
		int colNumber = 0;
	
		cols.add(new Column(localisation, colNumber++, "project_name", false, styles.get("header_tasks")));
		cols.add(new Column(localisation, colNumber++, "desc", false, styles.get("header_tasks")));
		cols.add(new Column(localisation, colNumber++, "changed_by", false, styles.get("group")));		// Ignore on upload
		cols.add(new Column(localisation, colNumber++, "changed_when", false, styles.get("group")));	// Ignore on upload
		
		return cols;
	}
	
	
	/*
	 * Create a header row and set column widths
	 */
	private void createHeader(
			ArrayList<Column> cols, 
			Sheet sheet) {
		
		// Set column widths
		for(int i = 0; i < cols.size(); i++) {
			sheet.setColumnWidth(i, cols.get(i).getWidth());
		}
		
		Row headerRow = sheet.createRow(0);
		int colIdx = 0;
		for(Column col : cols) {
			
            Cell cell = headerRow.createCell(colIdx++);
            cell.setCellStyle(col.style);
            cell.setCellValue(col.name);
        }
	}
	
	/*
	 * Add columns for initial data
	 */
	private void addInitialDataColumns(
			ResourceBundle localisation,
			ArrayList<Column> cols, 
			ArrayList<Project> projects,
			Map<String, CellStyle> styles) {
		
		HashMap<String, String> colsAdded = new HashMap<> ();
		
		for(Column col : cols) { 
            colsAdded.put(col.name, col.name);
        }
		
	}
	
	/*
	 * Convert a project list to XLS
	 */
	private void processProjectListForXLS(
			ArrayList<Project> projects, 
			Sheet sheet,
			Map<String, CellStyle> styles,
			ArrayList<Column> cols,
			String tz) throws IOException {
		
		DataFormat format = wb.createDataFormat();
		CellStyle styleTimestamp = wb.createCellStyle();
		
		styleTimestamp.setDataFormat(format.getFormat("yyyy-mm-dd h:mm"));	
		
		for(Project project : projects)  {
			
			Row row = sheet.createRow(rowNumber++);
			for(int i = 0; i < cols.size(); i++) {
				Column col = cols.get(i);	
				Cell cell = row.createCell(i);
				cell.setCellValue(col.getValue(project));
				
	        }	
		}
		
	}

	/*
	 * Create a project list from an XLS file
	 */
	public ArrayList<Project> getXLSProjectList(String type, InputStream inputStream, ResourceBundle localisation, String tz) throws Exception {

		Sheet sheet = null;
		Row row = null;
		int lastRowNum = 0;
		ArrayList<Project> projects = new ArrayList<Project> ();

		HashMap<String, Integer> projectDups = new HashMap<> ();
		HashMap<String, Integer> header = null;

		if(type != null && type.equals("xls")) {
			wb = new HSSFWorkbook(inputStream);
		} else {
			wb = new XSSFWorkbook(inputStream);
		}

		sheet = wb.getSheetAt(0);
		if(sheet == null) {
			throw new ApplicationException(localisation.getString("fup_nws"));
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
						String name = XLSUtilities.getColumn(row, "project_name", header, lastCellNum, null);
						String desc = XLSUtilities.getColumn(row, "desc", header, lastCellNum, null);

						// validate project name
						if(name == null || name.trim().length() == 0) {
							String msg = localisation.getString("fup_pnm");
							msg = msg.replace("%s1", String.valueOf(j));
							throw new ApplicationException(msg);
						}
						
						// Validate duplicate project names
						Integer firstRow = projectDups.get(name.toLowerCase());
						if(firstRow != null) {
							String msg = localisation.getString("fup_dpn");
							msg = msg.replace("%s1", name);
							msg = msg.replace("%s2", String.valueOf(j));
							msg = msg.replace("%s3", String.valueOf(firstRow));
							throw new ApplicationException(msg);
						} else {
							projectDups.put(name.toLowerCase(), j);
						}
						
						Project p = new Project();
						p.name = name;
						p.desc = desc;
						projects.add(p);
					}
				}

			}
		}

		return projects;


	}

	/*
	 * Get a hashmap of column name and column index
	 */
	private HashMap<String, Integer> getHeader(Row row, int lastCellNum) {
		HashMap<String, Integer> header = new HashMap<> ();
		
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
