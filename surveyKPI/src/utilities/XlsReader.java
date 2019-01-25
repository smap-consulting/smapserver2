package utilities;

import java.io.InputStream;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTSheet;

public class XlsReader {

	XSSFWorkbook wb = null;
	Sheet sheet;
	
	int rowNum = -1;
	int lastRowNum = -1;
	
	
	public XlsReader(InputStream is, String formName) throws Exception {
		
		final String sheetName = "d_" + formName;
		
		wb = new XSSFWorkbook(is) {
			 /** Avoid DOM parse of sheets we are not interested in */
            @Override
            public void parseSheet(java.util.Map<String,XSSFSheet> shIdMap, CTSheet ctSheet) {
                if (sheetName.equals(ctSheet.getName())) {
                    super.parseSheet(shIdMap, ctSheet);
                }
            }
		};
		sheet = wb.getSheet("d_" + formName);
		lastRowNum = sheet.getLastRowNum();
	}
	
	public String [] readNext(boolean header) {
		ArrayList<String> values = null;
		
		String value;
		Cell cell = null;
		boolean isNullRow = true;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		DataFormatter df = new DataFormatter();
		while(isNullRow) {
			values = new ArrayList<String> ();
			
			if(rowNum++ >= lastRowNum) {
				return null;
			}
			Row row = sheet.getRow(rowNum);
				
			for(int i = 0; i <= row.getLastCellNum(); i++) {
	            cell = row.getCell(i);
	            
	            if(cell != null) {
	            		
	            		value = df.formatCellValue(cell);	// Default
	            		
	            		if(!header) {
		            		switch (cell.getCellType()) {
		            		case XSSFCell.CELL_TYPE_NUMERIC:
		            			if(DateUtil.isCellDateFormatted(cell)) {
		            				try {
			            				Date dv = cell.getDateCellValue();
			    		                value = sdf.format(dv);
		            				} catch (Exception e) {
		            					
		            				}
		            			} 
		            			break;
		            		default:
		            			break;
		            		}
	            		}
	            		
	
	            } else {
	            		value = "";
	            }
	            if(value != null && value.trim().length() > 0) {
	            		isNullRow = false;
	            }
	            values.add(value);
	        }
		}
		
		String[] vArray = new String[values.size()];
		
		return values.toArray(vArray);
	}
	
	public void close() throws Exception {
		wb.close();
	}
}
