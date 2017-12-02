package utilities;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Level;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.smap.sdal.Utilities.ApplicationException;

public class XLSUtilities {

    /**
     * create a library of cell styles
     */
    public static Map<String, CellStyle> createStyles(Workbook wb){
        
    	Map<String, CellStyle> styles = new HashMap<String, CellStyle>();
		DataFormat format = wb.createDataFormat();

    	/*
    	 * Create fonts
    	 */
        Font largeFont = wb.createFont();
        largeFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
        largeFont.setFontHeightInPoints((short) 14);
        
        Font boldFont = wb.createFont();
        boldFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
        
		Font linkFont = wb.createFont();
		linkFont.setUnderline(Font.U_SINGLE);
	    linkFont.setColor(IndexedColors.BLUE.getIndex());
        
        /*
         * Create styles
         * Styles for XLS Form
         */
        CellStyle style = wb.createCellStyle();
        style.setFont(boldFont);
        styles.put("header", style);
        
        style = wb.createCellStyle();
        style.setFont(boldFont);
        style.setFillForegroundColor(IndexedColors.CORNFLOWER_BLUE.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        styles.put("header2", style);

        style = wb.createCellStyle();
        style.setWrapText(true);
        styles.put("label", style);
        
        style = wb.createCellStyle();
        style.setFillForegroundColor(IndexedColors.CORNFLOWER_BLUE.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        styles.put("begin repeat", style);
        
        style = wb.createCellStyle();
        style.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        styles.put("begin group", style);
        
        style = wb.createCellStyle();
        style.setFillForegroundColor(IndexedColors.LIGHT_GREEN.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        styles.put("is_required", style);
        
        style = wb.createCellStyle();
        style.setFillForegroundColor(IndexedColors.CORAL.getIndex());
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        styles.put("not_required", style);

        
        /*
         * Other styles
         */
        style = wb.createCellStyle();
        style.setWrapText(true);
        styles.put("default", style);
        
		style = wb.createCellStyle();
		style.setDataFormat(format.getFormat("yyyy-mm-dd h:mm"));	
		styles.put("datetime", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(format.getFormat("yyyy-mm-dd"));	
		styles.put("date", style);
		
		style = wb.createCellStyle();
		style.setDataFormat(HSSFDataFormat.getBuiltinFormat("0.00"));
		styles.put("numeric", style);
		
		style = wb.createCellStyle();
	    style.setFont(linkFont);
	    styles.put("link", style);
        
        /*
         * LQAS Styles
         */
        style = wb.createCellStyle();
        style.setFont(largeFont);
        style.setAlignment(CellStyle.ALIGN_LEFT);
        styles.put("title", style);

        style = wb.createCellStyle();	
        style.setWrapText(true);
        style.setAlignment(CellStyle.ALIGN_LEFT);
        styles.put("no_border", style);
        
        // Remaining styles are all derived from a common base style
        style = getBaseStyle(wb);
        style = wb.createCellStyle();
        style.setFillForegroundColor(HSSFColor.GREY_25_PERCENT.index);
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        style.setFont(boldFont);
        styles.put("group", style);
        
        style = getBaseStyle(wb);
        style.setAlignment(CellStyle.ALIGN_CENTER);
        styles.put("data", style);
        
        style = getBaseStyle(wb);
        style.setFillForegroundColor(HSSFColor.LIGHT_YELLOW.index);
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        style.setAlignment(CellStyle.ALIGN_CENTER);
        styles.put("source", style);
        
        style = getBaseStyle(wb);
        style.setFillForegroundColor(HSSFColor.LAVENDER.index);
        style.setFillPattern(CellStyle.SOLID_FOREGROUND);
        style.setAlignment(CellStyle.ALIGN_CENTER);
        styles.put("raw_source", style);
        
        style = getBaseStyle(wb);
        style.setAlignment(CellStyle.ALIGN_CENTER);
        style.setFont(boldFont);  
        styles.put("data_header", style);

        return styles;
    }
    
    private static CellStyle getBaseStyle(Workbook wb) {
    	
    	CellStyle style = wb.createCellStyle();
    	
        style.setWrapText(true);
        style.setAlignment(CellStyle.ALIGN_LEFT);
        style.setBorderBottom(HSSFCellStyle.BORDER_THIN);
        style.setBorderTop(HSSFCellStyle.BORDER_THIN);
        style.setBorderRight(HSSFCellStyle.BORDER_THIN);
        style.setBorderLeft(HSSFCellStyle.BORDER_THIN);
        
        return style;
    }
    
	/*
	 * Merge multiple choice values into a single value
	 */
	public static String updateMultipleChoiceValue(String dbValue, String choiceName, String currentValue) {
		boolean isSet = false;
		String newValue = currentValue;
		
		if(dbValue.equals("1")) {
			isSet = true;
		}
		
		if(isSet) {
			if(currentValue == null) {
				newValue = choiceName;
			} else {
				newValue += " " + choiceName;
			}
		}
		
		return newValue;
	}
	
	/*
	 * Get a hashmap of column name and column index
	 */
	public static HashMap<String, Integer> getHeader(Row row) {
		HashMap<String, Integer> header = new HashMap<String, Integer> ();
		
		int lastCellNum = row.getLastCellNum();
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
	
	/*
	 * Get the value of a cell at the specified column
	 */
	public static String getColumn(Row row, String name, HashMap<String, Integer> header, int lastCellNum, String def) throws ApplicationException {
		
		Integer cellIndex;
		int idx;
		String value = null;
	
		cellIndex = header.get(name);
		if(cellIndex != null) {
			idx = cellIndex;
			if(idx <= lastCellNum) {
				Cell c = row.getCell(idx);
				if(c != null) {
					value = getCellValue(c);
					

				}
			}
		} 

		if(value == null) {		// Set to default value if null
			value = def;
		}
		
		return value;
	}
	
	/*
	 * Get the text value of a cell and return null if the cell is empty
	 */
	public static String getTextColumn(Row row, String name, HashMap<String, Integer> header, int lastCellNum) throws ApplicationException {
		
		String value = null;
		Integer cellIndex;
		int idx;
	
		cellIndex = header.get(name);
		if(cellIndex != null && cellIndex < lastCellNum) {
			idx = cellIndex;
			Cell c = row.getCell(idx);
			if(c != null) {
				value = getCellValue(c);
				if(value != null) {
					value = value.trim();  	// Remove trailing whitespace, its not visible to users
					if(value.length() == 0) {
						value = null;
					}
				}
			}
		} 
		
		return value;
	}
	
	/*
	 * Get a cell value as String from XLS
	 */
	private static String getCellValue(Cell c) throws ApplicationException {
		String value = null;
		double dValue = 0.0;
		Date dateValue = null;
		boolean bValue = false;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm");
		
		if(c.getCellType() == Cell.CELL_TYPE_NUMERIC) {
			if (HSSFDateUtil.isCellDateFormatted(c)) {
				dateValue = c.getDateCellValue();
				value = dateFormat.format(dateValue);
			} else {
				dValue = c.getNumericCellValue();
				value = String.valueOf(dValue);
				if(value != null && value.endsWith(".0")) {
					value = value.substring(0, value.lastIndexOf('.'));
				}
			}
		} else if(c.getCellType() == Cell.CELL_TYPE_STRING) {
			value = c.getStringCellValue();
		} else if(c.getCellType() == Cell.CELL_TYPE_BOOLEAN) {
			bValue = c.getBooleanCellValue();
			value = String.valueOf(bValue);
		} else if(c.getCellType() == Cell.CELL_TYPE_BLANK) {
			value = c.getStringCellValue();	// !! Not sure what this is about
		} else {
			throw(new ApplicationException("Error: Unknown cell type: " + c.getCellType() + 
					" in sheet "  + c.getSheet().getSheetName() +
					" in row " + c.getRowIndex() + 
					", column " + c.getColumnIndex()));
		}
		
		return value;
	}
	
	public static ApplicationException getApplicationException(ResourceBundle localisation, String code, int row, String sheet, String param1, String param2) {
		
		StringBuffer buf = new StringBuffer(localisation.getString("tu_rn")).append(" ").append(localisation.getString(code));

		String msg = buf.toString();
		msg = msg.replace("%sheet", sheet);
		msg = msg.replaceAll("%row", String.valueOf(row));
		
		if(param1 != null) {
			msg = msg.replace("%s1", param1);
		}
		msg = msg.replace("%s2", sheet);
		if(param2 != null) {
			msg = msg.replace("%s3", param2);
		}
		
		return new ApplicationException(msg);
	}
}
