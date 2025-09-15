package org.smap.sdal.managers;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xddf.usermodel.PresetColor;
import org.apache.poi.xddf.usermodel.chart.AxisPosition;
import org.apache.poi.xddf.usermodel.chart.BarDirection;
import org.apache.poi.xddf.usermodel.chart.BarGrouping;
import org.apache.poi.xddf.usermodel.chart.ChartTypes;
import org.apache.poi.xddf.usermodel.chart.LegendPosition;
import org.apache.poi.xddf.usermodel.chart.XDDFBarChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFCategoryAxis;
import org.apache.poi.xddf.usermodel.chart.XDDFChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFChartLegend;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSourcesFactory;
import org.apache.poi.xddf.usermodel.chart.XDDFValueAxis;
import org.apache.poi.xssf.usermodel.XSSFChart;
import org.apache.poi.xssf.usermodel.XSSFDrawing;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.model.CustomDailyReportsConfig;
import org.smap.sdal.model.CustomReportColumn;
import org.smap.sdal.model.CustomReportMultiColumn;
import org.smap.sdal.model.Form;

public class CustomDailyReportsManager {

	private static Logger log =
			Logger.getLogger(CustomDailyReportsManager.class.getName());

	LogManager lm = new LogManager();		// Application log
	
	private ResourceBundle localisation = null;
	
	private class ChartItem {
		ArrayList<Integer> bars = new ArrayList<> ();
	}
	private ArrayList<Row> chartDataRows = new ArrayList<> ();

	public CustomDailyReportsManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
	}

	public void getDailyReport(
			Connection sd, 
			Connection cResults, 
			HttpServletResponse response,
			String filename,
			CustomDailyReportsConfig config, 
			int year, 
			int month) throws SQLException, ApplicationException {

		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		XSSFWorkbook wb = null;
		XSSFSheet sheet = null;
		
		int rowNumber = 0;
		CellStyle errorStyle = null;
		CustomConfigManager cm = new CustomConfigManager(localisation);
		
		try {
			wb = new XSSFWorkbook();
			Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);
			CellStyle headerStyle = styles.get("header");
			errorStyle = styles.get("error");					
			sheet = wb.createSheet();
			
			int sId = GeneralUtilityMethods.getSurveyId(sd, config.sIdent);
			Form tlf = GeneralUtilityMethods.getTopLevelForm(sd, sId);
			String surveyName = GeneralUtilityMethods.getSurveyName(sd, sId);
	
			GeneralUtilityMethods.setFilenameInResponse(filename + "." + "xlsx", response); // Set file name
			
			/*
			 * Validate report configuration
			 * This is required for security as much as error reporting
			 */
			cm.validateColumn(cResults, tlf.tableName, config.dateColumn, surveyName);
			boolean dateColumnIncluded = false;
			for(CustomReportColumn rc : config.columns) {
				cm.validateColumn(cResults, tlf.tableName, rc.column, surveyName);
				if(config.dateColumn.equals(rc.column)) {
					dateColumnIncluded = true;
				}
			}
			if(!dateColumnIncluded) {
				config.columns.add(new CustomReportColumn(config.dateColumn, config.dateColumn, 0, false));
			}
			for(CustomReportMultiColumn bar : config.bars) {
				for(String name : bar.columns) {
					cm.validateColumn(cResults, tlf.tableName, name, surveyName);
				}
			}
			
			/*
			 * Create query
			 */
			StringBuilder sb = new StringBuilder("select ");
			sb.append("extract(day from ").append(config.dateColumn).append(") as _day");
			for(CustomReportColumn rc : config.columns) {
				sb.append(",")
					.append(rc.column);
			}
	
			for(CustomReportMultiColumn bar : config.bars) {
				sb.append(",");
				int idx = 0;
				for(String name : bar.columns) {
					if(idx++ > 0) {
						sb.append(" + ");
					}
					sb.append(name);
				}
				sb.append(" as ").append(bar.name);
			}
	
			sb.append(" from ").append(tlf.tableName);
			
			sb.append(" where extract(month from ").append(config.dateColumn).append(") = ? ");
			sb.append(" and extract(year from ").append(config.dateColumn).append(") = ? ");
			
			// page the results to reduce memory usage
			pstmt = cResults.prepareStatement(sb.toString());
			cResults.setAutoCommit(false);		
			pstmt.setFetchSize(100);	
			
			pstmt.setInt(1,  month);
			pstmt.setInt(2,  year);
			log.info("Get daily report data: " + pstmt.toString());
			rs = pstmt.executeQuery();
		
			/*
			 * Write the header
			 */
			int colNumber = 0;
			Row headerRow = sheet.createRow(rowNumber++);	
			for (CustomReportColumn rc : config.columns) {
				Cell cell = headerRow.createCell(colNumber++);
				cell.setCellStyle(headerStyle);
				cell.setCellValue(rc.heading);
				
				if(rc.width > 0) {
					sheet.setColumnWidth(colNumber - 1, rc.width);
				}
			}
			
			/*
			 * Write the data rows and accumulate the chart data
			 */
			HashMap<Integer, ChartItem> chartItems = new HashMap<>();
			while(rs.next()) {
				colNumber = 0;
				Row row = sheet.createRow(rowNumber++);	
				for (CustomReportColumn rc : config.columns) {
					Cell cell = row.createCell(colNumber++);
					cell.setCellValue(rs.getString(rc.column));
				}
				int day = rs.getInt("_day");
				ChartItem item = chartItems.get(day);
				if(item == null) {
					item = new ChartItem();
					chartItems.put(day,  item);
					
					for(CustomReportMultiColumn rmc : config.bars) {
						item.bars.add(0);
					}
				}

				int i = 0;
				ArrayList<Integer> updates = new ArrayList<> ();
				for(CustomReportMultiColumn rmc : config.bars) {
					int ib = item.bars.get(i++);
					ib += rs.getInt(rmc.name);
					updates.add(ib);
				}			
				item.bars=updates;
			}
			
			/*
			 * Write the chart data
			 */
			rowNumber++;
			int chartDataRow = rowNumber;
			colNumber = 0;
			for(int j = 1; j <= 31; j++) {
				ChartItem item = chartItems.get(j);
				if(item != null) {
					Row row = getChartRow(chartDataRows, sheet, chartDataRow, 0);
					Cell cell = row.createCell(colNumber + 1);
					cell.setCellValue(j);
					
					if(colNumber == 0) {
						cell = row.createCell(colNumber);
						cell.setCellValue(localisation.getString("c_date"));
					}
					
					for(int i = 0; i < item.bars.size(); i++) {
						row = getChartRow(chartDataRows, sheet, chartDataRow, i + 1);
						cell = row.createCell(colNumber + 1);
						cell.setCellValue(item.bars.get(i));
						
						if(colNumber == 0) {
							cell = row.createCell(colNumber);
							cell.setCellValue(config.bars.get(i).title);
						}
					}
					colNumber++;
				}
			}
			rowNumber += 1 + config.bars.size();
			
			/*
			 * Create the chart
			 */
			if(chartItems.size() > 0) {
				int startRow = rowNumber++;
				int endRow = startRow + 10;
				rowNumber = endRow + 1;
				int endCol = chartItems.size() + 5;
				XSSFDrawing drawing = sheet.createDrawingPatriarch();
				ClientAnchor anchor = drawing.createAnchor(0, 0, 0, 0, 0, startRow, endCol, endRow);
				XSSFChart chart = drawing.createChart(anchor);
				
				chart.setTitleText("Chart Title");
				chart.setTitleOverlay(false);			
				XDDFChartLegend legend = chart.getOrAddLegend();
				legend.setPosition(LegendPosition.TOP_RIGHT);
				
				XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
				bottomAxis.setTitle("Date");
				XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
				leftAxis.setTitle("Count");
			 		
				XDDFDataSource<String> xs = XDDFDataSourcesFactory.fromStringCellRange(sheet,
						new CellRangeAddress(chartDataRow, chartDataRow, 1, chartItems.size()));
	
				XDDFBarChartData bar = (XDDFBarChartData) chart.createData(ChartTypes.BAR, bottomAxis, leftAxis);
				for(int i = 0; i < config.bars.size(); i++) {
					CustomReportMultiColumn rmc = config.bars.get(i);
					XDDFChartData.Series series = bar.addSeries(xs,XDDFDataSourcesFactory.fromNumericCellRange(sheet,
							new CellRangeAddress(chartDataRow + 1 + i, chartDataRow + 1 + i, 1, chartItems.size())));
					series.setTitle(rmc.title, null); 
				}
				chart.plot(bar);
				
				// correcting the overlap so bars really are stacked and not side by side
				// From: https://www.roytuts.com/how-to-generate-stacked-bar-chart-or-column-chart-in-excel-using-apache-poi/
				chart.getCTChart().getPlotArea().getBarChartArray(0).addNewOverlap().setVal((byte) 100);
							
				// Add the bars
				
				bar.setBarDirection(BarDirection.COL);
		        bar.setBarGrouping(BarGrouping.STACKED);
		        
	            ArrayList<PresetColor> colors = new ArrayList<PresetColor> ();
	            colors.add(PresetColor.CHARTREUSE);
	            colors.add(PresetColor.TURQUOISE);
	            colors.add(PresetColor.CORNFLOWER_BLUE);
	            colors.add(PresetColor.DARK_GREEN);
	            
	            for(int i = 0, col = 0; i < config.bars.size(); i++, col++) {
	            	if(col >= colors.size()) {
	            		col = 0;
	            	}
		            //solidFillSeries(bar, i, colors.get(col));
	            }
			}
            
			
			cResults.setAutoCommit(true);		// End paging
			
		} catch(Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			try {cResults.setAutoCommit(true);} catch(Exception ex) {};
			
			String msg = e.getMessage();
			if(msg.contains("does not exist")) {
				msg = localisation.getString("msg_no_data");
			}
			Row dataRow = sheet.createRow(rowNumber++);	
			Cell cell = dataRow.createCell(0);
			cell.setCellStyle(errorStyle);
			cell.setCellValue(msg);
			
		} finally {
			
			try {
				OutputStream outputStream = response.getOutputStream();
				wb.write(outputStream);
				wb.close();
				outputStream.close();
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Error", ex);
			}
			
			if(rs != null) {try {rs.close();} catch(Exception ex) {}} 
			if(pstmt != null) {try {pstmt.close();} catch(Exception ex) {}} 
		}

	}
	
	private Row getChartRow(ArrayList<Row> chartDataRows, Sheet sheet, int baseRowNumber, int idx) {
		Row row = null;
		if(chartDataRows.size() > idx) {
			row = chartDataRows.get(idx);
		} else {
			row = sheet.createRow(baseRowNumber + idx);	
			chartDataRows.add(row);
		}
		return row;
	}

}
