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

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.ReportConfig;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.TableColumn;
import org.smap.sdal.model.TableColumnMarkup;
import org.smap.sdal.model.Role;

public class XLSCustomReportsManager {
	
	private static Logger log =
			 Logger.getLogger(SurveyInfo.class.getName());
	
	Workbook wb = null;
	int rowNumber = 1;		// Heading row is 0
	
	ArrayList<KeyValue> markup = new ArrayList<KeyValue> ();

	public XLSCustomReportsManager() {
		// Add list of markup mappings between appearance values and bootstrap classes
		markup.add(new KeyValue("red", "bg-danger", false));
		markup.add(new KeyValue("green", "bg-success", false));
		markup.add(new KeyValue("blue", "bg-info", false));
		markup.add(new KeyValue("yellow", "bg-warning", false));
	}
	
	/*
	 * Create an oversight form definition from an XLS file
	 */
	public ReportConfig getOversightDefinition(Connection sd, int oId, String type, 
			InputStream inputStream, ResourceBundle localisation, boolean isSecurityManager) throws Exception {
		
		ReportConfig config = new ReportConfig();
		config.columns = new ArrayList<TableColumn> ();
		Sheet sheet = null;
		Sheet settingsSheet = null;
        Row row = null;
        int lastRowNum = 0;
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
		if(settingsSheet != null && settingsSheet.getPhysicalNumberOfRows() > 0) {
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
                    		break;
                    	}
                    }
                }
			}
		}
		
		sheet = wb.getSheet("definition");
		if(sheet == null) {
			if(wb.getNumberOfSheets() == 1) {
				sheet = wb.getSheetAt(0);
			} else {
				throw new Exception("A worksheet called 'definition' not found");
			}
		}
		if(sheet.getPhysicalNumberOfRows() > 0) {

			lastRowNum = sheet.getLastRowNum();
			boolean needHeader = true;
			boolean foundData = false;
			TableColumn currentCol = null;
			boolean processingConditions = false;
			PreparedStatement pstmtGetRoleId = null;
			String sqlGetRoleId = "select id from role where o_id = ? and name = ?";

			// Get ready to process roles
			pstmtGetRoleId = sd.prepareStatement(sqlGetRoleId);
			pstmtGetRoleId.setInt(1,oId);

			for(int j = 0; j <= lastRowNum; j++) {

				row = sheet.getRow(j);

				if(row != null) {

					int lastCellNum = row.getLastCellNum();

					if(needHeader) {
						header = XLSUtilities.getHeader(row, localisation, j, "definition");
						needHeader = false;
					} else {
						String rowType = XLSUtilities.getColumn(row, "row type", header, lastCellNum, null);

						if(rowType != null && rowType.trim().length() > 0) {
							foundData = true;
							rowType = rowType.trim().toLowerCase();

							// Close of any condition type calculations
							if(processingConditions && !rowType.equals("condition")) {
								processingConditions = false;
								currentCol.calculation.add("END");
							}

							// Process the row
							if(rowType.equals("column")) {
								currentCol = new TableColumn();
								config.columns.add(currentCol);

								// Get data type
								String dataType = XLSUtilities.getColumn(row, "data type", header, lastCellNum, null);
								if(dataType != null) {
									if(!dataType.equals("text") &&
											!dataType.equals("date") &&
											!dataType.equals("calculate") &&
											!dataType.equals("decimal") &&
											!dataType.equals("integer") &&
											!dataType.equals("select_one")){
										throw new Exception(localisation.getString("mf_idt") + ": " + dataType + 
												" " + localisation.getString("mf_or") + ": " + (j + 1));
									}
									currentCol.type = dataType;
								} else {
									throw new Exception(localisation.getString("mf_mdt") + " " + localisation.getString("mf_or") + ": " + (j + 1));
								}

								// Get column name
								String colName = XLSUtilities.getColumn(row, "name", header, lastCellNum, null);
								if(colName != null) {
									colName = colName.trim().toLowerCase();
									String modColName = colName.replaceAll("[^a-z0-9_]", "");
									modColName = GeneralUtilityMethods.cleanName(modColName, true, true, true);
									if(colName.length() != modColName.length()) {
										throw new Exception(localisation.getString("mf_in") +
												": " + colName + " " + localisation.getString("mf_or") + ": " + (j + 1));
									} else if(colName.length() > 60) {
										throw new Exception(localisation.getString("mf_ntl") + ": " + colName + 
												" " + localisation.getString("mf_or") + ": " + (j + 1));
									}

									currentCol.column_name = colName;
								} else {
									throw new Exception(localisation.getString("mf_mn") + 
											localisation.getString("mf_or") + ": " + (j + 1));
								}

								// Get display name
								String dispName = XLSUtilities.getColumn(row, "display name", header, lastCellNum, null);
								if(dispName != null) {

									currentCol.displayName = dispName;
								} else {
									throw new Exception(localisation.getString("mf_mdn") + 
											" " + localisation.getString("mf_or") + ": " + (j + 1));
								}

								// Get hide state
								String hide = XLSUtilities.getColumn(row, "hide", header, lastCellNum, null);
								currentCol.hide = false;
								if(hide != null) {
									hide = hide.toLowerCase().trim();
									if(hide.equals("yes") || hide.equals("true")) {
										currentCol.hide = true;
									}
								} 

								// Get readonly state
								String readonly = XLSUtilities.getColumn(row, "readonly", header, lastCellNum, null);
								currentCol.readonly = false;
								if(readonly != null) {
									readonly = readonly.toLowerCase().trim();
									if(readonly.equals("yes") || readonly.equals("true")) {
										currentCol.readonly = true;
									}
								}

								// Get filter state
								String filter = XLSUtilities.getColumn(row, "filter", header, lastCellNum, null);
								currentCol.filter = false;
								if(filter != null) {
									filter = filter.toLowerCase().trim();
									if(filter.equals("yes") || filter.equals("true")) {
										currentCol.filter = true;
									}
								}

								// Get parameters
								try {
									currentCol.parameters = getParamObj(XLSUtilities.getColumn(row, "parameters", header, lastCellNum, null));
								} catch (Exception e) {
									// Ignore errors if parameters are not found
								}

								// Get calculation state
								if(currentCol.type.equals("calculate")) {
									String calculation = XLSUtilities.getColumn(row, "calculation", header, lastCellNum, null);

									if(calculation != null && calculation.length() > 0) {
										calculation = calculation.trim();
										if(calculation.equals("condition")) {
											// Calculation set by condition rows
											currentCol.isCondition = true;
										} else if(calculation.length() > 0) {
											currentCol.calculation = new SqlFrag();
											currentCol.calculation.addSqlFragment(calculation, true, localisation, 0);
										} 
									} else {
										throw new Exception(localisation.getString("mf_mc") + 
												" " + localisation.getString("mf_or") + ": " + (j + 1));
									}
								}
							} else if(rowType.equals("choice")) {
								if(currentCol != null && currentCol.type.equals("select_one")) {
									String name = XLSUtilities.getColumn(row, "name", header, lastCellNum, null);
									String dispName = XLSUtilities.getColumn(row, "display name", header, lastCellNum, null);
									if(name == null) {
										name = XLSUtilities.getColumn(row, "value", header, lastCellNum, null);	// Legacy implementation
										dispName = name;
									}

									if(name != null && dispName != null) {
										if(currentCol.choices == null) {
											currentCol.choices = new ArrayList<KeyValue> ();
											currentCol.choices.add(new KeyValue("", ""));		// Add the not selected choice automatically as this has to be the default
										}
										currentCol.choices.add(new KeyValue(name, dispName));
										currentCol.filter = true;

										// Add conditional color
										String appearance = XLSUtilities.getColumn(row, "appearance", header, lastCellNum, null);
										if(appearance != null) {
											if(currentCol.markup == null) {
												currentCol.markup = new ArrayList<TableColumnMarkup> ();
											}
											currentCol.markup.add(new TableColumnMarkup(name, getMarkup(appearance)));	
										} 
									} else {
										throw new Exception(localisation.getString("mf_mv") + 
												" " + localisation.getString("mf_or") + ": " + (j + 1));
									}
								} else {
									throw new Exception(localisation.getString("mf_un_c") + 
											" " + localisation.getString("mf_or") + ": " + (j + 1));
								}

							} else if(rowType.equals("user_role")) {
								if(currentCol != null && currentCol.type.equals("select_one")) {
									String role = XLSUtilities.getColumn(row, "name", header, lastCellNum, null);
									if(role != null) {
										if(currentCol.choices == null) {
											currentCol.choices = new ArrayList<KeyValue> ();
											currentCol.choices.add(new KeyValue("", ""));		// Add the not selected choice automatically as this has to be the default
										}
										currentCol.choices.add(new KeyValue(role, role, true));
										currentCol.filter = true;

									} else {
										throw new Exception(localisation.getString("mf_mv_o") + 
												" " + localisation.getString("mf_or") + ": " + (j + 1));
									}
								} else {
									throw new Exception(localisation.getString("mf_un_u") + 
											" " + localisation.getString("mf_or") + ": " + (j + 1));
								}

							} else if(rowType.equals("action")) {
								if(currentCol != null) {
									String action = XLSUtilities.getColumn(row, "action", header, lastCellNum, null);
									if(action != null) {
										if(action.equals("respond")) {
											if(currentCol.actions == null) {
												currentCol.actions = new ArrayList<Action> ();
											}
										} else {
											throw new Exception(localisation.getString("mf_ia") + 
													" " + localisation.getString("mf_or") + ": " + (j + 1));
										}

										// Get the action details
										Action todo = new Action(action);
										todo.notify_type = XLSUtilities.getColumn(row, "notify type", header, lastCellNum, null);
										todo.notify_person = XLSUtilities.getColumn(row, "notify person", header, lastCellNum, null);
										if(isSecurityManager) {
											String roles = XLSUtilities.getColumn(row, "roles", header, lastCellNum, null);
											if(roles != null) {
												todo.roles = new ArrayList<Role> ();
												String rArray [] = roles.split(",");
												for(int i = 0; i < rArray.length; i++) {
													pstmtGetRoleId.setString(2, rArray[i].trim());
													ResultSet rs = pstmtGetRoleId.executeQuery();
													if(rs.next()) {
														todo.roles.add(new Role(rs.getInt(1), rArray[i].trim()));
													}
												}
											}
										}
										// Checks for a valid notification
										if(action.equals("respond") && todo.notify_type == null) {
											throw new Exception(localisation.getString("mf_mnt") + 
													" " + localisation.getString("mf_or") + ": " + (j + 1));
										} 

										currentCol.actions.add(todo);

									} else {
										throw new Exception(localisation.getString("mf_mv_a") + 
												" " + localisation.getString("mf_or") + ": " + (j + 1));
									}
								} else {
									throw new Exception(localisation.getString("mf_un_a") + 
											" " + localisation.getString("mf_or") + ": " + (j + 1));
								}
							} else if(rowType.equals("condition")) {

								if(currentCol != null && currentCol.type.equals("calculate")) {

									processingConditions = true;

									String condition = XLSUtilities.getColumn(row, "condition", header, lastCellNum, null);
									String value = XLSUtilities.getColumn(row, "value", header, lastCellNum, null);

									if(condition == null) {
										throw new Exception("Missing \"condition\" on row: " + (j + 1));
									} else {
										if(currentCol.calculation == null) {
											currentCol.calculation = new SqlFrag();
											currentCol.calculation.add("CASE");
										}
										if(condition.toLowerCase().trim().equals("all")) {
											currentCol.calculation.add("ELSE");
											currentCol.calculation.addSqlFragment(value, false, localisation, 0);
											//currentCol.calculation.addText(value);
										} else {
											currentCol.calculation.add("WHEN");
											currentCol.calculation.addSqlFragment(condition, true, localisation, 0);
											currentCol.calculation.add("THEN");
											//currentCol.calculation.addText(value);
											currentCol.calculation.addSqlFragment(value, false, localisation, 0);
										}
									}


									// Add conditional markup and save value for use in export of form
									String appearance = XLSUtilities.getColumn(row, "appearance", header, lastCellNum, null);
									if(currentCol.markup == null) {
										currentCol.markup = new ArrayList<TableColumnMarkup> ();        					
									} 
									currentCol.markup.add(new TableColumnMarkup(value, getMarkup(appearance)));

								} else {
									throw new Exception(localisation.getString("mf_uc") + 
											" " + localisation.getString("mf_or") + ": " + (j + 1));
								} 

							} else if(rowType.equals("settings")) {
								config.settings = getParamObj(XLSUtilities.getColumn(row, "parameters", header, lastCellNum, null));
							} else {
								throw new Exception(localisation.getString("mf_ur") + 
										" " + localisation.getString("mf_or") + ": " + (j + 1));
							}
						}	


					}
				}
			}
			if(!foundData) {
				throw new Exception(localisation.getString("mf_nd"));
			}

			// Close of any condition type calculations
			if(processingConditions ) {
				processingConditions = false;
				currentCol.calculation.add("END");
			}
		}

		// Final Validatation

		for(TableColumn col : config.columns) {

			// 1. Check for condition calculations without any corresponding contion entries
			if(col.isCondition && col.calculation == null) {
				throw new Exception(localisation.getString("mf_ncr") + ": " + col.column_name);
			}
		}
	
		return config;
			
	}
	
	/*
	 * Convert an appearance to jquery classes
	 * TODO markup should be handled in the client
	 */
	private String getMarkup(String app) {

		StringBuffer markupString = new StringBuffer("");
		
		if(app != null) {
			String [] apps = app.trim().toLowerCase().split(" ");
			boolean hasMarkup = false;
			for(int i = 0; i < apps.length; i++) {
				for(int j = 0; j < markup.size(); j++) {
					if(markup.get(j).k.equals(apps[i])) {
						if(hasMarkup) {
							markupString.append(" ");
						}
						hasMarkup = true;
						markupString.append(markup.get(j).v);
					}
				}
			}
		}
		
		return markupString.toString().trim();
	}
	

	
	private HashMap<String, String> getParamObj(String parameters) {
		
		HashMap<String, String> paramObj = null;
		
		// Remove any white space around the equal signs
		parameters = GeneralUtilityMethods.removeSurroundingWhiteSpace(parameters, '=');
		if(parameters != null) {
			paramObj = new HashMap<String, String> ();
			String [] params = parameters.split(" ");
			for(int i = 0; i < params.length; i++) {
				String[] p = params[i].split("=");
				if(p.length > 1) {
					if(p[0].equals("rows")) {
						try {
							@SuppressWarnings("unused")
							int rows = Integer.valueOf(p[1]);   // Check that rows is an integer
							paramObj.put(p[0], p[1]);
						} catch (Exception e) {
							// Ignore exceptions
						}
					} else if(p[0].equals("source")) {	
						paramObj.put(p[0], p[1]);						
					} else if(p[0].equals("form_data")) {	
						paramObj.put(p[0], p[1]);						
					} else if(p[0].equals("auto")) {	
						paramObj.put(p[0], p[1]);						
					}
					// Ignore parameters that we don't know about
				} 
			}
		}
		
		return paramObj;
	}
	
}
