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
import java.util.regex.Pattern;

import org.apache.poi.xssf.usermodel.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.XLSUtilities;
import org.smap.sdal.managers.UserManager;
import org.smap.sdal.model.Project;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.User;
import org.smap.sdal.model.UserGroup;

public class XLSUsersManager {
	
	private static Logger log =
			 Logger.getLogger(XLSUsersManager.class.getName());
	
	Workbook wb = null;
	int rowNumber = 1;		// Heading row is 0
	String scheme = null;
	String serverName = null;
	
	private class Column {
		String name;
		 CellStyle style;
		 boolean isSecurityGroup = false;
		
		public Column(ResourceBundle localisation, int col, String name, boolean a, 
				CellStyle style, boolean sec) {
			this.name = name;
			this.style = style;
			this.isSecurityGroup = sec;
		}
		
		// Return the width of this column
		public int getWidth() {
			int width = 256 * 20;		// 20 characters is default
			return width;
		}
		
		// Get a value for this column from the provided properties object
		public String getValue(User user) {
			String value = null;
			
			if(name.equals("ident")) {
				value = user.ident;
			} else if(name.equals("name")) {
				value = user.name;
			} else if(name.equals("email")) {
				value = user.email;
			} else if(isSecurityGroup) {
				value = getGroupValue(user.groups, name);
			} else if(name.equals("projects") && user.projects != null) {
				StringBuffer sb = new StringBuffer("");
				for(Project p : user.projects) {
					if(sb.length() > 0) {
						sb.append("; ");
					}
					sb.append(p.name);
				}
				value = sb.toString();
			} else if(name.equals("roles") && user.roles != null) {
				StringBuffer sb = new StringBuffer("");
				for(Role r : user.roles) {
					if(sb.length() > 0) {
						sb.append("; ");
					}
					sb.append(r.name);
				}
				value = sb.toString();
			} else if(name.equals("password")) {
				value = "";
			} else if(name.equals("language")) {
				if(user.language == null || user.language.trim().length() == 0) {
					value = "en";
				} else {
					value = user.language;
				}
			}
			
			if(value == null) {
				value = "";
			}
			return value;
		}
		
		private String getGroupValue(ArrayList<UserGroup> groups, String in) {
			if(groups != null) {
				for(UserGroup ug : groups) {
					if(ug.name != null && ug.name.equals(in)) {
						return "yes";
					}
				}
			}
			return null;
		}
	}

	public XLSUsersManager() {

	}
	
	public XLSUsersManager(String scheme, String serverName) {
		
		wb = new XSSFWorkbook();
		this.scheme = scheme;
		this.serverName = serverName;
	}
	
	/*
	 * Write a user list to an XLS file
	 */
	public void createXLSFile(Connection sd, OutputStream outputStream, ArrayList<User> users, 
			ResourceBundle localisation, String tz) throws IOException, SQLException {
		
		Sheet userSheet = wb.createSheet(localisation.getString("mf_u"));
		userSheet.createFreezePane(3, 1);
		
		Map<String, CellStyle> styles = XLSUtilities.createStyles(wb);

		ArrayList<Column> cols = getColumnList(sd, localisation, styles);
		createHeader(cols, userSheet);	
		processUserListForXLS(users, userSheet, styles, cols, tz);
		
		wb.write(outputStream);
		wb.close();
		outputStream.close();
	}
	
	/*
	 * Get the columns for the Users worksheet
	 */
	private ArrayList<Column> getColumnList(Connection sd, ResourceBundle localisation, Map<String, CellStyle> styles) throws SQLException {
		
		ArrayList<Column> cols = new ArrayList<Column> ();
		
		int colNumber = 0;
	
		cols.add(new Column(localisation, colNumber++, "ident", false, styles.get("header_tasks"), false));
		cols.add(new Column(localisation, colNumber++, "name", false, styles.get("header_tasks"), false));
		cols.add(new Column(localisation, colNumber++, "email", false, styles.get("header_tasks"), false));
		cols.add(new Column(localisation, colNumber++, "password", false, styles.get("header_tasks"), false));
		
		// Security Groups
		ArrayList<UserGroup> groups = GeneralUtilityMethods.getSecurityGroups(sd);
		for(UserGroup ug : groups) {
			cols.add(new Column(localisation, colNumber++, ug.name, false, styles.get("header_assignments"), true));
		}
		
		cols.add(new Column(localisation, colNumber++, "projects", false, styles.get("header_tasks"), false));
		cols.add(new Column(localisation, colNumber++, "roles", false, styles.get("header_tasks"), false));
		cols.add(new Column(localisation, colNumber++, "language", false, styles.get("header_tasks"), false));
		
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
	 * Convert a user list to XLS
	 */
	private void processUserListForXLS(
			ArrayList<User> users, 
			Sheet sheet,
			Map<String, CellStyle> styles,
			ArrayList<Column> cols,
			String tz) throws IOException {
		
		DataFormat format = wb.createDataFormat();
		CellStyle styleTimestamp = wb.createCellStyle();
		
		styleTimestamp.setDataFormat(format.getFormat("yyyy-mm-dd h:mm"));	
		
		for(User user : users)  {
			
			Row row = sheet.createRow(rowNumber++);
			for(int i = 0; i < cols.size(); i++) {
				Column col = cols.get(i);	
				Cell cell = row.createCell(i);
				cell.setCellValue(col.getValue(user));			
	        }	
		}
		
	}

	/*
	 * Create a user list from an XLS file
	 */
	public ArrayList<User> getXLSUsersList(Connection sd, String type, InputStream inputStream, ResourceBundle localisation, 
			String tz, int oId) throws Exception {

		Sheet sheet = null;
		Row row = null;
		int lastRowNum = 0;
		ArrayList<User> users = new ArrayList<User> ();

		// SQL to validate projects
		PreparedStatement pstmt = null;
		String sql = "select id from project where o_id = ? and name = ?";
		
		// SQL to validate roles
		PreparedStatement pstmtRoles = null;
		String sqlRoles = "select id from role where o_id = ? and name = ?";
		
		UserManager um = new UserManager(localisation);
		
		try {
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, oId);
			pstmtRoles = sd.prepareStatement(sqlRoles);
			pstmtRoles.setInt(1, oId);
			
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
				HashMap<String, Integer> userDups = new HashMap <> ();
				
				for(int j = 0; j <= lastRowNum; j++) {
	
					row = sheet.getRow(j);
					if(row != null) {
	
						int lastCellNum = row.getLastCellNum();
	
						if(needHeader) {
							header = getHeader(row, lastCellNum);
							needHeader = false;
						} else {
							
							User u = new User();
							
							u.imported = true;
							u.o_id = oId;
							u.ident = XLSUtilities.getColumn(row, "ident", header, lastCellNum, null);
							u.name = XLSUtilities.getColumn(row, "name", header, lastCellNum, null);
							u.email = XLSUtilities.getColumn(row, "email", header, lastCellNum, null);	
							u.password = XLSUtilities.getColumn(row, "password", header, lastCellNum, null);
							u.language = XLSUtilities.getColumn(row, "language", header, lastCellNum, null);
							
							// Get security groups
							ArrayList<UserGroup> groups = GeneralUtilityMethods.getSecurityGroups(sd);
							u.groups = new ArrayList<UserGroup> ();
							for(UserGroup ug : groups) {
								String v = XLSUtilities.getColumn(row, ug.name, header, lastCellNum, null);	
								if(v != null) {
									v = v.trim().toLowerCase();
									if(v.equals("yes") || v.equals("1") || v.equals("true")) {
										u.groups.add(ug);
									}
								}
							}						
							
							// Get Projects
							String projectString = XLSUtilities.getColumn(row, "projects", header, lastCellNum, null);
							u.projects = new ArrayList<Project> ();
							if(projectString != null && projectString.trim().length() > 0) {
								String [] pArray = projectString.split(";");
								for(int i = 0; i < pArray.length; i++) {
									Project p = new Project();
									p.name = pArray[i].trim();
									u.projects.add(p);
								}
							}
							
							// Get Roles
							String roleString = XLSUtilities.getColumn(row, "roles", header, lastCellNum, null);
							u.roles = new ArrayList<Role> ();
							if(roleString != null && roleString.trim().length() > 0) {
								String [] rArray = roleString.split(";");
								for(int i = 0; i < rArray.length; i++) {
									Role r = new Role();
									r.name = rArray[i].trim();
									if(r.name.length() > 0) {
										u.roles.add(r);
									}
								}
							}
							
							// validate user ident
							if(u.ident == null || u.ident.trim().length() == 0) {
								String msg = localisation.getString("fup_uim");
								msg = msg.replace("%s1", String.valueOf(j));
								throw new ApplicationException(msg);
							}
							if(!um.isValiduserIdent(u.ident)) {
								String msg = localisation.getString("fup_uif");
								msg = msg.replace("%s1", u.ident);
								msg = msg.replace("%s2", String.valueOf(j));
								throw new ApplicationException(msg);
							}
							
							// validate user name
							if(u.name == null || u.name.trim().length() == 0) {
								String msg = localisation.getString("fup_unm");
								msg = msg.replace("%s1", String.valueOf(j));
								throw new ApplicationException(msg);
							}
							// Validate projects and set project id
							for(Project p : u.projects) {
								pstmt.setString(2, p.name);
								ResultSet rs = pstmt.executeQuery();
								if(rs.next()) {
									p.id = rs.getInt(1);
								} else {
									String msg = localisation.getString("fup_inv_p");
									msg = msg.replace("%s1", p.name);
									msg = msg.replace("%s2", String.valueOf(j));
									throw new ApplicationException(msg);
								}
							}
							// Validate roles and set role id
							for(Role r : u.roles) {
								pstmtRoles.setString(2, r.name);
								ResultSet rs = pstmtRoles.executeQuery();
								if(rs.next()) {
									r.id = rs.getInt(1);
								} else {
									String msg = localisation.getString("fup_inv_r");
									msg = msg.replace("%s1", r.name);
									msg = msg.replace("%s2", String.valueOf(j));
									throw new ApplicationException(msg);
								}
							}
							
							// Validate duplicate user idents
							Integer firstRow = userDups.get(u.ident.toLowerCase());
							if(firstRow != null) {
								String msg = localisation.getString("fup_dun");
								msg = msg.replace("%s1", u.ident);
								msg = msg.replace("%s2", String.valueOf(j));
								msg = msg.replace("%s3", String.valueOf(firstRow));
								throw new ApplicationException(msg);
							} else {
								userDups.put(u.ident.toLowerCase(), j);
							}
							
							users.add(u);
						}
					}
				}

			}
		} catch(Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			String msg = e.getMessage();
			if (msg == null) {
				msg = localisation.getString("c_error");
			}
			throw new ApplicationException(msg);
		} finally {
			if(pstmt != null) try {pstmt.close();} catch (Exception e) {}
			if(pstmtRoles != null) try {pstmtRoles.close();} catch (Exception e) {}
		}

		return users;


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
