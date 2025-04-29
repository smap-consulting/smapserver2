package surveyKPI;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;

import org.apache.commons.lang3.StringEscapeUtils;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.constants.SmapServerMeta;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.RoleManager;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.SqlFrag;
import org.smap.sdal.model.SqlParam;
import org.smap.sdal.model.TableColumn;

/*
 * Provides a survey level export of a survey as an XLS file
 * If the optional parameter "flat" is passed then this is a flat export where 
 *   children are appended to the end of the parent record.
 *   
 * If this parameter is not passed then a pivot style export is created.
 *  * For example for a parent form with a repeating group of children we might get:
 *    P1 C1
 *    P1 C2
 *    P1 C3
 *    P2 C4
 *    P2 C5
 *    P3 ...    // No children
 *    P4 C6
 *    etc
 *    
 */
@Path("/exportSurvey/{sId}/{filename}")
public class ExportSurvey extends Application {

	Authorise a = null;

	private static Logger log =
			Logger.getLogger(ExportSurvey.class.getName());

	LogManager lm = new LogManager();		// Application log

	public ExportSurvey() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		a = new Authorise(authorisations, null);
	}
	
	private class RecordDesc {
		String prikey;
		String parkey;
		StringBuffer record;
	}

	private class FormDesc {
		int f_id;
		int parent;
		String table_name;
		String columns = null;
		String parkey = null;
		Integer maxRepeats;
		int columnCount = 0;					// Number of displayed columns for this form
		Boolean visible = true;					// Set to false if the data from this form should not be included in the export
		Boolean flat = false;					// Set to true if this forms data should be flattened so it appears on a single line
		ArrayList<RecordDesc> records = null;
		ArrayList<FormDesc> children = null;
		ArrayList<TableColumn> columnList = null;
		ArrayList<SqlParam> params = new ArrayList<SqlParam>();

		@SuppressWarnings("unused")
		public void debugForm() {
			System.out.println("Form=============");
			System.out.println("    f_id:" + f_id);
			System.out.println("    parent:" + parent);
			System.out.println("    table_name:" + table_name);
			System.out.println("    maxRepeats:" + maxRepeats);
			System.out.println("    visible:" + visible);
			System.out.println("    flat:" + flat);
			System.out.println("End Form=============");
		}

		public void clearRecords() {
			records = null;
			if(children != null) {
				for(int i = 0; i < children.size(); i++) {
					children.get(i).clearRecords();
				}
			}
		}

		public void addRecord(String prikey, String parkey, StringBuffer rec) {
			if(records == null) {
				records = new ArrayList<RecordDesc> ();
			}

			RecordDesc rd = new RecordDesc();
			rd.prikey = prikey;
			rd.parkey = parkey;
			rd.record = rec;
			records.add(rd);
		}

		// Used for debugging
		@SuppressWarnings("unused")
		public void printRecords(int spacing, boolean long_form) {
			String padding = "";
			for(int i = 0; i < spacing; i++) {
				padding += " ";
			}

			if(records != null) {		
				for(int i = 0; i < records.size(); i++) {
					if(long_form) {
						log.info(padding + f_id + ":  " + records.get(i).prikey + 
								" : " + records.get(i).record.toString() );
					} else { 
						log.info(padding + f_id + ":  " + records.get(i).record.substring(0, 50));
					}
				}
			}

			if(long_form && children != null) {
				for(int i = 0; i < children.size(); i++) {
					children.get(i).printRecords(spacing + 4, long_form);
				}
			}
		}
	}

	ArrayList<StringBuffer> parentRows = null;
	HashMap<String, String> surveyNames = null;

	@GET
	@Produces("application/x-download")
	public void exportSurvey (@Context HttpServletRequest request, 
			@PathParam("sId") int sId,
			@PathParam("filename") String filename,
			@QueryParam("flat") boolean flat,
			@QueryParam("split_locn") boolean split_locn,
			@QueryParam("merge_select_multiple") boolean merge_select_multiple,
			@QueryParam("language") String language,
			@QueryParam("exp_ro") boolean exp_ro,
			@QueryParam("forms") String include_forms,
			@QueryParam("from") Date startDate,
			@QueryParam("to") Date endDate,
			@QueryParam("dateId") int dateId,
			@QueryParam("tz") String tz,					// Timezone
			@QueryParam("filter") String filter,

			@Context HttpServletResponse response) throws ApplicationException, Exception {

		String urlprefix = request.getScheme() + "://" + request.getServerName() + "/";		
		HashMap<String, String> selectMultipleColumnNames = new HashMap<String, String> ();
		HashMap<String, String> selMultChoiceNames = new HashMap<String, String> ();
		surveyNames = new HashMap<String, String> ();

		log.info("New export:" + " flat:" + flat + " split:" + split_locn + 
				" forms:" + include_forms + " filename: " + filename + ", merge select: " + merge_select_multiple);

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-ExportSurvey");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		tz = (tz == null) ? "UTC" : tz;
		
		lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.VIEW, "Export to XLS", 0, request.getServerName());

		response.setHeader("Content-type",  "application/vnd.ms-excel; charset=UTF-8");
		GeneralUtilityMethods.setFilenameInResponse(filename + ".xls", response);

		if(language != null) {
			language = language.replace("'", "''");	// Escape apostrophes
		} else {
			language = "none";
		}

		/*
		 * Get the list of forms to include in the output and their types (ie flat or pivot)
		 */
		int inc_id [] = null;
		boolean inc_flat [] = null;
		if(include_forms != null) {
			String iForms [] = include_forms.split(",");
			if(iForms.length > 0) {
				inc_id = new int [iForms.length];
				inc_flat = new boolean [iForms.length];
				for(int i = 0; i < iForms.length; i++) {
					String f[] = iForms[i].split(":");
					if(f.length > 1) {
						try {
							inc_id[i] = Integer.parseInt(f[0]);
							inc_flat[i] = Boolean.valueOf(f[1]);
						} catch (Exception e) {
							log.info("Invalid form argument in export: " + iForms[i]);
						}
					}

				}
			}
		}

		PrintWriter outWriter = null;
		StringBuffer qName = new StringBuffer();
		StringBuffer qText = new StringBuffer();

		if(sId != 0) {

			PreparedStatement pstmt = null;
			PreparedStatement pstmt2 = null;
			PreparedStatement pstmtQType = null;
			Connection connectionResults = null;


			try {

				// Localisation
				Organisation organisation = UtilityMethodsEmail.getOrganisationDefaults(sd, null, request.getRemoteUser());
				Locale locale = new Locale(organisation.locale);
				ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

				// Prepare the statement to get the question type and read only attribute
				String sqlQType = "select q.qtype, q.readonly from question q, form f " +
						" where q.f_id = f.f_id " +
						" and f.table_name = ? " +
						" and q.qname = ?;";
				pstmtQType = sd.prepareStatement(sqlQType);

				// Create an output writer to incrementally download the spreadsheet
				outWriter = response.getWriter();

				HashMap<String, FormDesc> forms = new HashMap<String, FormDesc> ();			// A description of each form in the survey
				ArrayList <FormDesc> formList = new ArrayList<FormDesc> ();					// A list of all the forms
				FormDesc topForm = null;

				connectionResults = ResultsDataSource.getConnection("surveyKPI-ExportSurvey");

				/*
				 * Get the tables / forms in this survey 
				 */
				String sql = null;
				sql = "SELECT f_id, table_name, parentform FROM form" +
						" WHERE s_id = ? " +
						" ORDER BY f_id;";	

				pstmt = sd.prepareStatement(sql);	
				pstmt.setInt(1, sId);
				ResultSet resultSet = pstmt.executeQuery();

				while (resultSet.next()) {

					FormDesc fd = new FormDesc();
					fd.f_id = resultSet.getInt("f_id");
					fd.parent = resultSet.getInt("parentform");
					fd.table_name = resultSet.getString("table_name");
					if(inc_id != null) {
						boolean showForm = false;
						boolean setFlat = false;
						int fId = fd.f_id;
						for(int i = 0; i < inc_id.length; i++) {
							if(fId == inc_id[i]) {
								showForm = true;
								setFlat = inc_flat[i];
								break;
							}
						}
						fd.visible = showForm;
						fd.flat = setFlat;
					}
					forms.put(String.valueOf(fd.f_id), fd);
					if(fd.parent == 0) {
						topForm = fd;
					}
					// Get max records for flat export
					fd.maxRepeats = 1;	// Default
					if(fd.flat && fd.parent != 0) {
						fd.maxRepeats = getMaxRepeats(sd, connectionResults, sId, fd.f_id);
					}
				}

				/*
				 * Put the forms into a list in top down order
				 */
				formList.add(topForm);		// The top level form
				addChildren(topForm, forms, formList);

				/*
				 * Add headers to output buffer 
				 */
				outWriter.print("<html xmlns:o=\"urn:schemas-microsoft-com:office:office\" xmlns:x=\"urn:schemas-microsoft-com:office:excel\" xmlns=\"http://www.w3.org/TR/REC-html40\">");
				outWriter.print("<head>");
				outWriter.print("<meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">");
				outWriter.print("<meta name=ProgId content=Excel.Sheet>");					// Gridlines
				outWriter.print("<meta name=Generator content=\"Microsoft Excel 11\">");		// Gridlines
				outWriter.print("<title>");
				outWriter.print(filename);
				outWriter.print("</title>");

				outWriter.print("<style <!--table @page{} -->>");
				outWriter.print("h1 {text-align:center; font-size: 2em; } ");
				outWriter.print("h2 {font-size: 1.6em; } ");
				outWriter.print("h3 {font-size: 1.2em; } ");
				outWriter.print("table,th,td {border:0.5 } ");
				outWriter.print("table {border-collapse:collapse;} ");
				outWriter.print("td {padding: 5px; } ");
				outWriter.print(".xl1 {mso-number-format:0;} ");										// _device style
				outWriter.print(".xl2 {mso-number-format:\"yyyy\\\\-mm\\\\-dd\\\\ h\\:mm\\:ss\";}");	// Date style

				outWriter.print("</style>");
				outWriter.print("<xml>");							// Gridlines
				outWriter.print("<x:ExcelWorkbook>");
				outWriter.print("<x:ExcelWorksheets>");
				outWriter.print("<x:ExcelWorksheet>");
				outWriter.print("<x:Name>Results Export</x:Name>");
				outWriter.print("<x:WorksheetOptions>");
				outWriter.print("<x:Panes>");
				outWriter.print("</x:Panes>");
				outWriter.print("</x:WorksheetOptions>");
				outWriter.print("</x:ExcelWorksheet>");
				outWriter.print("</x:ExcelWorksheets>");
				outWriter.print("</x:ExcelWorkbook>");
				outWriter.print("</xml>");
				outWriter.print("</head>");

				outWriter.print("<body>");
				outWriter.print("<table><thead>");
				if(topForm.visible) {
					qName.append("<tr><th>prikey</th>");
					qText.append("<tr><th></th>");
				}

				/*
				 * Add to each form description
				 *  1) The maximum number of repeats (if the form is to be flattened)
				 *  2) The columns that contain the data to be shown
				 */
				String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
				for(FormDesc f : formList) {
					TableColumn c;
					int parentId = 0;
					String geomType = null;
					if(f.parent != 0) {
						parentId = f.parent;
					}
					f.columnList = GeneralUtilityMethods.getColumnsInForm(
							sd,
							connectionResults,
							localisation,
							language,
							sId,
							sIdent,
							request.getRemoteUser(),
							null,		// Roles to apply
							parentId,
							f.f_id,
							f.table_name,
							exp_ro,
							false,		// Don't include parent key
							false,		// Don't include "bad" columns
							false,		// Don't include instance id
							true,		// Include prikey
							f.parent == 0,		// Include HRK
							true,		// Include other meta data
							true,		// Incude preloads
							true,		// instancename
							false,		// survey duration
							true,		// Case management
							superUser,	// In this case we will export all data if super user
							false,		// TODO add HXL export processing
							false,		// Don't include audit data
							tz,
							false,		// mgmt
							false,		// Accuracy and Altitude
							true		// Server calculates
							);


					for(int k = 0; k < f.maxRepeats; k++) {
						for(int j = 0; j < f.columnList.size(); j++) {

							c = f.columnList.get(j);
							String name = c.column_name;
							String qType = c.type;
							boolean ro = c.readonly;
							String humanName = c.question_name;

							boolean isAttachment = false;
							boolean isSelectMultiple = false;
							String selectMultipleQuestionName = null;
							String optionName = null;

							if(!exp_ro && ro) {
								continue;			// Drop read only columns if they are not selected to be exported				
							}

							isAttachment = GeneralUtilityMethods.isAttachmentType(qType);

							if(qType.equals("select")) {
								isSelectMultiple = true;
								selectMultipleQuestionName = c.question_name;
								optionName = c.option_name;
							}

							if(isSelectMultiple && merge_select_multiple) {
								humanName = selectMultipleQuestionName;

								// Add the name of sql column to a look up table for the get data stage
								selMultChoiceNames.put(name, optionName);
							}

							if(qType.equals("dateTime")) {
								humanName += " (" + tz +")";
							}

							if(f.maxRepeats > 1) {	// Columns need to be repeated horizontally
								humanName += "(r " + (k + 1) + ")";
							}

							// If the user has requested that select multiples be merged then we only want the question added once
							boolean skipSelectMultipleOption = false;
							if(isSelectMultiple && merge_select_multiple) {
								String n = selectMultipleColumnNames.get(humanName);
								if(n != null) {
									skipSelectMultipleOption = true;
								} else {
									selectMultipleColumnNames.put(humanName, humanName);
								}
							}

							if(!name.equals("prikey") && !skipSelectMultipleOption) {	// Primary key is only added once for all the tables
								if(f.visible) {	// Add column headings if the form is visible
									if(qType.equals("select") && !merge_select_multiple && c.choices != null &&  c.compressed) {
										// Add headings for choices
										for(int i = 0; i < c.choices.size(); i++) {
											String hName = humanName + " - " + c.choices.get(i).v;
											qName.append(getContent(sd, hName, true,false, hName, qType, split_locn));
										}
									} else {
										qName.append(getContent(sd, humanName, true,false, name, qType, split_locn));
									}
									if(!language.equals("none")) {
										if(qType.equals("select") && !merge_select_multiple && c.choices != null &&  c.compressed) {
											// Add headings for choices
											for(int i = 0; i < c.choices.size(); i++) {
												String hName = getQuestion(sd, name, sId, f, language, merge_select_multiple);
												hName = hName + " - " + c.choices.get(i).k;
												qText.append(getContent(sd, hName, true,false, hName, qType, split_locn));
											}
										} else {
											qText.append(getContent(sd, getQuestion(sd, name, sId, f, language, merge_select_multiple), true, false, name, qType, split_locn));
										}
									}
								}
							}

							// Set the sql selection text for this column (Only need to do this once, not for every repeating record)
							if(k == 0) {

								String selName = null;
								if(GeneralUtilityMethods.isGeometry(c.type)) {
									selName = "ST_AsTEXT(" + name + ") ";
									geomType = c.type;
								} else if(qType.equals("dateTime")) {
									selName = "timezone(?, " + name + ") as " + name;	
									f.params.add(new SqlParam("string", tz));
								} else {

									if(isAttachment) {
										selName = "'" + urlprefix + "' || " + name + " as " + name;
									} else {
										selName = name;
									}

								}

								if(f.columns == null) {
									f.columns = selName;
								} else {
									f.columns += "," + selName;
								}
								f.columnCount++;

								// Increment the column count if this is a geopoint question and the lat/lon are being split
								if(GeneralUtilityMethods.isGeometry(c.type) && split_locn) {
									if(geomType.equals("geopoint")) {
										f.columnCount++;
									}
								}
							}
						}

					}

				}
				qName.append("</tr>\n");

				if(!language.equals("none")) {	// Add the questions / option labels if requested
					qText.append("</tr>\n");
					outWriter.print(qText.toString().replace("&amp;", "&"));	// unescape ampersand for excel
				} 
				outWriter.print(qName.toString());
				outWriter.print("</thead><tbody>");

				/*
				 * Validate the filter and convert to an SQL Fragment
				 * 1. Verify that all columns are in the top level form (Only required for XLS style exports)
				 */
				SqlFrag filterFrag = null;
				if(filter != null && filter.length() > 0) {

					filterFrag = new SqlFrag();
					filterFrag.addSqlFragment(filter, false, localisation, 0);


					for(String filterCol : filterFrag.columns) {
						boolean valid = false;
						for(TableColumn tc : topForm.columnList) {
							if(filterCol.equals(tc.column_name)) {
								valid = true;
								break;
							}
						}
						if(!valid) {
							String msg = localisation.getString("inv_qn_misc");
							msg = msg.replace("%s1", filterCol);
							throw new Exception(msg);
						}
					}
				}

				/*
				 * Add the data
				 */
				getData(sd, 
						connectionResults, 
						localisation,
						sId,
						sIdent,
						request.getRemoteUser(),
						outWriter, 
						formList, 
						topForm, 
						flat, 
						split_locn, 
						merge_select_multiple, 
						selMultChoiceNames,
						startDate,
						endDate,
						dateId,
						superUser,
						filterFrag,
						tz);
				outWriter.print("</tbody><table></body></html>");

				log.info("Content Type:" + response.getContentType());
				log.info("Buffer Size:" + response.getBufferSize());
				log.info("Flushing buffers");
				outWriter.flush(); 
				outWriter.close();

			} catch (Exception e) {
				lm.writeLog(sd, sId, request.getRemoteUser(), "error", "Exporting survey to XLS: " + e.getMessage(), 0, request.getServerName());
				log.log(Level.SEVERE, "Exception", e);
			} finally {

				try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
				try {if (pstmt2 != null) {pstmt2.close();	}} catch (SQLException e) {	}
				try {if (pstmtQType != null) {pstmtQType.close();	}} catch (SQLException e) {	}

				SDDataSource.closeConnection("surveyKPI-ExportSurvey", sd);
				ResultsDataSource.closeConnection("surveyKPI-ExportSurvey", connectionResults);
			}
		}

	}

	private int getMaxRepeats(Connection con, Connection results_con, int sId, int formId)  {
		int maxRepeats = 1;

		String sql = "SELECT table_name, parentform FROM form" +
				" WHERE s_id = ? " +
				" AND f_id = ?;";	

		PreparedStatement pstmt = null;
		PreparedStatement pstmtGetCount = null;
		try {
			pstmt = con.prepareStatement(sql);
			ArrayList<String> tables = new ArrayList<String> ();
			getTableHierarchy(pstmt, tables, sId, formId);
			int numTables = tables.size();

			StringBuffer sqlBuf = new StringBuffer();
			sqlBuf.append("select max(t.cnt)  from " +
					"(select count(");
			sqlBuf.append(tables.get(numTables - 1));
			sqlBuf.append(".prikey) cnt " +
					" from ");


			for(int i = 0; i < numTables; i++) {
				if(i > 0) {
					sqlBuf.append(",");
				}
				sqlBuf.append(tables.get(i));
			}

			// where clause
			sqlBuf.append(" where ");
			sqlBuf.append(tables.get(0));
			sqlBuf.append("._bad='false'");
			if(numTables > 1) {
				for(int i = 0; i < numTables - 1; i++) {

					sqlBuf.append(" and ");

					sqlBuf.append(tables.get(i));
					sqlBuf.append(".parkey = ");
					sqlBuf.append(tables.get(i+1));
					sqlBuf.append(".prikey");
				}
			}

			sqlBuf.append(" group by ");
			sqlBuf.append(tables.get(numTables - 1));
			sqlBuf.append(".prikey) AS t;");

			pstmtGetCount = results_con.prepareStatement(sqlBuf.toString());
			ResultSet rsCount = pstmtGetCount.executeQuery();
			if(rsCount.next()) {
				maxRepeats = rsCount.getInt(1);
			}



		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetCount != null) {pstmtGetCount.close();	}} catch (SQLException e) {	}
		}
		return maxRepeats;
	}

	private void getTableHierarchy(PreparedStatement pstmt, ArrayList<String> tables, int sId, int formId) throws SQLException {

		pstmt.setInt(1, sId);
		pstmt.setInt(2, formId);
		ResultSet rs = pstmt.executeQuery();
		if(rs.next()) {
			tables.add(rs.getString(1));
			getTableHierarchy(pstmt, tables, sId, rs.getInt(2));
		}
	}

	/*
	 * Return the text formatted for html or csv
	 */
	private String getContent(Connection con, String in, boolean head, boolean firstCol, String columnName,
			String columnType, boolean split_locn) throws NumberFormatException, SQLException {

		String out = in;
		if(out == null) {
			out = "";
		}


		if(head) {
			if(split_locn && columnType != null && columnType.equals("geopoint")) {
				// Create two columns for lat and lon
				out = "<th>Latitude</th><th>Longitude</th>";
			} else {
				out = "<th>" + StringEscapeUtils.escapeHtml3(out) + "</th>";
			}
		} else {
			if(split_locn && out.startsWith("POINT")) {

				String coords [] = getLonLat(out);

				if(coords.length > 1) {
					out = "<td>" + coords[1] + "</td><td>" + coords[0] + "</td>"; 
				} else {
					out = "<td>" + StringEscapeUtils.escapeHtml3(out) + "</td><td>" + 
							StringEscapeUtils.escapeHtml3(out) + "</td>";
				}


			} else if(split_locn && (out.startsWith("POLYGON") || out.startsWith("LINESTRING"))) {

				// Can't split linestrings and polygons, leave latitude and longitude as blank
				out= "<td></td><td></td>";

			} else if(split_locn && columnType != null & columnType.equals("geopoint") ) {
				// Geopoint that needs to be split but there is no data
				out= "<td></td><td></td>";
			} else if(out.startsWith("POINT")) {
				String coords [] = getLonLat(out);
				if(coords.length > 1) {
					out = "<td>" + "<a href=\"http://www.openstreetmap.org/?mlat=" +
							coords[1] +
							"&mlon=" +
							coords[0] +
							"&zoom=14\">" +
							out + "</a>" + "</td>";
				} else {
					out = "<td>" + StringEscapeUtils.escapeHtml3(out) + "</td>"; 
				}
			} else if(out.startsWith("http")) {
				out = "<td>" + "<a href=\"" + out + "\">" + out + "</a></td>";
			} else if(out.startsWith("//")) {
				out = "<td>" + "<a href=\"https:" + out + "\">https:" + out + "</a></td>";
			} else if(columnName.equals("_device")) {
				out = "<td class='xl1'>" + StringEscapeUtils.escapeHtml3(out) + "</td>";

			} else if(columnName.equals("_complete")) {
				out = (out.equals("f")) ? "<td>No</td>" : "<td>Yes</td>"; 

			} else if(columnName.equals(SmapServerMeta.SURVEY_ID_NAME)) {
				String displayName = surveyNames.get(out);
				if(displayName == null) {
					try {
						displayName = GeneralUtilityMethods.getSurveyName(con, Integer.parseInt(out));
					} catch (Exception e) {
						displayName = "";
					}
					surveyNames.put(out, displayName);
				}
				out = "<td>" + displayName + "</td>"; 

			} else if(columnType.equals("dateTime")) {
				// Convert the timestamp to the excel format specified in the xl2 mso-format
				int idx1 = out.indexOf('.');	// Drop the milliseconds
				if(idx1 > 0) {
					out = out.substring(0, idx1);
				} 
				out = "<td class='xl2'>" + StringEscapeUtils.escapeHtml3(out) + "</td>";
			} else {
				out = "<td>" + StringEscapeUtils.escapeHtml3(out) + "</td>";
			}

		}

		return out;
	}

	/*
	 * Get the longitude and latitude from a WKT POINT
	 */
	private String [] getLonLat(String point) {
		String [] coords = null;
		int idx1 = point.indexOf("(");
		int idx2 = point.indexOf(")");
		if(idx2 > idx1) {
			String lonLat = point.substring(idx1 + 1, idx2);
			coords = lonLat.split(" ");
		}
		return coords;
	}

	/*
	 * For each record in the top level table all records in other tables that
	 * can link back to the top level record are retrieved.  These are then combined 
	 * to create a tree structure containing the output
	 * 
	 * The function is called recursively until the last table
	 */
	private void getData(
			Connection sd, 
			Connection connectionResults, 
			ResourceBundle localisation,
			int sId,
			String sIdent,
			String user,
			PrintWriter outWriter, 
			ArrayList<FormDesc> formList, 
			FormDesc f, 
			boolean flat, 
			boolean split_locn, 
			boolean merge_select_multiple, 
			HashMap<String, String> choiceNames,
			Date startDate,
			Date endDate,
			int dateId,
			boolean superUser,
			SqlFrag filterFrag,
			String tz) throws Exception {

		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		boolean hasRbacFilter = false;
		RoleManager rm = new RoleManager(localisation);
		ArrayList<SqlFrag> rfArray = null;

		/*
		 * Retrieve the data for this table
		 */
		StringBuffer sql = new StringBuffer("");
		sql.append("select ");
		sql.append(f.columns);
		sql.append(" from ");
		sql.append(f.table_name);
		sql.append(" where _bad is false ");				

		String sqlRestrictToDateRange = null;
		if(dateId != 0  && (f.parkey == null || f.parkey.equals("0"))) {	// Top level form with date filtering
			String dateName = GeneralUtilityMethods.getColumnNameFromId(sd, sId, dateId);
			sqlRestrictToDateRange = GeneralUtilityMethods.getDateRange(startDate, endDate, dateName);
			if(sqlRestrictToDateRange.trim().length() > 0) {
				sql.append("and ");
				sql.append(sqlRestrictToDateRange);
			}
		}

		// Add the advanced filter fragment
		if(filterFrag != null) {
			sql.append( " and (");
			sql.append(filterFrag.sql);
			sql.append(") ");
		}

		if(f.parkey != null && !f.parkey.equals("0")) {
			sql.append(" and parkey=?");
		} else {
			// RBAC filter
			if(!superUser) {
				rfArray = rm.getSurveyRowFilter(sd, sIdent, user);
				if(rfArray.size() > 0) {
					String rFilter = rm.convertSqlFragsToSql(rfArray);
					if(rFilter.length() > 0) {
						sql.append(" and ");
						sql.append(rFilter);
						hasRbacFilter = true;
					}
				}
			}
		}

		sql.append(" order by prikey asc");	

		try {
			pstmt = connectionResults.prepareStatement(sql.toString());
			int paramCount = 1;

			// Add any parameters in the select
			paramCount = GeneralUtilityMethods.addSqlParams(pstmt, paramCount, f.params);
			
			// if date filter is set then add it
			if(sqlRestrictToDateRange != null && sqlRestrictToDateRange.trim().length() > 0) {
				if(startDate != null) {
					pstmt.setTimestamp(paramCount++, GeneralUtilityMethods.startOfDay(startDate, tz));
				}
				if(endDate != null) {
					pstmt.setTimestamp(paramCount++, GeneralUtilityMethods.endOfDay(endDate, tz));
				}
			}

			if(filterFrag != null) {
				paramCount = GeneralUtilityMethods.setFragParams(pstmt, filterFrag, paramCount, tz);
			}
			
			if(f.parkey != null) {
				pstmt.setInt(paramCount++, Integer.parseInt(f.parkey));
			} else if(hasRbacFilter) {
				paramCount = GeneralUtilityMethods.setArrayFragParams(pstmt, rfArray, paramCount, tz);
			}
			//log.info("Get data: " + pstmt.toString());
			resultSet = pstmt.executeQuery();

			while (resultSet.next()) {

				String prikey = resultSet.getString(1);
				StringBuffer record = new StringBuffer();

				// If this is the top level form reset the current parents and add the primary key
				if(f.parkey == null || f.parkey.equals("0")) {
					f.clearRecords();
					record.append(getContent(sd, prikey, false, true, "prikey", "key", split_locn));
				}


				// Add the other questions to the output record
				String currentSelectMultipleQuestionName = null;
				String multipleChoiceValue = null;
				for(int i = 1; i < f.columnList.size(); i++) {

					TableColumn c = f.columnList.get(i);

					String columnName = c.column_name;
					String columnType = c.type;
					String value = resultSet.getString(i + 1);

					if(value == null) {
						value = "";	
					}

					if(c.type.equals("select") && merge_select_multiple && !c.compressed) {
						String choice = choiceNames.get(columnName);
						if(choice != null) {
							// Have to handle merge of select multiple
							String selectMultipleQuestionName = columnName.substring(0, columnName.indexOf("__"));
							if(currentSelectMultipleQuestionName == null) {
								currentSelectMultipleQuestionName = selectMultipleQuestionName;
								multipleChoiceValue = updateMultipleChoiceValue(value, choice, multipleChoiceValue);
							} else if(currentSelectMultipleQuestionName.equals(selectMultipleQuestionName) && (i != f.columnList.size() - 1)) {
								// Continuing on with the same select multiple and its not the end of the record
								multipleChoiceValue = updateMultipleChoiceValue(value, choice, multipleChoiceValue);
							} else if (i == f.columnList.size() - 1) {
								//  Its the end of the record		
								multipleChoiceValue = updateMultipleChoiceValue(value, choice, multipleChoiceValue);

								record.append(getContent(sd, multipleChoiceValue, false, false, columnName, columnType, split_locn));
							} else {
								// A second select multiple directly after the first - write out the previous
								record.append(getContent(sd, multipleChoiceValue, false, false, currentSelectMultipleQuestionName, "select", split_locn));

								// Restart process for the new select multiple
								currentSelectMultipleQuestionName = selectMultipleQuestionName;
								multipleChoiceValue = null;
								multipleChoiceValue = updateMultipleChoiceValue(value, choice, multipleChoiceValue);
							}
						} else {
							if(currentSelectMultipleQuestionName != null) {
								// Write out the previous multiple choice value before continuing with the non multiple choice value
								record.append(getContent(sd, multipleChoiceValue, false, false, currentSelectMultipleQuestionName, "select", split_locn));

								// Restart Process
								multipleChoiceValue = null;
								currentSelectMultipleQuestionName = null;
							}
							record.append(getContent(sd, value, false, false, columnName, columnType, split_locn));
						}
					} else if(c.type.equals("select") && !merge_select_multiple && c.choices != null &&  c.compressed) {
						String [] vArray = {""};
						if(value != null) {
							vArray = value.split(" ");
						} 
						
						for(int j = 0; j < c.choices.size(); j++) {	
							
							String v = "0";
							if(vArray != null) {
								
								String choiceValue = c.choices.get(j).k;
								for(int k = 0; k < vArray.length; k++) {
									if(vArray[k].equals(choiceValue)) {
										v = "1";
										break;
									}
								}
							}
							
							
							record.append(getContent(sd, v, false, false, columnName, columnType, split_locn));
								
						}
								
					} else {
						record.append(getContent(sd, value, false, false, columnName, columnType, split_locn));
					}
				}
				f.addRecord(prikey, f.parkey, record);

				// Process child tables
				if(f.children != null) {
					for(int j = 0; j < f.children.size(); j++) {
						FormDesc nextForm = f.children.get(j);
						nextForm.parkey = prikey;
						getData(
								sd, 
								connectionResults, 
								localisation,
								sId,
								sIdent,
								user,
								outWriter, 
								formList, 
								nextForm, 
								flat, 
								split_locn, 
								merge_select_multiple, 
								choiceNames,
								startDate,
								endDate,
								dateId,
								superUser,
								null,
								tz);
					}
				}

				/*
				 * For each complete survey retrieved combine the results
				 *  into a serial list that match the column headers. Where there are missing forms in the
				 *  data, Ie there was no data recorded for a form, then the results are padded
				 *  with empty values.
				 */
				if(f.parkey == null || f.parkey.equals("0")) {

					//f.printRecords(4, true);

					appendToOutput(sd, outWriter, new StringBuffer(""), formList.get(0), formList, 0, null);

				}
			}

		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Error", e);
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
		} finally {
			try{
				if(resultSet != null) {resultSet.close();};
				if(pstmt != null) {pstmt.close();};
			} catch (Exception ex) {
				log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
			}
		}

	}

	/*
	 * 
	 */
	String updateMultipleChoiceValue(String dbValue, String choiceName, String currentValue) {
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
	 * Add the list of children to parent forms
	 */
	private void addChildren(FormDesc parentForm, HashMap<String, FormDesc> forms, ArrayList<FormDesc> formList) {

		for(FormDesc fd : forms.values()) {
			if(fd.parent != 0 && fd.parent == parentForm.f_id) {
				if(parentForm.children == null) {
					parentForm.children = new ArrayList<FormDesc> ();
				}
				parentForm.children.add(fd);
				formList.add(fd);
				addChildren(fd,  forms, formList);
			}
		}

	}

	/*
	 * Construct the output
	 */
	private void appendToOutput(Connection sd, PrintWriter outWriter, StringBuffer in, 
			FormDesc f, ArrayList<FormDesc> formList, int index, String parent) throws NumberFormatException, SQLException {

		int number_records = 0;
		if(f.records != null) {
			number_records = f.records.size(); 
		} 

		if(f.visible) {

			if(f.flat) {
				StringBuffer newRec = new StringBuffer(in);
				for(int i = 0; i < number_records; i++) {
					newRec.append(f.records.get(i).record);
				}

				log.info("flat------>" + f.table_name + "Number records: " + number_records);
				// Pad up to max repeats
				for(int i = number_records; i < f.maxRepeats; i++) {
					StringBuffer record = new StringBuffer();
					for(int j = 1; j < f.columnCount; j++) {	// Start from one to ignore primary key
						record.append(getContent(sd, "", false, false, "", "empty", false));
					}
					newRec.append(record);
				}

				if(index < formList.size() - 1) {
					appendToOutput(sd, outWriter, newRec , formList.get(index + 1), 
							formList, index + 1, null);
				} else {
					closeRecord(outWriter, newRec);
				}

			} else {
				boolean found_non_matching_record = false;
				boolean hasMatchingRecord = false;
				if(number_records == 0) {
					if(index < formList.size() - 1) {

						/*
						 * Add an empty record for this form
						 */
						StringBuffer newRec = new StringBuffer(in);
						for(int j = 1; j < f.columnCount; j++) {	// Start from one to ignore primary key
							newRec.append(getContent(sd, "", false, false, "", "empty", false));
						}

						FormDesc nextForm = formList.get(index + 1);
						String filter = null;

						appendToOutput(sd, outWriter, newRec , nextForm, formList, index + 1, filter);
					} else {
						closeRecord(outWriter, in);
					}
				} else {
					/*
					 * First check all the records to see if there is at least one matching record
					 */
					for(int i = 0; i < number_records; i++) {
						RecordDesc rd = f.records.get(i);

						if(parent == null || parent.equals("0") || parent.equals(rd.parkey)) {
							hasMatchingRecord = true;
						}
					}

					for(int i = 0; i < number_records; i++) {
						RecordDesc rd = f.records.get(i);

						if(parent == null || parent.equals("0") || parent.equals(rd.parkey)) {
							StringBuffer newRec = new StringBuffer(in);
							newRec.append(f.records.get(i).record);

							if(index < formList.size() - 1) {
								/*
								 * If the next form is a child of this one then pass the primary key of the current record
								 * to filter the matching child records
								 */
								FormDesc nextForm = formList.get(index + 1);
								String filter = null;
								if(nextForm.parent == f.f_id) {
									filter = rd.prikey;
								}
								appendToOutput(sd, outWriter, newRec , nextForm, formList, index + 1, filter);
							} else {
								closeRecord(outWriter, newRec);
							} 
						} else {
							/*
							 * Non matching record  Continue processing the other forms in the list once.
							 * This is only done once as multiple non matching records are effectively duplicates
							 * It is also only done if we have not already found a matching record as 
							 *  
							 */
							if(!found_non_matching_record) {
								found_non_matching_record = true;

								if(index < formList.size() - 1) {

									/*
									 * Add an empty record for this form
									 */
									StringBuffer newRec = new StringBuffer(in);
									for(int j = 1; j < f.columnCount; j++) {	// Start from one to ignore primary key
										newRec.append(getContent(sd, "", false, false, "", "empty", false));
									}

									FormDesc nextForm = formList.get(index + 1);
									String filter = null;

									appendToOutput(sd, outWriter, newRec , nextForm, formList, index + 1, filter);
								} else {
									/*
									 * Add the record if there are no matching records for this form
									 * This means that if a child form was not completed the main form will still be shown (outer join)
									 */
									if(!hasMatchingRecord) {
										closeRecord(outWriter, in);
									}
								}
							}
						}
					}
				    }
			}
		} else {
			// Proceed with any lower level forms
			if(index < formList.size() - 1) {
				appendToOutput(sd, outWriter, in , formList.get(index + 1), 
						formList, index + 1, null);
			} else {
				closeRecord(outWriter, in);
			}
		}

	}

	/*
	 * Close the record
	 */
	private void closeRecord(PrintWriter outWriter, StringBuffer in) {

		outWriter.print("<tr>");
		outWriter.print(in.toString());
		outWriter.print("</tr>");
	}

	private String getQuestion(Connection conn, String colName, int sId, FormDesc form, String language, boolean merge_select_multiple) throws SQLException {
		String questionText = "";
		String qColName = null;
		String optionColName = null;
		String qType = null;
		PreparedStatement pstmt = null;
		ResultSet resultSet = null;
		int qId = -1;

		if(colName != null && language != null) {
			// Split the column name into the question and option part
			// Assume that double underscore is a unique separator
			int idx = colName.indexOf("__");
			if(idx == -1) {
				qColName = colName;
			} else {
				qColName = colName.substring(0, idx);
				optionColName = colName.substring(idx+2);
			}

			String sql = null;

			sql = "SELECT t.value AS qtext, q.qtype AS qtype, q.q_id FROM question q, translation t" +
					" WHERE q.f_id = ? " +
					" AND q.qtext_id = t.text_id " +
					" AND t.language = ? " +
					" AND t.s_id = ? " +
					" AND q.column_name = ?;";

			pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, form.f_id);
			pstmt.setString(2, language);
			pstmt.setInt(3, sId);
			pstmt.setString(4, qColName);
			resultSet = pstmt.executeQuery();

			if (resultSet.next()) {
				questionText = resultSet.getString("qtext");
				qType = resultSet.getString("qtype");
				qId = resultSet.getInt("q_id");
			}

			// Get any option text
			if(qType != null && qType.startsWith("select")) {
				sql = "SELECT t.value AS otext, o.ovalue AS ovalue, o.column_name FROM option o, question q, translation t" +
						" WHERE q.q_id = ? " +
						" AND o.l_id = q.l_id " +
						" AND o.label_id = t.text_id" +
						" AND t.language = ? " +
						" AND t.s_id = ? " +
						" ORDER BY o.seq ASC;";

				pstmt = conn.prepareStatement(sql);	 
				pstmt.setInt(1, qId);
				pstmt.setString(2, language);
				pstmt.setInt(3, sId);
				resultSet = pstmt.executeQuery();

				while (resultSet.next()) {
					String name = resultSet.getString("ovalue");
					String columnName = resultSet.getString("column_name").toLowerCase();
					String label = resultSet.getString("otext");
					if(qType.equals("select1") || merge_select_multiple) {
						// Put all options in the same column
						questionText += " " + name + "=" + label;
					} else if(optionColName != null) {
						// Only one option in each column
						if(columnName.equals(optionColName)) {
							questionText += " " + label;
						}
					}
				}
			}

		}

		try{
			if(resultSet != null) {resultSet.close();};
			if(pstmt != null) {pstmt.close();};
		} catch (Exception ex) {
			log.log(Level.SEVERE, "Unable to close resultSet or prepared statement");
		}

		return questionText;
	}

}
