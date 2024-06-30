package org.smap.sdal.managers;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.smap.notifications.interfaces.ImageProcessing;
import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.model.SelectChoice;
import org.smap.sdal.model.SelectKeys;
import org.smap.sdal.model.ServerCalculation;
import org.smap.sdal.model.SqlFrag;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/*****************************************************************************

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

 ******************************************************************************/

/*
 * Manage the log table
 */
public class LookupManager {
	
	Authorise a = new Authorise(null, Authorise.ENUM);
	
	private static Logger log =
			 Logger.getLogger(LookupManager.class.getName());
	
	LogManager lm = new LogManager();		// Application log
	
	private final String CONTAINS = "contains";
	private final String MATCHES = "matches";
	private final String IN = "in";
	private final String NOT_IN = "not in";
	private final String STARTS = "startswith";
	private final String ENDS = "endswith";
	
    // Valid pulldata functions
    public static final String FN_COUNT = "count";
    public static final String FN_LIST = "list";
    public static final String FN_INDEX = "index";
    public static final String FN_SUM = "sum";
    public static final String FN_MAX = "max";
    public static final String FN_MIN = "min";
    public static final String FN_MEAN = "mean";
	
	private class ColDetails {
		String colName = null;
		public ArrayList<SqlFrag> filterArray = null;
		
		public String getExpression() {
			if(filterArray != null) {
				SqlFrag frag = filterArray.get(0);
				return frag.sql.toString();
			} else {
				return colName;
			}
		}
	};
	
	/*
	 * Lookup a value in the survey
	 * This is an online equivalent of pulldata
	 */
	public Response lookup(HttpServletRequest request, 
			String surveyIdent,
			String fileName, 
			String keyColumn,
			String keyValue,
			String indexFn,
			String searchType,
			String expression) {
		Response response = null;
		String connectionString = "surveyMobileAPI-Lookup";
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		int sId = 0;
		
		log.info("Lookup: Filename=" + fileName + " key_column=" + keyColumn + " key_value=" + keyValue + " searchType=" + searchType + " Expression=" + expression);

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);		
		a.isAuthorised(sd, request.getRemoteUser());
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			sId = GeneralUtilityMethods.getSurveyId(sd, surveyIdent);
		} catch (Exception e) {
		}
		
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		Connection cResults = null;
		PreparedStatement pstmt = null;
		
		// Extract the data
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);			

			if(searchType == null) {
				searchType = "matches";
			}
			
			int index = 0;
			if(indexFn != null) {
				   // Support legacy function values
                if(indexFn.equals("-1")) { // legacy
                    indexFn = FN_COUNT;
                } else if(indexFn.equals("0")) { // legacy
                    indexFn = FN_LIST;
                }
                
                /*
                 * If the function is a number greater than 0 then set the function to "index",
                 * if less than 0 set it to "count" otherwise if equal to 0 then it should be "list"
                 * if it is not a number then it will not be changed
                 */
                try {
                    index = Integer.valueOf(indexFn);
                    if(index > 0) {
                        indexFn = FN_INDEX;
                    } else if(index < 0) {
                        indexFn = FN_COUNT;
                    } else {
                        indexFn = FN_LIST;
                    }
                } catch (Exception e) {

                }
			}
			
			String tz = "UTC";
			ArrayList<String> arguments = new ArrayList<> ();
			ColDetails colDetails = getDetails(sd, gson, localisation, sId, keyColumn, fileName);		
			StringBuilder selection = new StringBuilder(createLikeExpression(colDetails.getExpression(), keyValue, searchType, arguments));
			
			ArrayList<HashMap<String, String>> resultsArray = null;
			HashMap<String, String> results = null;
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			if(fileName != null) {
				if(fileName.startsWith("linked_s") || fileName.startsWith("chart_s")) {
					// Get data from a survey
					
					// Get expression sql fragment
					SqlFrag expressionFrag = null;
					if(expression != null) {
						// Convert #{qname} syntax to ${qname} syntax - also remove any enclosing single quotes
						expression = expression.replace("#{", "${");
						expression = expression.replace("\'${", "${");
						expression = expression.replace("}\'", "}");
						expressionFrag = new SqlFrag();
						log.info("Lookup with expression: " + expression);
						expressionFrag.addSqlFragment(expression, false, localisation, 0);
					}
					
					cResults = ResultsDataSource.getConnection(connectionString);				
					SurveyTableManager stm = new SurveyTableManager(sd, cResults, localisation, oId, sId, fileName, request.getRemoteUser());
					stm.initData(pstmt, "lookup", selection.toString(), arguments, 
							expressionFrag, 		// expression Fragment
							tz, null, null);
					HashMap<String, String> line = null;
					resultsArray = new ArrayList<> ();
					while((line = stm.getLineAsHash()) != null) {
						resultsArray.add(line);
					}
					
				} else {
					// Get data from a csv file
					CsvTableManager ctm = new CsvTableManager(sd, localisation);
					resultsArray = ctm.lookup(oId, sId, fileName + ".csv", keyColumn, keyValue, expression, tz, selection.toString(), arguments);
				} 

				results = new HashMap<String, String> ();
				if(resultsArray != null) {
					if(indexFn == null) {
						if(resultsArray.size() > 0) {
							results = resultsArray.get(0);
						} 
					} else if(indexFn.equals(FN_COUNT)) {				
						results.put("_count", String.valueOf(resultsArray.size()));
					} else if(indexFn.equals(FN_INDEX)) {
						if(index < resultsArray.size() + 1) {
							results = resultsArray.get(index - 1);
						} else {
							//throw new ApplicationException("Index: " + index + " is out of bounds.  There are only " + resultsArray.size() + " items");
						}
					} else if(indexFn.equals(FN_SUM) || indexFn.equals(FN_SUM) || indexFn.equals(FN_MEAN) ||
							indexFn.equals(FN_MIN) || indexFn.equals(FN_MAX) || indexFn.equals(FN_LIST)) {
						
						for(HashMap<String, String> r : resultsArray) {
							for(String k : r.keySet()) {
								String v = r.get(k);
								String l = results.get(k);
								
								if(indexFn.equals(FN_LIST)) {
									if(l == null) {
										l = "";
									}
									if(l.length() == 0) {
										results.put(k,  v);
									} else {
										results.put(k, l + " " + v);
									}
								} else if(indexFn.equals(FN_SUM) || indexFn.equals(FN_MEAN) ||
										indexFn.equals(FN_MIN) || indexFn.equals(FN_MAX)) {
									double cValue = 0.0;
									double vValue = 0.0;
									
									// initialise max / min
									if(indexFn.equals(FN_MIN) || indexFn.equals(FN_MAX)) {
										if(indexFn.equals(FN_MIN)) {
											cValue = Double.MAX_VALUE;
										} else if(indexFn.equals(FN_MAX)) {
											cValue = Double.MIN_VALUE;
										}
									}
									if(l != null) {
										try {
											cValue = Double.valueOf(l);
										} catch (Exception e) { }
									}
									if(v != null) {
										try {
											vValue = Double.valueOf(v);
										} catch (Exception e) { }
									}
									
									if(indexFn.equals(FN_SUM) || indexFn.equals(FN_MEAN)) {
										results.put(k, String.valueOf(cValue + vValue));
									} else if(indexFn.equals(FN_MIN)) {
										if(vValue < cValue) {
											results.put(k, String.valueOf(vValue));
										} else {
											results.put(k, String.valueOf(cValue));
										}
									} else if(indexFn.equals(FN_MAX)) {
										if(vValue > cValue) {
											results.put(k, String.valueOf(vValue));
										} else {
											results.put(k, String.valueOf(cValue));
										}
									}
								}
							}
						}
						if(indexFn.equals(FN_MEAN)) {
							for(String k : results.keySet()) {
								String l = results.get(k);
								if(resultsArray.size() > 0) {
									Double cValue = Double.valueOf(l);
									results.put(k, String.valueOf(cValue / resultsArray.size()));
								}
							}
						}
					}
				}
			} 
			
			response = Response.ok(gson.toJson(results)).build();
		
		}  catch (Exception e) {
			log.log(Level.SEVERE,"Exception", e);
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}  finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}} 
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}
				
		return response;
	}
	
	public Response choices(HttpServletRequest request, 
			String surveyIdent,
			String fileName,
			String valueColumn,
			String labelColumns,
			String searchType,
			String qColumn,
			String qValue,
			String fColumn,
			String fValue,
			String expression) {
		
		Response response = null;
		String connectionString = "surveyMobileAPI-Lookup-choices";
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		int sId = 0;
		
 		log.info("Lookup choices: Filename=" + fileName + " value_column=" + valueColumn + " label_column=" + labelColumns);

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);		
		a.isAuthorised(sd, request.getRemoteUser());
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			sId = GeneralUtilityMethods.getSurveyId(sd, surveyIdent);
		} catch (Exception e) {
		}
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
			
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		// Extract the data
		try {
			
			ArrayList<SelectChoice> results = getChoices(sd, cResults, request, gson, sId,
					surveyIdent,		// Survey that needs to lookup some data
					fileName,				// CSV filename, could be the identifier of another survey
					valueColumn,
					labelColumns,
					null,			// multi language label columns
					searchType,
					qColumn,
					qValue,
					fColumn,
					fValue,
					expression);         	

			log.info("Got the choices: " + results.size());
			response = Response.ok(gson.toJson(results)).build();
		
		}  catch (Exception e) {
			log.log(Level.SEVERE,"Exception", e);
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}  finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}
				
		return response;
	}
	
	/*
	 * Multi language version of choices function
	 */
	public Response mlchoices(HttpServletRequest request, 
			String surveyIdent,
			String fileName,
			String questionName,
			String searchType,
			String qColumn,
			String qValue,
			String fColumn,
			String fValue,
			String expression) {
		Response response = null;
		String connectionString = "surveyMobileAPI-Lookup-multilangugage-choices";
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		int sId = 0;
		
 		log.info("Lookup multi language choices: Filename=" + fileName + " questionName=" + questionName);

		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);		
		a.isAuthorised(sd, request.getRemoteUser());
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			sId = GeneralUtilityMethods.getSurveyId(sd, surveyIdent);
		} catch (Exception e) {
		}
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
			
		Connection cResults = ResultsDataSource.getConnection(connectionString);
		// Extract the data
		try {
			
			/*
			 * Get the columns
			 */
			SelectKeys sk = GeneralUtilityMethods.getSelectKeys(sd, sId,questionName);
			if(sk.valueColumn == null) {
				throw new Exception("Cannot find choices for question: " + questionName);
			}
			/*
			 * Get the choices
			 */
			ArrayList<SelectChoice> results = getChoices(sd, cResults, request, gson, sId,
					surveyIdent,			// Survey that needs to lookup some data
					fileName,				// CSV filename, could be the identifier of another survey
					sk.valueColumn,
					null,					// Single language label columns
					sk.labelColumns,
					searchType,
					qColumn,
					qValue,
					fColumn,
					fValue,
					expression);         	

			response = Response.ok(gson.toJson(results)).build();
		
		}  catch (Exception e) {
			log.log(Level.SEVERE,"Exception", e);
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}  finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);
		}
				
		return response;
	}
	
	/*
	 * Get labels from an image
	 */
	public Response imageLookup(HttpServletRequest request, 
			String surveyIdent) {
		Response response = null;
		
		String connectionString = "surveyMobileAPI-Lookup-imagelabels";
		int sId = 0;
		
		// Authorisation - Access
		
		Connection sd = SDDataSource.getConnection(connectionString);		
		a.isAuthorised(sd, request.getRemoteUser());
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
			sId = GeneralUtilityMethods.getSurveyId(sd, surveyIdent);
		} catch (Exception e) {
		}
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser); 
		
		
		String basePath = GeneralUtilityMethods.getBasePath(request);
		
		/*
		 * Parse the request
		 */
		DiskFileItemFactory  fileItemFactory = new DiskFileItemFactory ();
		fileItemFactory.setSizeThreshold(20*1024*1024);
		ServletFileUpload uploadHandler = new ServletFileUpload(fileItemFactory);
		try {
					
			List<?> items = uploadHandler.parseRequest(request);
			Iterator<?> itr = items.iterator();
			File savedFile = null;
			String contentType = null;
			String tempFileName = UUID.randomUUID().toString();
			while(itr.hasNext()) {
				FileItem item = (FileItem) itr.next();
	
				if(!item.isFormField()) {
					if(!item.isFormField()) {
						contentType = item.getContentType();
						String filePath = basePath + "/temp/" + tempFileName;								
						savedFile = new File(filePath);
						item.write(savedFile);  // Save the new file
					}
				}
			}		

			if(savedFile == null) {
				throw new ApplicationException("File not loaded");
			}
			
			if(contentType == null || !contentType.startsWith("image")) {
				// Rekognition supports what? TODO
				throw new ApplicationException("Content type not supported: " + contentType);
			}
			
			//Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			//ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);				

			String region = GeneralUtilityMethods.getSettingFromFile("/smap/settings/region");
			ImageProcessing ip = new ImageProcessing(region, basePath);
			try {
				log.info("xxxxxxxx Image lookup: " + tempFileName + " : " + region);
				String labels = ip.getLabels(
						basePath + "/temp/",
						tempFileName, 
						"text",
						null);
				log.info("Labels: " + labels);
				response = Response.ok(labels).build();
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
				response = Response.ok(e.getMessage()).build();
			}
				
			lm.writeLog(sd, sId, request.getRemoteUser(), LogManager.REKOGNITION, "Online for survey: " + surveyIdent, 0, request.getServerName());
		} catch (Exception e) {
			log.log(Level.SEVERE,"Exception", e);
			response = Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}  finally {
			SDDataSource.closeConnection(connectionString, sd);
		}
		
		return response;
	}
	
	private ColDetails getDetails(Connection sd, Gson gson, ResourceBundle localisation, int sId, String colName, String fileName) throws Exception {
		ColDetails colDetails = null;
		
		int linked_sId = 0;
		if(fileName != null && fileName.startsWith("linked_s")) {
			linked_sId = GeneralUtilityMethods.getLinkedSId(sd, sId, fileName);
			colDetails = getColDetails(sd, gson, localisation, linked_sId, colName);
		} else {
			colDetails = new ColDetails();
			colDetails.colName = colName;
		}
		return colDetails;
	}
	
	private ColDetails getColDetails(Connection sd, Gson gson, ResourceBundle localisation, int sId, String qname) throws Exception {
		
		
		String sql = "select q.column_name, q.qtype, q.server_calculate " 
				+ " from question q, form f" 
				+ " where q.f_id = f.f_id "
				+ " and f.s_id = ? " 
				+ " and q.qname = ?";
		
		ColDetails details = new ColDetails();
		
		String colType = null;
		String serverCalculate = null;
		PreparedStatement pstmt = null;
		
		try {
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, sId);
			pstmt.setString(2, qname);
			ResultSet rs = pstmt.executeQuery();
			log.info(pstmt.toString());
			if (rs.next()) {
				details.colName = rs.getString(1);
				colType = rs.getString(2);
				serverCalculate = rs.getString(3);
				SqlFrag calculation = null;
				
				if(colType.equals("server_calculate") && serverCalculate != null) {
					ServerCalculation sc = gson.fromJson(serverCalculate, ServerCalculation.class);
					calculation = new SqlFrag();
					sc.populateSql(calculation, localisation);
					details.filterArray = new ArrayList<>();
					details.filterArray.add(calculation);
				} 
			}
			
		} finally {
			if (pstmt != null) {try{pstmt.close();}catch(Exception e) {}}
		}
		
		return details;
	}
	
	private ArrayList<SelectChoice> getChoices(Connection sd, Connection cResults, HttpServletRequest request, Gson gson, int sId,
			String surveyIdent,
			String fileName,
			String valueColumn,
			String labelColumns,
			HashMap<String, String> mlLabelColumns,
			String searchType,
			String qColumn,
			String qValue,
			String fColumn,
			String fValue,
			String expression) throws Exception {
		
		ArrayList<SelectChoice> results = null;
		
		PreparedStatement pstmt = null;
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);			
		
			String tz = "UTC";
			
			// Clean the data
		
			if(searchType != null) {
				searchType = searchType.trim().toLowerCase();
			}
			if(qColumn != null) {
				qColumn = qColumn.trim();
			}
			if(qValue != null) {
				qValue = qValue.trim();
			}
			if(fValue != null) {
				fValue = fValue.trim();
			}
			if(fColumn != null) {
				fColumn = fColumn.trim();
			}
			// Create a where clause and where parameters
			StringBuilder selection = new StringBuilder("");
			String selectionString = null;
			ArrayList<String> arguments = new ArrayList<String> ();
			
			// Convert qname into an expression
			ColDetails qDetails = null;
			ColDetails fDetails = null;
			int linked_sId = 0;
			if(fileName != null && fileName.startsWith("linked_s")) {
				linked_sId = GeneralUtilityMethods.getLinkedSId(sd, sId, fileName);
				qDetails = getColDetails(sd, gson, localisation, linked_sId, qColumn);
				fDetails = getColDetails(sd, gson, localisation, linked_sId, fColumn);
			} else {
				qDetails = new ColDetails();
				qDetails.colName = qColumn;
				
				fDetails = new ColDetails();
				fDetails.colName = fColumn;
			}
			SqlFrag frag = null;
			if(expression != null) {
				// Convert #{qname} syntax to ${qname} syntax
				expression = expression.replace("#{", "${");
				frag = new SqlFrag();
				log.info("Lookup with expression: " + expression);
				frag.addSqlFragment(expression, false, localisation, 0);
				selection.append("( ").append(frag.sql).append(")");
			} else if (searchType != null && fColumn != null) {
	            selection.append("( ").append(createLikeExpression(qDetails.getExpression(), qValue, searchType, arguments)).append(" ) and ");
	            selection.append(fDetails.getExpression()).append(" = ? ");
	            arguments.add(fValue);
	        } else if (searchType != null) {
	            selection.append(createLikeExpression(qDetails.getExpression(), qValue, searchType, arguments)); 
	        } else if (fColumn != null) {
	            selection.append(fDetails.getExpression()).append(" = ? ");
	            arguments.add(fValue);
	        } else {
	            arguments = null;
	        }
			if(selection.length() > 0) {
				selectionString = selection.toString();
			} else {
				selectionString = null;
			}
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			if(fileName != null) {
				if(fileName.startsWith("linked_s")) {
					// Get data from a survey				
					SurveyTableManager stm = new SurveyTableManager(sd, cResults, localisation, oId, sId, fileName, request.getRemoteUser());
					stm.initData(pstmt, "choices",
							selectionString, arguments, frag, tz, qDetails.filterArray, fDetails.filterArray);
					
					HashMap<String, String> choiceMap = new HashMap<>();	// Use for uniqueness
					HashMap<String, String> line = null;
					int idx = 0;
					results = new ArrayList<SelectChoice> ();
					
					while((line = stm.getLineAsHash()) != null) {
						SelectChoice choice = new SelectChoice();
						if(mlLabelColumns != null) {  // multi language request
							choice.mlChoices = new HashMap<>();
							for(String lang : mlLabelColumns.keySet()) {
								String lc = mlLabelColumns.get(lang);
								
								String[] lArray = lc.split(",");
								StringBuffer lOutput = new StringBuffer("");
								for(String l : lArray) {
									if(lOutput.length() > 0) {
										lOutput.append(", ");
									}
									lOutput.append(line.get(l.trim()));
									choice.mlChoices.put(lang, lOutput.toString());
								}
							}
						} else {
							String[] lArray = labelColumns.split(",");
							StringBuffer lOutput = new StringBuffer("");
							for(String l : lArray) {
								if(lOutput.length() > 0) {
									lOutput.append(", ");
								}
								lOutput.append(line.get(l.trim()));
							}
							choice.labelInnerText = lOutput.toString();
						}
						String value = line.get(valueColumn);
						if(value != null) {
							value = value.trim();
							if(choiceMap.get(value) == null) {		// Only add unique values
								choice.value = value;
								choice.index = idx++;
								choiceMap.put(value, value);
								results.add(choice);
							}
						}
					}
				} else {
					// Get data from a csv file
					CsvTableManager ctm = new CsvTableManager(sd, localisation);
					results = ctm.lookupChoices(oId, sId, fileName + ".csv", valueColumn, labelColumns, mlLabelColumns,
							selectionString, arguments, frag);
				}
			}
			if (results == null) {
				results =  new ArrayList<SelectChoice> ();
			}
		} finally {
			if(pstmt != null) {try {pstmt.close();}catch(Exception e) {}}
		}
		
		return results;
	}
	
	// Based on function in odkCollect
	private String createLikeExpression(String qColumn,  String qValue, String type, ArrayList<String> arguments) {

		StringBuilder sb = new StringBuilder();
		type = type.trim().toLowerCase();

		if(type.equals(IN) || type.equals(NOT_IN)) {
			if(type.equals(NOT_IN)) {
				sb.append(qColumn).append(" not in (");
			} else {
				sb.append(qColumn).append(" in (");
			}

			String [] values = qValue.split(",");
			if(values.length == 1 && qValue.contains(" ")) {
				values = qValue.split(" ");
			}
			int idx = 0;
			for (String v : values) {
				if (idx++ > 0) {
					sb.append(", ");
				}
				sb.append("?");
				arguments.add(v);
			}
			sb.append(")");

		} else {
			sb.append(qColumn).append(" LIKE ? ");
			if(type.equals(MATCHES)) {
				arguments.add(qValue);
			} else if(type.equals(CONTAINS)) {
				arguments.add("%" + qValue + "%");
			} else if(type.equals(STARTS)) {
				arguments.add(qValue + "%");
			} else if(type.equals(ENDS)) {
				arguments.add("%" + qValue);
			}
		}
		return sb.toString();

	}
}


