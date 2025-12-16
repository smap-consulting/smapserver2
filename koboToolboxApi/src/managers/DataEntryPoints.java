package managers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.RateLimiter;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.ServerSettings;
import org.smap.sdal.managers.CustomReportsManager;
import org.smap.sdal.managers.DataManager;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.TableDataManager;
import org.smap.sdal.model.AuditItem;
import org.smap.sdal.model.DataEndPoint;
import org.smap.sdal.model.KeyValue;
import org.smap.sdal.model.ReportConfig;
import org.smap.sdal.model.TableColumn;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class DataEntryPoints {

	Authorise a = null;
	Authorise aSuper = null;
	boolean forDevice = true;	// Attachment URL prefixes for API should have the device/API format
	
	private static Logger log =
			Logger.getLogger(DataEntryPoints.class.getName());
	
	LogManager lm = new LogManager(); // Application log
	
	public DataEntryPoints() {
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.VIEW_DATA);
		authorisations.add(Authorise.VIEW_OWN_DATA);
		authorisations.add(Authorise.ADMIN);
		authorisations.add(Authorise.MANAGE);
		a = new Authorise(authorisations, null);

		ArrayList<String> authorisationsSuper = new ArrayList<String> ();	
		authorisationsSuper.add(Authorise.ANALYST);
		authorisationsSuper.add(Authorise.VIEW_DATA);
		authorisationsSuper.add(Authorise.VIEW_OWN_DATA);
		authorisationsSuper.add(Authorise.ADMIN);
		aSuper = new Authorise(authorisationsSuper, null);
	}
	
	public Response getData(String version,
			Connection sd, 
			String connectionString,
			HttpServletRequest request,
			String remoteUser) {
		
		Response response = null;
		
		if(remoteUser == null) {
			return Response.status(Status.UNAUTHORIZED).build();
		}
		aSuper.isAuthorised(sd, remoteUser);
		// End Authorisation
		
		try {
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, remoteUser));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			ServerSettings.setBasePath(request);
			
			DataManager dm = new DataManager(localisation, "UTC");
			
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
			ArrayList<DataEndPoint> data = dm.getDataEndPoints(sd, remoteUser, false, urlprefix, version);

			Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(data);
			response = Response.ok(resp).build();
		} catch (SQLException e) {
			e.printStackTrace();
			response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
		}

		return response;
	}
	
	public Response getSingleDataRecord(Connection sd,
			String connectionString,
			HttpServletRequest request,
			String remoteUser,
			String sIdent,
			String uuid,
			String meta,
			String hierarchy,
			String merge,
			String tz) {
		
		Response response;
		Connection cResults = null;
		
		// Authorisation - Access
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, remoteUser);
		} catch (Exception e) {
		}
		
		try {
			
			cResults = ResultsDataSource.getConnection(connectionString);
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, remoteUser));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			if(!GeneralUtilityMethods.isApiEnabled(sd, remoteUser)) {
				throw new ApplicationException(localisation.getString("susp_api"));
			}
			
			DataManager dm = new DataManager(localisation, tz);
			int sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);	
			
			a.isAuthorised(sd, remoteUser);
			a.isValidSurvey(sd, remoteUser, sId, false, superUser);
			// End Authorisation
			
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
			String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, forDevice);
			
			boolean includeHierarchy = false;
			boolean includeMeta = false;		// Default to false for single record (Historical consistency reason)
			String mergeExp = "no";
			if(meta != null && (meta.equals("true") || meta.equals("yes"))) {
				includeMeta = true;
			}
			if(hierarchy != null && (hierarchy.equals("true") || hierarchy.equals("yes"))) {
				includeHierarchy = true;
			}
			if(merge != null && (merge.equals("true") || merge.equals("yes"))) {
				mergeExp = "yes";
			}
			
			if(includeHierarchy) {
				response = dm.getRecordHierarchy(sd, cResults, 
						request.getRemoteUser(),
						sIdent,
						sId,
						uuid,
						mergeExp, 			// If set to yes then do not put choices from select multiple questions in separate objects
						localisation,
						tz,				// Timezone
						includeMeta,
						urlprefix,
						attachmentPrefix,
						false
						);	
			} else {
				response = dm.getSingleRecord(
						sd,
						cResults,
						request,
						sIdent,
						sId,
						uuid,
						mergeExp, 			// If set to yes then do not put choices from select multiple questions in separate objects
						localisation,
						tz,				// Timezone
						includeMeta,
						urlprefix,
						attachmentPrefix
						);	
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "Exception", e);
			String resp = "{error: " + e.getMessage() + "}";
			response = Response.serverError().entity(resp).build();
		} finally {
			SDDataSource.closeConnection(connectionString, sd);
			ResultsDataSource.closeConnection(connectionString, cResults);	
		}
		
		return response;
	}
	
	public void getCSVData(String version, 
			Connection sd,
			String connectionString,
			HttpServletRequest request,
			HttpServletResponse response,
			String remoteUser,
			String sIdent,
			String formName,
			String sort,
			String dirn,
			String audit_set,
			String merge,
			String include_bad,
			String filename,
			String tz,
			boolean mgmt,
			String hrk,
			boolean group,
			int start,
			int parkey,
			int start_parkey,
			String filter,
			int limit) throws SQLException, IOException {
		
		if(remoteUser == null) {
			throw new AuthorisationException();
			
		}
		
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, remoteUser);
		} catch (Exception e) {
		}
		int sId = 0;
		int fId = 0;
		try {
			sId = GeneralUtilityMethods.getSurveyId(sd, sIdent);
			if(formName != null) {
				fId = GeneralUtilityMethods.getFormId(sd, sId, formName);
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		a.isAuthorised(sd, remoteUser);
		a.isValidSurvey(sd, remoteUser, sId, false, superUser);
		// End Authorisation

		String language = "none";
		lm.writeLog(sd, sId, remoteUser, LogManager.API_CSV_VIEW, "", 0, request.getServerName());

		Connection cResults = ResultsDataSource.getConnection(connectionString);

		String sqlGetManagedId = "select managed_id from survey where s_id = ?";
		PreparedStatement pstmtGetManagedId = null;

		String sqlGetMainForm = "select f_id, table_name from form where s_id = ? and parentform = 0;";
		PreparedStatement pstmtGetMainForm = null;

		String sqlGetForm = "select parentform, table_name from form where s_id = ? and f_id = ?;";
		PreparedStatement pstmtGetForm = null;

		PreparedStatement pstmt = null;

		//StringBuffer columnSelect = new StringBuffer();
		StringBuffer columnHeadings = new StringBuffer();
		StringBuffer record = null;
		PrintWriter outWriter = null;

		String table_name = null;
		int parentform = 0;
		int managedId = 0;
		boolean getParkey = false;
		ResultSet rs = null;

		if (sort != null && dirn == null) {
			dirn = "asc";
		}

		boolean audit = false;
		if (audit_set != null && audit_set.equals("yes")) {
			audit = true;
		}
		
		boolean mergeSelectMultiple = false;
		if(merge != null && merge.equals("yes")) {
			mergeSelectMultiple = true;
		}

		if (include_bad == null) {
			include_bad = "none";
		}

		boolean isDt = false;

		if (filename == null) {
			filename = "data.csv";
		}
		
		if(tz == null) {
			tz = GeneralUtilityMethods.getOrganisationTZ(sd, 
					GeneralUtilityMethods.getOrganisationId(sd, remoteUser));
		}
		tz = (tz == null) ? "UTC" : tz;

		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, remoteUser));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, remoteUser);
			
			response.setHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
			response.setHeader("content-type", "text/plain; charset=utf-8");
			outWriter = response.getWriter();
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
			String attachmentPrefix = GeneralUtilityMethods.getAttachmentPrefix(request, forDevice);

			/*
			 * Check rate Limiter and whether or not the api is disabled
			 */
			if(!GeneralUtilityMethods.isApiEnabled(sd, remoteUser)) {
				throw new ApplicationException(localisation.getString("susp_api"));
			}
			RateLimiter.isPermitted(sd, oId, response, localisation);
			
			// Get the managed Id
			if (mgmt) {
				pstmtGetManagedId = sd.prepareStatement(sqlGetManagedId);
				pstmtGetManagedId.setInt(1, sId);
				rs = pstmtGetManagedId.executeQuery();
				if (rs.next()) {
					managedId = rs.getInt(1);
				}
				rs.close();
			}

			if (fId == 0) {
				pstmtGetMainForm = sd.prepareStatement(sqlGetMainForm);
				pstmtGetMainForm.setInt(1, sId);

				log.info("Getting main form: " + pstmtGetMainForm.toString());
				rs = pstmtGetMainForm.executeQuery();
				if (rs.next()) {
					fId = rs.getInt(1);
					table_name = rs.getString(2);
				}
				rs.close();
			} else {
				getParkey = true;
				pstmtGetForm = sd.prepareStatement(sqlGetForm);
				pstmtGetForm.setInt(1, sId);
				pstmtGetForm.setInt(2, fId);

				log.info("Getting specific form: " + pstmtGetForm.toString());
				rs = pstmtGetForm.executeQuery();
				if (rs.next()) {
					parentform = rs.getInt(1);
					table_name = rs.getString(2);
				}
				rs.close();
			}

			ArrayList<TableColumn> columns = GeneralUtilityMethods.getColumnsInForm(sd, cResults, 
					localisation,
					language,
					sId,
					sIdent,
					remoteUser, 
					null,
					parentform, 
					fId, 
					table_name, 
					true,				// Read Only 
					getParkey, 			// Include parent key if the form is not the top level form (fId is 0)
					(include_bad.equals("yes") || include_bad.equals("only")), true, // include instance id
					true,				// Include prikey
					parentform == 0, 	// Include HRK if this is the top form		
					true,				// include other meta data if this is the top level form
					true, 				// include preloads
					true, 				// include instancename
					true, 				// include survey duration
					true,				// include case management
					superUser, false, 	// TODO include HXL
					audit,
					tz,
					false,				// mgmt
					false,				// Accuracy and Altitude
					true		// Server calculates
					);

			if (mgmt) {
				CustomReportsManager crm = new CustomReportsManager();
				ReportConfig config = crm.get(sd, managedId, -1);
				columns.addAll(config.columns);
			}

			boolean colHeadingAdded = false;
			for (int i = 0; i < columns.size(); i++) {
				TableColumn c = columns.get(i);
				
				if (!c.column_name.equals("_audit")) {
					if (colHeadingAdded) {
						columnHeadings.append(",");
					}
					colHeadingAdded = true;
					if(c.type != null && (c.type.equals("select") || c.type.equals("rank")) && c.compressed && !mergeSelectMultiple) {
						// Split the select multiple into its choices
						int idx = 0;
						for(KeyValue kv: c.choices) {
							if(idx++ > 0) {
								columnHeadings.append(",");
							}
							String choiceName = null;
							if(c.type.equals("rank")) {
								choiceName = c.column_name + " - " + idx;
							} else {
								if(c.selectDisplayNames) {
									choiceName = kv.v;
								} else {
									choiceName = c.column_name + " - " + kv.v;
								}
							}
							columnHeadings.append(choiceName);
							
						}
					} else {
						columnHeadings.append(c.displayName);
					}
				}
			}

			// Add the audit columns
			if(audit) {
				for (TableColumn c : columns) {
					if (includeInAudit(c.column_name)) {
						columnHeadings.append(",");
						columnHeadings.append(c.displayName);
						columnHeadings.append(" ");
						columnHeadings.append("(time ms)");
					}
				}
				for (TableColumn c : columns) {
					if (includeInAudit(c.column_name)) {
						columnHeadings.append(",");
						columnHeadings.append(c.displayName);
						columnHeadings.append(" ");
						columnHeadings.append("(lat)");
						columnHeadings.append(",");
						columnHeadings.append(c.displayName);
						columnHeadings.append(" ");
						columnHeadings.append("(lon)");
					}
				}
			}
			columnHeadings.append("\n");
			outWriter.print(columnHeadings.toString());

			if (GeneralUtilityMethods.tableExists(cResults, table_name)) {

				TableDataManager tdm = new TableDataManager(localisation, tz);

				pstmt = tdm.getPreparedStatement(sd, 
						cResults, 
						columns, 
						urlprefix,
						attachmentPrefix,
						sId, 
						sIdent,
						0,		// Sub form Id, only needed if _assigned has to be retrieved
						table_name, parkey, hrk,
						remoteUser, 
						null,		// roles (for anonymous calls)
						sort, dirn, mgmt, group, isDt, start, 
						getParkey, 
						start_parkey,
						superUser, 
						false, 		// Return records greater than or equal to primary key
						include_bad,
						"yes",			// return completed
						null,			// case management settings can be null
						null,
						null	,	// key filter
						tz,
						null	,	// instance id
						filter,	// advanced filter
						null,	// Date filter name
						null,	// Start date
						null		// End date
						);

				if(pstmt != null) {
					log.info("Get CSV data: " + pstmt.toString());
					HashMap<String, AuditItem> auditData = null;
					Type auditItemType = new TypeToken<HashMap<String, AuditItem>>() {}.getType();
					Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
	
					sd.setAutoCommit(false);		// page the results to reduce memory usage
					pstmt.setFetchSize(100);	
					
					rs = pstmt.executeQuery();
	
					int index = 0;
					while (rs.next()) {
	
						if (limit > 0 && index >= limit) {
							break;
						}
						index++;
	
						record = new StringBuffer();
	
						// Add the standard data
						for (int i = 0; i < columns.size(); i++) {
							TableColumn c = columns.get(i);
	
							String val = rs.getString(i + 1);
							if (val == null) {
								val = "";
							}
							if (!c.column_name.equals("_audit")) {
								if (i > 0) {
									record.append(",");
								}
								if(c.type != null && (c.type.equals("select") || c.type.equals("rank")) && c.compressed && !mergeSelectMultiple) {
									// Split the select multiple into its choices
									
									String[] selected = {""};
									selected = val.split(" ");
									int idx = 0;
									for(KeyValue kv: c.choices) {
										boolean addChoice = false;
										for(String selValue : selected) {
											if(selValue.equals(kv.k)) {
												addChoice = true;
												break;
											}	
										}
										if(idx++ > 0) {
											record.append(",");
										}
										String choiceValue = addChoice ? "1" : "0";
										record.append("\"" + choiceValue + "\"");
										
									}
								} else if (c.type != null && c.type.equals("select1") && c.selectDisplayNames) {
									// Convert value to display name
									for(KeyValue kv: c.choices) {
										if(kv.k.equals(val)) {
											val = kv.v;
											break;
										}
									}
									record.append("\"" + val.replaceAll("\"", "\"\"") + "\"");
								} else {
									record.append("\"" + val.replaceAll("\"", "\"\"") + "\"");
								}
							} else {
								auditData = gson.fromJson(val, auditItemType);
							}
						}
	
						// Add the audit data
						if (audit && auditData != null) {
							for (TableColumn c : columns) {
								if (includeInAudit(c.column_name)) {
									record.append(",");
									AuditItem item = auditData.get(c.column_name);
									if(item != null) {
										record.append(item.time);
									}
								}
							}
							
							for (TableColumn c : columns) {
								if (includeInAudit(c.column_name)) {
									record.append(",");
									AuditItem item = auditData.get(c.column_name);
									if(item != null && item.location != null) {
										record.append(item.location.lat);
										record.append(",");
										record.append(item.location.lon);
									}
								}
							}
						}
	
						record.append("\n");
						outWriter.print(record.toString());
	
					}
					
					sd.setAutoCommit(true);		// page the results to reduce memory
	
				} else {
					outWriter.print(localisation.getString("msg_no_data"));
				}
			}

		} catch(ApplicationException ae) {
			
			try {sd.setAutoCommit(true);} catch(Exception ex) {};
			response.setContentType("text/plain");
			response.setStatus(429);
			response.getWriter().append(ae.getMessage());
			log.info(ae.getMessage());
			
		} catch (Exception e) {
			try {sd.setAutoCommit(true);} catch(Exception ex) {};
			log.log(Level.SEVERE, "Exception", e);
			outWriter.print(e.getMessage());
			
		} finally {

			outWriter.flush();
			outWriter.close();
			
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			try {if (pstmtGetMainForm != null) {	pstmtGetMainForm.close();}} catch (SQLException e) {}
			try {if (pstmtGetForm != null) {	pstmtGetForm.close();}} catch (SQLException e) {}
			try {if (pstmtGetManagedId != null) {pstmtGetManagedId.close();}} catch (SQLException e) {}

			ResultsDataSource.closeConnection(connectionString, cResults);
			SDDataSource.closeConnection(connectionString, sd);
		}

	}
	
	private boolean includeInAudit(String name) {
		boolean include = true;
		
		if(name.startsWith("_")) {
			include  = false;
		} else if(name.equals("prikey") || name.equals("parkey") || name.equals("instanceid") || name.equals("instancename")) {
			include = false;
		}
		return include;
	}
}
