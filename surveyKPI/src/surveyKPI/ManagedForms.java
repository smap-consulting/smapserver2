package surveyKPI;

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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.Utilities.ServerSettings;
import org.smap.sdal.legacy.SurveyTemplate;
import org.smap.sdal.legacy.TableManager;
import org.smap.sdal.managers.ActionManager;
import org.smap.sdal.managers.CaseManager;
import org.smap.sdal.managers.EmailManager;
import org.smap.sdal.managers.SurveyViewManager;
import org.smap.sdal.model.Action;
import org.smap.sdal.model.ActionLink;
import org.smap.sdal.model.AutoUpdate;
import org.smap.sdal.model.Filter;
import org.smap.sdal.model.Form;
import org.smap.sdal.model.ManagedFormItem;
import org.smap.sdal.model.Organisation;
import org.smap.sdal.model.Role;
import org.smap.sdal.model.SurveyViewDefn;
import org.smap.sdal.model.TableColumn;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Get the questions in the top level form for the requested survey
 */
@Path("/managed")
public class ManagedForms extends Application {
	
	Authorise a = null;
	Authorise aBulk = null;
	Authorise aSuper = new Authorise(null, Authorise.ANALYST);
	Authorise aAdmin = new Authorise(null, Authorise.ADMIN);
	
	boolean forDevice = false;	// URL prefixes should be in the client format
	
	private static Logger log =
			 Logger.getLogger(Review.class.getName());
	
	public ManagedForms() {
		
		ArrayList<String> authorisations = new ArrayList<String> ();	
		authorisations.add(Authorise.ANALYST);
		authorisations.add(Authorise.MANAGE);		// Enumerators with MANAGE access can process managed forms
		a = new Authorise(authorisations, null);
		
		ArrayList<String> bulkAuthorisations = new ArrayList<String> ();	
		bulkAuthorisations.add(Authorise.ANALYST);
		bulkAuthorisations.add(Authorise.ADMIN);
		bulkAuthorisations.add(Authorise.SECURITY);
		aBulk = new Authorise(bulkAuthorisations, null);
	}

	/*
	 * Return the surveys in the project along with their management information
	 */
	@GET
	@Path("/surveys/{pId}")
	@Produces("application/json")
	public Response getSurveys(@Context HttpServletRequest request,
			@PathParam("pId") int pId) { 
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-GetManagedForms");
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidProject(sd, request.getRemoteUser(), pId);
		// End Authorisation
		
		Response response = null;
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";
			
			SurveyViewManager mf = new SurveyViewManager(localisation, tz);
			ArrayList<ManagedFormItem> items = mf.getManagedForms(sd, pId);
			response = Response.ok(gson.toJson(items)).build();
		
				
		} catch (Exception e) {
			log.log(Level.SEVERE, "SQL Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();			
		} finally {
			SDDataSource.closeConnection("surveyKPI-GetManagedForms", sd);
		}

		return response;
	}
	
	/*
	 * Update a managed record from the managed forms page
	 */
	@POST
	@Produces("text/html")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/update_gs/{sId}/{groupSurvey}")
	public Response updateManagedRecordGroupSurvey(
			@Context HttpServletRequest request, 
			@PathParam("sId") int sId,
			@PathParam("groupSurvey") String groupSurvey,
			@FormParam("groupForm") String groupForm,
			@FormParam("updates") String updatesString,
			@FormParam("instanceid") String instanceid,
			@FormParam("prikey") int prikey,	// Needed for sub forms
			@FormParam("bulkInstances") String bulkInstanceString,
			@FormParam("tz") String tz
			) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String requester = "surveyKPI-UpdateManagedRecord";
		
		// Get records to update
		ArrayList<String> instances = null;
		boolean bulk = false;
		if(bulkInstanceString != null) {
			Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
			instances = gson.fromJson(bulkInstanceString,  new TypeToken<ArrayList<String>>() {}.getType());
			if(instances.size() > 1) {
				bulk = true;
			}
		}
					
					
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requester);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		if(bulk) {
			aBulk.isAuthorised(sd, request.getRemoteUser());
		} else {
			a.isAuthorised(sd, request.getRemoteUser());
		}
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		a.isValidOversightSurvey(sd, request.getRemoteUser(), sId, groupSurvey);
		// End Authorisation

		Connection cResults = ResultsDataSource.getConnection(requester);
		
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			if(tz == null) {
				tz = "UTC";
			}
			
			ActionManager am = new ActionManager(localisation, tz);
			
			int subFormPrimaryKey = 0;
			if(instances == null || instances.size() == 0) {
				if(instanceid != null) {
					instances = new ArrayList<> ();
					instances.add(instanceid);
					/*
					 * In the case of a single edit we can also update subforms hence add the primary key
					 */
					subFormPrimaryKey = prikey;
				}
			}
			if(instances == null || instances.size() == 0) {
				throw(new ApplicationException("No instances to bulk update"));
			}
			String urlprefix = GeneralUtilityMethods.getUrlPrefix(request);
			for(String instance : instances) {
				response = am.processUpdateGroupSurvey(request, sd, cResults, 
						request.getRemoteUser(), sId, instance, subFormPrimaryKey, 
						groupSurvey, groupForm, updatesString, bulk,
						urlprefix);
			}
			
			
			GeneralUtilityMethods.clearLinkedForms(sd, sId, localisation);  // Clear any entries in linked_forms for this survey - The CSV files will need to be refreshed

		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);   // log the error but otherwise ignore
		} finally {
			
			SDDataSource.closeConnection(requester, sd);
			ResultsDataSource.closeConnection(requester, cResults);
			
		}
		
		return response;

	}
	
	/*
	 * Lock a record for editing
	 */
	@POST
	@Produces("text/html")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/lock/{sId}")
	public Response lockManagedRecord(
			@Context HttpServletRequest request, 
			@PathParam("sId") int sId,
			@FormParam("record") String instanceId
			) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String requester = "surveyKPI - lockManagedRecord";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requester);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		Connection cResults = ResultsDataSource.getConnection(requester);
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tableName = GeneralUtilityMethods.getMainResultsTable(sd, cResults, sId);
			if(tableName != null) {
				
				String surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
				
				if(instanceId != null) {
					CaseManager cm = new CaseManager(localisation);
					int count = cm.assignRecord(sd, cResults, localisation, tableName, instanceId, request.getRemoteUser(), "lock", 
							surveyIdent, null, request.getRemoteUser());
					if(count == 0) {
						response = Response.serverError().entity(localisation.getString("mf_aa")).build();
					} else {
						response = Response.ok().build();
					}
				} else {
					response = Response.serverError().entity(localisation.getString("mf_nf")).build();
				}
			} else {
				response = Response.serverError().entity(localisation.getString("mf_nf")).build();
			}
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, e.getMessage(), e);   
		} finally {
			
			SDDataSource.closeConnection(requester, sd);
			ResultsDataSource.closeConnection(requester, cResults);
			
		}
		
		return response;

	}
	
	/*
	 * Assign a user
	 */
	@POST
	@Produces("text/html")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/assign/{sId}/{user}")
	public Response assignManagedRecord(
			@Context HttpServletRequest request, 
			@PathParam("sId") int sId,
			@PathParam("user") String uIdent,
			@FormParam("record") String instanceId
			) throws SQLException { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String requester = "surveyKPI - assignManagedRecord";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requester);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		if(!uIdent.equals("_none")) {
			a.isValidUser(sd, request.getRemoteUser(), GeneralUtilityMethods.getUserId(sd, uIdent));
		}
		// End Authorisation
		
		ServerSettings.setBasePath(request);
		
		Connection cResults = ResultsDataSource.getConnection(requester);
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			String surveyName = GeneralUtilityMethods.getSurveyName(sd, sId);
			String tableName = GeneralUtilityMethods.getMainResultsTable(sd, cResults, sId);
			int oId = GeneralUtilityMethods.getOrganisationIdForSurvey(sd, sId);		
			Organisation organisation = GeneralUtilityMethods.getOrganisation(sd, oId);
			if(tableName != null) {
				
				CaseManager cm = new CaseManager(localisation);
				int count = cm.assignRecord(sd, cResults, localisation, tableName, instanceId, uIdent, "assign", 
						surveyIdent, null, request.getRemoteUser());
				if(count == 0) {
					response = Response.serverError().entity(localisation.getString("mf_nf")).build();
				} else {
					response = Response.ok().build();
				}
				
				/*
				 * Send an email to the assigned user
				 */
				if(organisation.can_notify) {
					
					String content = localisation.getString("mf_ca2");
					content = content.replace("%s1", surveyName);
					
					EmailManager em = new EmailManager(localisation);
					String emails = em.getAssignedUserEmails(sd, cResults, sId, instanceId);
					em.sendEmails(sd, cResults, log, emails, organisation, sId, 
							null, 			// log content
							null, 			// doc URL
							null,			// name of a notification
							null,			// Unsubscribed list
							null, 			// filepath
							null, 			// filename
							0,				// message Id
							false, 			// Create Pending
							"none",			// Topic
							request.getRemoteUser(), 
							request.getServerName(),
							null,			// Survey Name 
							null,			// Project Name
							localisation.getString("mf_ca"),	// Subject
							null, 								// from
							content, 							// Message content
							null,								// Case reference
							request.getScheme(), 
							null								// Submission message
							);
				}
				
			} else {
				response = Response.serverError().entity(localisation.getString("mf_nf")).build();
			}
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, e.getMessage(), e);   
		} finally {
			
			SDDataSource.closeConnection(requester, sd);
			ResultsDataSource.closeConnection(requester, cResults);
			
		}
		
		return response;

	}
	
	/*
	 * Release a record
	 */
	@POST
	@Produces("text/html")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/release/{sId}")
	public Response releaseManagedRecord(
			@Context HttpServletRequest request, 
			@PathParam("sId") int sId,
			@FormParam("record") String instanceId
			) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		String requester = "surveyKPI - releaseManagedRecord";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(requester);
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		Connection cResults = ResultsDataSource.getConnection(requester);
		try {
			// Localisation			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tableName = GeneralUtilityMethods.getMainResultsTable(sd, cResults, sId);
			if(tableName != null) {
				CaseManager cm = new CaseManager(localisation);
				int count = cm.assignRecord(sd, cResults, localisation, tableName, instanceId, 
						request.getRemoteUser(), "release", null, null, request.getRemoteUser());
				if(count == 0) {
					response = Response.serverError().entity(localisation.getString("mf_nf")).build();
				} else {
					response = Response.ok().build();
				}
			} else {
				response = Response.serverError().entity(localisation.getString("mf_nf")).build();
			}
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE, e.getMessage(), e);   
		} finally {
			
			SDDataSource.closeConnection(requester, sd);
			ResultsDataSource.closeConnection(requester, cResults);
			
		}
		
		return response;

	}
	
	
	/*
	 * Make a survey managed
	 * Deprecate
	 */
	class AddManaged {
		int sId;
		int manageId;
	}
	
	@POST
	@Produces("text/html")
	@Consumes("application/json")
	@Path("/add")
	public Response setManaged(
			@Context HttpServletRequest request, 
			@FormParam("settings") String settings
			) { 
		
		// Check for Ajax and reject if not
		if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With")) ){
			log.info("Error: Non ajax request");
	        throw new AuthorisationException();   
		} 
		
		Response response = null;
		
		Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
		AddManaged am = gson.fromJson(settings, AddManaged.class);
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-managedForms");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		aSuper.isAuthorised(sd, request.getRemoteUser());
		aSuper.isValidSurvey(sd, request.getRemoteUser(), am.sId, false, superUser);
		// End Authorisation

		String sql = "update survey set managed_id = ? where s_id = ?;";
		PreparedStatement pstmt = null;
		
		Connection cResults = ResultsDataSource.getConnection("surveyKPI-Add Managed Forms");
		
		try {
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());

			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";
			
			Form f = GeneralUtilityMethods.getTopLevelForm(sd, am.sId);	// Get the table name of the top level form
			TableManager tm = new TableManager(localisation, tz);
			
			// 0. Ensure that the form data columns are fully published, don't add managed columns at this stage
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, am.sId);
			SurveyTemplate template = new SurveyTemplate(localisation);
			template.readDatabase(sd, cResults, sIdent, false);	
			if(am.manageId > 0) {
			
				ArrayList<String> tablesCreated = tm.writeAllTableStructures(sd, cResults, am.sId, template,  0);
				
				// Apply any updates that have been made to the table structure since the last submission
				boolean tableChanged = tm.applyTableChanges(sd, cResults, am.sId, tablesCreated);
				
				//boolean tableChanged = tm.createTable(cResults, sd, f.tableName, sIdent, am.sId, 0);
				// Add any previously unpublished columns not in a changeset (Occurs if this is a new survey sharing an existing table)
				boolean tablePublished = tm.addUnpublishedColumns(sd, cResults, am.sId, f.tableName);			
				if(tableChanged || tablePublished) {
					List<org.smap.sdal.legacy.Form> forms = template.getAllForms();	
					for(org.smap.sdal.legacy.Form form : forms) {
						tm.markPublished(sd, form.getId(), am.sId);		// only mark published if there have been changes made
					}
				}
			}
			
			// 1. Check that the managed form is compatible with the survey
			if(am.manageId > 0) {
				String compatibleMsg = compatibleManagedForm(sd, cResults, localisation, am.sId, 
						am.manageId, request.getRemoteUser(), oId, superUser, tz);
				if(compatibleMsg != null) {
					throw new Exception(localisation.getString("mf_nc") + " " + compatibleMsg);
				}
			}
			
			// 2. Add the management id to the survey record
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, am.manageId);
			pstmt.setInt(2, am.sId);
			log.info("Adding managed survey: " + pstmt.toString());
			pstmt.executeUpdate();
			
			// 3. Create results tables if they do not exist
			if(am.manageId > 0) {
				
				tm.writeAllTableStructures(sd, cResults, am.sId, template,  am.manageId);
				
				//boolean tableChanged = tm.createTable(cResults, sd, f.tableName, sName, am.sId, am.manageId);
				// Add any previously unpublished columns not in a changeset (Occurs if this is a new survey sharing an existing table)
				//boolean tablePublished = tm.addUnpublishedColumns(sd, cResults, am.sId, f.tableName);			
				//if(tableChanged || tablePublished) {
				//	tm.markPublished(sd, am.sId);		// only mark published if there have been changes made
				//}
			}
			
			// 4. Clear the auto update indicators if the managed form is being removed
			if(am.manageId == 0) {
				GeneralUtilityMethods.setAutoUpdates(sd, am.sId, am.manageId, null);
			}
			
			response = Response.ok().build();
				
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			
			try {if (pstmt != null) {pstmt.close();}} catch (Exception e) {}
			
			SDDataSource.closeConnection("surveyKPI-managedForms", sd);	
			ResultsDataSource.closeConnection("surveyKPI-Add Managed Forms", cResults);
		}
		
		return response;
	}
	
	/*
	 * Verify that
	 *  1. Calculations in the managed form refer to questions in either the managed form or the form
	 *     we are attaching to
	 *     
	 *  Also add details on the auto updates to the survey so that they can be readily applied on changes to that
	 *  survey.
	 */
	private String compatibleManagedForm(Connection sd, Connection cResults, ResourceBundle localisation, int sId, 
			int managedId,
			String user,
			int oId,
			boolean superUser,
			String tz) {
		
		StringBuffer compatibleMsg = new StringBuffer("");
		ArrayList<AutoUpdate> autoUpdates = new ArrayList<AutoUpdate> ();
		String language = "none";
			
		if(managedId > 0 && sId > 0) {
				
			try {	
				SurveyViewDefn svd = new SurveyViewDefn();
				SurveyViewManager qm = new SurveyViewManager(localisation, tz);
				qm.getDataProcessingConfig(sd, managedId, svd, null, oId);
					
				org.smap.sdal.model.Form f = GeneralUtilityMethods.getTopLevelForm(sd, sId);	// Get the table name of the top level form		
				String surveyIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
				ArrayList<TableColumn> formColumns = GeneralUtilityMethods.getColumnsInForm(sd, 
						cResults,
						localisation,
						language,
						sId,
						surveyIdent,
						user,
						null,	// Roles to apply
						0,
						f.id, 
						f.tableName, 
						false, 
						false, 
						false, 
						true, 	// include instance id
						true,	// Include prikey
						true,	// Include HRK
						true,	// Include other meta data
						true,	// Include preloads
						true,	// Include instancename
						false,	// Include survey duration
						true,	// Include case management
						superUser,
						false,		// HXL only include with XLS exports
						false,		// Don't include audit data
						tz,
						false,		// mgmt
						false,		// Accuracy and Altitude
						true		// Server calculates
						);
				
				for(TableColumn mc : svd.columns) {
					
					if(mc.type.equals("calculate")) {
						
						for(int i = 0; i < mc.calculation.columns.size(); i++) {
							String refColumn = mc.calculation.columns.get(i);
							TableColumn refDetails = new TableColumn();
							compatibleMsg.append(getCompatabilityMsg(refColumn, svd, formColumns, localisation, refDetails));
						}		
					}
					
					if(mc.parameters != null && mc.parameters.get("source") != null) {
						String refColumn = mc.parameters.get("source");
						TableColumn refDetails = new TableColumn();
						compatibleMsg.append(getCompatabilityMsg(refColumn, svd, formColumns, localisation, refDetails));
						
						if(refDetails.type != null && refDetails.type.equals("image") && mc.parameters.get("auto") != null && mc.parameters.get("auto").equals("yes")) {
							AutoUpdate au = new AutoUpdate("imagelabel");
							au.labelColType = "text";
							au.sourceColName = refColumn;
							au.targetColName = mc.column_name;
							au.tableName =f.tableName;
							autoUpdates.add(au);
						}
					}		
				}
				
				if(compatibleMsg.length() > 0) {
					autoUpdates = null;		// Managed form is not compatible and will be rejected
				} else if(autoUpdates != null && autoUpdates.isEmpty()) {
					autoUpdates = null;
				}
				GeneralUtilityMethods.setAutoUpdates(sd, sId, managedId, autoUpdates);
				
				
				
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
				compatibleMsg.append(e.getMessage());
			}
		}
		
		if(compatibleMsg.length() == 0) {
			return null;
		} else {
			return compatibleMsg.toString();
		}

	}
	
	/*
	 * Get a compatability error message if the referenced column is not in the form
	 */
	String getCompatabilityMsg(String refColumn, SurveyViewDefn svd, 
			ArrayList<TableColumn> formColumns, 
			ResourceBundle localisation,
			TableColumn refDetails) {
		String msg = "";
		
		boolean referenceExists = false;
		
		// Check to see if the referenced column is in the managed form
		for(TableColumn mc2 : svd.columns) {
			if(refColumn.equals(mc2.column_name)) {
				refDetails.type = mc2.type;
				referenceExists = true;
				break;
			}
		}
		
		// Check to see if the referenced column is in the form that is being attached to
		if(!referenceExists) {
			for(TableColumn fc2 : formColumns) {
				if(refColumn.equals(fc2.question_name)) {
					refDetails.type = fc2.type;
					referenceExists = true;
					break;
				}
			}
		}
		
		// Report the missing reference
		if(!referenceExists) {
			msg = localisation.getString("mf_col") + " " + 
					refColumn + " " + localisation.getString("mf_cninc") + "\n";
		}
		
		return msg;
	}
	
	/*
	 * Get link to an action without creating an alert
	 */
	@GET
	@Produces("application/json")
	@Path("/actionlink/{sId}/{prikey}")
	public Response getActionLink(
			@Context HttpServletRequest request, 
			@PathParam("sId") int sId,
			@PathParam("prikey") int prikey,
			@QueryParam("roles") String roles,
			@QueryParam("groupSurvey") String groupSurvey
			) { 
		
		Response response = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-Get Action Link");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		if(groupSurvey != null) {
			a.isValidOversightSurvey(sd, request.getRemoteUser(), sId, groupSurvey);
		}
		// End Authorisation
		
		try {

			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			String tz = "UTC";
			
			int oId = GeneralUtilityMethods.getOrganisationId(sd, request.getRemoteUser());
			int pId = GeneralUtilityMethods.getProjectId(sd, sId);
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, sId);
			ActionManager am = new ActionManager(localisation, tz);
			Action action = new Action("respond");
			action.surveyIdent = sIdent;
			action.groupSurvey = groupSurvey;
			action.prikey = prikey;
			action.pId = pId;
			
			
			if(roles != null) {
				String [] rArray = roles.split(",");
				if(rArray.length > 0) {
					action.roles = new ArrayList<Role> ();
					for (int i = 0; i < rArray.length; i++) {
						Role r = new Role();
						try {
							r.id = Integer.parseInt(rArray[i]);
							action.roles.add(r);
						} catch (Exception e) {
							log.info("Error: Invalid Role Id: " + rArray[i] + " : " + e.getMessage());
						}
					}
				}
			}
			
			log.info("Creating action for prikey: " + prikey);
			ActionLink al = new ActionLink();
			al.link = request.getScheme() +
					"://" +
					request.getServerName() + 
					am.getLink(sd, action, oId, true);	// Single submission user
			
			Gson gson=  new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
			String resp = gson.toJson(al, ActionLink.class);
			response = Response.ok(resp).build();
				
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			
			SDDataSource.closeConnection("surveyKPI-Get Action Link", sd);
		}
		
		return response;
	}
	
	
	/*
	 * Get the configuration settings
	 */
	@GET
	@Produces("application/json")
	@Path("/getreportconfig/{sId}/{key}")
	public Response getManageConfig(
			@Context HttpServletRequest request, 
			@PathParam("sId") int sId,
			@PathParam("key") String key
			) { 
		
		Response response = null;
		
		String sql = "select settings from general_settings where u_id = ? and s_id = ? and key = ?;";
		PreparedStatement pstmt = null;
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-managedForms");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation
		
		try {

			int uId = GeneralUtilityMethods.getUserId(sd, request.getRemoteUser());	// Get user id
			
			pstmt = sd.prepareStatement(sql);
			pstmt.setInt(1, uId);
			pstmt.setInt(2, sId);
			pstmt.setString(3, key);
			
			log.info("Getting settings: " + pstmt.toString());
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				response = Response.ok(rs.getString(1)).build();
			} else {

				response = Response.ok().build();
			}
				
		} catch (Exception e) {
			response = Response.serverError().entity(e.getMessage()).build();
			log.log(Level.SEVERE,"Error", e);
		} finally {
			
			
			try {if (pstmt != null) {pstmt.close();}} catch (Exception e) {}
	
			SDDataSource.closeConnection("surveyKPI-managedForms", sd);
		}
		
		return response;
	}
	
	/*
	 * Get data connected to the passed in record
	 * Disabled due to issues with the accuracy of fingerprint matching
	@GET
	@Path("/connected/{sId}/{fId}/{prikey}")
	@Produces("application/json")
	public Response getLinks(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("fId") int fId,
			@PathParam("prikey") int prikey) { 
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-ManagedForms-getLinks");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		Response response = null;
		
		Connection cResults = ResultsDataSource.getConnection("surveyKPI-ManagedForms-getLinks");
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		try {
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			LinkageManager linkMgr = new LinkageManager(localisation);
			ArrayList<Link> links = linkMgr.getSurveyLinks(sd, cResults, sId, fId, prikey);
			response = Response.ok(gson.toJson(links)).build();
		
				
		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			
			SDDataSource.closeConnection("surveyKPI-ManagedForms-getLinks", sd);
			ResultsDataSource.closeConnection("surveyKPI-ManagedForms-getLinks", cResults);
		}


		return response;
	}
	*/

	
	/*
	 * Get the filterable data associated with a column in a results table
	 */
	@GET
	@Path("/filters/{sId}/{fId}/{colname}")
	@Produces("application/json")
	public Response getFilter(@Context HttpServletRequest request,
			@PathParam("sId") int sId,
			@PathParam("fId") int fId,
			@PathParam("colname") String colname) { 
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyKPI-QuestionsInForm");
		boolean superUser = false;
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		} catch (Exception e) {
		}
		a.isAuthorised(sd, request.getRemoteUser());
		a.isValidSurvey(sd, request.getRemoteUser(), sId, false, superUser);
		// End Authorisation

		Response response = null;
		Filter filter = new Filter();
		String tableName = null;
		int count = 0;
		final int MAX_VALUES = 10;
		
		colname = colname.replace("'", "''");	// Escape apostrophes
		
		// SQL to get the column type
		String sqlColType = "select data_type from information_schema.columns "
				+ "where table_name = ? "
				+ "and column_name = ?"; 
		PreparedStatement pstmtGetColType = null;
		
		PreparedStatement pstmt = null;
		PreparedStatement pstmtGetMin = null;
		PreparedStatement pstmtGetMax = null;
		PreparedStatement pstmtGetVals = null;
		
		Connection cResults = ResultsDataSource.getConnection("surveyKPI-filters");
		ResultSet rs = null;
		Gson gson=  new GsonBuilder().disableHtmlEscaping().setDateFormat("yyyy-MM-dd").create();
		try {

			// If the form id was not provided assume the top level form for the survey is required
			Form f = null;
			if(fId <= 0) {
				f = GeneralUtilityMethods.getTopLevelForm(sd, sId); // Get formId of top level form and its table name
			} else {
				f = GeneralUtilityMethods.getForm(sd, sId, fId);
			}
			fId = f.id;
			tableName = f.tableName;
			
			String sqlGetMax = "select max(" + colname + ") from " + tableName; 	// SQL to get max value
			String sqlGetMin = "select min(" + colname + ") from " + tableName; 	// SQL to get min value
			
			String sqlGetVals = "select distinct(" + colname + ") from " + tableName + 
					" order by " + colname + " asc"; 	// SQL to get distinct values
			
			/*
			 * Get the column type
			 */
			pstmtGetColType = cResults.prepareStatement(sqlColType);
			pstmtGetColType.setString(1, tableName);
			pstmtGetColType.setString(2, colname);
			rs = pstmtGetColType.executeQuery();
			if(!rs.next()) {
				throw new Exception("Unknown table " + tableName + " or column " + colname);
			} else {
				filter.qType = rs.getString(1);
			}
			rs.close();
			
			/*
			 * Get the count of distinct data values
			 */
			String sql = "select count(distinct " + colname + ") as n from " + tableName;
			pstmt = cResults.prepareStatement(sql);
			rs = pstmt.executeQuery();
			if(rs.next()) {
				count = rs.getInt(1);
			}
			rs.close();
			
			/*
			 * Get the range of valid values or a list of valid values
			 */
			pstmtGetMin = cResults.prepareStatement(sqlGetMin);
			pstmtGetMax = cResults.prepareStatement(sqlGetMax);
			
			if(filter.qType.equals("integer")) {
			
				if(count > MAX_VALUES) {
					
					filter.range = true;
					
					rs = pstmtGetMin.executeQuery();
					if(rs.next()) {
						filter.iMin = rs.getInt(1);
					}
					rs.close();
					
					rs = pstmtGetMax.executeQuery();
					if(rs.next()) {
						filter.iMax = rs.getInt(1);
					}
					rs.close();
				} else {
					pstmtGetVals = cResults.prepareStatement(sqlGetVals);
						
					rs = pstmtGetVals.executeQuery();
					filter.iValues = new ArrayList<Integer> ();
					while(rs.next()) {
						filter.iValues.add(rs.getInt(1));
					}
					rs.close();
						
				}
			} else if(filter.qType.equals("text")) {
				filter.search = true;
			} else if(filter.qType.startsWith("timestamp")) {
				filter.range = true;
				
				rs = pstmtGetMin.executeQuery();
				if(rs.next()) {
					filter.tMin = rs.getTimestamp(1);
				}
				rs.close();
				
				rs = pstmtGetMax.executeQuery();
				if(rs.next()) {
					filter.tMax = rs.getTimestamp(1);
				}
				rs.close();
				
			} else {
				filter.search = true;
			}
			
			response = Response.ok(gson.toJson(filter)).build();
		
				
		} catch (SQLException e) {
			log.log(Level.SEVERE, "SQL Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();			
		} catch (Exception e) {
			log.log(Level.SEVERE, "Error", e);
		    response = Response.serverError().entity(e.getMessage()).build();
		} finally {
			try {if (pstmt != null) {pstmt.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetColType != null) {pstmtGetColType.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetMin != null) {pstmtGetMin.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetMax != null) {pstmtGetMax.close();	}} catch (SQLException e) {	}
			try {if (pstmtGetVals != null) {pstmtGetVals.close();	}} catch (SQLException e) {	}
			
			SDDataSource.closeConnection("surveyKPI-QuestionsInForm", sd);
			ResultsDataSource.closeConnection("surveyKPI-QuestionsInForm", cResults);
		}

		return response;
	}
}

