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

package surveyKPI;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.smap.sdal.Utilities.AuthorisationException;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.ResultsDataSource;
import org.smap.sdal.Utilities.SDDataSource;

/*
 * Upload submissions from a folder.  A survey ident is specified and all submissions found in that folder are reapplied if they
 * have not already been applied.  This can happen if users were not authorised to submit results from a webform link because the link 
 * had expired and they cannot refresh their browsers.
 * 
 * This is a beta service added to address a production issue
 * Currently it should not be used until the following issues are addressed
 *   1.  A password has to be submitted in the URL which is not secure
 *   2.  submissions that include attachments are not supported
 *   
 * The final solution will be expanded to replace the smapUploader function to upload
 * submissions copied directly from a device.
 */

@Path("/uploadSubmissions")
public class Uploader extends Application {

	Authorise auth = null;

	private static Logger log = Logger.getLogger(Uploader.class.getName());

	public Uploader() {

		ArrayList<String> authorisations = new ArrayList<String>();
		authorisations.add(Authorise.MANAGE); // Enumerators with MANAGE access can process managed forms
		auth = new Authorise(authorisations, null);
	}
	

	/*
	 * Get data identified in an action
	 */
	@GET
	public Response uploadSubmissions(
			@Context HttpServletRequest request, 
			@Context HttpServletResponse response,
			@QueryParam("survey") String surveyIdent,
			@QueryParam("password") String password)
			throws IOException {

		Response responseVal = null;
		StringBuffer outputString = new StringBuffer();
		String requester = "surveyKPI-uploadSubmissions";

		Connection sd = SDDataSource.getConnection(requester);
		Connection cResults = ResultsDataSource.getConnection(requester);
		PreparedStatement pstmt = null;

		try {
			
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, request.getRemoteUser()));
			ResourceBundle localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);

			CloseableHttpClient httpclient = null;
			ContentType ct = null;
			String host_name = request.getServerName();
			HttpResponse reqResponse = null;
			int responseCode = 0;
			String responseReason = null;
			
			
			int port;
			String protocol = "https";
			port = 443;
			if(host_name.contains("localhost")) {
				protocol = "http";
				port = 80;
			}
			String user = request.getRemoteUser();
			

			
			
			String urlString = protocol + "://" + host_name +  "/submission";
			urlString += "?deviceID=uploader"; 
			
			String sql = "select count(*) from upload_event where file_path = ?";
			pstmt = sd.prepareStatement(sql);
			
			/*
			 * Process the directories
			 */
			String basePath = GeneralUtilityMethods.getBasePath(request);
			String folderPath = basePath + "/uploadedSurveys/" + surveyIdent;
			
			File folder = new File(folderPath);
			if(folder.exists()) {
				String dirs [] = folder.list();
				for(String dir : dirs) {
					try {
						HttpPost req = new HttpPost(URI.create(urlString));
						File instanceDir = new File(folderPath + "/" + dir);
						if(instanceDir.exists() && instanceDir.isDirectory()) {
							String[] files = instanceDir.list();
							
							for(String file : files) {
								if(file.endsWith(".xml")) {
									File instanceFile = new File(folderPath + "/" + dir + "/" + file);
									
									// check this instance has not already been submitted - When used to resubmit survey data this would create a loop of ever increasing duplicates
									pstmt.setString(1, instanceFile.getAbsolutePath());
									ResultSet rs = pstmt.executeQuery();
									if(rs.next() && rs.getInt(1) > 0) {
										log.info("Skipping as already uploaded: " + instanceFile.getName());
									} else {
										MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
										FileBody fb = new FileBody(instanceFile);
										entityBuilder.addPart("xml_submission_file", fb);	
										req.setEntity(entityBuilder.build());	// TODO do this after adding media
										
										log.info("	Info: submitting to: " + req.getURI().toString());
										HttpHost target = new HttpHost(host_name, port, protocol);
										CredentialsProvider credsProvider = new BasicCredentialsProvider();
										credsProvider.setCredentials(
												new AuthScope(target.getHostName(), target.getPort()),
												new UsernamePasswordCredentials(user, password));
										httpclient = HttpClients.custom()
												.setDefaultCredentialsProvider(credsProvider)
												.build();
										HttpClientContext localContext = HttpClientContext.create();
										
										reqResponse = httpclient.execute(target, req, localContext);
										responseCode = reqResponse.getStatusLine().getStatusCode();
										responseReason = reqResponse.getStatusLine().getReasonPhrase(); 
				
										
										// verify that the response was a 201 or 202.
										// If it wasn't, the submission has failed.
										log.info("	Info: Response code: " + responseCode + " : " + responseReason);
										if (responseCode != HttpStatus.SC_CREATED && responseCode != HttpStatus.SC_ACCEPTED) {      
											log.info("	Error: local upload failed: " + dir + " : " + responseCode + ":" + responseReason);
										} else {
											log.info("  Success: local upload sent: " + dir);
										}
										
									}
								} else {
									// TODO
									throw new Exception("Error:  Not processing attachments");
								}
							}
						} else {
							log.info("Ignoring file: " + dir);
						}
						
					} catch (Exception e) {
						log.log(Level.SEVERE, dir, e);
					} finally {
						try {
							if(httpclient != null) {
								httpclient.close();
							}
						} catch (Exception e) {
							
						}
					}
				}
			} else {
				throw new Exception("Submission folder not found");
			}
			
			responseVal = Response.status(Status.OK).entity(outputString.toString()).build();
		} catch (AuthorisationException e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			throw e;
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
			responseVal = Response.status(Status.OK).entity(e.getMessage()).build();
		} finally {
			if(pstmt != null) {try{pstmt.close();}catch(Exception e) {}};
			SDDataSource.closeConnection(requester, sd);
			ResultsDataSource.closeConnection(requester, cResults);
		}

		return responseVal;
	}


}
