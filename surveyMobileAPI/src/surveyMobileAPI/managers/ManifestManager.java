package surveyMobileAPI.managers;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;
import org.smap.sdal.managers.ExternalFileManager;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.managers.TranslationManager;
import org.smap.sdal.model.CustomUserReference;
import org.smap.sdal.model.ManifestValue;
import org.smap.sdal.model.Survey;

import surveyMobileAPI.FormsManifest;

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
 * This class supports access to unique information in the database
 * All surveys in a bundle share the same unique key
 */
public class ManifestManager {
	
	Authorise a = new Authorise(null, Authorise.ENUM);
	private static Logger log = Logger.getLogger(FormsManifest.class.getName());

	
	/*
	 * Process a submission
	 */
	public String getManifest(@Context HttpServletRequest request, 
			HttpServletResponse resp,
			String key) {
		
		String host = request.getServerName();
		int portNumber = request.getLocalPort();
		String javaRosaVersion = request.getHeader("X-OpenRosa-Version");
		String protocol = "";
		StringBuilder responseStr = new StringBuilder();
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection("surveyMobileAPI-FormsManifest");
		String user = request.getRemoteUser();
		// If the user is still null try token authentication
		if(user == null) {
			try {
				user = GeneralUtilityMethods.getUserFromRequestKey(sd, request, "app");
			} catch (Exception e) {
				log.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		a.isAuthorised(sd, user);
		
		boolean superUser = false;
		ResourceBundle localisation = null;
		SurveyManager sm = null;
		Survey survey = null;
		
		try {
			superUser = GeneralUtilityMethods.isSuperUser(sd, user);
			// Get the users locale
			Locale locale = new Locale(GeneralUtilityMethods.getUserLanguage(sd, request, user));
			localisation = ResourceBundle.getBundle("org.smap.sdal.resources.SmapResources", locale);
			
			sm = new SurveyManager(localisation, "UTC");
			survey = sm.getSurveyId(sd, key);	// Get the survey id from the templateName / key
		} catch (Exception e) {
		}
		a.isValidSurvey(sd, user, survey.surveyData.id, false, superUser);	// Validate that the user can access this survey
		// End Authorisation
		
		if(portNumber == 443) {
			protocol = "https://";
		} else {
			protocol = "http://";
		}

		PreparedStatement pstmt = null;
		try {
			
			if(key == null) {
				throw new Exception("Error: Missing Parameter key");
			}			

			if(javaRosaVersion != null) {
				resp.setHeader("X-OpenRosa-Version", javaRosaVersion);
			} 

			responseStr.append("<manifest xmlns=\"http://openrosa.org/xforms/xformsManifest\">\n");

			/*
			 * Get the per-question and per-option media files from the translation table
			 */
 			String basepath = GeneralUtilityMethods.getBasePath(request);
			TranslationManager translationMgr = new TranslationManager();
			
			/*
			 * Get the manifest list setting the URLS as per device access
			 * Device access to URLs is authenticated using Basic authentication
			 * Hence all URLs must point to an end point that does that
			 */
			List<ManifestValue> manifestList = translationMgr.
					getManifestBySurvey(sd, user, survey.surveyData.id, basepath, 
							key, true);
			
			String sIdent = GeneralUtilityMethods.getSurveyIdent(sd, survey.surveyData.id);
			for( ManifestValue m : manifestList) {

				String filepath = null;

				if(m.type.equals("linked")) {
					ExternalFileManager efm = new ExternalFileManager(null);
					CustomUserReference cur = GeneralUtilityMethods.hasCustomUserReferenceData(sd, m.linkedSurveyIdent);
					filepath = efm.getLinkedPhysicalFilePath(sd, 
							efm.getLinkedLogicalFilePath(efm.getLinkedDirPath(basepath, sIdent, user, cur.needCustomFile()), m.fileName)) 
							+ ".csv";
					m.fileName += ".csv";
				} else {
					filepath = m.filePath;
				}
				
				// Check that the file exists
				if(filepath != null) {
					File f = new File(filepath);
					
					if(f.exists()) {
						// Get the MD5 hash
						String md5 = GeneralUtilityMethods.getMd5(filepath);
						
						String fullUrl = protocol + host + m.url;
		
						responseStr.append("<mediaFile>\n");
						responseStr.append("<filename>" + m.fileName + "</filename>\n");
						responseStr.append("<hash>" + md5 + "</hash>\n");
						responseStr.append("<downloadUrl>" + fullUrl + "</downloadUrl>\n");
						responseStr.append("</mediaFile>");
					} else {
						log.info("Error: " + filepath + " not found");
					}
				} else {
					log.info("Error: Manifest file path is null: " + m.type + " : " + m.fileName + " : " + m.filePath);
				}
					
			}
			responseStr.append("</manifest>\n");
			
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			try {if (pstmt != null) {pstmt.close();}} catch (SQLException e) {}
			
			SDDataSource.closeConnection("surveyMobileAPI-FormsManifest", sd);
		}		

		return responseStr.toString();
	
	}
	
	
}
