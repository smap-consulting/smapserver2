package managers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.ResourceBundle;

import javax.servlet.http.HttpServletRequest;

import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.SurveyManager;
import org.smap.sdal.model.DataEndPoint;
import org.smap.sdal.model.Survey;

public class AuditManager {


	private ResourceBundle localisation;
	public AuditManager(ResourceBundle l) {
		localisation = l;
	}
	public ArrayList<DataEndPoint> getDataEndPoints(Connection sd, 
			HttpServletRequest request,
			boolean csv,
			String urlprefix) throws SQLException {
		
		ArrayList<DataEndPoint> data = new ArrayList<DataEndPoint> ();
		
		/*
		 * Use existing survey manager call to get a list of surveys that the user can access
		 */
		ArrayList<Survey> surveys = null;	
		SurveyManager sm = new SurveyManager(localisation, "UTC");
		boolean superUser = GeneralUtilityMethods.isSuperUser(sd, request.getRemoteUser());
		surveys = sm.getSurveysAndForms(sd, request.getRemoteUser(), superUser);
		
		if(csv) {
			urlprefix += "/api/v1/audit.csv/";	
		} else {
			urlprefix += "/api/v1/audit/";
		}
		
		for(Survey s: surveys) {
			DataEndPoint dep = new DataEndPoint();
			dep.id = s.surveyData.id;
			dep.id_string = s.surveyData.ident;
			dep.title = s.surveyData.displayName;
			dep.description = s.surveyData.displayName;
			dep.url = urlprefix + dep.id_string;
	
			data.add(dep);
		}
		
		return data;
	}
}
