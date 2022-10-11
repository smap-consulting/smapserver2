package org.smap.sdal.managers;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.ResourceBundle;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.UtilityMethodsEmail;
import org.smap.sdal.model.Option;
import org.smap.sdal.model.Question;

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

public class ChoiceManager {

	private ResourceBundle localisation;
	private String tz;
	
	public ChoiceManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}
	
	
	public String getLabel(Connection sd, Connection cResults, String user, 
			int oId, int sId, int qId, int l_id, boolean external_choices, String external_table, String languageName,
			int languageIdx,
			ArrayList<String> matches,
			String surveyIdent,
			boolean getImage) throws Exception {
		
		StringBuffer labels = new StringBuffer("");
		
		// Only check the labels if the pdfvalue appearance is not set
		Question q = GeneralUtilityMethods.getQuestion(sd,  qId);
		boolean checkLabels = true;
		if(q != null && q.appearance != null && q.appearance.contains("pdfvalue")) {
			
			String [] appValues = q.appearance.split(" ");	// Make sure the apperance value is exactly pdfvalue
			for(int i = 0; i < appValues.length; i++) {
				String app = appValues[i].trim().toLowerCase();
				if(app.equals("pdfvalue")) {
					checkLabels = false;
				}
			}
		}
		if(checkLabels) {
			
			/*
			 * 1. Search choices which are stored in the survey meta definition
			 * If the choices are external then only numeric values are checked
			 */			 
			for(String match : matches) {
				
				boolean foundStatic = false;
				boolean isInteger = false;
				try {
					Integer.parseInt(match);
					isInteger = true;
				} catch (Exception e) {
					
				}

				// Try for static choices first	
				if(!external_choices || isInteger) {
					String label = UtilityMethodsEmail.getSingleLabel(sd, sId, languageName, l_id, match, getImage);
					if(label != null) {
						if(labels.length() > 0) {
							labels.append(", ");
						}
						labels.append(label);
						foundStatic = true;
					} 
				}
				/*
				 * Get external choices
				 */
				if(external_choices && !foundStatic) {
					// 2. Search choices which are stored in an external table
					ArrayList<String> miniMatch = new ArrayList<> ();
					miniMatch.add(match);
					ArrayList<Option> choices = GeneralUtilityMethods.getExternalChoices(sd, cResults, 
							localisation, user, oId, sId, qId, miniMatch, surveyIdent, tz, null, null);

					if(choices != null && choices.size() > 0) {
						for(Option choice : choices) {
							if(choice.labels != null && choice.labels.size() > languageIdx) {
								if(labels.length() > 0) {
									labels.append(", ");
								}
								labels.append(choice.labels.get(languageIdx).text);
							}
						}
					}			
				}
			}
			
		}
		
		if(labels.length() == 0) {
			// No label found, return the original value as the label
			int idx = 0;
			for(String match : matches) {
				if(match != null) {
					if(idx++ > 0) {
						labels.append(", ");
					}
					labels.append(match);
				}
			}
		}
		
		return labels.toString();
	}

}
