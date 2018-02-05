package org.smap.sdal.managers;

import java.sql.Connection;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.UtilityMethodsEmail;

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
	
	private static Logger log =
			 Logger.getLogger(ChoiceManager.class.getName());

	private ResourceBundle localisation;
	
	public ChoiceManager(ResourceBundle l) {
		localisation = l;
	}
	
	
	public String getLabel(Connection sd, int sId, String value, boolean external_choices, String external_table, String languageName) {
		
		
		String label = null;
		
		try {
			/*
			 * 1. Search choices which are stored in the survey meta definition
			 */
			if(!external_choices) {
				label = UtilityMethodsEmail.getSingleLabel(sd, sId, languageName, value);
			}
			
			/*
			 * 2. TODO Search choices which are stored in an eternal table
			 */
		
		} catch (Exception e) {
			
		} finally {
			
		}
		
		if(label == null) {
			// return the original value
			label = value;
		}
		
		return label;
	}

}
