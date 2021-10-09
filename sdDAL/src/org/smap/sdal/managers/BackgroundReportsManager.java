package org.smap.sdal.managers;

import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.smap.sdal.model.BackgroundReport;

/*****************************************************************************
 * 
 * This file is part of SMAP.
 * 
 * SMAP is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * SMAP is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * SMAP. If not, see <http://www.gnu.org/licenses/>.
 * 
 ******************************************************************************/

/*
 * Manage the creation and completion of background reports
 */
public class BackgroundReportsManager {

	private static Logger log = Logger.getLogger(BackgroundReportsManager.class.getName());

	private ResourceBundle localisation;
	private String tz;
	
	public BackgroundReportsManager(ResourceBundle l, String tz) {
		localisation = l;
		if(tz == null) {
			tz = "UTC";
		}
		this.tz = tz;
	}
	
	/*
	 * Localisation can change during the life of the manager
	 */
	public boolean processNextReport() {
		BackgroundReport report = getNextReport();
		if(report == null) {
			return false;	// no more reports
		} else {
			// Process the report
		}
		return true;
	}
	
	
	private BackgroundReport getNextReport() {
		return null;
	}

}
