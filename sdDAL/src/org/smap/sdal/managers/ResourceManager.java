package org.smap.sdal.managers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import org.smap.sdal.Utilities.GeneralUtilityMethods;

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
 * Manage usage of costly resources
 */
public class ResourceManager {
	
	private static Logger log =
			 Logger.getLogger(ResourceManager.class.getName());
	
	/*
	 * Check to see if the person can use the resource
	 */
	public boolean canUse(Connection sd, 
			int oId,
			String resource) throws SQLException {
		
		int limit = GeneralUtilityMethods.getLimit(sd, oId, resource);
		System.out.println("xxxxxxxxxxxxxxx " + limit);
		boolean decision = false;
		return decision;
	}
	
	/*
	 * Update the usage count
	 */
	public void updateUsage(Connection sd, int oId, String resource)  {
		
		
		try {
			
		} finally {
			
		}

	}
	
	
}


