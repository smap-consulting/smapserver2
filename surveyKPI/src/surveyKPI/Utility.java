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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.smap.sdal.managers.TimeZoneManager;

/*
 * Miscellaneous utility functions
 */

@Path("/utility")
public class Utility extends Application {
	
	/*
	 * Get available time zones
	 * Based on StackOverflow response http://stackoverflow.com/questions/33359955/sort-timezone-based-on-gmt-time-in-a-list-of-timezone-string
	 */
	@Path("/timezones")
	@GET
	@Produces("application/json")
	public Response getTimezones() { 

		TimeZoneManager tmz = new TimeZoneManager();
		return tmz.get();
		
	}
}

