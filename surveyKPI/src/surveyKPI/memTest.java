package surveyKPI;


import java.sql.Connection;
import java.sql.SQLException;

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

import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.Utilities.SDDataSource;

/*
 * Login functions
 */
@Path("/memtest")
public class memTest extends Application {
	
	Authorise a = new Authorise(null, Authorise.OWNER);
	
	/*
	 * Login and get a key for future authentication
	 */
	@GET
	public void memtest(@Context HttpServletRequest request,
			@QueryParam("form") String formIdent) throws SQLException {
		
		String connectionString = "memtest";
		
		// Authorisation - Access
		Connection sd = SDDataSource.getConnection(connectionString);	
		a.isAuthorised(sd, request.getRemoteUser());
		sd.close();
		
		
		ArrayList<String> data = new ArrayList<> ();
		 		
		
		while(true) {
			data.add("hello there here is some more memory");
		}

	}

	



}

