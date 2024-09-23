package surveyKPI;

import java.util.ArrayList;

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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import org.smap.notifications.interfaces.EmitAwsSES;
import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;

@Path("/emailTest")
public class EmailTest extends Application {
	
	Authorise a = null;
	
	public EmailTest() {
		a = new Authorise(null, Authorise.OWNER);
	}
	
	/*
	 * Send a test email
	 */
	@GET
	public void emailTest(@Context HttpServletRequest request) throws Exception {
		
		EmitAwsSES mgr = new EmitAwsSES("ap-southeast-2", 
				GeneralUtilityMethods.getBasePath(request));
		
		ArrayList<String> recipients = new ArrayList<>();
		recipients.add("neilpenman@gmail.com");
		mgr.sendSES(recipients, "Hello There", 1, "Email Body");

	}


}

