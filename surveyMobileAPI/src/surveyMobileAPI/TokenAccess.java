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

package surveyMobileAPI;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.smap.sdal.Utilities.ApplicationException;

import surveyMobileAPI.managers.FormListManager;

/*
 * Get surveys assigned to the user (ODK Format)
 * Example output:
	<forms>
		<form url="//{server}/formXml?odkFormKey={generated key}">{name}</form>
	</forms>

 * 
 */

@Path("/token")

public class TokenAccess extends Application {
	
	private static Logger log =
			 Logger.getLogger(FormList.class.getName());
	
	// Respond with XML 
	@GET
	@Path("/formList")
	@Produces({MediaType.TEXT_XML, MediaType.APPLICATION_XML})   
	public Response getFormListToken(@Context HttpServletRequest request) throws IOException, ApplicationException {
		
		log.info("Token request for formList");
		FormListManager flm = new FormListManager();
		return flm.getFormList(request);
	}
 

}

