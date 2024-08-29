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
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
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

@Path("/formList")

public class FormList extends Application {
	
	// Respond with XML 
	@GET
	@Produces({MediaType.TEXT_XML, MediaType.APPLICATION_XML})   
	public Response getFormListXML(@Context HttpServletRequest request) throws IOException, ApplicationException {
		FormListManager flm = new FormListManager();
		return flm.getFormList(request);
	}
}

