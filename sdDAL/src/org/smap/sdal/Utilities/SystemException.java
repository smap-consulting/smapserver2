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

package org.smap.sdal.Utilities;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class SystemException extends WebApplicationException {
	
	private static final long serialVersionUID = 1L;

	public SystemException() {
	        this("System Error", "Smap");
	}
	
	public SystemException(String msg) {
        this("System Error: " + msg, "Smap");
	}
	
	public SystemException(String message, String system) {
	        super(Response.status(Status.INTERNAL_SERVER_ERROR)
	                .entity(message).build());
	}
}
