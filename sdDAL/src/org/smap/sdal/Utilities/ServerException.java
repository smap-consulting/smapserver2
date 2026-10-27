package org.smap.sdal.Utilities;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

public class ServerException extends WebApplicationException {
	
	private static final long serialVersionUID = 1L;

	public ServerException() {
		super(Response.serverError().build());
	}
	
}
