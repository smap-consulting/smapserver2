package exceptions;

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

public class ServerException extends WebApplicationException {
	
	private static final long serialVersionUID = 1L;

	public ServerException() {
		super(Response.serverError().build());
	}
	
}
