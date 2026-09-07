package org.smap.sdal.mcp;

import java.sql.Connection;
import java.util.ResourceBundle;

import jakarta.servlet.http.HttpServletRequest;

/*
 * Everything a tool is allowed to know about the caller, assembled once per request.
 *
 * Tools never open their own database connections and never call getRemoteUser().  Both are
 * supplied here, so a tool cannot accidentally run as somebody else or leak a connection, and the
 * dispatcher can close what it opened in one place.
 */
public class McpToolContext {

	public final Connection sd;
	public final Connection cResults;
	public final HttpServletRequest request;

	public final String user;			// The ident the token was issued to
	public final int uId;
	public final int oId;				// Organisation the grant was given in
	public final String scope;			// Space separated scopes on the token
	public final boolean superUser;

	public final ResourceBundle localisation;
	public final String timezone;

	/*
	 * The most rows any one tool call may return.  An agent asking for "all the data" should get a
	 * bounded answer rather than the server spending itself trying to produce an unbounded one.
	 */
	public final int maxRows;

	public McpToolContext(Connection sd, Connection cResults, HttpServletRequest request,
			String user, int uId, int oId, String scope, boolean superUser,
			ResourceBundle localisation, String timezone, int maxRows) {
		this.sd = sd;
		this.cResults = cResults;
		this.request = request;
		this.user = user;
		this.uId = uId;
		this.oId = oId;
		this.scope = scope;
		this.superUser = superUser;
		this.localisation = localisation;
		this.timezone = timezone;
		this.maxRows = maxRows;
	}

	public boolean hasScope(String required) {
		return MCPScope.has(scope, required);
	}

	/*
	 * Cap whatever limit a tool was asked for.  A tool that ignores this is a tool that can be
	 * asked to read a whole results table into memory.
	 */
	public int cap(int requested) {
		if(maxRows <= 0) {
			return requested;
		}
		return requested <= 0 || requested > maxRows ? maxRows : requested;
	}
}
