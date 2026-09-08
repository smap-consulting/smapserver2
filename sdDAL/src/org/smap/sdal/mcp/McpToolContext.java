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

	/*
	 * Which application is acting, as distinct from who it acts for.
	 *
	 * Both belong in the audit trail of anything this writes.  A record changed through MCP was
	 * changed by a program on a person's instruction, and a trail naming only the person cannot
	 * later answer whether they typed it or approved it.  Null for a token minted in the console,
	 * where there is no third party involved.
	 */
	public final String clientId;

	public final ResourceBundle localisation;
	public final String timezone;

	/*
	 * The most rows any one tool call may return.  An agent asking for "all the data" should get a
	 * bounded answer rather than the server spending itself trying to produce an unbounded one.
	 *
	 * Always a real number.  MCP has its own limit rather than borrowing the API's, and a server
	 * setting of zero resolves to McpProtocol.DEFAULT_MAX_ROWS before it reaches here, so there is
	 * no configuration in which a tool is unbounded.
	 */
	public final int maxRows;

	public McpToolContext(Connection sd, Connection cResults, HttpServletRequest request,
			String user, int uId, int oId, String scope, boolean superUser,
			ResourceBundle localisation, String timezone, int maxRows, String clientId) {
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
		this.clientId = clientId;
	}

	public boolean hasScope(String required) {
		return MCPScope.has(scope, required);
	}

	/*
	 * Cap whatever limit a tool was asked for.  A tool that ignores this is a tool that can be
	 * asked to read a whole results table into memory.
	 *
	 * The zero branch is kept as a guard rather than as a supported setting: a context built with a
	 * limit of zero by some future caller should refuse to be unbounded, not silently become so.
	 */
	public int cap(int requested) {
		int ceiling = maxRows > 0 ? maxRows : McpProtocol.DEFAULT_MAX_ROWS;
		return requested <= 0 || requested > ceiling ? ceiling : requested;
	}
}
