package org.smap.sdal.mcp;

/*
 * Constants for the Model Context Protocol, revision 2026-07-28.
 *
 * That revision made the protocol stateless: there is no initialize handshake and no session, and
 * every request carries its own protocol version, client identity and capabilities in _meta.  Which
 * means a Smap server needs no shared session store and no sticky routing to sit behind a load
 * balancer - the thing that would otherwise have been the hardest part of hosting this.
 */
public class McpProtocol {

	public static final String VERSION = "2026-07-28";

	public static final String SERVER_NAME = "Smap";

	/* Reserved _meta keys, from the specification */
	public static final String META_PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion";
	public static final String META_CLIENT_INFO = "io.modelcontextprotocol/clientInfo";
	public static final String META_CLIENT_CAPABILITIES = "io.modelcontextprotocol/clientCapabilities";
	public static final String META_SERVER_INFO = "io.modelcontextprotocol/serverInfo";
	public static final String META_PROGRESS_TOKEN = "progressToken";

	/* Every result says which kind it is.  An absent value means "complete" to older clients */
	public static final String RESULT_COMPLETE = "complete";
	public static final String RESULT_INPUT_REQUIRED = "input_required";

	/* JSON-RPC codes, plus the three the specification reserves for MCP */
	public static final int PARSE_ERROR = -32700;
	public static final int INVALID_REQUEST = -32600;
	public static final int METHOD_NOT_FOUND = -32601;
	public static final int INVALID_PARAMS = -32602;
	public static final int INTERNAL_ERROR = -32603;
	public static final int HEADER_MISMATCH = -32020;
	public static final int MISSING_CLIENT_CAPABILITY = -32021;
	public static final int UNSUPPORTED_PROTOCOL_VERSION = -32022;

	/* Response headers, so a gateway can route and meter per tool without reading the body */
	public static final String HEADER_METHOD = "Mcp-Method";
	public static final String HEADER_NAME = "Mcp-Name";
}
