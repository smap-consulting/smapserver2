package org.smap.sdal.model;

/**
 * Represents a JSON-RPC 2.0 response for MCP
 */
public class MCPResponse {
	private String jsonrpc = "2.0";
	private Object id;
	private Object result;
	private MCPError error;

	public MCPResponse(Object id) {
		this.id = id;
	}

	public MCPResponse(Object id, Object result) {
		this.id = id;
		this.result = result;
	}

	public MCPResponse(Object id, MCPError error) {
		this.id = id;
		this.error = error;
	}

	public String getJsonrpc() {
		return jsonrpc;
	}

	public void setJsonrpc(String jsonrpc) {
		this.jsonrpc = jsonrpc;
	}

	public Object getId() {
		return id;
	}

	public void setId(Object id) {
		this.id = id;
	}

	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}

	public MCPError getError() {
		return error;
	}

	public void setError(MCPError error) {
		this.error = error;
	}
}
