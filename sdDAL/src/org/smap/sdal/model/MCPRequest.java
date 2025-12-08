package org.smap.sdal.model;

import java.util.Map;

/**
 * Represents a JSON-RPC 2.0 request for MCP
 */
public class MCPRequest {
	private String jsonrpc = "2.0";
	private Object id;
	private String method;
	private Map<String, Object> params;

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

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public Map<String, Object> getParams() {
		return params;
	}

	public void setParams(Map<String, Object> params) {
		this.params = params;
	}
}
