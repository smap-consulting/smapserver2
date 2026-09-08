package org.smap.sdal.model;

import java.util.Map;

import com.google.gson.annotations.SerializedName;

/**
 * Represents a JSON-RPC 2.0 request for MCP
 */
public class MCPRequest {
	private String jsonrpc = "2.0";
	private Object id;
	private String method;
	private Map<String, Object> params;

	/*
	 * 2026-07-28 moved the protocol version, the client's identity and its capabilities out of a
	 * handshake and into every request, so this is where a stateless server learns who it is talking
	 * to.  Gson needs the name spelled out because a Java field cannot be called _meta.
	 */
	@SerializedName("_meta")
	private Map<String, Object> meta;

	public Map<String, Object> getMeta() {
		return meta;
	}

	public void setMeta(Map<String, Object> meta) {
		this.meta = meta;
	}

	/* What the client says it can do, or null if it said nothing */
	@SuppressWarnings("unchecked")
	public Map<String, Object> getClientCapabilities() {
		if(meta == null) {
			return null;
		}
		Object caps = meta.get("io.modelcontextprotocol/clientCapabilities");
		return caps instanceof Map ? (Map<String, Object>) caps : null;
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
