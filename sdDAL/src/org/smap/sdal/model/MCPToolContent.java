package org.smap.sdal.model;

/**
 * Represents content in an MCP tool result
 */
public class MCPToolContent {
	private String type;
	private String text;

	public MCPToolContent(String type, String text) {
		this.type = type;
		this.text = text;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
}
