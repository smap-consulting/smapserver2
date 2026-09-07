package org.smap.sdal.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the result of an MCP tool execution
 */
public class MCPToolResult {
	private List<MCPToolContent> content;
	private boolean isError;

	/*
	 * The machine readable half of a result.  A model reading prose has to parse it back out again
	 * and will sometimes get it wrong; structuredContent is the same answer as data, validated
	 * against the tool's outputSchema.  The text block is kept alongside it because the
	 * specification asks for both.
	 */
	private Object structuredContent;

	public Object getStructuredContent() {
		return structuredContent;
	}

	public void setStructuredContent(Object structuredContent) {
		this.structuredContent = structuredContent;
	}

	public MCPToolResult() {
		this.content = new ArrayList<>();
		this.isError = false;
	}

	public MCPToolResult(String text) {
		this();
		addTextContent(text);
	}

	public MCPToolResult(String text, boolean isError) {
		this(text);
		this.isError = isError;
	}

	public void addTextContent(String text) {
		content.add(new MCPToolContent("text", text));
	}

	public List<MCPToolContent> getContent() {
		return content;
	}

	public void setContent(List<MCPToolContent> content) {
		this.content = content;
	}

	public boolean isError() {
		return isError;
	}

	public void setError(boolean isError) {
		this.isError = isError;
	}
}
