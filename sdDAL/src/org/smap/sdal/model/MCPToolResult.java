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

	/*
	 * When set, this is not an answer but a request for the caller to go and ask somebody.
	 *
	 * The dispatcher turns it into an input_required result carrying these requests and a state
	 * handle; the client puts them to the user and calls again with the answers.  Kept on the same
	 * class as an ordinary result so a tool can decide, part way through, that it needs to ask -
	 * which is exactly when it knows enough to say what it is about to do.
	 */
	private Object inputRequests;

	public Object getInputRequests() {
		return inputRequests;
	}

	public void setInputRequests(Object inputRequests) {
		this.inputRequests = inputRequests;
	}

	public boolean isInputRequired() {
		return inputRequests != null;
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
