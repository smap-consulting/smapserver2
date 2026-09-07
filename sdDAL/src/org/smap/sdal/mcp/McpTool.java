package org.smap.sdal.mcp;

import java.util.List;
import java.util.Map;

import org.smap.sdal.model.MCPToolResult;

/*
 * What every MCP tool has to declare about itself.
 *
 * The permission fields are not documentation.  The dispatcher uses them twice: once to decide
 * whether the tool appears in tools/list at all, and again before it runs.  Filtering the listing
 * matters as much as refusing the call - a model that can see a tool will try it, and an agent that
 * spends its turns collecting refusals is worse than one that never saw the tool.
 *
 * getReversal() is the reversibility rule made structural.  Every mutating tool has to say how its
 * effect is undone, and the registry refuses to register one that does not, so the rule cannot
 * quietly erode as tools are added over the coming stages.
 */
public interface McpTool {

	String getName();

	/* Human readable label.  Optional; the name is used when this is null */
	default String getTitle() {
		return null;
	}

	String getDescription();

	/*
	 * JSON Schema 2020-12.  Must be an object schema, never null; a tool with no arguments returns
	 * {"type":"object","additionalProperties":false}
	 */
	Map<String, Object> getInputSchema();

	/* Optional.  When present the result's structuredContent must conform to it */
	default Map<String, Object> getOutputSchema() {
		return null;
	}

	/*
	 * readOnlyHint, destructiveHint, idempotentHint, openWorldHint.  Advisory to the client, which
	 * is told to treat them as untrusted, so they are a courtesy rather than a control.  The
	 * controls are getRequiredGroups, getRequiredScope and the checks inside the tool.
	 */
	default Map<String, Object> getAnnotations() {
		return null;
	}

	/*
	 * Smap security groups, any one of which lets the caller use this tool.  Empty means any
	 * authenticated MCP user, which should be rare.
	 */
	List<String> getRequiredGroups();

	/* The single scope the token must carry */
	String getRequiredScope();

	/* True if the tool changes anything */
	default boolean isMutating() {
		return false;
	}

	/*
	 * How the change this tool makes is undone.  Required of every mutating tool; null is only
	 * valid for a read only one.
	 */
	default String getReversal() {
		return null;
	}

	MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception;
}
