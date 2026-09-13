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
	 * The argument naming whose access this tool changes, for a tool that changes access.
	 *
	 * When it is set, the dispatcher refuses the call if that argument names the caller.  This is
	 * the self modification invariant: a session may administer other people's permissions but never
	 * its own, so a grant that was given to do one job cannot be used to widen itself into another.
	 *
	 * It lives in the dispatcher rather than in each tool because the one place it must not be is
	 * optional.  A tool that forgot the check would be the whole of the hole.
	 */
	default String getSelfProtectedArgument() {
		return null;
	}

	/*
	 * How the change this tool makes is undone.  Required of every mutating tool; null is only
	 * valid for a read only one.
	 */
	default String getReversal() {
		return null;
	}

	/*
	 * Whether this tool stops to ask a person before it acts.
	 *
	 * Scoped to what cannot be taken back rather than to everything that changes.  A tool whose
	 * effect is undoable, audited and confined to one record does not ask: if every field edit
	 * asked, nobody would read any of them, and the one prompt that matters - this will send two
	 * emails that cannot be recalled - would be clicked through with the rest.  Rarity is what makes
	 * the question worth answering.
	 *
	 * Note this is the server's own gate, not the client's.  Clients like Claude Code already ask
	 * before each tool call, but another client need not, so anything that must be approved has to
	 * be approved here.
	 */
	default Confirmation getConfirmation() {
		return Confirmation.NONE;
	}

	enum Confirmation {
		/* Reversible, audited, one record at a time */
		NONE,

		/* The tool works out per call whether this one escapes: a send, or a bulk change */
		CONDITIONAL,

		/* Nothing this tool does can be taken back */
		ALWAYS
	}

	MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception;
}
