package org.smap.sdal.mcp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/*
 * Boilerplate every tool would otherwise repeat: building a JSON Schema by hand, and reading an
 * argument that arrived as whatever JSON happened to contain.
 */
public abstract class AbstractMcpTool implements McpTool {

	@Override
	public List<String> getRequiredGroups() {
		return new ArrayList<>();
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.READ;
	}

	/* Most tools read.  A mutating one says so, and then must also declare its reversal */
	@Override
	public Map<String, Object> getAnnotations() {
		Map<String, Object> a = new LinkedHashMap<>();
		a.put("readOnlyHint", !isMutating());
		a.put("destructiveHint", Boolean.FALSE);
		a.put("idempotentHint", !isMutating());
		/*
		 * Everything here acts on this server's own database and nothing outside it, which is what
		 * openWorldHint false says.
		 */
		a.put("openWorldHint", Boolean.FALSE);
		return a;
	}

	protected static List<String> groups(String... names) {
		return Arrays.asList(names);
	}

	/* A schema with no arguments at all */
	protected static Map<String, Object> noArguments() {
		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("additionalProperties", Boolean.FALSE);
		return schema;
	}

	protected static Map<String, Object> schema(Object... propertyPairs) {
		Map<String, Object> properties = new LinkedHashMap<>();
		for(int i = 0; i + 1 < propertyPairs.length; i += 2) {
			properties.put((String) propertyPairs[i], propertyPairs[i + 1]);
		}
		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		return schema;
	}

	protected static Map<String, Object> property(String type, String description) {
		Map<String, Object> p = new LinkedHashMap<>();
		p.put("type", type);
		p.put("description", description);
		return p;
	}

	/*
	 * JSON numbers arrive as Double through Gson even when the client wrote an integer, so every
	 * numeric argument has to be read through Number rather than cast
	 */
	protected static int intArg(Map<String, Object> args, String name, int fallback) {
		Object v = args.get(name);
		return v instanceof Number ? ((Number) v).intValue() : fallback;
	}

	protected static boolean boolArg(Map<String, Object> args, String name, boolean fallback) {
		Object v = args.get(name);
		return v instanceof Boolean ? (Boolean) v : fallback;
	}

	protected static String stringArg(Map<String, Object> args, String name) {
		Object v = args.get(name);
		return v == null ? null : v.toString();
	}
}
