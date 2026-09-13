package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.ServerData;

/*
 * The server's operational settings.
 *
 * Everything here is a number or a switch that changes how the server behaves for everybody on it.
 * Nothing here is a secret, and that is a decision rather than a coincidence: the settings row also
 * holds the mail relay password, the map and SMS keys, the Turnstile secret and a SharePoint private
 * key, and a tool that returned "the server settings" would have put all of it into a chat transcript
 * the first time somebody asked an open question.
 *
 * Whether a credential is *set* is reported, because "is mail configured" is a real operational
 * question and answering it does not require saying what the password is.
 */
public class ServerSettingsGetTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "server_settings_get";
	}

	@Override
	public String getTitle() {
		return "Server settings";
	}

	@Override
	public String getDescription() {
		return "The server's operational settings: rate and record limits, password strength, how "
				+ "long erased data is kept, and the MCP settings. Reports whether mail, SMS, maps "
				+ "and SharePoint are configured, but never the keys or passwords themselves.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.SERVER;
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return noArguments();
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		ServerData s = new ServerManager().getServer(ctx.sd, ctx.localisation);

		Map<String, Object> limits = new LinkedHashMap<>();
		limits.put("apiRatePerMinute", s.ratelimit);
		limits.put("apiMaxRecords", s.getMaxRecords());
		limits.put("passwordStrength", s.password_strength);
		limits.put("keepErasedDays", s.keep_erased_days);

		Map<String, Object> mcp = new LinkedHashMap<>();
		mcp.put("enabled", s.mcp_enabled);
		mcp.put("clientRegistration", s.mcp_client_registration);
		mcp.put("maxRows", s.mcp_max_rows);
		mcp.put("tokenTtlSeconds", s.mcp_token_ttl);
		mcp.put("allowAccessChanges", s.mcp_allow_access);

		/*
		 * Set or not set, never the value.  Enough to answer "is mail working", which is what gets
		 * asked, without answering "what is the password", which does not.
		 */
		Map<String, Object> configured = new LinkedHashMap<>();
		configured.put("email", set(s.smtp_host) || "aws".equalsIgnoreCase(s.email_type));
		configured.put("emailType", s.email_type == null ? "" : s.email_type);
		configured.put("sms", set(s.sms_url) || set(s.vonage_application_id));
		configured.put("maps", set(s.mapbox_default) || set(s.google_key) || set(s.maptiler_key));
		configured.put("turnstile", set(s.turnstile_site_key));
		configured.put("sharepoint", set(s.sharepoint_url));
		configured.put("customCss", set(s.css));

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("version", s.version);
		data.put("limits", limits);
		data.put("mcp", mcp);
		data.put("configured", configured);

		StringBuilder text = new StringBuilder();
		text.append("Smap ").append(s.version).append("\n");
		text.append("\nLimits");
		text.append("\n- API rate: ").append(s.ratelimit == 0 ? "no limit"
				: s.ratelimit + " per minute");
		text.append("\n- API records per request: ").append(s.getMaxRecords() == 0
				? "no limit" : String.valueOf(s.getMaxRecords()));
		text.append("\n- password strength: ").append(s.password_strength);
		text.append("\n- erased data kept for: ").append(s.keep_erased_days).append(" day(s)");

		text.append("\n\nMCP");
		text.append("\n- server: ").append(s.mcp_enabled ? "on" : "OFF");
		text.append("\n- client registration: ").append(s.mcp_client_registration);
		text.append("\n- max rows a tool returns: ").append(s.mcp_max_rows == 0
				? "the built in default" : String.valueOf(s.mcp_max_rows));
		text.append("\n- token lifetime: ").append(s.mcp_token_ttl).append("s");
		text.append("\n- may change who can reach what: ")
				.append(s.mcp_allow_access ? "yes" : "no");

		text.append("\n\nConfigured");
		for(Map.Entry<String, Object> e : configured.entrySet()) {
			if(e.getValue() instanceof Boolean) {
				text.append("\n- ").append(e.getKey()).append(": ")
						.append(((Boolean) e.getValue()) ? "yes" : "no");
			}
		}
		text.append("\n\nKeys, passwords and certificates are not reported. Change them in the "
				+ "console. server_settings_set changes the limits above.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	private boolean set(String s) {
		return s != null && !s.trim().isEmpty();
	}
}
