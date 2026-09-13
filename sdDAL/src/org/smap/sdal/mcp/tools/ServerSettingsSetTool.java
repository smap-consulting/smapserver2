package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.LogManager;
import org.smap.sdal.managers.ServerManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.ServerData;

/*
 * Change the server's operational limits.
 *
 * Four numbers, and the list of what is absent is the design.
 *
 * **No credentials.**  The settings row holds the mail relay password, the SMS and map keys, the
 * Turnstile secret and a SharePoint private key.  The console's save writes every column from one
 * object, so nudging a rate limit through it would mean sending every credential back - and blanking
 * the ones the caller did not have.  Fifth manager method written to avoid exactly that.
 *
 * **No MCP settings, and that one is not about credentials.**  A client that could set
 * mcp_allow_access could switch on the permission to change permissions and then ask for it.  The
 * switch exists to make that a decision a person takes, so it cannot be reachable from the thing it
 * is meant to hold shut.  Same for mcp_enabled, which is the switch under everything here.
 *
 * **No CSS.**  css_set is in the plan and is not built: it writes a stylesheet served to every
 * console user, which is a way to change what other people see without changing any data, and the
 * only person who would notice is the one who did it.
 *
 * Every one of these applies to everybody on the server, so the answer says what each change does
 * rather than echoing the number back.
 */
public class ServerSettingsSetTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "server_settings_set";
	}

	@Override
	public String getTitle() {
		return "Change server limits";
	}

	@Override
	public String getDescription() {
		return "Changes the server's operational limits: API rate, API records per request, minimum "
				+ "password entropy and how long erased data is kept. These apply to everybody on the "
				+ "server. Keys, passwords, the custom stylesheet and the MCP settings cannot be "
				+ "changed here - those are console only. Returns the previous values.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.SERVER;
	}

	@Override
	public boolean isMutating() {
		return true;
	}

	@Override
	public String getReversal() {
		return "server_settings_set again with the previous values, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"api_rate_per_minute", property("integer",
						"Optional. API requests allowed per minute. 0 means no limit."),
				"api_max_records", property("integer",
						"Optional. Most records one API request returns. 0 means no limit."),
				"password_strength", property("number",
						"Optional. Minimum password entropy in bits - not a score out of five. "
								+ "Around 15 is weak, 40 is reasonable, 60 is strong. 0 switches "
								+ "the check off entirely."),
				"keep_erased_days", property("integer",
						"Optional. Days erased data is kept before it goes for good."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		ServerManager sm = new ServerManager();
		ServerData s = sm.getServer(ctx.sd, ctx.localisation);

		boolean named = false;
		for(String key : new String[] { "api_rate_per_minute", "api_max_records",
				"password_strength", "keep_erased_days" }) {
			if(arguments.get(key) != null) {
				named = true;
				break;
			}
		}
		if(!named) {
			return new MCPToolResult("Name something to change. server_settings_get shows what is "
					+ "set now.", true);
		}

		int rate = intArg(arguments, "api_rate_per_minute", s.ratelimit);
		int maxRecords = intArg(arguments, "api_max_records", s.getMaxRecords());
		int keepDays = intArg(arguments, "keep_erased_days", s.keep_erased_days);

		Object strengthArg = arguments.get("password_strength");
		double strength = strengthArg instanceof Number
				? ((Number) strengthArg).doubleValue() : s.password_strength;

		if(rate < 0 || maxRecords < 0 || keepDays < 0) {
			return new MCPToolResult("These cannot be negative. Use 0 for no limit.", true);
		}
		/*
		 * Entropy in bits, passed to nbvcxz as a minimum - not a score out of five, which is what it
		 * was first built as and would have refused this server's own setting of 15.  The upper bound
		 * is a sanity check rather than a rule: nothing below it is wrong, but a number above it
		 * would refuse every password anybody could type.
		 */
		if(strength < 0 || strength > 200) {
			return new MCPToolResult("Minimum password entropy is measured in bits. Around 15 is "
					+ "weak, 40 reasonable, 60 strong; above 200 nobody could type a password that "
					+ "passes.", true);
		}
		if(strength == 0 && s.password_strength != 0) {
			return new MCPToolResult("Setting the minimum entropy to 0 switches password strength "
					+ "checking off completely, for everybody. If that is meant, set it in the "
					+ "console where it is presented as the choice it is.", true);
		}
		/*
		 * Refused rather than accepted quietly.  Nothing stops the number being zero, but erased data
		 * kept for no days is a retention policy somebody should choose on purpose, and a caller that
		 * meant to set the API limit and mistyped the field name would otherwise turn it off.
		 */
		if(keepDays == 0 && s.keep_erased_days != 0) {
			return new MCPToolResult("Setting erased data to be kept for 0 days means it goes "
					+ "immediately and cannot be brought back. If that is really the policy, set it "
					+ "in the console where it is presented as one.", true);
		}

		Map<String, Object> previous = new LinkedHashMap<>();
		previous.put("api_rate_per_minute", s.ratelimit);
		previous.put("api_max_records", s.getMaxRecords());
		previous.put("password_strength", s.password_strength);
		previous.put("keep_erased_days", s.keep_erased_days);

		sm.updateOperationalLimits(ctx.sd, rate, maxRecords, strength, keepDays, ctx.user);

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		new LogManager().writeLogOrganisation(ctx.sd, oId, ctx.user, LogManager.SERVER,
				"Server limits changed via MCP", 0);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("api_rate_per_minute", rate);
		data.put("api_max_records", maxRecords);
		data.put("password_strength", strength);
		data.put("keep_erased_days", keepDays);
		data.put("previous", previous);

		StringBuilder text = new StringBuilder("Changed, for everybody on this server.");
		if(rate != s.ratelimit) {
			text.append("\n- API rate: ").append(describeLimit(s.ratelimit, "per minute"))
					.append(" to ").append(describeLimit(rate, "per minute"));
		}
		if(maxRecords != s.getMaxRecords()) {
			text.append("\n- API records per request: ")
					.append(describeLimit(s.getMaxRecords(), "records"))
					.append(" to ").append(describeLimit(maxRecords, "records"));
		}
		if(strength != s.password_strength) {
			text.append("\n- minimum password entropy: ").append(s.password_strength)
					.append(" to ").append(strength)
					.append(" bits - existing passwords are not affected, only ones set from now on");
		}
		if(keepDays != s.keep_erased_days) {
			text.append("\n- erased data kept: ").append(s.keep_erased_days).append(" to ")
					.append(keepDays).append(" day(s)");
			if(keepDays < s.keep_erased_days) {
				text.append(" - anything already older than that goes at the next clear out");
			}
		}
		text.append("\n\nKeys, passwords, the stylesheet and the MCP settings are unchanged and "
				+ "cannot be changed from here.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	private String describeLimit(int v, String unit) {
		return v == 0 ? "no limit" : v + " " + unit;
	}
}
