package org.smap.sdal.mcp.tools;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Organisation;

/*
 * What this organisation is, and what it is currently allowed to do.
 *
 * The second half is the useful half.  An organisation can have submitting, the API, notifications
 * or SMS switched off at the organisation level, and when that has happened every explanation
 * further down is wrong: the survey is fine, the user is fine, the device is fine, and nothing is
 * arriving.  Reported here so that answer is reachable rather than deduced.
 *
 * The mail relay settings are not reported.  They hold the password the server sends mail with, and
 * an organisation's own administrator asking an open question about "the settings" should not get it
 * back in a chat transcript.  Same reasoning as server_info.
 */
public class OrganisationGetTool extends AbstractMcpTool {

	@Override
	public String getName() {
		return "organisation_get";
	}

	@Override
	public String getTitle() {
		return "Organisation settings";
	}

	@Override
	public String getDescription() {
		return "The organisation this connection belongs to: its name and contact details, its "
				+ "locale and time zone, and whether submitting, the API, notifications and SMS are "
				+ "switched on for it. Check this first when data is not arriving or messages are "
				+ "not being sent - any of these being off stops it for everybody. Mail relay "
				+ "settings are not reported.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.ADMIN;
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ORG, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return noArguments();
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		Organisation o = GeneralUtilityMethods.getOrganisation(ctx.sd, oId);
		if(o == null) {
			return new MCPToolResult("This connection's organisation could not be read.", true);
		}

		/*
		 * Empty rather than null, because Gson drops a null field entirely.  A website that has never
		 * been set would simply not appear, which does not read as "there isn't one" - it reads as a
		 * field this tool does not report, and the next question is asked somewhere else.
		 */
		Map<String, Object> details = new LinkedHashMap<>();
		details.put("name", blank(o.name));
		details.put("companyName", blank(o.company_name));
		details.put("address", blank(o.company_address));
		details.put("phone", blank(o.company_phone));
		details.put("email", blank(o.company_email));
		details.put("website", blank(o.website));
		details.put("adminEmail", blank(o.admin_email));
		details.put("locale", blank(o.locale));
		details.put("timezone", blank(o.timeZone));

		Map<String, Object> allowed = new LinkedHashMap<>();
		allowed.put("submit", o.can_submit);
		allowed.put("api", o.can_use_api);
		allowed.put("notifications", o.can_notify);
		allowed.put("sms", o.can_sms);
		allowed.put("emailTasks", o.email_task);
		allowed.put("editSubmittedData", o.can_edit);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("organisation_id", o.id);
		data.put("details", details);
		data.put("allowed", allowed);
		data.put("lastChangedBy", o.changed_by);
		data.put("lastChanged", o.changed_ts);

		StringBuilder text = new StringBuilder();
		text.append(o.name).append("\n");
		append(text, "company", o.company_name);
		append(text, "address", o.company_address);
		append(text, "phone", o.company_phone);
		append(text, "email", o.company_email);
		append(text, "website", o.website);
		append(text, "notices go to", o.admin_email);
		append(text, "locale", o.locale);
		append(text, "time zone", o.timeZone);

		text.append("\nAllowed to:");
		append(text, "receive submissions", o.can_submit);
		append(text, "use the API", o.can_use_api);
		append(text, "send notifications", o.can_notify);
		append(text, "send SMS", o.can_sms);
		append(text, "send task emails", o.email_task);
		append(text, "edit submitted data", o.can_edit);

		/*
		 * Said plainly rather than left to be read off the list.  Somebody asking this while chasing
		 * missing data needs the "and that is why" said out loud.
		 */
		StringBuilder off = new StringBuilder();
		if(!o.can_submit) { off.append("\n- No data can be submitted to any survey in this organisation."); }
		if(!o.can_use_api) { off.append("\n- The API is switched off, so nothing using it will work."); }
		if(!o.can_notify) { off.append("\n- No notification will be sent, however it is set up."); }
		if(!o.can_sms) { off.append("\n- No SMS will be sent."); }
		if(off.length() > 0) {
			text.append("\n\nWorth knowing:").append(off);
		}

		if(o.changed_by != null) {
			text.append("\n\nLast changed by ").append(o.changed_by);
			if(o.changed_ts != null) {
				text.append(" on ").append(o.changed_ts);
			}
			text.append(".");
		}
		text.append("\n\nThe name, contact details, locale and time zone can be changed with "
				+ "organisation_update. What the organisation is allowed to do is set in the "
				+ "console and cannot be changed from here.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	private String blank(String s) {
		return s == null ? "" : s;
	}

	private void append(StringBuilder text, String label, String value) {
		if(value != null && !value.trim().isEmpty()) {
			text.append("\n- ").append(label).append(": ").append(value);
		}
	}

	private void append(StringBuilder text, String label, boolean value) {
		text.append("\n- ").append(label).append(": ").append(value ? "yes" : "NO");
	}
}
