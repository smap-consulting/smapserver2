package org.smap.sdal.mcp.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.OrganisationManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Organisation;

/*
 * Correct the organisation's name, contact details, locale or time zone.
 *
 * Those, and nothing else.  What the organisation is allowed to do - submitting, the API,
 * notifications, SMS - is not changeable from here at any price: switching submitting off stops
 * every device in the organisation, and that is a decision somebody should make on a screen that
 * tells them so, not a side effect of a sentence about updating the address.  Nor are the mail
 * relay settings, which hold a password, or the storage limits, which are what is being paid for.
 *
 * The console's own save writes thirty seven columns from one object.  Using it here would have
 * meant sending back every one of those values to change a phone number, so this goes through a
 * manager method that writes only what was named - the same shape as project_update and user_update,
 * and for the same reason.
 *
 * It acts on the caller's own organisation and takes no organisation id, so there is no reading of
 * the argument under which it could reach another one.
 */
public class OrganisationUpdateTool extends AbstractMcpTool {

	/* The languages the server actually has messages in.  Anything else silently falls back to English */
	private static final List<String> LOCALES = Arrays.asList("en", "ar", "es", "fr", "pt", "uk");

	@Override
	public String getName() {
		return "organisation_update";
	}

	@Override
	public String getTitle() {
		return "Correct organisation details";
	}

	@Override
	public String getDescription() {
		return "Changes this organisation's name, contact details, locale or time zone. What the "
				+ "organisation is allowed to do, its mail settings and its limits are untouched and "
				+ "cannot be changed from here. The time zone is the one dates are reported in, so "
				+ "changing it changes how every existing submission time reads. Returns the "
				+ "previous values.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.ADMIN;
	}

	@Override
	public boolean isMutating() {
		return true;
	}

	@Override
	public String getReversal() {
		return "organisation_update again with the previous values, which this returns";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.ORG, Authorise.OWNER);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		return schema(
				"name", property("string", "Optional. What the organisation is called."),
				"company_name", property("string", "Optional. The legal or trading name."),
				"address", property("string", "Optional. Postal address."),
				"phone", property("string", "Optional. Contact telephone number."),
				"email", property("string", "Optional. Contact email address."),
				"website", property("string", "Optional. Web address."),
				"locale", property("string",
						"Optional. Language for server messages: en, ar, es, fr, pt or uk."),
				"timezone", property("string",
						"Optional. An IANA time zone such as Australia/Melbourne. Dates are "
								+ "reported in this."));
	}

	@Override
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int oId = GeneralUtilityMethods.getOrganisationId(ctx.sd, ctx.user);
		Organisation o = GeneralUtilityMethods.getOrganisation(ctx.sd, oId);
		if(o == null) {
			return new MCPToolResult("This connection's organisation could not be read.", true);
		}

		/*
		 * The console's rule, applied here rather than borrowed: canUserUpdateOrganisation closes the
		 * connection it was given before it throws, which would take this request's connection with
		 * it and turn a refusal into a failure several tools later.
		 *
		 * An organisation administrator may change any organisation they can see.  A plain
		 * administrator may only change one they are recorded as owning.
		 */
		if(!GeneralUtilityMethods.hasSecurityGroup(ctx.sd, ctx.user, Authorise.ORG_ID)
				&& !isOwner(ctx, oId)) {
			return new MCPToolResult("Changing the organisation needs an organisation administrator, "
					+ "or the administrator recorded as its owner.", true);
		}

		String name = arg(arguments, "name", o.name);
		String companyName = arg(arguments, "company_name", o.company_name);
		String address = arg(arguments, "address", o.company_address);
		String phone = arg(arguments, "phone", o.company_phone);
		String email = arg(arguments, "email", o.company_email);
		String website = arg(arguments, "website", o.website);
		String locale = arg(arguments, "locale", o.locale);
		String timezone = arg(arguments, "timezone", o.timeZone);

		if(nothingNamed(arguments)) {
			return new MCPToolResult("Name something to change. organisation_get shows what is "
					+ "there now.", true);
		}

		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("An organisation cannot have an empty name.", true);
		}
		name = name.trim();

		if(email != null && !email.trim().isEmpty()
				&& !email.trim().matches("[^@\\s]+@[^@\\s]+\\.[^@\\s]+")) {
			return new MCPToolResult("\"" + email + "\" is not an email address.", true);
		}

		if(locale != null && !locale.trim().isEmpty() && !LOCALES.contains(locale.trim())) {
			return new MCPToolResult("\"" + locale + "\" is not a language this server has messages "
					+ "in. It has " + String.join(", ", LOCALES) + ".", true);
		}

		/*
		 * Checked against the database's own list rather than a guess, the same way the console does
		 * it.  A time zone Postgres does not know is not a typo that shows up later - every date this
		 * organisation reports would be converted against something that does not exist.
		 */
		if(timezone != null && !timezone.trim().isEmpty()
				&& !GeneralUtilityMethods.isValidTimezone(ctx.sd, timezone.trim())) {
			return new MCPToolResult("\"" + timezone + "\" is not a time zone this server knows. Use "
					+ "an IANA name such as Australia/Melbourne or UTC.", true);
		}

		/*
		 * Empty rather than null.  Gson drops a null field, so a value that was not set would be
		 * missing from previous altogether - and previous is what the stated reversal depends on:
		 * "update again with these" cannot put back a field that was not reported as having been
		 * anything.
		 */
		Map<String, Object> previous = new LinkedHashMap<>();
		previous.put("name", blank(o.name));
		previous.put("company_name", blank(o.company_name));
		previous.put("address", blank(o.company_address));
		previous.put("phone", blank(o.company_phone));
		previous.put("email", blank(o.company_email));
		previous.put("website", blank(o.website));
		previous.put("locale", blank(o.locale));
		previous.put("timezone", blank(o.timeZone));

		new OrganisationManager(ctx.localisation).updateOrganisationDetails(ctx.sd, oId,
				name, companyName, address, phone, email, website,
				locale == null ? null : locale.trim(),
				timezone == null ? null : timezone.trim(),
				ctx.user);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("updated", Boolean.TRUE);
		data.put("organisation_id", oId);
		data.put("name", name);
		data.put("previous", previous);

		StringBuilder text = new StringBuilder();
		text.append("Updated ").append(o.name).append(".");
		changed(text, "name", o.name, name);
		changed(text, "company name", o.company_name, companyName);
		changed(text, "address", o.company_address, address);
		changed(text, "phone", o.company_phone, phone);
		changed(text, "email", o.company_email, email);
		changed(text, "website", o.website, website);
		changed(text, "locale", o.locale, locale);
		changed(text, "time zone", o.timeZone, timezone);

		if(timezone != null && !equal(o.timeZone, timezone)) {
			text.append("\n\n**Every date this organisation reports now reads in ").append(timezone)
					.append("**, including submissions that were made before the change. Nothing "
							+ "stored has moved - only how it is shown.");
		}
		text.append("\n\nWhat the organisation is allowed to do, its mail settings and its limits "
				+ "are unchanged.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}

	/* Anything not named keeps what it had, so the current value is the fallback */
	private String arg(Map<String, Object> arguments, String name, String current) {
		String v = stringArg(arguments, name);
		return v == null ? current : v;
	}

	private boolean nothingNamed(Map<String, Object> arguments) {
		for(String key : new String[] { "name", "company_name", "address", "phone", "email",
				"website", "locale", "timezone" }) {
			if(arguments.get(key) != null) {
				return false;
			}
		}
		return true;
	}

	private boolean isOwner(McpToolContext ctx, int oId) throws Exception {
		String sql = "select count(*) from users u, organisation o "
				+ "where u.ident = ? and o.id = ? and o.owner = u.id";
		try (PreparedStatement pstmt = ctx.sd.prepareStatement(sql)) {
			pstmt.setString(1, ctx.user);
			pstmt.setInt(2, oId);
			ResultSet rs = pstmt.executeQuery();
			return rs.next() && rs.getInt(1) > 0;
		}
	}

	private String blank(String s) {
		return s == null ? "" : s;
	}

	private boolean equal(String a, String b) {
		return a == null ? b == null : a.equals(b);
	}

	private void changed(StringBuilder text, String label, String from, String to) {
		if(equal(from, to)) {
			return;
		}
		text.append("\n- ").append(label).append(": ")
				.append(from == null || from.isEmpty() ? "(none)" : from)
				.append(" to ").append(to == null || to.isEmpty() ? "(none)" : to);
	}
}
