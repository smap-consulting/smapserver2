package org.smap.sdal.mcp.tools;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.smap.sdal.Utilities.Authorise;
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.managers.NotificationManager;
import org.smap.sdal.mcp.AbstractMcpTool;
import org.smap.sdal.mcp.MCPScope;
import org.smap.sdal.mcp.McpData;
import org.smap.sdal.mcp.McpToolContext;
import org.smap.sdal.model.MCPToolResult;
import org.smap.sdal.model.Notification;
import org.smap.sdal.model.NotifyDetails;
import org.smap.sdal.model.Survey;

/*
 * Set up an email to go out when a form is submitted.
 *
 * **Created switched off, always.**  That is the whole design of this tool.  A notification that
 * started sending the moment it was made would be a send in everything but name: nobody would have
 * read the subject, checked the addresses or seen the filter before mail began arriving.  Made off,
 * it is a draft - somebody reads it and turns it on with notification_enable, which is a separate
 * act that a separate person can refuse.
 *
 * Only email, only on submission, only to addresses given here.  A notification can also fire on a
 * timer, send SMS, call a webhook, escalate a case, write to SharePoint, or take its recipients from
 * an answer in the form - and each of those is a different thing that goes wrong differently.  A
 * tool that offered all of them behind one set of arguments would be one where a plausible-looking
 * call sends the wrong thing to the wrong people.  The rest are built in the console, where the
 * person choosing can see what each option means.
 */
public class NotificationCreateTool extends AbstractMcpTool {

	/* Enough of a check to catch a name typed where an address belongs */
	private static final String EMAIL = "[^@\\s]+@[^@\\s]+\\.[^@\\s]+";

	@Override
	public String getName() {
		return "notification_create";
	}

	@Override
	public String getTitle() {
		return "Set up an email notification";
	}

	@Override
	public String getDescription() {
		return "Sets up an email to be sent when a form is submitted, to the addresses you give. "
				+ "It is created SWITCHED OFF and sends nothing until somebody turns it on with "
				+ "notification_enable, so it can be read over first. Only email on submission to "
				+ "fixed addresses is made here; SMS, webhooks, timed notifications and recipients "
				+ "taken from an answer are set up in the console.";
	}

	@Override
	public String getRequiredScope() {
		return MCPScope.WRITE;
	}

	@Override
	public boolean isMutating() {
		return true;
	}

	@Override
	public String getReversal() {
		return "notification_delete removes it, and it sends nothing before it is switched on";
	}

	@Override
	public List<String> getRequiredGroups() {
		return groups(Authorise.ADMIN, Authorise.MANAGE);
	}

	@Override
	public Map<String, Object> getInputSchema() {
		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put("survey_id", property("integer",
				"The survey whose submissions trigger the email, from survey_list"));
		properties.put("name", property("string",
				"What this notification is called, as it appears in the console"));
		properties.put("send_to", listOfStrings(
				"The email addresses to send to"));
		properties.put("subject", property("string", "The subject line"));
		properties.put("content", property("string", "What the email says"));
		properties.put("only_when", property("string",
				"Optional. Only send when a submission matches, written over the survey's question "
				+ "names such as ${status} = 'urgent'."));

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", new String[] { "survey_id", "name", "send_to", "subject", "content" });
		return schema;
	}

	private static Map<String, Object> listOfStrings(String description) {
		Map<String, Object> items = new LinkedHashMap<>();
		items.put("type", "string");
		Map<String, Object> p = new LinkedHashMap<>();
		p.put("type", "array");
		p.put("items", items);
		p.put("description", description);
		return p;
	}

	@Override
	@SuppressWarnings("unchecked")
	public MCPToolResult execute(McpToolContext ctx, Map<String, Object> arguments) throws Exception {

		int surveyId = intArg(arguments, "survey_id", 0);
		if(surveyId <= 0) {
			return new MCPToolResult("A survey_id is required. Use survey_list to find one.", true);
		}
		Survey survey = McpData.surveyById(ctx, surveyId);
		if(survey == null) {
			return new MCPToolResult("No such survey, or you do not have access to it.", true);
		}
		String name = stringArg(arguments, "name");
		if(name == null || name.trim().isEmpty()) {
			return new MCPToolResult("A name is required, so the notification can be recognised in "
					+ "the console.", true);
		}
		String subject = stringArg(arguments, "subject");
		if(subject == null || subject.trim().isEmpty()) {
			return new MCPToolResult("A subject is required. An email with no subject is the kind "
					+ "that gets deleted unread.", true);
		}
		String content = stringArg(arguments, "content");
		if(content == null || content.trim().isEmpty()) {
			return new MCPToolResult("Content is required: what the email actually says.", true);
		}

		/*
		 * The addresses.  Checked one at a time and reported by name, because an address that is
		 * wrong in a list of six is not found by being told the list is wrong.
		 */
		Object toArg = arguments.get("send_to");
		if(!(toArg instanceof List) || ((List<Object>) toArg).isEmpty()) {
			return new MCPToolResult("send_to is required: at least one email address to send to.",
					true);
		}
		ArrayList<String> emails = new ArrayList<>();
		List<String> bad = new ArrayList<>();
		for(Object o : (List<Object>) toArg) {
			String address = o == null ? "" : o.toString().trim();
			if(address.isEmpty()) {
				continue;
			}
			if(!address.matches(EMAIL)) {
				bad.add(address);
			} else if(!emails.contains(address)) {
				emails.add(address);
			}
		}
		if(!bad.isEmpty()) {
			return new MCPToolResult("These are not email addresses: " + String.join(", ", bad)
					+ ". Every entry in send_to has to be one, because each is somebody who will "
					+ "receive this.", true);
		}
		if(emails.isEmpty()) {
			return new MCPToolResult("send_to held no addresses.", true);
		}

		Notification n = new Notification();
		n.name = name.trim();
		n.s_id = surveyId;
		n.sIdent = survey.getIdent();
		n.trigger = "submission";
		n.target = "email";
		/*
		 * Off.  Not a default the caller can override - turning it on is a separate call, so that
		 * somebody has the chance to read what was written before any of it is sent.
		 */
		n.enabled = false;
		n.filter = stringArg(arguments, "only_when");
		n.p_id = GeneralUtilityMethods.getProjectIdFromSurveyIdent(ctx.sd, survey.getIdent());

		NotifyDetails nd = new NotifyDetails();
		nd.emails = emails;
		nd.subject = subject.trim();
		nd.content = content.trim();
		/*
		 * "-1" rather than null or zero: that is what the rest of Smap writes when no question was
		 * chosen to supply an address, and a plain absence here is read elsewhere as a recipient
		 * that does not exist.
		 */
		nd.emailQuestionName = "-1";
		n.notifyDetails = nd;

		int id = new NotificationManager(ctx.localisation)
				.addNotification(ctx.sd, null, ctx.user, n, ctx.timezone);

		Map<String, Object> data = new LinkedHashMap<>();
		data.put("created", Boolean.TRUE);
		data.put("notification_id", id);
		data.put("name", n.name);
		data.put("enabled", Boolean.FALSE);
		data.put("survey", survey.getDisplayName());
		data.put("sendTo", emails);
		data.put("subject", nd.subject);
		if(n.filter != null && !n.filter.trim().isEmpty()) {
			data.put("onlyWhen", n.filter);
		}

		StringBuilder text = new StringBuilder();
		text.append("Set up \"").append(n.name).append("\" on \"")
				.append(survey.getDisplayName()).append("\".\n\n");
		text.append("It would send to ").append(emails.size()).append(" address(es): ")
				.append(String.join(", ", emails)).append("\n");
		text.append("Subject: ").append(nd.subject).append("\n");
		if(n.filter != null && !n.filter.trim().isEmpty()) {
			text.append("Only when: ").append(n.filter).append("\n");
		}
		text.append("\n**It is switched off and has sent nothing.** Read it over, then switch it on "
				+ "with notification_enable and it will send on the next matching submission. "
				+ "notification_delete removes it instead.");

		MCPToolResult result = new MCPToolResult(text.toString());
		result.setStructuredContent(data);
		return result;
	}
}
