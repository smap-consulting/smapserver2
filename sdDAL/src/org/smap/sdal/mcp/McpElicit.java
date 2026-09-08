package org.smap.sdal.mcp;

import java.util.LinkedHashMap;
import java.util.Map;

/*
 * Asking the person on the other side of the client a question, in the shape the specification
 * expects.
 *
 * There is no confirm mode: 2026-07-28 offers form and url, and url is for things that must not pass
 * through the client at all, such as credentials. An approval is therefore a form with one boolean
 * in it. The message carries what is about to happen, because the schema cannot - a client renders
 * the message and the field, and the field is only a yes.
 */
public class McpElicit {

	/* The key the confirmation travels under, and comes back under */
	public static final String CONFIRM = "confirm";

	private static final String FIELD = "proceed";

	/*
	 * A yes or no, with the whole of the reason for asking in the message.
	 *
	 * Form mode only. The specification forbids asking for anything sensitive this way and nothing
	 * here does, but it is worth saying why: a confirmation is not a credential, and putting it in
	 * a form is what lets the client show it plainly and offer a decline.
	 */
	public static Map<String, Object> confirmation(String message) {

		Map<String, Object> field = new LinkedHashMap<>();
		field.put("type", "boolean");
		field.put("title", "Go ahead");
		field.put("description", "Tick to allow this. Leave it unticked, or decline, to stop.");
		field.put("default", Boolean.FALSE);

		Map<String, Object> properties = new LinkedHashMap<>();
		properties.put(FIELD, field);

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("type", "object");
		schema.put("properties", properties);
		schema.put("required", new String[] { FIELD });

		Map<String, Object> params = new LinkedHashMap<>();
		params.put("mode", "form");
		params.put("message", message);
		params.put("requestedSchema", schema);

		Map<String, Object> request = new LinkedHashMap<>();
		request.put("method", "elicitation/create");
		request.put("params", params);

		Map<String, Object> requests = new LinkedHashMap<>();
		requests.put(CONFIRM, request);
		return requests;
	}

	/*
	 * Whether the person actually said yes.
	 *
	 * Three answers are possible and only one of them is a yes.  A decline is a no, a cancel is the
	 * dialog being dismissed without an answer, and an accept still carries the field, which a
	 * client can submit unticked.  Anything that is not an explicit yes is treated as a no, because
	 * the failure that matters here is sending something nobody agreed to.
	 */
	public static boolean accepted(Map<String, Object> inputResponses) {

		if(inputResponses == null) {
			return false;
		}
		Object response = inputResponses.get(CONFIRM);
		if(!(response instanceof Map)) {
			return false;
		}
		Map<?, ?> r = (Map<?, ?>) response;
		if(!"accept".equals(String.valueOf(r.get("action")))) {
			return false;
		}
		Object content = r.get("content");
		if(!(content instanceof Map)) {
			return false;
		}
		return Boolean.TRUE.equals(((Map<?, ?>) content).get(FIELD));
	}
}
