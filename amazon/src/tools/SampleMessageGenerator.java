package tools;

/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;


public class SampleMessageGenerator {

	/*
	 * This message is delivered if a platform specific message is not specified
	 * for the end point. It must be set. It is received by the device as the
	 * value of the key "default".
	 */
	public static final String defaultMessage = "This is the default message";

	private static final ObjectMapper objectMapper = new ObjectMapper();

	public static enum Platform {
		// Apple Push Notification Service
		APNS,
		// Sandbox version of Apple Push Notification Service
		APNS_SANDBOX,
		// Amazon Device Messaging
		ADM,
		// Google Cloud Messaging
		GCM,
		// Baidu CloudMessaging Service
		BAIDU,
		// Windows Notification Service
		WNS,
		// Microsoft Push Notificaion Service
		MPNS;
	}

	public static String jsonify(Object message) {
		try {
			return objectMapper.writeValueAsString(message);
		} catch (Exception e) {
			e.printStackTrace();
			throw (RuntimeException) e;
		}
	}

	/*
	 * The device ignores the message itself and just refreshes, but it needs to know which
	 * server asked.  A device registers against one server at a time, and a server looks up
	 * every host name it answers to, so a device that has been pointed somewhere else since
	 * the row was written can still be sent a refresh that is not for it.
	 */
	private static Map<String, String> getData(String server) {
		Map<String, String> payload = new HashMap<String, String>();
		/*
		 * FieldTask 5 ignores this and just refreshes.  Kept, with the sample text it
		 * arrived with replaced, only because FieldTask 4 devices are still in the field
		 * sharing the same registrations and it is not known whether they read it.  Drop it
		 * once they are gone or confirmed not to care.
		 */
		payload.put("message", "refresh");
		if(server != null) {
			payload.put("server", server);
		}
		return payload;
	}

	public static String getSampleAppleMessage() {
		Map<String, Object> appleMessageMap = new HashMap<String, Object>();
		Map<String, Object> appMessageMap = new HashMap<String, Object>();
		appMessageMap.put("alert", "You have got email.");
		appMessageMap.put("badge", 9);
		appMessageMap.put("sound", "default");
		appleMessageMap.put("aps", appMessageMap);
		return jsonify(appleMessageMap);
	}

	public static String getSampleKindleMessage(String server) {
		Map<String, Object> kindleMessageMap = new HashMap<String, Object>();
		kindleMessageMap.put("data", getData(server));
		kindleMessageMap.put("consolidationKey", "Welcome");
		kindleMessageMap.put("expiresAfter", 1000);
		return jsonify(kindleMessageMap);
	}

	/*
	 * How long FCM should keep trying to deliver a refresh to a device that is offline
	 *
	 * This was 125 seconds against a send interval of about 11 minutes, and nothing retries
	 * a message once sent, so a device out of coverage for two minutes lost the refresh for
	 * good.  Field devices are offline routinely.  All refreshes share a collapse key, so a
	 * device that has been away for a while gets one refresh on reconnect rather than a
	 * backlog, and a long life here costs nothing.
	 */
	private static final int REFRESH_TIME_TO_LIVE_SECS = 4 * 60 * 60;

	public static String getSampleAndroidMessage(String server) {
		Map<String, Object> androidMessageMap = new HashMap<String, Object>();
		androidMessageMap.put("collapse_key", "Welcome");
		androidMessageMap.put("data", getData(server));
		/*
		 * Without this FCM treats a data only message as normal priority and holds it while
		 * the device is in doze, which the old two minute life then outlived.
		 */
		androidMessageMap.put("priority", "high");
		androidMessageMap.put("time_to_live", REFRESH_TIME_TO_LIVE_SECS);
		androidMessageMap.put("dry_run", false);
		return jsonify(androidMessageMap);
	}

	public static String getSampleBaiduMessage() {
		Map<String, Object> baiduMessageMap = new HashMap<String, Object>();
		baiduMessageMap.put("title", "New Notification Received from SNS");
		baiduMessageMap.put("description", "Hello World!");
		return jsonify(baiduMessageMap);
	}

	public static String getSampleWNSMessage() {
		Map<String, Object> wnsMessageMap = new HashMap<String, Object>();
		wnsMessageMap.put("version", "1");
		wnsMessageMap.put("value", "23");
		return "<badge version=\"" + wnsMessageMap.get("version")
				+ "\" value=\"" + wnsMessageMap.get("value") + "\"/>";
	}

	public static String getSampleMPNSMessage() {
		Map<String, String> mpnsMessageMap = new HashMap<String, String>();
		mpnsMessageMap.put("count", "23");
		mpnsMessageMap.put("payload", "This is a tile notification");
		return "<?xml version=\"1.0\" encoding=\"utf-8\"?><wp:Notification xmlns:wp=\"WPNotification\"><wp:Tile><wp:Count>"
				+ mpnsMessageMap.get("count")
				+ "</wp:Count><wp:Title>"
				+ mpnsMessageMap.get("payload")
				+ "</wp:Title></wp:Tile></wp:Notification>";
	}
}