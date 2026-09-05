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
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.smap.notifications.interfaces.EmitNotifications;

import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreatePlatformEndpointRequest;
import software.amazon.awssdk.services.sns.model.CreatePlatformEndpointResponse;
import software.amazon.awssdk.services.sns.model.DeleteEndpointRequest;
import software.amazon.awssdk.services.sns.model.EndpointDisabledException;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

import model.DeviceTable;
import tools.SampleMessageGenerator.Platform;

public class AmazonSNSClientWrapper {

	private static Logger log = Logger.getLogger(EmitNotifications.class.getName());

	/*
	 * An endpoint is stable for the life of a token, and creating one is a management call
	 * with a far lower rate limit than publishing.  Shared users have thousands of devices,
	 * so calling create for every device on every run is the expensive part of a refresh.
	 * Bounded, as the estate has tens of thousands of tokens and this must not grow without
	 * limit in a process that runs for months.
	 */
	private static final int MAX_CACHED_ENDPOINTS = 100000;
	private static final ConcurrentHashMap<String, String> endpointArns = new ConcurrentHashMap<>();

	private final SnsClient snsClient;
	private final DeviceTable deviceTable;

	public AmazonSNSClientWrapper(SnsClient client, DeviceTable deviceTable) {
		this.snsClient = client;
		this.deviceTable = deviceTable;
	}

	private CreatePlatformEndpointResponse createPlatformEndpoint(Platform platform, String customData,
			String platformToken, String applicationArn) {

		CreatePlatformEndpointRequest platformEndpointRequest = CreatePlatformEndpointRequest.builder()
				.customUserData(customData)
				.token(platformToken)
				.platformApplicationArn(applicationArn)
				.build();
		return snsClient.createPlatformEndpoint(platformEndpointRequest);
	}

	private PublishResponse publish(String endpointArn, Platform platform,
			Map<Platform, Map<String, MessageAttributeValue>> attributesMap, String platformToken,
			String server) {

		PublishRequest.Builder publishRequest = PublishRequest.builder();
		Map<String, MessageAttributeValue> notificationAttributes = getValidNotificationAttributes(
				attributesMap.get(platform));
		if (notificationAttributes != null && !notificationAttributes.isEmpty()) {
			publishRequest.messageAttributes(notificationAttributes);
		}
		publishRequest.messageStructure("json");
		// If the message attributes are not set in the requisite method,
		// notification is sent with default attributes
		String message = getPlatformSampleMessage(platform, server);
		Map<String, String> messageMap = new HashMap<String, String>();
		messageMap.put(platform.name(), message);
		message = SampleMessageGenerator.jsonify(messageMap);
		// For direct publish to mobile end points, topicArn is not relevant.
		publishRequest.targetArn(endpointArn);

		// Display the message that will be sent to the endpoint/
		// log.info("{Message Body: " + message + "}");
		StringBuilder builder = new StringBuilder();
		builder.append("{Message Attributes: ");
		for (Map.Entry<String, MessageAttributeValue> entry : notificationAttributes.entrySet()) {
			builder.append("(\"" + entry.getKey() + "\": \"" + entry.getValue().stringValue() + "\"),");
		}
		builder.deleteCharAt(builder.length() - 1);
		builder.append("}");
		// log.info(builder.toString());

		publishRequest.message(message);

		PublishResponse result = null;
		try {
			result = snsClient.publish(publishRequest.build());
		} catch (EndpointDisabledException e) {
			log.info("End point disabled " + endpointArn + " deleting. " + e.getMessage());
			endpointArns.remove(platformToken);
			deviceTable.deleteToken(platformToken);
			deleteEndpoint(endpointArn);
		} catch (Exception e) {
			log.log(Level.SEVERE, e.getMessage(), e);
		}

		return result;
	}

	public void sendNotification(Platform platform, String platformToken,
			Map<Platform, Map<String, MessageAttributeValue>> attrsMap, String platformApplicationArn,
			String server) {

		try {
			// An endpoint corresponds to an app on a device, and does not change
			String endpointArn = endpointArns.get(platformToken);
			if (endpointArn == null) {
				endpointArn = createPlatformEndpoint(platform, "smap", platformToken, platformApplicationArn)
						.endpointArn();
				if (endpointArns.size() >= MAX_CACHED_ENDPOINTS) {
					endpointArns.clear();
				}
				endpointArns.put(platformToken, endpointArn);
			}

			try {
				// Publish a push notification to an Endpoint.
				PublishResponse publishResult = publish(endpointArn, platform, attrsMap, platformToken, server);
				if (publishResult != null) {
					log.fine("Published {MessageId=" + publishResult.messageId() + "}");
				}
			} catch (Exception e) {
				/*
				 * Forget the endpoint rather than keep publishing to one that may no longer
				 * exist.  The next run creates it again.
				 */
				endpointArns.remove(platformToken);
				throw e;
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to publish to end point", e);
		}
	}

	private String getPlatformSampleMessage(Platform platform, String server) {
		switch (platform) {
		case APNS:
			return SampleMessageGenerator.getSampleAppleMessage();
		case APNS_SANDBOX:
			return SampleMessageGenerator.getSampleAppleMessage();
		case GCM:
			return SampleMessageGenerator.getSampleAndroidMessage(server);
		case ADM:
			return SampleMessageGenerator.getSampleKindleMessage(server);
		case BAIDU:
			return SampleMessageGenerator.getSampleBaiduMessage();
		case WNS:
			return SampleMessageGenerator.getSampleWNSMessage();
		case MPNS:
			return SampleMessageGenerator.getSampleMPNSMessage();
		default:
			throw new IllegalArgumentException("Platform not supported : " + platform.name());
		}
	}

	public static Map<String, MessageAttributeValue> getValidNotificationAttributes(
			Map<String, MessageAttributeValue> notificationAttributes) {
		Map<String, MessageAttributeValue> validAttributes = new HashMap<String, MessageAttributeValue>();

		if (notificationAttributes == null)
			return validAttributes;

		for (Map.Entry<String, MessageAttributeValue> entry : notificationAttributes.entrySet()) {
			if (!StringUtils.isBlank(entry.getValue().stringValue())) {
				validAttributes.put(entry.getKey(), entry.getValue());
			}
		}
		return validAttributes;
	}

	private void deleteEndpoint(String endpointArn) {
		// Delete the disabled platform endpoint
		DeleteEndpointRequest deleteEndPointRequest = DeleteEndpointRequest.builder().endpointArn(endpointArn).build();
		try {
			snsClient.deleteEndpoint(deleteEndPointRequest);
		} catch (Exception ex) {
			log.log(Level.SEVERE, "Deleting disabled end point: " + endpointArn, ex);
		}
	}
}