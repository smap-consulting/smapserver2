package model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

public class DeviceTable {

	// DynamoDB clients are thread-safe and expensive to create, so share one per region
	private static final java.util.concurrent.ConcurrentHashMap<String, DynamoDbClient> clients =
			new java.util.concurrent.ConcurrentHashMap<>();

	private DynamoDbClient client;
	private String tableName;

	private static Logger log = Logger.getLogger(DeviceTable.class.getName());

	public DeviceTable(String region, String tableName) {
		// Get a shared DynamoDB client for this region
		log.info("Getting client: " + region);
		client = clients.computeIfAbsent(region, r -> DynamoDbClient.builder()
				.region(Region.of(r))
				.credentialsProvider(DefaultCredentialsProvider.create())
				.build());
		this.tableName = tableName;
		log.info("DeviceTable: " + region + " : " + tableName);
	}

	public List<Map<String, AttributeValue>> getUserDevices(String server, String user) {

		Map<String, AttributeValue> values = new HashMap<>();
		values.put(":v_user_ident", AttributeValue.fromS(user));
		values.put(":v_smap_server", AttributeValue.fromS(server));

		QueryRequest spec = QueryRequest.builder()
				.tableName(tableName)
				.indexName("userIdent-smapServer-index")
				.keyConditionExpression("userIdent = :v_user_ident and smapServer = :v_smap_server")
				.expressionAttributeValues(values)
				.build();

		return client.query(spec).items();
	}

	/*
	 * Delete the token
	 */
	public void deleteToken(String token) {

		// Delete the obsolete token
		Map<String, AttributeValue> key = new HashMap<>();
		key.put("registrationId", AttributeValue.fromS(token));
		client.deleteItem(DeleteItemRequest.builder()
				.tableName(tableName)
				.key(key)
				.build());
	}
}
