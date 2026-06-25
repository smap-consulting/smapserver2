package model;

import java.util.logging.Logger;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

public class DeviceTable {

	private AmazonDynamoDB client;
	private DynamoDB dynamoDB;
	private String tableName;
	
	private static Logger log = Logger.getLogger(DeviceTable.class.getName());

	public DeviceTable(String region, String tableName) {
		// create a new DynamoDB client
		log.info("Getting client: " + region);
		client= AmazonDynamoDBClient.builder().withRegion(region).withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
				.build();
		dynamoDB = new DynamoDB(client);
		this.tableName = tableName;
		log.info("DeviceTable: " + region + " : " + tableName);
	}

	public ItemCollection<QueryOutcome> getUserDevices(String server, String user) {
	
		Table table = dynamoDB.getTable(tableName);
		
		Index index = table.getIndex("userIdent-smapServer-index");
		QuerySpec spec = new QuerySpec()
			    .withKeyConditionExpression("userIdent = :v_user_ident and smapServer = :v_smap_server")
			    .withValueMap(new ValueMap()
			        .withString(":v_user_ident", user)
			        .withString(":v_smap_server",server));
		ItemCollection<QueryOutcome> items = index.query(spec);
		
		return items;
	}
	
	/*
	 * Delete the token 
	 */
	public void deleteToken(String token) {
		
		// Delete the obsolete token
		Table table = dynamoDB.getTable(tableName);	
		DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(new PrimaryKey("registrationId", token));	 
		table.deleteItem(deleteItemSpec);
	}
}
