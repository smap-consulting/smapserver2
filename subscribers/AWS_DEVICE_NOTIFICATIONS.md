# AWS Device Notification System

## Overview

Device notifications send push messages to mobile devices (FieldTask app) when data changes that users need to know about - tasks assigned, surveys updated, projects modified, or resources changed. Uses AWS SNS (Simple Notification Service) and DynamoDB to deliver real-time updates.

## Architecture

### Components

1. **Subscribers Module** (`subscribers/src/SubscriberBatch.java`)
   - Triggers device notification processing every 10 cycles (throttled for performance)
   - Calls `MessagingManagerApply.applyDeviceMessages()`

2. **Messaging Manager** (`sdDAL/src/.../MessagingManagerApply.java`)
   - Aggregates message changes by topic (task/survey/project/resource/user)
   - Determines affected users
   - Calls AWS notification interface

3. **AWS Interface** (`smap2/amazon/src/.../EmitDeviceNotification.java`)
   - Queries DynamoDB for device registration tokens
   - Sends push notifications via AWS SNS

4. **Supporting Classes**
   - `DeviceTable.java`: DynamoDB access for user-device mappings
   - `AmazonSNSClientWrapper.java`: SNS client for push notifications
   - `SampleMessageGenerator.java`: Platform-specific message formats

## Detailed Flow

### 1. Trigger (SubscriberBatch.java:294-312)

```java
// Runs in forward mode only, every 10 cycles (~10 minutes)
if(deviceRefreshInterval-- <= 0) {
    String awsPropertiesFile = basePath + "_bin/resources/properties/aws.properties";
    if (pFile.exists()) {
        mma.applyDeviceMessages(sd, results, serverName, awsPropertiesFile);
    }
    deviceRefreshInterval = 10;
}
```

**Key Points**:
- Throttled to reduce server load (TODO comment indicates this is temporary)
- Requires AWS properties file to be configured
- Only runs in "forward" mode (not "upload" mode)

### 2. Message Aggregation (MessagingManagerApply.java:381-505)

#### 2.1 Query Messages (lines 402-448)

```sql
SELECT id, topic, data
FROM message
WHERE outbound
  AND processed_time IS NULL
  AND topic IN ('task', 'survey', 'user', 'project', 'resource')
```

**Topics that trigger device notifications**:
- `task`: Task assignments/updates
- `survey`: Survey changes (new, updated, deleted)
- `user`: User account changes
- `project`: Project modifications
- `resource`: Shared resource updates

#### 2.2 Accumulate Changes (lines 416-447)

Messages are aggregated into HashMaps to eliminate duplicates:
- `changedTasks`: Map<taskId, TaskMessage>
- `changedSurveys`: Map<surveyId, SurveyMessage>
- `changedProjects`: Map<projectId, ProjectMessage>
- `changedResources`: Map<resourceName, OrgResourceMessage>
- `usersImpacted`: Map<userIdent, userIdent>

Each message is immediately marked as processed:
```java
pstmtConfirm.setString(1, "success");
pstmtConfirm.setInt(2, id);
pstmtConfirm.executeUpdate();
```

**Important**: Messages are marked processed BEFORE notification sent. Failures won't retry automatically.

#### 2.3 User Resolution (lines 454-488)

For each changed entity, find affected users:

**Tasks** (lines 455-461):
```sql
SELECT u.ident
FROM tasks t, assignments a, users u
WHERE a.task_id = t.id
  AND a.assignee = u.id
  AND NOT u.temporary
  AND t.id = ?
```

**Surveys** (lines 464-470, 734-764):
```sql
SELECT u.ident
FROM users u, user_project up, survey s, user_group ug
WHERE u.id = up.u_id
  AND u.id = ug.u_id
  AND ug.g_id = 3  -- enum role
  AND s.p_id = up.p_id
  AND s.s_id = ?
  AND NOT u.temporary
  -- Plus survey_role filtering for role-based access
```

**Projects** (lines 473-479, 769-797):
```sql
SELECT u.ident
FROM users u, user_project up, user_group ug
WHERE u.id = up.u_id
  AND u.id = ug.u_id
  AND NOT u.temporary
  AND ug.g_id = 3  -- enum role
  AND up.p_id = ?
```

**Resources** (lines 482-487):
Uses `GeneralUtilityMethods.getResourceUsers(sd, fileName, orgId)` to find users with access to shared resource.

**Users** (lines 437-439):
Direct user ident from message.

#### 2.4 Send Notifications (lines 491-496)

```java
if(awsProperties != null && usersImpacted.size() > 0) {
    EmitDeviceNotification emitDevice = new EmitDeviceNotification(awsProperties);
    for(String user : usersImpacted.keySet()) {
        emitDevice.notify(serverName, user);
    }
}
```

### 3. AWS Configuration (EmitDeviceNotification.java:45-66)

Reads from `aws.properties` file:
```properties
userDevices_table=<DynamoDB table name>
userDevices_region=<AWS region>
fieldTask_platform=<SNS Platform Application ARN>
```

Example ARN: `arn:aws:sns:us-west-2:123456789:app/GCM/FieldTask`

Creates AWS SNS client:
```java
sns = AmazonSNSClient.builder()
    .withRegion(region)
    .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
    .build();
```

### 4. Device Token Lookup (EmitDeviceNotification.java:71-116)

#### 4.1 Query DynamoDB (DeviceTable.java:36-49)

```java
Index index = table.getIndex("userIdent-smapServer-index");
QuerySpec spec = new QuerySpec()
    .withKeyConditionExpression("userIdent = :v_user_ident and smapServer = :v_smap_server")
    .withValueMap(new ValueMap()
        .withString(":v_user_ident", user)
        .withString(":v_smap_server", server));
ItemCollection<QueryOutcome> items = index.query(spec);
```

**DynamoDB Schema**:
- Primary Key: `registrationId` (device token from Firebase/GCM)
- GSI: `userIdent-smapServer-index`
  - Partition key: `userIdent` (username)
  - Sort key: `smapServer` (server hostname)

Returns all devices registered for user on this server.

#### 4.2 Send to Each Device (EmitDeviceNotification.java:87-98)

```java
while (iter.hasNext()) {
    Item item = iter.next();
    String token = item.getString("registrationId");

    Map<Platform, Map<String, MessageAttributeValue>> attrsMap = new HashMap<>();
    snsClientWrapper.sendNotification(Platform.GCM, token, attrsMap, platformApplicationArn);
}
```

**Handling Multiple Devices**:
- User may have multiple devices (phone, tablet)
- Each gets separate notification
- Commented code for deleting obsolete tokens (disabled due to Firebase endpoint issues)

### 5. SNS Notification (AmazonSNSClientWrapper.java:104-123)

#### 5.1 Create Platform Endpoint (lines 48-56, 109-112)

```java
CreatePlatformEndpointRequest platformEndpointRequest = new CreatePlatformEndpointRequest();
platformEndpointRequest.setCustomUserData("smap");
platformEndpointRequest.setToken(platformToken);
platformEndpointRequest.setPlatformApplicationArn(applicationArn);
CreatePlatformEndpointResult result = snsClient.createPlatformEndpoint(platformEndpointRequest);
```

Creates or retrieves SNS endpoint for device token.

#### 5.2 Publish Message (lines 58-102, 114-118)

```java
PublishRequest publishRequest = new PublishRequest();
publishRequest.setMessageStructure("json");
publishRequest.setTargetArn(endpointArn);
publishRequest.setMessage(message);
PublishResult result = snsClient.publish(publishRequest);
```

**Message Format** (SampleMessageGenerator.java:84-92):
```json
{
  "GCM": "{\"collapse_key\":\"Welcome\",\"data\":{\"message\":\"Hello World!\"},\"delay_while_idle\":true,\"time_to_live\":125,\"dry_run\":false}"
}
```

**GCM/Firebase Parameters**:
- `collapse_key`: "Welcome" - groups notifications
- `data.message`: "Hello World!" - static message
- `delay_while_idle`: true - defer if device idle
- `time_to_live`: 125 seconds - notification lifespan
- `dry_run`: false - actually send

#### 5.3 Error Handling (lines 93-99)

```java
try {
    result = snsClient.publish(publishRequest);
} catch (EndpointDisabledException e) {
    log.info("End point disabled, deleting: " + endpointArn);
    deviceTable.deleteToken(platformToken);
    deleteEndpoint(endpointArn);
}
```

Automatically removes disabled/invalid device registrations.

## Message Content

### Current Implementation

**Static Message**: "Hello World!" hardcoded in `SampleMessageGenerator.getData()` (line 62)

**Purpose**: Silent data notification
- FieldTask app receives notification
- App refreshes data from server
- No user-visible message required

### Platform-Specific Messages

**Android/GCM** (lines 84-92):
```json
{
  "collapse_key": "Welcome",
  "data": {"message": "Hello World!"},
  "delay_while_idle": true,
  "time_to_live": 125,
  "dry_run": false
}
```

**iOS/APNS** (lines 66-74):
```json
{
  "aps": {
    "alert": "You have got email.",
    "badge": 9,
    "sound": "default"
  }
}
```

**Other Platforms**: Kindle (ADM), Baidu, Windows (WNS), Windows Phone (MPNS) supported but unused.

## AWS Resource Setup

### DynamoDB Table: userDevices

**Schema**:
```
Primary Key: registrationId (String)
Attributes:
  - userIdent (String) - username
  - smapServer (String) - server hostname
  - timestamp (Number) - registration time

Global Secondary Index: userIdent-smapServer-index
  Partition Key: userIdent
  Sort Key: smapServer
```

**Registration Flow** (not in this module):
- FieldTask app registers device token via REST API
- API writes to DynamoDB with user/server/token mapping
- Multiple devices per user supported

### SNS Platform Application

**Setup**:
1. Create SNS Platform Application in AWS Console
2. Configure with Firebase Server Key (for GCM)
3. Note ARN (e.g., `arn:aws:sns:us-west-2:123456789:app/GCM/FieldTask`)
4. Add to `aws.properties`

**Platform Application Types**:
- GCM (Google Cloud Messaging) - Android
- APNS (Apple Push Notification Service) - iOS
- Others: ADM, Baidu, WNS, MPNS

## Configuration Files

### aws.properties Location

```
/smap_bin/resources/properties/aws.properties
```

**Required Properties**:
```properties
# DynamoDB table storing user-device mappings
userDevices_table=smap_user_devices

# AWS region for DynamoDB
userDevices_region=us-west-2

# SNS Platform Application ARN for FieldTask
fieldTask_platform=arn:aws:sns:us-west-2:123456789:app/GCM/FieldTask
```

### AWS Credentials

Uses `DefaultAWSCredentialsProviderChain`:
1. Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
2. System properties: `aws.accessKeyId`, `aws.secretKey`
3. Credentials file: `~/.aws/credentials`
4. IAM role (if running on EC2)

**Required Permissions**:
```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:Query",
    "dynamodb:DeleteItem",
    "sns:CreatePlatformEndpoint",
    "sns:DeleteEndpoint",
    "sns:Publish"
  ],
  "Resource": "*"
}
```

## Performance Considerations

### Throttling (SubscriberBatch.java:294)

```java
// TODO: temporary salve to high load caused by these refreshes
if(deviceRefreshInterval-- <= 0) {
    // Process device messages
    deviceRefreshInterval = 10;
}
```

**Current Behavior**:
- Processes device messages every ~10 minutes (10 cycles × 60 seconds)
- Prevents overwhelming AWS SNS rate limits
- Reduces database query load

**Impact**:
- Notifications delayed up to 10 minutes
- Multiple changes within window batched
- Duplicate notifications eliminated by aggregation

### Message Aggregation Benefits

1. **Deduplication**: Multiple task updates → single notification
2. **Batching**: All changes in 10-minute window processed together
3. **User consolidation**: Changes affecting same user merged

**Example**:
```
Minute 0: Task 123 assigned to user A
Minute 2: Task 123 updated
Minute 5: Task 456 assigned to user A
Minute 10: Single notification sent to user A
```

## Debugging

### Log Locations

**Subscribers Log**:
```bash
tail -f /var/log/tomcat9/catalina.out | grep "device_message"
```

**Key Log Messages**:
```
# Device message processing triggered
"Skipping Message Processing. No aws properties file at: ..."

# User affected by changes
"zzzzzzzzzzzzzzz: task change users: username"
"zzzzzzzzzzzzzzz: survey change users: username"

# Token lookup
"Token: ABC123... for server.example.com:username"
"Token not found for server.example.com:username"

# Notification sent
"Published! {MessageId=abc-123-def}"

# Endpoint disabled
"End point disabled ... deleting."
```

### Common Issues

**1. No aws.properties file**
```
Error: Skipping Message Processing. No aws properties file at: /smap_bin/resources/properties/aws.properties
```
**Solution**: Create properties file with correct values

**2. No users impacted**
```
Log shows message processing but no "zzzzz" user lines
```
**Causes**:
- User marked as temporary
- User doesn't have enum role (g_id != 3)
- Survey role restrictions excluding user
**Solution**: Verify user setup in database

**3. Token not found**
```
Log: "Token not found for server.example.com:username"
```
**Causes**:
- Device never registered
- Device uninstalled/cleared data
- Server name mismatch
**Solution**:
- Check DynamoDB for registration
- Re-register device from FieldTask app
- Verify server hostname matches

**4. Endpoint disabled**
```
Log: "End point disabled ... deleting"
```
**Causes**:
- App uninstalled
- Token expired/invalid
- Firebase project changed
**Solution**: Device will auto-reregister on next app launch

## Testing

### Manual Test

```java
// In SubscriberBatch.java, temporarily set:
deviceRefreshInterval = 1;  // Test every cycle

// Create test message:
INSERT INTO message (o_id, topic, data, outbound, processed_time)
VALUES (1, 'task', '{"id":123}', true, null);

// Watch logs:
tail -f /var/log/tomcat9/catalina.out | grep -E "device_message|zzzzz|Published"
```

### DynamoDB Query Test

```bash
aws dynamodb query \
  --table-name smap_user_devices \
  --index-name userIdent-smapServer-index \
  --key-condition-expression "userIdent = :user AND smapServer = :server" \
  --expression-attribute-values '{":user":{"S":"testuser"},":server":{"S":"dev.smap.com.au"}}'
```

### SNS Publish Test

```bash
# Get endpoint ARN first, then:
aws sns publish \
  --target-arn "arn:aws:sns:us-west-2:123:endpoint/GCM/FieldTask/abc-123" \
  --message '{"GCM":"{\"data\":{\"message\":\"Test\"}}"}'
```

## Security

### Credentials
- Never commit `aws.properties` to version control
- Use IAM roles on EC2 when possible
- Rotate access keys regularly

### DynamoDB
- Table should have encryption at rest enabled
- Use VPC endpoints to avoid public internet

### SNS
- Platform Application secrets managed by AWS
- Endpoint ARNs expire automatically when token invalid
- Messages encrypted in transit (TLS)

## Future Enhancements

### Potential Improvements

1. **Dynamic Message Content**: Include change details in notification payload
2. **Priority Queuing**: Urgent notifications bypass 10-minute throttle
3. **User Preferences**: Per-user notification settings
4. **Analytics**: Track delivery rates, failed tokens
5. **Multi-Region**: Failover to backup SNS region
6. **Retry Logic**: Requeue failed notifications
7. **Rate Limiting**: Per-user throttling to prevent spam

### Code Locations for Modifications

**Change message content**: `SampleMessageGenerator.java:60-64`
**Adjust throttling**: `SubscriberBatch.java:294,311`
**Add notification types**: `MessagingManagerApply.java:408`
**Custom user filtering**: `MessagingManagerApply.java:734-832`
