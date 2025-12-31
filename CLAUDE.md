# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

### Full Build and Deployment
```bash
# Build all modules and deploy to ~/deploy/smap/deploy/version1
./dep.sh
```

### Individual Module Builds
```bash
# Shared libraries (build in this order)
cd sdDAL && mvn clean install && cd ..
cd sdDataAccess && mvn clean install && cd ..

# Web services (WAR files)
cd surveyMobileAPI && mvn clean install && cd ..
cd koboToolboxApi && mvn clean install && cd ..
cd surveyKPI && mvn clean install && cd ..

# Batch processor (JAR file)
cd subscribers && ant -f subscriber3.xml && cd ..
```

### Installing JavaRosa Dependency
```bash
# Required for sdDAL - run from repository root
mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file \
  -Dfile=./surveyKPI/src/main/webapp/WEB-INF/lib/javarosa-3.1.4.jar \
  -DgroupId=smapserver -DartifactId=javarosa -Dversion=3.1.4 -Dpackaging=jar
```

## Architecture Overview

### Module Structure
Smap Server is a multi-module Java web application for survey management and data collection:

- **surveyMobileAPI** - REST APIs for mobile data collection clients (form submission, retrieval)
- **surveyKPI** - Admin console REST APIs (survey management, user management, reporting, analytics)
- **koboToolboxApi** - Kobo Toolbox compatible data APIs (ODK compliant)
- **subscribers** - Batch processor that dequeues and applies submitted XML to the database

### Shared Libraries
- **sdDAL** - Core data access layer containing 79+ Manager classes and 255+ model/DAO classes
- **sdDataAccess** - Legacy JDBC wrapper for backward compatibility
- **amazon** - AWS service integration (located in separate `smap2` repository - must be cloned separately)
- **sms** - SMS and WhatsApp messaging interface

### Database Architecture
Two PostgreSQL databases with PostGIS and pgcrypto extensions:

- **survey_definitions** - Survey metadata, forms, questions, users, roles, organizations
- **results** - Dynamic tables created per survey to store submission data

JNDI datasources: `jdbc/survey_definitions` and `jdbc/results`

## Key Architectural Patterns

### REST API Pattern (Jersey/JAX-RS)
All web services use Jersey 2.40. Endpoints are classes with `@Path` annotations:

```java
@Path("/resource")
public class ResourceEndpoint {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getResource(@Context HttpServletRequest request) {
        // Get connection
        Connection sd = SDDataSource.getConnection("surveyKPI");
        try {
            // Use Manager classes for business logic
            SomeManager mgr = new SomeManager(localization, timezone);
            // Return Response
        } finally {
            SDDataSource.closeConnection("surveyKPI", sd);
        }
    }
}
```

### Manager Pattern
Business logic resides in Manager classes in sdDAL (`org.smap.sdal.managers`). Key managers:

- **SurveyManager** - Survey CRUD, hierarchy, options, server calculations
- **NotificationManager** - Message queue and notification forwarding
- **SubmissionsManager** - Results database operations
- **UserManager** - User and role management
- **TaskManager** - Task assignment and tracking
- **KeyManager** - Authentication keys and tokens
- **UploadManager** - Form submission handling

Managers follow this pattern:
- Constructor accepts ResourceBundle (i18n) and TimeZone
- Methods perform atomic database operations using PreparedStatements
- Throw ApplicationException, NotFoundException, etc. for error handling

### Database Connection Pattern
```java
Connection sd = SDDataSource.getConnection("contextName");
Connection cResults = SDDataSource.getConnection("contextName");
try {
    // Database operations
} finally {
    SDDataSource.closeConnection("contextName", cResults);
    SDDataSource.closeConnection("contextName", sd);
}
```

Always close connections in finally blocks. Use PreparedStatement for SQL safety.

### Authorization Pattern
```java
Authorise a = new Authorise(Arrays.asList(Authorise.ANALYST, Authorise.ADMIN), null);
a.isAuthorised(sd, userName); // Throws exception if unauthorized
```

Roles: ENUM, ANALYST, ADMIN, ORG_ADMIN, MANAGE, SECURITY, VIEW_DATA, etc.

### Batch Processing (Subscribers)
The subscribers module processes form submissions asynchronously:

1. **Upload** - surveyMobileAPI receives XML submission, creates `upload_event` record
2. **Queue** - Entry added to `submission_queue` table
3. **Dequeue** - SubmissionProcessor dequeues using `FOR UPDATE SKIP LOCKED`
4. **Apply** - SubRelationalDB parses XML and inserts into results database
5. **Notify** - NotificationManager creates forward records for downstream processing

Run subscribers as: `java -jar subscribers.jar default /smap upload` (or `forward`)

Multiple processor types handle different queues:
- SubmissionProcessor - Form submissions
- MessageProcessor - Outbound SMS/email/webhooks
- ReportProcessor - Report generation
- SubEventProcessor - Submission events
- AutoUpdateProcessor - S3 sync
- StorageProcessor - Media file storage

## Detailed Submission Flow: POST /submission → Database

This section documents the complete flow from receiving a form submission to updating the database.

### 1. REST Endpoint (surveyMobileAPI)

**File**: `surveyMobileAPI/src/surveyMobileAPI/Upload.java:55-66`

```java
@POST
@Path("/submission")
@Consumes(MediaType.MULTIPART_FORM_DATA)
public Response postInstance(@QueryParam("deviceID") String deviceId,
                             @Context HttpServletRequest request) {
    UploadManager ulm = new UploadManager();
    return ulm.submission(request, null, null, deviceId);
}
```

**What happens**:
- Jersey routes POST to `/submission` to this method
- Receives multipart form data (XML + media attachments)
- Delegates to UploadManager.submission()

### 2. Upload Manager Processing

**File**: `surveyMobileAPI/src/surveyMobileAPI/managers/UploadManager.java:86-202`

**Authentication** (lines 97-123):
- Checks for dynamic user key (task assignments)
- Falls back to session user (request.getRemoteUser())
- Falls back to token authentication
- Validates authorization with Authorise class

**Main processing** (line 161):
```java
XFormData xForm = new XFormData();
xForm.loadMultiPartMime(request, user, instanceId, deviceId, isDynamicUser);
```

**What happens**:
- Gets database connection from SDDataSource
- Authenticates user via key, session, or token
- Creates XFormData instance
- Calls loadMultiPartMime() to process the submission
- Returns OpenRosa-compliant XML response

### 3. XForm Data Processing

**File**: `surveyMobileAPI/src/surveyMobileAPI/XFormData.java:98-430`

**Parse multipart form** (lines 108-306):
- Uses Apache Commons FileUpload to parse multipart request
- Extracts XML submission file and media attachments
- Parses XML into SurveyInstance object using JavaRosa
- Saves files to disk in basePath directory structure
- Validates survey is not deleted or blocked
- Checks organization submission limits

**Create upload event** (lines 377-425):
```java
UploadEvent ue = new UploadEvent();
ue.setUserName(user);
ue.setSurveyId(survey.surveyData.id);
ue.setFilePath(saveDetails.filePath);
ue.setInstanceId(thisInstanceId);
ue.setStatus("success");
// ... set all metadata fields

JdbcUploadEventManager uem = new JdbcUploadEventManager(sd);
uem.write(ue, false);  // results_db_applied = false
```

**What happens**:
- Parses XML submission and media files from multipart request
- Creates SurveyInstance object (JavaRosa model)
- Saves XML file and attachments to filesystem
- Validates survey permissions and resource limits
- Creates UploadEvent with metadata (user, device, location, timestamps)
- Writes to `upload_event` table with `results_db_applied=false`

### 4. Upload Event Table Write

**File**: `sdDataAccess/src/JdbcManagers/JdbcUploadEventManager.java:119-152`

**SQL Insert** (lines 33-68):
```sql
INSERT INTO upload_event (
    ue_id, upload_time, user_name, file_name, survey_name,
    imei, status, location, server_name, s_id, p_id, o_id,
    file_path, instanceid, assignment_id, results_db_applied, ...
) VALUES (
    nextval('ue_seq'), now(), ?, ?, ?, ?, 'success', ...
)
```

**What happens**:
- Inserts record into `upload_event` table in `survey_definitions` database
- Sets `results_db_applied = false` (data not yet in results DB)
- Sets `queued = false` (not yet added to submission_queue)
- Stores file path, instance ID, survey ID, user, timestamps
- At this point, the REST API returns success to the client

### 5. Queue Population (SubscriberBatch)

**File**: `subscribers/src/SubscriberBatch.java:196-228`

**Runs continuously in "upload" mode**, polling for pending uploads:

**Get pending uploads** (line 203):
```java
List<UploadEvent> uel = uem.getPending();
```

Uses query from JdbcUploadEventManager.java:99-104:
```sql
SELECT * FROM upload_event
WHERE status = 'success'
  AND s_id IS NOT NULL
  AND NOT incomplete
  AND NOT results_db_applied
  AND NOT queued
ORDER BY ue_id ASC
```

**Enqueue to submission_queue** (lines 210-226):
```java
pstmtEnqueue.setInt(1, ue.getId());          // ue_id
pstmtEnqueue.setString(2, ue.getInstanceId()); // instanceid
pstmtEnqueue.setBoolean(3, ue.getRestore());   // restore flag
pstmtEnqueue.setString(4, gson.toJson(ue));    // payload (full UploadEvent as JSON)
pstmtEnqueue.executeUpdate();

// Mark as queued
pstmtQueueDone.setInt(1, ue.getId());
pstmtQueueDone.executeUpdate();
```

**SQL Insert** (lines 120-121):
```sql
INSERT INTO submission_queue (
    element_identifier, time_inserted, ue_id, instanceid, restore, payload
) VALUES (
    gen_random_uuid(), current_timestamp, ?, ?, ?, ?::jsonb
)
```

**What happens**:
- SubscriberBatch polls `upload_event` table continuously
- Finds records with `results_db_applied=false` and `queued=false`
- Checks for duplicates in `submission_queue` (by instanceid)
- Inserts into `submission_queue` with full UploadEvent as JSON payload
- Updates `upload_event` setting `queued=true`

### 6. Submission Dequeue and Processing

**File**: `subscribers/src/SubmissionProcessor.java:85-360`

**Runs continuously as multi-threaded processor**, one thread per queue (qu1, qu2, etc.)

**Dequeue SQL** (lines 106-118):
```sql
DELETE FROM submission_queue q
WHERE q.element_identifier = (
    SELECT q_inner.element_identifier
    FROM submission_queue q_inner
    [WHERE NOT restore]  -- optional filter
    ORDER BY q_inner.time_inserted ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING q.time_inserted, q.ue_id, q.payload
```

**FOR UPDATE SKIP LOCKED** ensures:
- Only one processor thread grabs each submission
- No blocking if another thread is processing
- FIFO ordering by time_inserted

**Process submission** (lines 162-274):
```java
ResultSet rs = pstmt.executeQuery();  // Dequeue
if (rs.next()) {
    // Deserialize UploadEvent from JSON payload
    UploadEvent ue = gson.fromJson(rs.getString("payload"), UploadEvent.class);

    // Read XML file from disk
    InputStream is = new FileInputStream(ue.getFilePath());
    SurveyInstance instance = new SurveyInstance(is);

    // Get survey template and extend instance with metadata
    SurveyTemplate template = new SurveyTemplate(orgLocalisation);
    template.readDatabase(sd, cResults, templateName, false);
    template.extendInstance(sd, instance, true, sdalSurvey);

    // Call subscriber to write to results database
    Subscriber subscriber = new SubRelationalDB();
    mediaChanges = subscriber.upload(log, instance, is3,
                                     ue.getUserName(), ..., survey);
}
```

**Update upload_event** (lines 93-100):
```sql
UPDATE upload_event
SET results_db_applied = 'true',
    processed_time = now(),
    queued = false,
    db_status = ?,
    db_reason = ?,
    queue_name = ?
WHERE ue_id = ?
```

**What happens**:
- Dequeues one submission using FOR UPDATE SKIP LOCKED
- Deserializes UploadEvent from JSON payload
- Reads XML file from filesystem (or retrieves from S3 if archived)
- Parses into SurveyInstance object
- Loads survey template to understand form structure
- Calls SubRelationalDB.upload() to write to results database
- Updates `upload_event` with `results_db_applied=true`
- Sends media files to S3 if configured

### 7. Results Database Write (SubRelationalDB)

**File**: `subscribers/src/org/smap/subscribers/SubRelationalDB.java:103-425`

**Main upload method** (lines 103-107):
```java
public ArrayList<MediaChange> upload(Logger log, SurveyInstance instance,
    InputStream is, String submittingUser, boolean temporaryUser,
    String server, String device, SubscriberEvent se, ..., Survey survey)
```

**Write all tables** (line 136-138):
```java
String thread = writeAllTableContent(sd, cResults, instance,
                                     submittingUser, server, device,
                                     formStatus, updateId, uploadTime, ...);
```

**File**: `subscribers/src/org/smap/subscribers/SubRelationalDB.java:326-425`

**Create tables if needed** (lines 577-578):
```java
UtilityMethods.createSurveyTables(sd, cResults, localisation,
                                 sId, sIdent, tz, lockTableChange);
```

**Write record** (lines 637-661):
```java
pstmt = getSubmissionStatement(sd, cResults, sIdent, device, server,
                               tableName, columns, thisTableKeys,
                               parent_key, remoteUser, cms, complete,
                               uploadTime, sId, version, ...);

log.info("Insert: " + pstmt.toString());
pstmt.executeUpdate();
ResultSet rs = pstmt.getGeneratedKeys();
if (rs.next()) {
    parent_key = rs.getInt(1);  // Get new primary key
    keys.newKey = parent_key;
}
```

**Recursive processing** (lines 700+):
- After writing main form record, recursively processes child elements
- Repeating groups written to separate tables with `parkey` foreign key
- Geopolygon/linestring questions create separate tables
- Each record gets generated primary key

**What happens**:
- Creates survey tables in `results` database if they don't exist
- Checks for duplicate submissions (by UUID)
- Begins transaction with autocommit=false
- Builds INSERT statement dynamically based on form questions
- Inserts main form record, getting generated primary key
- Recursively processes repeating groups and child forms
- Each repeating group row links to parent via `parkey` column
- Commits transaction
- Handles merge/replace logic if Human Readable Key (HRK) exists
- Creates submission event for downstream notifications

### 8. Post-Processing

**File**: `subscribers/src/org/smap/subscribers/SubRelationalDB.java:144`

**Create submission event** (line 144):
```java
SubmissionEventManager sem = new SubmissionEventManager();
sem.writeToQueue(log, sd, ue_id, null, thread);
```

This creates an entry in the submission event queue that triggers:
- Notification processing (email, SMS, webhooks)
- Task updates
- Case management actions
- Server-side calculations
- Foreign key linkages

**What happens**:
- Submission event added to queue for SubEventProcessor
- Allows notifications, tasks, and case management to be processed asynchronously
- Separates data persistence from business logic side effects

### Summary: Complete Flow

```
1. POST /submission
   ↓
2. Upload.java (@Path("/submission"))
   ↓
3. UploadManager.submission()
   ↓
4. XFormData.loadMultiPartMime()
   - Parse multipart form
   - Extract XML + media files
   - Save files to disk
   - Create UploadEvent object
   ↓
5. JdbcUploadEventManager.write()
   INSERT INTO upload_event (..., results_db_applied=false, queued=false)
   ↓
   [Client receives 201 Created response - submission accepted]
   ↓
6. SubscriberBatch (upload mode, continuous polling)
   SELECT * FROM upload_event WHERE NOT results_db_applied AND NOT queued
   ↓
   INSERT INTO submission_queue (ue_id, instanceid, payload)
   UPDATE upload_event SET queued=true
   ↓
7. SubmissionProcessor (continuous dequeue, multi-threaded)
   DELETE FROM submission_queue ... FOR UPDATE SKIP LOCKED RETURNING payload
   ↓
   - Deserialize UploadEvent from JSON
   - Read XML file
   - Parse into SurveyInstance
   - Load SurveyTemplate
   ↓
8. SubRelationalDB.upload()
   - Create tables if needed (results DB)
   - Check duplicates
   - Begin transaction
   ↓
   writeAllTableContent()
     ↓
     writeTableContent() (recursive)
       - Build INSERT statement for form
       - Execute INSERT, get primary key
       - Process repeating groups (recursive)
       - Link via parkey foreign keys
   ↓
   - Commit transaction
   - Update upload_event SET results_db_applied=true
   ↓
9. SubmissionEventManager.writeToQueue()
   - Queue for notifications
   - Queue for task updates
   - Queue for case management
   ↓
   [Submission fully processed and in database]
```

### Key Database Tables

**survey_definitions database**:
- `upload_event` - Tracks all submissions, their status, and metadata
- `submission_queue` - Queue of submissions awaiting processing
- `message_queue` - Queue of outbound messages
- `submission_event` - Queue of submission events for post-processing
- `forward` - Notification forwarding records
- `message` - Outbound messages (SMS, email, webhooks)

**results database**:
- `s{survey_id}_f{form_id}` - Main form data tables (dynamic)
- Repeating group tables with `parkey` linking to parent
- Geospatial columns with PostGIS geometry types
- Standard metadata columns: `_user`, `_survey_notes`, `_location_trigger`, `_upload_time`, etc.

### Concurrency and Reliability

**Queue-based architecture**:
- `FOR UPDATE SKIP LOCKED` prevents contention between processor threads
- Multiple processor instances (qu1, qu2) process in parallel
- Failures leave items in queue for retry
- Transaction rollback on error prevents partial writes

**Idempotency**:
- Duplicate check by instance UUID prevents double-processing
- Queue checks prevent duplicate enqueuing
- Human Readable Key (HRK) enables merge/replace policies

**Scalability**:
- Asynchronous processing decouples submission receipt from database write
- REST API returns quickly after writing upload_event
- Batch processors scale horizontally (multiple queue instances)
- Database writes happen in dedicated worker processes

### Notification/Event System
Topic-based messaging via `forward` table with topics:
- `submission` - Form submissions
- `task` - Task notifications
- `email_task` - Email task notifications
- `cm_alert` - Case management alerts
- `periodic` - Periodic notifications
- `mailout` - Email campaigns
- `server_calc` - Server calculations
- `survey`, `user`, `project`, `resource` - Entity changes

## Development Setup

### Prerequisites
- Java SDK 11
- Maven 3.x
- Ant (for subscribers build)
- PostgreSQL with PostGIS and pgcrypto extensions
- Apache Tomcat 9
- Clone both repositories:
  - `smapserver2` (this repo)
  - `smap2` (contains amazon module)

### IDE Setup
Can use Apache Netbeans or Eclipse. Key requirements:
- Project facets: Dynamic Web Module 4.0, Java 11, JAX-RS 2.1
- Add module dependencies to build path and deployment assembly per README

### Test Environment Database Setup
```sql
-- Create user and databases
CREATE USER ws WITH PASSWORD 'ws1234';
CREATE DATABASE survey_definitions;
CREATE DATABASE results;

-- Add extensions to both databases
CREATE EXTENSION postgis;
CREATE EXTENSION pgcrypto;

-- Initialize schemas
\i setup/install/setupDb.sql      -- Run in survey_definitions
\i setup/install/resultsDb.sql     -- Run in results
```

### Tomcat Configuration
Edit `conf/context.xml` to add JNDI datasources (see `setup/install/config_files/context.xml`):
```xml
<Resource name="jdbc/survey_definitions" ... />
<Resource name="jdbc/results" ... />
```

Edit `conf/server.xml` to enable port 8009 for AJP (see `setup/install/config_files/server.xml.tomcat9`).

### Running Subscribers Locally
As Java applications with main class `Manager`:
- Upload mode: Arguments `default /smap upload`
- Forward mode: Arguments `default /smap forward`

## Common Development Tasks

### Adding a New REST Endpoint
1. Create class in appropriate module's package (e.g., `surveyKPI/src`)
2. Add `@Path("/newEndpoint")` annotation
3. Implement methods with `@GET`, `@POST`, etc.
4. Use Manager classes from sdDAL for business logic
5. Jersey auto-discovers via package scanning

### Modifying Database Operations
1. Locate or create Manager class in sdDAL
2. Use PreparedStatement for all SQL
3. Get connections via SDDataSource
4. Always close connections in finally blocks
5. Add localized error messages to ResourceBundle

### Working with Submissions
- Submission XML follows XForm standard (ODK/JavaRosa)
- Upload creates `upload_event` with JSON metadata
- SubmissionProcessor dequeues from `submission_queue`
- SubRelationalDB.upload() applies XML to dynamic result tables
- Repeating groups create separate relational tables

### Dynamic Result Tables
Each survey form gets its own table in the `results` database:
- Table name based on form ID
- Columns created dynamically from question definitions
- Geospatial columns use PostGIS geometry types
- Parent-child relationships via `parkey` foreign keys

## Security Considerations

- **SQL Injection**: Always use PreparedStatement, never string concatenation
- **XSS**: Use OWASP HTML sanitizer for user input
- **XXE**: DocumentBuilderFactory configured with secure defaults (see GeneralUtilityMethods)
- **Authentication**: Session-based, API tokens, or temporary task keys
- **Authorization**: Role-based via Authorise class, organization-level isolation

## Dependencies External to Repository

- **smap2 repository**: Contains `amazon` module for AWS integration
  - Clone from: https://github.com/nap2000/smap2.git
  - Required for surveyKPI, subscribers, sdDAL builds

## File Structure Reference

```
sdDAL/
  src/org/smap/sdal/
    managers/        - 79+ Manager classes (business logic)
    model/           - 255+ model/DAO classes
    Utilities/       - Database, auth, query utilities
    constants/       - Application constants
    resources/       - Localization bundles

surveyKPI/          - Admin REST APIs (50+ endpoint classes)
surveyMobileAPI/    - Mobile client APIs (form list, submission, lookup)
koboToolboxApi/     - Kobo-compatible data APIs
subscribers/        - Batch processors (8 processor types)
  src/
    subscribers/    - Main Manager.java, SubscriberBatch
    processors/     - SubmissionProcessor, MessageProcessor, etc.

setup/
  install/          - Database schemas, config templates
  deploy/           - SQL migration scripts
```

## Code Location Patterns

When searching for functionality:
- REST endpoints: Look in module's `src/` directory for `@Path` annotations
- Business logic: Search sdDAL managers (`org.smap.sdal.managers`)
- Data models: Search sdDAL models (`org.smap.sdal.model`)
- Database operations: Manager classes use PreparedStatement
- Batch processing: subscribers/src/processors/
- Configuration: setup/install/config_files/
