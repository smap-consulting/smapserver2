# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

### Building the Subscribers Module

```bash
# Full build sequence (run in order)
cd ../sdDAL && mvn clean install && cd ../subscribers
mvn clean package

# Output: target/subscribers.jar (~29MB, not self contained - see Shade Build Process)
```

External dependency: `amazon` module from separate `smap2` repository must be cloned and built at `~/git/smap2/amazon`.

### Running Locally

The shared libraries are not in the jar, so build them first and put them on the class
path (`../shared_libs.sh` writes them to `~/deploy/smap/deploy/version1/lib`):

```bash
LIB=~/deploy/smap/deploy/version1/lib

# Upload mode (processes form submissions)
java -cp "target/subscribers.jar:$LIB/*" Manager default /smap upload

# Forward mode (processes notifications, reports, background tasks)
java -cp "target/subscribers.jar:$LIB/*" Manager default /smap forward
```

Arguments: `{smapId} {basePath} {mode}`

## Architecture Overview

### Purpose
Batch processor that asynchronously applies form submissions to database and handles notification/messaging queue. Decouples submission receipt (REST API) from database write for scalability.

### Two Operating Modes

**Upload Mode**: Enqueues and processes form submissions
- SubscriberBatch: Polls `upload_event`, enqueues to `submission_queue`
- SubmissionProcessor (qu1..qu4): Dequeues, writes to results DB
- MessageProcessor (qmu1..qmu3): Sends outbound messages

**Forward Mode**: Background processing and notifications
- SubmissionProcessor (qf1, qf2_restore): Additional submission queues
- MessageProcessor (qmf1..qmf3): Message sending
- SubEventProcessor: Post-submission events (notifications, tasks, case management)
- ReportProcessor: Report generation
- AutoUpdateProcessor: S3 sync
- StorageProcessor: Media file storage
- MonitorProcessor: Queue monitoring (disabled)

### Queue-Based Architecture

Uses PostgreSQL queues with `FOR UPDATE SKIP LOCKED` for concurrency:

1. **submission_queue**: Form submissions awaiting processing
   - Dequeue: `DELETE...FOR UPDATE SKIP LOCKED RETURNING`
   - Multiple workers process in parallel without contention

2. **message_queue**: Outbound notifications/messages
   - Email, SMS, webhooks
   - Populated by SubscriberBatch from `message` table

3. **submission_event**: Post-processing triggers
   - Created after successful submission
   - Triggers notifications, task updates, case management

### Key Processing Flow

**Form Submission Path**:
```
REST API → upload_event (results_db_applied=false, queued=false)
  ↓ SubscriberBatch polls
  → submission_queue (with full UploadEvent as JSON payload)
  ↓ SubmissionProcessor dequeues
  → SubRelationalDB.upload() writes to results DB
  ↓
  → upload_event (results_db_applied=true)
  → submission_event created for post-processing
```

**JSON Submission Support**: Recent addition supports JSON uploads via `JsonRelationalDB` instead of XML/JavaRosa parsing.

### Database Connections

Two PostgreSQL databases via JNDI:
- **survey_definitions**: Metadata, queues, users, upload_event
- **results**: Dynamic survey data tables

Configuration in `./default/metaDataModel.xml` and `./forward/metaDataModel.xml`

### Multi-Threading Pattern

Each processor (SubmissionProcessor, MessageProcessor, etc.) spawns dedicated threads in Manager.java:
- Multiple SubmissionProcessors on different queue names (qu1..qu4, qf1, qf2_restore)
- MessageProcessors (qmf1..qmf3 in forward, qmu1..qmu3 in upload)
- Worker counts are `UPLOAD_WORKERS` and `MESSAGE_WORKERS` in Manager.java
- The message workers share a bounded pool of smtp connections, not one each, because
  relays cap concurrent connections per mailbox (office 365 allows three)
- Threads run indefinitely with connection pooling
- Graceful shutdown via `/smap/settings/subscriber` file

### Shade Build Process

`maven-shade-plugin` is bound to the `package` phase and creates the jar with:
- Manifest: `Main-Class: Manager` (ManifestResourceTransformer)
- Most Maven dependencies, with `META-INF/services` entries merged by ServicesResourceTransformer - Jersey service discovery needs this
- Compiled classes from subscribers, sdDAL, amazon
- Signature files (`*.SF`, `*.DSA`, `*.RSA`) stripped so the shaded jar stays loadable
- Result: `target/subscribers.jar`, about 29MB

The `artifactSet/excludes` leave out the libraries in `shared-libs.txt` - the AWS SDK,
iText, POI, Netty and the logging jars, 87 of the 189 dependencies and 75MB of them.
Those are deployed once to `/smap_bin/lib` and shared with the war files, so the jar is
**not** self contained and cannot be started with `java -jar`. `shared_libs.sh` checks
the built jar for classes that should have been excluded.

commons-io, commons-codec and slf4j-api are pinned in the pom to the versions in
shared-libs.txt. Transitive resolution picks older ones, which would mean building
against a different version to the one it runs against.

`dep.sh` copies it to `~/deploy/smap/deploy/version1/subscribers.jar`, and `deploy.sh`
installs that to `/smap_bin/subscribers.jar` alongside `/smap_bin/lib`.

## Key Implementation Details

### Dequeue Pattern
All processors use similar SQL:
```sql
DELETE FROM queue_table q
WHERE q.element_identifier = (
  SELECT q_inner.element_identifier
  FROM queue_table q_inner
  ORDER BY q_inner.time_inserted ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING q.payload
```

### Submission Processing (SubmissionProcessor)

1. Dequeue from `submission_queue`
2. Deserialize `UploadEvent` from JSON payload
3. Read XML from filesystem (or S3 if archived)
4. Parse into `SurveyInstance` (JavaRosa) OR process JSON directly
5. Call `SubRelationalDB.upload()` or `JsonRelationalDB.upload()`
6. Update `upload_event.results_db_applied = true`
7. Create `submission_event` for downstream processing

### Restore Processing
Separate queues (qu2, qf2_restore) handle restore operations with `incRestore=true` flag to filter on `submission_queue.restore` column.

### Incomplete Submissions
FieldTask app sends large attachments separately. `getAttachmentsFromIncompleteSurveys()` merges files from incomplete posts before final processing.

### Media Changes
After submission, `processMediaChanges()` updates XML with final media paths and deletes temporary files.

### Batch Operations (SubscriberBatch)

In upload mode:
- Polls `upload_event` for pending submissions
- Checks for duplicates in queue by instanceid
- Enqueues to `submission_queue` with full UploadEvent JSON

In forward mode (runs every 60 seconds):
- Case management reminders: Checks `cms_alert` for overdue cases
- Periodic notifications: Daily/weekly/monthly/quarterly/yearly schedules
- Server calculation notifications: Triggers when calculated values match criteria
- Mailout campaigns: Processes pending mailouts
- Cleanup: Deletes old surveys, CSV files, alerts
- Foreign key application: Deferred constraint processing
- Timezone refresh: Periodic cache invalidation
- Temporary user expiration: 30-day cleanup

### Error Handling

- Failed submissions: `upload_event.db_status='error'`, `db_reason` contains details
- Duplicate detection: Prevents reprocessing same instanceid
- Transaction rollback: Database writes are atomic
- Logging: Uses `LogManager` for audit trail by topic (SUBMISSION, SUBMISSION_TASK, SUBMISSION_ANON, etc.)

## Configuration Files

- `default/metaDataModel.xml`: Database connection config for upload mode
- `forward/metaDataModel.xml`: Database connection config for forward mode
- `default/results_db.xml`: Results DB config for upload mode
- `forward/results_db.xml`: Results DB config for forward mode
- `/smap/settings/subscriber`: Control file (set to "stop" to halt)
- `/smap/settings/bucket`: S3 bucket name
- `/smap/settings/region`: AWS region
- `/smap/settings/smtp_max_connections`: Open smtp connections allowed per subscriber process (default 1)
- `/smap/settings/smtp_retry_minutes`: How long to pause email after the relay reports a sending limit (default 60)

## Module Structure

```
src/
  Manager.java              - Main entry point, spawns all processor threads
  SubscriberBatch.java      - Polls upload_event, enqueues submissions/messages
  SubmissionProcessor.java  - Dequeues and applies submissions to results DB
  MessageProcessor.java     - Sends outbound messages (email, SMS, webhooks)
  SubEventProcessor.java    - Post-submission event processing
  ReportProcessor.java      - Report generation queue
  AutoUpdateProcessor.java  - S3 sync operations
  StorageProcessor.java     - Media file storage to S3
  MonitorProcessor.java     - Queue monitoring (disabled)

  org/smap/subscribers/
    Subscriber.java         - Abstract base class
    SubRelationalDB.java    - XML submission processor (JavaRosa → SQL)
    JsonRelationalDB.java   - JSON submission processor
```

## Dependencies on Other Modules

- **sdDAL**: All Manager classes, database utilities, model objects
- **amazon** (from smap2 repo): S3AttachmentUpload for media storage
- **Vonage SDK**: SMS messaging via MessageProcessor

## Important Notes

- Never close DB connections in processor loops - they run indefinitely
- Queue name parameter distinguishes multiple worker instances
- Restore flag prevents infinite reprocessing of restore submissions
- All processors use same database connection pattern via `GeneralUtilityMethods.getDatabaseConnections()`
- UploadEvent payload in queue contains full metadata to avoid join queries during dequeue
- Polling interval: 2s (upload mode), 60s (forward mode)
