# Plan: Implement JSON Upload Endpoint

## Requirements Summary

- **Endpoint**: POST `/upload` accepting `application/json`
- **JSON format**: Single object or array of objects with key-value pairs (values always strings)
  - `_survey`: Survey identifier (string, required)
  - `instanceid` (optional): If present, update existing record; else generate UUID
  - `_device` (optional): Device identifier
  - Other keys: Question names in survey (unmatched keys silently ignored)
- **Flow**: Async via upload_event → submission_queue (like XML)
- **Scope**: Top-level form only (no repeating groups)
- **Authentication**: Follow existing pattern (session user)
- **Versioning**: Use latest survey version, write to _version in results table
- **File retention**: Permanent (like XML files)

## Implementation Approach - Multi-Phase

Follow XML submission pattern but adapt for JSON. Implement in 3 phases for incremental delivery:

### Phase 1: Core JSON Upload (Single Submission, INSERT Only) ✅ COMPLETED
Minimal viable implementation:
- UploadJson endpoint accepting single JSON object
- JsonFormData saves JSON to disk, creates upload_event
- SubmissionProcessor routes to JsonRelationalDB
- JsonRelationalDB writes INSERT to results DB
- No batch, no updates yet

### Phase 2: Update Capability ✅ COMPLETED
Add update support:
- Modify JsonRelationalDB to detect existing instanceid
- Implement UPDATE logic alongside INSERT
- Add table creation logic (like XML submissions)

### Phase 3: Batch Upload Support
Add batch processing:
- Modify JsonFormData to accept JSON arrays
- Loop processing, create multiple upload_events
- Response with batch success/failure counts

## Detailed Steps - Phase 1 ✅ COMPLETED

### Step 1: Modify UploadJson.java ✅

**File**: `surveyMobileAPI/src/surveyMobileAPI/UploadJson.java`

Changes completed:
- Changed `@Consumes` from `MULTIPART_FORM_DATA` to `APPLICATION_JSON`
- Removed XFormData usage
- Uses JsonFormData class to handle JSON
- Removed hardcoded test user mapping
- Removed incomplete `load()` method stub

### Step 2: Create JsonFormData.java ✅

**File**: `surveyMobileAPI/src/surveyMobileAPI/JsonFormData.java`

Implemented functionality:
- Reads JSON from request InputStream
- Parses JSON as single object (uses Gson)
- Extracts `_survey` identifier
- Extracts `instanceid` if present (generates UUID if missing)
- Extracts `_device` if present (optional)
- Validates survey exists and not deleted/blocked
- Saves JSON to disk: `basePath/uploadedSurveys/surveyIdent/instanceId/instanceId.json`
- Creates UploadEvent object with metadata
- Writes to upload_event via JdbcUploadEventManager

### Step 3: Modify upload_event Table Schema ✅

**Files Updated**:
- `setup/deploy/sd.sql` - Added upgrade patch
- `setup/install/setupDb.sql` - Added column to CREATE TABLE

Changes:
```sql
ALTER TABLE upload_event ADD COLUMN IF NOT EXISTS file_type text DEFAULT 'xml';
COMMENT ON COLUMN upload_event.file_type IS 'Type of uploaded file: xml or json';
```

### Step 4: Modify SubmissionProcessor ✅

**File**: `subscribers/src/SubmissionProcessor.java`

Changes:
- Added JsonRelationalDB import
- Detects JSON files by `.json` extension
- Routes JSON submissions to JsonRelationalDB
- Routes XML submissions to SubRelationalDB (existing)
- Skips XML parsing for JSON submissions

### Step 5: Create JsonRelationalDB.java ✅

**File**: `subscribers/src/org/smap/subscribers/JsonRelationalDB.java`

Implemented functionality:
- Implements `Subscriber` interface
- Reads JSON from file
- Extracts `_survey` identifier and looks up survey
- Gets survey structure via SurveyManager.getById() with full=true
- Gets latest survey version
- Gets top-level form and table name
- Builds dynamic INSERT statement
- Maps JSON keys to question.columnName
- Silently skips unknown keys
- Adds metadata columns (_user, _upload_time, _s_id, _version, etc.)
- Type conversion for integers, decimals, dates, timestamps, geopoints, etc.
- Updates upload_event.results_db_applied = true
- Creates submission event for notifications

### Step 6: Error Handling ✅

Implemented error handling:
- Invalid JSON → Exception with message
- Missing _survey key → Exception
- Survey not found → Exception
- Survey deleted/blocked → Exception
- Type conversion errors → Exception with details
- Database errors → Logged and upload_event updated with error status

### Step 7: Testing Considerations

Phase 1 test cases to verify:
1. Valid JSON without instanceid (auto-generate UUID, INSERT)
2. Valid JSON with instanceid (INSERT)
3. Invalid survey identifier
4. Missing _survey key
5. Question names not in survey (silently ignored)
6. Type conversion errors (e.g., "abc" for integer field)
7. Authentication failures
8. Survey deleted/blocked
9. Optional _device field present/absent

## Detailed Steps - Phase 2 ✅ COMPLETED

### Step 1: Add Table Creation Logic ✅

**File**: `subscribers/src/org/smap/subscribers/JsonRelationalDB.java`

Added imports and table creation (like XML submissions):
- Import `AdvisoryLock` and `UtilityMethods`
- Added `lockTableChange` instance variable
- Initialize lock after getting surveyId
- Call `UtilityMethods.createSurveyTables()` before checking existence
- Release lock in finally block

### Step 2: Modify JsonRelationalDB for UPDATE Support ✅

**File**: `subscribers/src/org/smap/subscribers/JsonRelationalDB.java`

Implemented UPDATE logic:

**New methods added**:
1. `checkInstanceIdExists()` - Check if instanceid exists in results table:
```java
SELECT prikey FROM {tableName} WHERE instanceid = ? LIMIT 1
```

2. `buildInsertStatement()` - Refactored INSERT logic into separate method
3. `buildUpdateStatement()` - Build UPDATE statement:
```sql
UPDATE {tableName}
SET question_col1 = ?, question_col2 = ?, _upload_time = ?
WHERE instanceid = ?
```

4. `addUpdateColumn()` - Helper for UPDATE SET clauses (handles geometry types)

**Modified upload() method**:
- Create tables if needed (first submission to survey)
- Check if record exists by instanceid
- Branch to UPDATE (if exists) or INSERT (if new)
- Removed info logging (only log.severe for errors)

**Key differences UPDATE vs INSERT**:
- UPDATE: Preserves parkey, _user, _complete, _s_id, _version; updates _upload_time and question columns
- INSERT: Include parkey=0, _user, _complete, all metadata
- Note: _modified flag NOT set (column doesn't exist in results schema)

### Step 3: Testing Considerations (Phase 2)

Test cases to verify:
1. First submission to new survey → Tables created, INSERT succeeds
2. Valid JSON with existing instanceid → UPDATE succeeds
3. UPDATE changes question values
4. UPDATE preserves original _user
5. INSERT vs UPDATE based on instanceid existence
6. Multiple updates to same record → Incremental updates work

## Detailed Steps - Phase 3 (Not Yet Implemented)

### Step 1: Modify JsonFormData for Batch Support

**File**: `surveyMobileAPI/src/surveyMobileAPI/JsonFormData.java`

Add array handling:

**Parse JSON**:
1. Read as JsonElement first
2. Check `isJsonArray()` vs `isJsonObject()`
3. If object: Process single submission (existing logic)
4. If array: Loop through array, process each object:
   - Extract survey/instanceid from each object
   - Save each to separate file (unique instanceid per file)
   - Create separate upload_event per submission
   - Track success/failure counts

**Return value**:
- Single: Return status
- Batch: Return summary { "total": N, "success": M, "failed": K, "errors": [...] }

### Step 2: Modify UploadJson Response

**File**: `surveyMobileAPI/src/surveyMobileAPI/UploadJson.java`

Update response handling:
- Single submission: Return 201 CREATED
- Batch submission: Return 200 OK with JSON summary

### Step 3: Testing Considerations (Phase 3)

Additional test cases:
1. Batch of valid submissions (all succeed)
2. Batch with mixed success/failure
3. Batch with different surveys (multi-survey batch)
4. Empty array
5. Large batch (performance test)

## Files Created/Modified

### Phase 1 (Completed)
**Created**:
- `surveyMobileAPI/src/surveyMobileAPI/JsonFormData.java`
- `subscribers/src/org/smap/subscribers/JsonRelationalDB.java`

**Modified**:
- `surveyMobileAPI/src/surveyMobileAPI/UploadJson.java`
- `subscribers/src/SubmissionProcessor.java`
- `setup/deploy/sd.sql` (upgrade patch)
- `setup/install/setupDb.sql` (table definition)

### Phase 2 (Completed) ✅
**Modified**:
- `subscribers/src/org/smap/subscribers/JsonRelationalDB.java`
  - Added imports: AdvisoryLock, UtilityMethods
  - Added instance variable: lockTableChange
  - Added methods: checkInstanceIdExists(), buildInsertStatement(), buildUpdateStatement(), addUpdateColumn()
  - Modified upload() method: table creation, existence check, UPDATE/INSERT branching
  - Updated comment to "Phase 2: INSERT and UPDATE support"
  - Removed info logging for unknown keys

### Phase 3 (Pending)
**Modify**:
- `surveyMobileAPI/src/surveyMobileAPI/JsonFormData.java` (add array handling)
- `surveyMobileAPI/src/surveyMobileAPI/UploadJson.java` (batch response)

## Dependencies

From sdDAL:
- SurveyManager (get survey by identifier)
- GeneralUtilityMethods (getSurveyId, type conversions)
- JdbcUploadEventManager (write upload_event)
- SubmissionEventManager (create submission events)
- Authorise (authentication)
- SDDataSource (connections)

From existing code:
- UploadEvent model
- Subscriber interface
- PreparedStatement pattern

## Deployment

### Build Commands
```bash
# Build shared libraries
cd sdDAL && mvn clean install && cd ..

# Build surveyMobileAPI
cd surveyMobileAPI && mvn clean install && cd ..

# Build subscribers
cd subscribers && ant -f subscriber3.xml && cd ..
```

**Phase 2 Build Verification** ✅
- Subscribers module built successfully (2026-01-03)
- No compilation errors
- JAR created at `/Users/neilpenman/deploy/subscribers.jar`

### Database Update
```bash
# Apply schema patch (on existing installations)
psql -d survey_definitions -f setup/deploy/sd.sql
```

### Testing
Example JSON payload for testing:
```json
{
  "_survey": "my_survey_identifier",
  "instanceid": "uuid-12345-optional",
  "_device": "device-id-optional",
  "question1": "answer1",
  "question2": "42",
  "question3": "2025-01-03"
}
```

POST to: `http://server/surveyMobileAPI/upload`
Content-Type: `application/json`
Authentication: Required (session user)

## Notes

- Follows async queue pattern for consistency
- Reuses existing infrastructure (upload_event, submission_queue)
- Parallel to XML path, doesn't modify XML handling
- ✅ Update capability (Phase 2) COMPLETED - supports UPDATE by instanceid
- Batch support (Phase 3) pending implementation
- Top-level only keeps scope manageable (no repeating groups)
- File type tracking allows future expansion
- Table creation handled automatically (like XML submissions)
- Advisory locks prevent race conditions during table creation

## Resolved Requirements

1. ✅ **file_type column**: Added to upload_event
2. ✅ **Unknown JSON keys**: Silently ignored if not question name or reserved value
3. ✅ **deviceId support**: Yes, optional with key `_device`
4. ✅ **UPDATE capability**: Implemented in Phase 2 - updates existing records by instanceid
5. ✅ **Table creation**: Automatic table creation on first submission (Phase 2)
6. 🔄 **Batch uploads**: Pending in Phase 3 (array of submissions)
7. ✅ **File retention**: Retained like XML files (permanent)
8. ✅ **Version field**: Uses latest survey version, writes to results table

## Reserved JSON Keys

- `_survey` - Survey identifier (required)
- `instanceid` - Instance ID for updates (optional, auto-generate UUID if missing)
- `_device` - Device ID (optional)

## JSON Format Support

- **Single submission** (Phase 1): Object with key-value pairs
- **Batch submissions** (Phase 3): Array of objects, each with key-value pairs
