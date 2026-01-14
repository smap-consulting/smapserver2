# AGENTS.md

This file contains build commands, test procedures, and code style guidelines for agentic coding agents working in this repository.

## Build Commands

### Full Build and Deployment
```bash
./dep.sh                    # Build all modules and deploy to ~/deploy/smap/deploy/version1
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

### JavaRosa Dependency Installation
```bash
# Required for sdDAL - run from repository root
mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file \
  -Dfile=./surveyKPI/src/main/webapp/WEB-INF/lib/javarosa-3.1.4.jar \
  -DgroupId=smapserver -DartifactId=javarosa -Dversion=3.1.4 -Dpackaging=jar
```

## Testing

### Current Test Status
- **No automated test framework** - This codebase lacks formal unit tests
- **Manual testing endpoints** - Use test classes like `EmailTest.java`, `memTest.java` as REST endpoints
- **Integration testing** - Done through live application deployment

### Running Manual Tests
```bash
# Deploy application first, then access test endpoints:
# GET /surveyKPI/test/email - Test email functionality
# GET /surveyKPI/test/memory - Test memory usage
# POST /surveyKPI/test/inbound_sms - Test SMS processing
```

## Code Style Guidelines

### File Headers
All core files must include GPL header:
```java
/*****************************************************************************

This file is part of SMAP.

SMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

*******************************************************************************/
```

### Import Organization
Group imports in this order:
1. Java standard libraries (java.*)
2. Third-party libraries (javax.*, org.*, com.*)
3. Internal packages (org.smap.*)

```java
// Java standard libraries
import java.sql.Connection;
import java.util.ArrayList;

// Third-party libraries
import javax.servlet.http.HttpServletRequest;
import org.codehaus.jettison.json.JSONArray;

// Internal packages
import org.smap.sdal.Utilities.GeneralUtilityMethods;
import org.smap.sdal.model.Survey;
```

- **No wildcard imports** (avoid `import java.util.*`)
- **Alphabetical within groups**
- **Remove unused imports**

### Naming Conventions
- **Classes**: PascalCase (`SurveyManager`, `UploadEvent`)
- **Methods**: camelCase (`getConnection`, `processSubmission`)
- **Variables**: camelCase (`userName`, `surveyId`)
- **Constants**: UPPER_SNAKE_CASE (`MAX_FILE_SIZE`, `DEFAULT_TIMEOUT`)
- **Packages**: lowercase with dots (`org.smap.sdal.managers`)

### Class Structure Patterns

#### Manager Classes
```java
public class SurveyManager {
    private ResourceBundle localisation;
    private String tz;
    
    public SurveyManager(ResourceBundle l, String tz) {
        localisation = l;
        this.tz = tz;
    }
    
    // Business logic methods
}
```

#### REST Endpoints (Jersey/JAX-RS)
```java
@Path("/resource")
public class ResourceEndpoint {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getResource(@Context HttpServletRequest request) {
        Connection sd = SDDataSource.getConnection("surveyKPI");
        try {
            // Use Manager classes for business logic
            return Response.ok().build();
        } finally {
            SDDataSource.closeConnection("surveyKPI", sd);
        }
    }
}
```

### Database Operations
**Always use PreparedStatement** to prevent SQL injection:

```java
Connection sd = SDDataSource.getConnection("contextName");
try {
    String sql = "SELECT * FROM survey WHERE s_id = ?";
    PreparedStatement pstmt = sd.prepareStatement(sql);
    pstmt.setInt(1, surveyId);
    ResultSet rs = pstmt.executeQuery();
    
    // Process results
} finally {
    SDDataSource.closeConnection("contextName", sd);
}
```

### Error Handling
- **Use custom exceptions**: `ApplicationException`, `AuthorisationException`
- **Authorization pattern**: `Authorise` class throws exception if unauthorized
- **Log errors**: Use `log.log(Level.SEVERE, "message", exception)`

```java
// Authorization
Authorise a = new Authorise(Arrays.asList(Authorise.ANALYST, Authorise.ADMIN), null);
a.isAuthorised(sd, userName); // Throws exception if unauthorized

// Custom exceptions
if (survey == null) {
    throw new NotFoundException("Survey not found: " + surveyId);
}
```

### Logging
```java
private static Logger log = Logger.getLogger(ClassName.class.getName());

log.info("Information message");
log.log(Level.SEVERE, "Error message", exception);
```

### Code Formatting
- **Indentation**: 4 spaces (no tabs)
- **Brace style**: Allman style (opening brace on new line)
- **Line length**: Maximum 120 characters
- **Spacing**: Single space around operators, after commas

### Security Guidelines
- **SQL Injection**: Always use PreparedStatement, never string concatenation
- **XSS**: Use OWASP HTML sanitizer for user input
- **XXE**: DocumentBuilderFactory configured with secure defaults
- **Authentication**: Session-based, API tokens, or temporary task keys
- **Authorization**: Role-based via Authorise class, organization-level isolation

### Key Architectural Patterns

#### Database Connection Pattern
```java
Connection sd = SDDataSource.getConnection("surveyKPI");
Connection cResults = SDDataSource.getConnection("surveyKPI");
try {
    // Database operations
} finally {
    SDDataSource.closeConnection("surveyKPI", cResults);
    SDDataSource.closeConnection("surveyKPI", sd);
}
```

#### Manager Pattern
Business logic resides in Manager classes in sdDAL:
- Constructor accepts ResourceBundle (i18n) and TimeZone
- Methods perform atomic database operations using PreparedStatements
- Throw ApplicationException, NotFoundException, etc. for error handling

#### Submission Processing Pattern
1. REST endpoint receives multipart form data
2. UploadManager processes via XFormData
3. Creates UploadEvent with metadata
4. SubscriberBatch enqueues to submission_queue
5. SubmissionProcessor dequeues and applies to results database
6. SubRelationalDB writes to dynamic tables

## Dependencies

### Required External Repository
- **smap2 repository**: Contains `amazon` module for AWS integration
  - Clone from: https://github.com/nap2000/smap2.git
  - Required for surveyKPI, subscribers, sdDAL builds

### Key Dependencies
- **Jersey**: 2.40 (JAX-RS REST framework)
- **PostgreSQL**: 42.6.1 with PostGIS and pgcrypto extensions
- **Jackson**: 2.14.1/2.15.0 (JSON processing)
- **JavaRosa**: 3.1.4 (XForm processing - custom install)
- **Log4j**: 2.17.2
- **Tomcat**: 9.0.107 (Servlet container)

## Module Dependencies

Build order is critical:
1. **sdDAL** - Core data access layer
2. **sdDataAccess** - Legacy JDBC wrapper
3. **amazon** - AWS integration (external repo)
4. **surveyMobileAPI** - Mobile client APIs
5. **surveyKPI** - Admin console APIs
6. **koboToolboxApi** - Kobo-compatible APIs
7. **subscribers** - Batch processor

## Common Tasks

### Adding New REST Endpoint
1. Create class in appropriate module's `src/` directory
2. Add `@Path("/newEndpoint")` annotation
3. Implement methods with `@GET`, `@POST`, etc.
4. Use Manager classes from sdDAL for business logic
5. Follow database connection and authorization patterns

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