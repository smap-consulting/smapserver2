# Ubuntu 26.04 Support Plan

## Ubuntu 26.04 Default Packages

| Package | 20.04 | 22.04 | 24.04 | 26.04 |
|---|---|---|---|---|
| OpenJDK | 11 | 11 | 21 | **25** |
| PostgreSQL | 12 | 14 | 16 | **18** |
| Tomcat (apt) | tomcat9 | tomcat9 | tomcat10 | **tomcat10** |
| Apache | 2.4.x | 2.4.x | 2.4.x | 2.4.x |

**Root cause**: Tomcat 10+ requires Jakarta EE 9+ (`jakarta.*` namespace). Jersey 2.40 uses the old `javax.*` namespace — incompatible.

---

## Strategy: Single Codebase, Migrate to Jakarta EE 9+

Maintain one codebase targeting the Jakarta namespace. For Ubuntu 22.04 (which ships Tomcat 9), the install script will manually download Tomcat 10 — the same pattern already used for 24.04 with Tomcat 9.

Ubuntu 20.04 is EOL (April 2025) and will be dropped from the support matrix.

---

## Scope of Java Source Changes

~195 Java files need namespace changes. Import substitutions are mechanical:

| Old import | New import | Files |
|---|---|---|
| `javax.ws.rs.*` | `jakarta.ws.rs.*` | 162 |
| `javax.servlet.*` | `jakarta.servlet.*` | 161 |
| `javax.mail.*` | `jakarta.mail.*` | 9 |
| `javax.xml.bind.*` | `jakarta.xml.bind.*` | 3 |
| `javax.activation.*` | `jakarta.activation.*` | 1 |

**Not affected** (JDK, not Jakarta EE): `javax.xml.parsers`, `javax.xml.transform`, `javax.naming`, `javax.sql`, `javax.imageio`, `javax.swing`.

**Commons FileUpload (16 files)**: Beyond import changes, `ServletFileUpload` → `JakartaServletFileUpload` (class rename in fileupload2). These need code changes, not just import substitution.

---

## Phase 1 — Maven Dependency Bumps

### `sdDAL/pom.xml`

| Current | Replace with |
|---|---|
| `jersey.version=2.40` | `3.1.9` |
| `org.apache.tomcat:tomcat-catalina:9.0.117` | `10.1.40` |
| `com.sun.mail:javax.mail:1.6.2` | `org.eclipse.angus:angus-mail:2.0.3` |
| `javax.xml.bind:jaxb-api:2.3.1` | `jakarta.xml.bind:jakarta.xml.bind-api:4.0.0` |
| `com.sun.xml.bind:jaxb-core:2.3.0.1` | `org.glassfish.jaxb:jaxb-core:4.0.3` |
| `com.sun.xml.bind:jaxb-impl:2.3.0` | `org.glassfish.jaxb:jaxb-impl:4.0.3` |
| `commons-fileupload:commons-fileupload:1.6.0` | `org.apache.commons:commons-fileupload2-jakarta-servlet6:2.0.0-M2` |

### `sdDataAccess/pom.xml`

| Current | Replace with |
|---|---|
| `javax.servlet:javax.servlet-api:3.1.0` | `jakarta.servlet:jakarta.servlet-api:6.0.0` |

### `cloudInterface/pom.xml`

| Current | Replace with |
|---|---|
| `javax.activation:activation:1.1.1` | `jakarta.activation:jakarta.activation-api:2.1.0` |
| Java source/target `1.8` | `11` |

---

## Phase 2 — Replace Ant Build with Maven Shade Plugin — **DONE**

`subscriber3.xml` (Ant) hardcoded every dependency JAR path from `~/.m2/repository` by version number, requiring manual updates on every dep bump. Already resolved in `master`; applied to `ubuntu_upgrade` branch.

`subscribers/pom.xml`: replaced `maven-assembly-plugin` (unbound) with `maven-shade-plugin:3.5.2` bound to the `package` phase. Shade is preferred over assembly because `ServicesResourceTransformer` merges `META-INF/services` files correctly (required for Jersey service discovery). `ManifestResourceTransformer` sets `Main-Class: Manager`. Output: `target/subscribers.jar`.

`dep.sh`: replaced `ant -f subscriber3.xml` + `mv` with `mvn clean package` + `cp target/subscribers.jar`. Added `-p` flag to `mkdir` for idempotency.

`subscriber3.xml` can be deleted once this branch is merged.

---

## Phase 3 — `web.xml` Files (4 files)

All four WARs declare the old Java EE namespace and version. Update each:

- `surveyMobileAPI/src/main/webapp/WEB-INF/web.xml`
- `surveyKPI/src/main/webapp/WEB-INF/web.xml`
- `koboToolboxApi/src/main/webapp/WEB-INF/web.xml`
- `sms/src/main/webapp/WEB-INF/web.xml`

Changes in each:

```xml
<!-- Before -->
<web-app xmlns="https://java.sun.com/xml/ns/javaee"
         xsi:schemaLocation="https://java.sun.com/xml/ns/javaee https://java.sun.com/xml/ns/javaee/web-app_4_0.xsd"
         version="4.0">

<!-- After -->
<web-app xmlns="https://jakarta.ee/xml/ns/jakartaee"
         xsi:schemaLocation="https://jakarta.ee/xml/ns/jakartaee https://jakarta.ee/xml/ns/jakartaee/web-app_6_0.xsd"
         version="6.0">
```

---

## Phase 4 — Install Script Changes

### `setup/install/install.sh`

1. Add Ubuntu 26.04 detection:
   ```sh
   u2604=`lsb_release -r | grep -c "26\.04"`
   ```

2. Add to supported version list and version-specific blocks:
   ```sh
   if [ $u2604 -eq 1 ]; then
       PGV=18
       TOMCAT_VERSION=tomcat10
       TOMCAT_USER=tomcat
   fi
   ```

3. On 26.04: `apt-get install tomcat10` works natively (unlike 24.04 which needed a manual wget install).

4. On 22.04: switch from apt Tomcat 9 to manual Tomcat 10 install using the same wget pattern already used for 24.04. The apt Tomcat 9 on 22.04 cannot run the migrated Jakarta namespace WARs.

5. Update Tomcat path variables for 26.04:
   ```sh
   tc_server_xml="/var/lib/tomcat10/conf/server.xml"
   tc_context_xml="/var/lib/tomcat10/conf/context.xml"
   ```

6. Install `openjdk-21-jdk-headless` on 26.04. Java 21 (LTS) is the safest runtime — the code targets Java 11 bytecode and runs on 21 or 25. Using 21 avoids coupling the service to the non-LTS JDK 25 default.

Apply the same Ubuntu version detection changes to `setup/install/apacheConfig.sh` and `setup/deploy/deploy.sh`.

---

## Phase 5 — New Config Files

### `config_files/server.xml.tomcat10`

Copy `server.xml.tomcat9`. The AJP connector config (`secretRequired="false"`, port 8009) is syntactically identical in Tomcat 10 — no changes needed there.

### `config_files/tomcat10.service`

Copy `tomcat9.service` with:
- All `tomcat9` path references → `tomcat10`
- `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64/`
- `CATALINA_HOME=/var/lib/tomcat10`

### `config_files/override.conf`

Update `ReadWritePaths` from `/var/lib/tomcat9` to `/var/lib/tomcat10` (or add a separate `override.conf.tomcat10` variant).

### `config_files/postgresql.conf.18`

Copy from `postgresql.conf.16` — PG 18 does not break backwards-compatible configuration options.

### Subscriber service file

The subscriber service file uses `User=tomcat` which is the same for both Tomcat 9 and Tomcat 10 packages. Review whether the existing `subscribers.service.u2004` variant needs a 26.04-specific copy or whether the user/path is already generic enough.

---

## Phase 6 — Upgrade Path for Existing Servers (`deploy.sh`)

### Problem

`deploy.sh` currently infers the Tomcat version from the Ubuntu version and hardcodes `tomcat9` for 24.04, 22.04, and 20.04. After the Jakarta migration, deploying new WARs to a server still running Tomcat 9 will fail silently — Tomcat 9 will refuse to load `jakarta.*` WARs with `ClassNotFoundException`.

### Solution: Detect Actual Installed Tomcat, Auto-Migrate If Needed

Replace the Ubuntu-version-based `TOMCAT_VERSION` block with a detection of the actually-installed Tomcat service. If Tomcat 9 is found, perform the Tomcat 9 → 10 upgrade inline before deploying the WARs. On subsequent deploys, Tomcat 10 is already present so the migration block is a no-op.

### Detection Logic (replaces the existing Ubuntu version block at lines 4–26)

```sh
u2604=`lsb_release -r | grep -c "26\.04"`

if [ -d /var/lib/tomcat10 ] || systemctl is-enabled tomcat10 2>/dev/null | grep -q "enabled"; then
    TOMCAT_VERSION=tomcat10
    TOMCAT_USER=tomcat
elif [ -d /var/lib/tomcat9 ] || systemctl is-enabled tomcat9 2>/dev/null | grep -q "enabled"; then
    TOMCAT_VERSION=tomcat9
    TOMCAT_USER=tomcat
    NEEDS_TOMCAT_UPGRADE=true
else
    echo "ERROR: No supported Tomcat installation found"
    exit 1
fi
```

The `u2604` flag is still needed separately to select the correct `server.xml` template and PostgreSQL version.

### Migration Block (runs before the stop-services block if `NEEDS_TOMCAT_UPGRADE=true`)

```sh
if [ "$NEEDS_TOMCAT_UPGRADE" = "true" ]; then
    echo "=== Migrating Tomcat 9 → Tomcat 10 ==="

    # Locate and save the existing context.xml (JNDI datasource config)
    if [ -f /etc/tomcat9/context.xml ]; then
        OLD_CONTEXT=/etc/tomcat9/context.xml          # 22.04 apt install
    else
        OLD_CONTEXT=/var/lib/tomcat9/conf/context.xml  # 24.04 manual install
    fi
    cp $OLD_CONTEXT /tmp/context.xml.bak

    # Stop and disable Tomcat 9
    systemctl stop tomcat9
    systemctl disable tomcat9

    # Install Tomcat 10 (manual wget — works on 22.04 and 24.04)
    TC10_VER=10.1.40
    TC10_URL=https://archive.apache.org/dist/tomcat/tomcat-10/v${TC10_VER}/bin/apache-tomcat-${TC10_VER}.tar.gz
    wget -q $TC10_URL -O /tmp/tomcat10.tar.gz
    mkdir -p /var/lib/tomcat10
    tar -xzf /tmp/tomcat10.tar.gz -C /var/lib/tomcat10 --strip-components=1
    rm /tmp/tomcat10.tar.gz

    # Apply config files
    cp $cwd/../install/config_files/server.xml.tomcat10 /var/lib/tomcat10/conf/server.xml
    cp /tmp/context.xml.bak /var/lib/tomcat10/conf/context.xml

    # Install and enable systemd service
    cp $cwd/../install/config_files/tomcat10.service /etc/systemd/system/tomcat10.service
    cp $cwd/../install/config_files/override.conf /etc/systemd/system/tomcat10.service.d/override.conf
    systemctl daemon-reload
    systemctl enable tomcat10

    chown -R tomcat /var/lib/tomcat10

    TOMCAT_VERSION=tomcat10
    TOMCAT_USER=tomcat
    echo "=== Tomcat 10 migration complete ==="
fi
```

### What Is Preserved

- **`context.xml`** (JNDI datasource config, including database passwords) is copied from the old Tomcat 9 location and applied to the new Tomcat 10 install. No database connection config needs to be re-entered.
- **`server.xml`** is replaced with the new `server.xml.tomcat10` template (AJP connector config is identical between Tomcat 9 and 10, so no functional change).
- The rest of `deploy.sh` continues unchanged — WAR deployment, chown, service start/stop all use `$TOMCAT_VERSION` which is now `tomcat10`.

### Behaviour Per Ubuntu Version After This Change

| Ubuntu | Tomcat before deploy | Action taken | Tomcat after deploy |
|---|---|---|---|
| 22.04 (first Jakarta deploy) | tomcat9 (apt) | migrate to tomcat10 (manual) | tomcat10 |
| 22.04 (subsequent deploys) | tomcat10 | none | tomcat10 |
| 24.04 (first Jakarta deploy) | tomcat9 (manual) | migrate to tomcat10 (manual) | tomcat10 |
| 24.04 (subsequent deploys) | tomcat10 | none | tomcat10 |
| 26.04 (fresh install) | tomcat10 (apt) | none | tomcat10 |

---

## Phase 7 — Testing Strategy

1. **Ubuntu 24.04 first**: run deploy.sh on an existing 24.04 server; verify auto-migration from Tomcat 9 → 10 completes and all REST endpoints respond.
2. **AJP proxy**: Apache httpd → Tomcat 10 AJP port 8009 — critical path since Apache config uses `proxy_ajp`.
3. **JNDI datasource**: verify `context.xml` `org.apache.tomcat.jdbc.pool.DataSourceFactory` works under Tomcat 10 (expected unchanged).
4. **Multipart uploads**: `UserSvc.java` and `TableReports.java` use Jersey `@FormDataParam` — test these endpoints.
5. **commons-fileupload2**: test all 16 refactored files; file uploads are core to form submission.
6. **JavaMail**: test SMTP email sending via Angus Mail.
7. **JAXB**: test form list serialization (`FormListManager.java` uses `JAXB.marshal()`).
8. **Ubuntu 26.04 VM**: run the updated `install.sh` on a fresh 26.04 VM end-to-end.
9. **Ubuntu 22.04**: verify auto-migration from apt Tomcat 9 to manual Tomcat 10 preserves `context.xml` and connects to the database correctly.
10. **Idempotency**: run deploy.sh a second time on a migrated server — confirm it detects Tomcat 10 already present and skips the migration block.

---

## Risks and Unresolved Questions

1. ~~**`JAXB.marshal()` removed in JAXB 4.x**~~ — **RESOLVED**. `FormListManager.java` updated to use `JAXBContext.newInstance(formList.getClass()).createMarshaller().marshal(formList, writer)`.

2. ~~**javarosa custom artifact** (`smapserver:javarosa:3.1.4`)~~ — **RESOLVED**. Source in `~/git/javarosa` contains no Jakarta EE imports — only standard JDK XML classes (`javax.xml.parsers`, `javax.xml.transform`) which are unaffected. No rebuild needed.

3. **commons-fileupload2 API surface** — 16 files need actual code changes (class renames), not just import substitution. The upload flow is central to form submission — these need careful testing.

4. **Ubuntu 22.04 manual Tomcat 10 install** — After migration, 22.04 will need wget-based Tomcat 10 (same as 24.04 does for Tomcat 9 today). The path layout from manual vs apt install may differ; test this before releasing.

5. **PostgreSQL JDBC driver 42.6.x vs PG 18** — Likely compatible but worth confirming against the PostgreSQL JDBC compatibility matrix before deploying on 26.04.
