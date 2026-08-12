# Tuning a Smap database on AWS RDS

`install.sh` and `patchdb.sh` configure PostgreSQL only when `DBHOST` is `127.0.0.1`. On RDS the
server configuration lives in a **DB parameter group** instead, `ALTER SYSTEM` is blocked, and
there is no `postgresql.conf` to copy. The steps below cover what the scripts cannot do for you.

Everything applied by `sd.sql` — indexes, and the `upload_event` autovacuum settings — works
normally on RDS and needs nothing here. Those are table level settings, not server configuration.

## Enabling pg_stat_statements

This records per statement call counts and timings, and is how you find which query is costing
the server time. Without it you are guessing.

**Check first — it may already be on.** RDS default parameter groups often include it, and
enabling Performance Insights loads it automatically:

```sql
SHOW shared_preload_libraries;
```

If `pg_stat_statements` is listed, you only need the extension, and no reboot:

```sql
\c survey_definitions
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

`sd.sql` also creates it automatically once the library is preloaded, so a later deploy will pick
this up on its own.

**If it is not listed**, the parameter is static and needs a custom parameter group and a reboot.
Default parameter groups cannot be edited.

```sh
# 1. See whether the instance is on a default group
aws rds describe-db-instances --db-instance-identifier <instance> \
  --query 'DBInstances[0].DBParameterGroups'

# 2. Create one if needed, matching the engine version family
aws rds create-db-parameter-group \
  --db-parameter-group-name smap-pg16 \
  --db-parameter-group-family postgres16 \
  --description "Smap tuning"

# 3. shared_preload_libraries is static, hence pending-reboot.
#    If the group already lists other libraries, include them - the value replaces the list.
aws rds modify-db-parameter-group \
  --db-parameter-group-name smap-pg16 \
  --parameters \
    "ParameterName=shared_preload_libraries,ParameterValue=pg_stat_statements,ApplyMethod=pending-reboot" \
    "ParameterName=pg_stat_statements.max,ParameterValue=10000,ApplyMethod=pending-reboot" \
    "ParameterName=pg_stat_statements.track,ParameterValue=top,ApplyMethod=immediate"

# 4. Attach it, then reboot - attaching alone does not apply a static parameter
aws rds modify-db-instance --db-instance-identifier <instance> \
  --db-parameter-group-name smap-pg16 --apply-immediately
aws rds reboot-db-instance --db-instance-identifier <instance>
```

On Multi-AZ, `--force-failover` on the reboot reduces the outage to the failover time.

Then create the extension as above, and reset the baseline before you start reading it:

```sql
SELECT pg_stat_statements_reset();
```

Leave it collecting for a day or more, ideally spanning a monthly billing or report run.

### Reading it

```sql
SELECT calls, mean_exec_time, total_exec_time, rows, left(query, 150)
FROM pg_stat_statements
WHERE query ILIKE '%upload_event%'
ORDER BY total_exec_time DESC LIMIT 15;
```

On PostgreSQL 12 and earlier these columns are `mean_time` / `total_time`.

### Alternatives that need no reboot

- **Performance Insights** — console, RDS, your instance, Performance Insights. Sort **Top SQL** by
  load over a week. Built on `pg_stat_statements` anyway, seven days retention free.
- **Slow query logging** — `log_min_duration_statement` is dynamic, so it applies with
  `ApplyMethod=immediate` and no reboot. Output goes to the RDS logs, and to CloudWatch Logs if
  PostgreSQL log export is enabled. Set it back to `-1` afterwards.

## One off vacuum after upgrading

The `upload_event` indexes added in 26.08 are only fast if PostgreSQL can use index only scans,
and that requires the visibility map to be current. Autovacuum maintains it going forward — the
scale factors set by `sd.sql` make it run far more often on this table — but it does not
retroactively cover history that accumulated before. On a server with a large existing
`upload_event`, run once in a maintenance window:

```sql
VACUUM (ANALYZE) upload_event;
```

`ANALYZE` alone is not enough. It updates statistics but does not touch the visibility map, so
index only scans will still fall back to heap fetches and the new indexes will look ineffective.

Confirm it worked:

```sql
SELECT relpages, relallvisible,
       round(100.0 * relallvisible / nullif(relpages, 0), 1) AS pct_all_visible
FROM pg_class WHERE relname = 'upload_event';
```

A percentage near zero means the vacuum has not run or could not mark pages visible.

## Checking index health

```sql
-- Which indexes earn their keep.  High tuples per scan means the index is being
-- read in bulk and then filtered, which usually means a column is missing from it.
SELECT indexrelname, idx_scan, idx_tup_read,
       idx_tup_read / nullif(idx_scan, 0) AS tuples_per_scan,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes WHERE relname = 'upload_event'
ORDER BY idx_scan;
```

Counters are cumulative since the last reset, so check `stats_reset` in `pg_stat_database` covers
a representative period. If it is null they have never been reset and cover the life of the
cluster, which makes absolute numbers meaningless — compare indexes against each other instead.
`pg_stat_reset_single_table_counters('upload_event'::regclass)` starts a clean window.

Note that a read replica keeps its own counters. Reporting served from a replica will not appear
in the primary's statistics.

## Long running transactions

A connection left idle in a transaction holds back the xmin horizon, which stops vacuum marking
pages all visible and quietly disables index only scans across the whole database:

```sql
SELECT pid, state, age(backend_xmin) AS xmin_age,
       now() - xact_start AS duration, left(query, 60)
FROM pg_stat_activity WHERE backend_xmin IS NOT NULL
ORDER BY age(backend_xmin) DESC LIMIT 10;
```
