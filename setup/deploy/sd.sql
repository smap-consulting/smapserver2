-- 
-- Apply upgrade patches to survey definitions database
--

-- Version 23.07
alter table forward add column p_id integer;
alter table forward add column periodic_time time;
alter table forward add column periodic_period text;
alter table forward add column periodic_day_of_week integer;
alter table forward add column periodic_day_of_month integer;
alter table forward add column periodic_local_day_of_month integer;
alter table forward add column periodic_month integer;	
alter table forward add column periodic_local_month integer;	
alter table forward add column r_id integer;	

create table periodic (
	last_checked_time time
);
ALTER TABLE periodic OWNER TO ws;

alter table forward drop constraint forward_s_id_fkey;

create index log_org_idx on log (o_id);

-- Version 23.09

alter table mailout add column anonymous boolean;

CREATE SEQUENCE subevent_queue_seq START 1;
ALTER SEQUENCE subevent_queue_seq OWNER TO ws;

CREATE TABLE IF NOT EXISTS subevent_queue (
	id integer DEFAULT NEXTVAL('subevent_queue_seq') CONSTRAINT pk_subevent_queue PRIMARY KEY,
	ue_id integer,
	linkage_items text,    -- JSON
	status text,    -- new or failed
	reason text,	-- failure reason
	processed_time TIMESTAMP WITH TIME ZONE		-- Time of processing
	);
ALTER TABLE subevent_queue OWNER TO ws;

alter table upload_event add column processed_time timestamp with time zone;
create index idx_ue_processed_time on upload_event (processed_time);

alter table s3upload add column created_time timestamp with time zone;

alter table cms_alert add column filter text;

alter table server add column max_rate integer default 0;

-- Version 23.11
alter table question add column required_expression text;
alter table question add column readonly_expression text;

-- Version 24.01
alter table users add column basic_password text;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Version 24.02
create sequence email_id START 1;
ALTER SEQUENCE email_id OWNER TO ws;

alter table forward add column updated boolean;

alter table users add column current_survey_ident text;
update users set current_survey_ident = (select ident from survey where s_id = current_survey_id) where current_survey_ident is null and current_survey_id > 0;

-- Version 24.05
alter table users add column api_key text;

-- Version 24.06
CREATE UNLOGGED TABLE IF NOT EXISTS submission_queue
(
    element_identifier UUID PRIMARY KEY,
    time_inserted TIMESTAMP,
    ue_id integer,
    instanceid text,	-- Don't allow duplicates in the submission queue where they can be worked on in parallel
    restore boolean,
    payload JSON
);
ALTER TABLE submission_queue OWNER TO ws;

alter table upload_event add column queue_name text;
alter table upload_event add column queued boolean default false;
alter table upload_event add column restore boolean default false;

alter table subevent_queue add column created_time TIMESTAMP WITH TIME ZONE;

CREATE UNLOGGED TABLE IF NOT EXISTS monitor_data
(
    recorded_at TIMESTAMP WITH TIME ZONE,
    payload JSON
);
ALTER TABLE monitor_data OWNER TO ws;

CREATE UNLOGGED TABLE IF NOT EXISTS message_queue
(
    element_identifier UUID PRIMARY KEY,
    time_inserted TIMESTAMP,
    m_id integer,
    o_id integer,
    topic text,	
    description text,
    data text
);
ALTER TABLE message_queue OWNER TO ws;

alter table message add column queue_name text;
alter table message add column queued boolean default false;

-- Version 24.09
CREATE UNLOGGED TABLE IF NOT EXISTS key_queue
(
    element_identifier UUID PRIMARY KEY,
    key text,
    group_survey_ident text
);
ALTER TABLE key_queue OWNER TO ws;

alter table upload_event add column submission_type text;
alter table upload_event add column payload text;

CREATE TABLE IF NOT EXISTS sms_number (
    element_identifier UUID PRIMARY KEY,
    o_id integer,					-- Organisation that the number is allocated to
    time_modified TIMESTAMP WITH TIME ZONE,
    our_number text,			-- Our number that sends or receives messages
    survey_ident text,
    their_number_question text, -- The question in the survey that holds the number of the counterpart
    message_question text,		-- The question name in the survey that holds the message details
    description text
);
ALTER TABLE sms_number OWNER TO ws;
CREATE UNIQUE INDEX IF NOT EXISTS sms_number_to_idx ON sms_number(our_number);

ALTER TABLE server add column vonage_application_id text;
ALTER TABLE server add column vonage_webhook_secret text;

ALTER TABLE log  DROP CONSTRAINT log_o_id_fkey;

ALTER TABLE users add column app_key text;

ALTER TABLE organisation add column email_type text;
ALTER TABLE server add column email_type text;
update server set email_type = 'smtp' where email_type is null;

alter table organisation add column ft_force_token boolean default false;

alter table sms_number add column channel text;
update sms_number set channel = 'sms' where channel is null;
alter table record_event add column message text;

-- Version 24.10
--alter table survey_role add column group_survey_ident text;  -- Removed in 25.01

-- Version 24.11
ALTER TABLE organisation add column email_type text;
ALTER TABLE organisation add column aws_region text;
ALTER TABLE server add column aws_region text;
alter table sms_number add column mc_msg text;	-- Message to send if there is more than one case to update

-- Remove links security group
delete from groups where name = 'links';
delete from user_group where g_id = 13;

create index if not exists question_l_id_idx on question(l_id);	-- Address performance issue
create index if not exists form_table_name on form(table_name);
create index if not exists tasks_survey_idx on tasks(survey_ident);
create index if not exists linked_files_logical_path on linked_files(logical_path);

-- Console Admin
insert into groups(id,name) values(14,'console admin');

-- Improve Timezone Performance
CREATE TABLE IF NOT EXISTS timezone (
    name text,
    utc_offset text
);
ALTER TABLE timezone OWNER TO ws;

ALTER TABLE users add column total_tasks integer default -1;

alter table group_survey drop constraint group_survey_u_ident_fkey;			-- Remove constraint on group survey as sometimes this has to be null, for example when sub form is set up not group survey
delete from group_survey where group_ident is null and f_name is null;  	-- Remove unused values

-- Notification bundles
alter table forward add column bundle boolean default false;
alter table forward add column bundle_ident text;

-- Performance
create index if not exists survey_p_id on survey(p_id);

CREATE SEQUENCE bundle_seq START 1;
ALTER SEQUENCE bundle_seq OWNER TO ws;

CREATE TABLE if not exists bundle (
	id integer DEFAULT NEXTVAL('bundle_seq') CONSTRAINT pk_bundle PRIMARY KEY,
	group_survey_ident text,
	bundle_roles boolean,
	changed_by text,
	changed_ts TIMESTAMP WITH TIME ZONE	
	);
CREATE UNIQUE INDEX if not exists bundle_group_idx ON bundle(group_survey_ident);
ALTER TABLE bundle OWNER TO ws;

-- performance
create index message_created_idx on message (created_time);

-- remove dashboard security group
delete from groups where name = 'dashboard';
delete from user_group where g_id = 12;

-- Drop survey role constraint that causes issues when replacing a survey that has roles
alter table survey_role drop constraint survey_role_survey_ident_fkey;

-- Allow server setting to require security manager privilege to delete
alter table server add column sec_mgr_del boolean default false;

-- Add a flag to force reset of total tasks
alter table users add column reset_total_tasks boolean default false;

-- Add survey role groups
alter table survey_role add column role_group text default 'A';
update survey_role set role_group = 'A' where role_group is null;

-- Ensure users have a name
update users set name = ident where name = '' and not temporary;

-- Version 25.08
alter table subevent_queue add column thread text;

CREATE SEQUENCE notified_record_seq START 1;
ALTER SEQUENCE notified_record_seq OWNER TO ws;

CREATE TABLE public.notified_record (
	id integer default nextval('notified_record_seq') not null PRIMARY KEY,
	n_id integer REFERENCES forward(id) ON DELETE CASCADE,
	thread text
	);
ALTER TABLE notified_record OWNER TO ws;
CREATE INDEX n_thread ON notified_record(thread);

-- Remove foreign key on log archive file
alter table log_archive drop constraint log_archive_o_id_fkey;

-- Version 25.12
-- insert into groups(id,name) values(15,'mcp access');

-- Performance tuning
create index if not exists linked_forms_idx on linked_forms(linked_s_id);
delete from pending_message where created_time < now() - interval '1 year';
create index if not exists opted_in_people_idx on people(opted_in);
create index if not exists unsub_people_idx on people(unsubscribed);

-- Version 25.01 Multi-server subscriber support
-- Drop old indexes that interfere with query planner
drop index if exists idx_ue_ra;
drop index if exists idx_ue_queued;
drop index if exists idx_ue_applied;
-- Composite index for pending upload queries
create index concurrently if not exists idx_ue_pending on upload_event (results_db_applied, queued, incomplete, ue_id);

-- Add API rate limiting configuration
alter table server add column if not exists api_max_records integer default 0;

-- Performance improvement to get tasks
CREATE INDEX idx_tasks_tg_schedule_desc ON tasks(tg_id, schedule_at DESC);
delete from groups where id = 15;

-- Cloudflare Turnstile anti-bot support
alter table server add column if not exists turnstile_site_key text;
alter table server add column if not exists turnstile_secret_key text;
alter table survey add column if not exists turnstile boolean default false;

-- Self assign tasks
alter table task_rejected add column t_id integer REFERENCES tasks(id) ON DELETE CASCADE;
delete from task_rejected where t_id is null;

alter table dashboard_settings add column ds_chart_type text default 'histogram';
alter table dashboard_settings add column ds_show_meta boolean default true;
alter table dashboard_settings add column ds_wrap_text boolean default true;

-- Version 26.04 Subscriber worker identification
CREATE UNLOGGED TABLE IF NOT EXISTS subscriber_worker (
	id serial PRIMARY KEY,
	hostname text,
	pid bigint,
	subscriber_type text,
	queue_name text,
	started_time timestamptz DEFAULT now(),
	heartbeat timestamptz DEFAULT now()
);
ALTER TABLE subscriber_worker OWNER TO ws;

alter table upload_event add column if not exists worker_host text;
alter table s3upload add column if not exists worker_id text;
alter table message add column if not exists worker_host text;

-- Version 26.05 SharePoint integration
alter table server add column if not exists sharepoint_url text;
alter table server add column if not exists sharepoint_client_id text;
alter table server add column if not exists sharepoint_realm text;
alter table server add column if not exists sharepoint_cert_pem text;
alter table server add column if not exists sharepoint_auth_type text default 's2s';
alter table server add column if not exists sharepoint_username text;
alter table server add column if not exists sharepoint_password text;
alter table server add column if not exists sharepoint_domain text;

CREATE SEQUENCE IF NOT EXISTS sharepoint_list_map_seq START 1;
ALTER SEQUENCE sharepoint_list_map_seq OWNER TO ws;

CREATE TABLE IF NOT EXISTS sharepoint_list_map (
	id integer DEFAULT nextval('sharepoint_list_map_seq') NOT NULL PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	smap_name text NOT NULL,
	list_title text NOT NULL,
	refresh_minutes integer DEFAULT 60,
	last_sync TIMESTAMP WITH TIME ZONE,
	csv_table_id integer REFERENCES csvtable(id) ON DELETE SET NULL,
	enabled boolean DEFAULT true
	);
CREATE INDEX IF NOT EXISTS sharepoint_list_map_org_idx ON sharepoint_list_map(o_id);
ALTER TABLE sharepoint_list_map OWNER TO ws;

-- Version 26.04 Workflow node positions per user per organisation
CREATE TABLE IF NOT EXISTS workflow_node_positions (
	user_ident   text,
	o_id         integer references organisation(id) on delete cascade,
	positions    jsonb,
	PRIMARY KEY (user_ident, o_id)
);
ALTER TABLE workflow_node_positions OWNER TO ws;
-- Version 26.04.1 Explicit workflow starting-point forms
CREATE TABLE IF NOT EXISTS workflow_start (
    id      serial PRIMARY KEY,
    s_ident text,
    p_id    integer references project(id) on delete cascade
);
ALTER TABLE workflow_start OWNER TO ws;
-- Workflow explicit sequence: predecessor node ID set by workflow canvas
ALTER TABLE forward     ADD COLUMN IF NOT EXISTS wf_prev_node_id text;
ALTER TABLE task_group  ADD COLUMN IF NOT EXISTS wf_prev_node_id text;
-- Version 26.04.2 Prevent duplicate submissions in queue via unique index
create unique index if not exists submission_queue_instanceid_idx on submission_queue(instanceid);

-- Show form index panel in webforms
alter table survey add column if not exists show_form_index boolean default false;

-- Allow notifications to be sent from WebForms (per org)
alter table organisation add column if not exists notification_webform boolean default false;
-- Allow redactions (per org)
alter table organisation add column if not exists enable_redact boolean default false;

-- PII / anonymise flag on questions
alter table question add column if not exists pii text;

-- DPO role
insert into groups(id,name) values(16,'dpo') on conflict(id) do nothing;

-- Version 26.05 Email reply tracking
alter table server add column if not exists email_response_bucket text;
alter table server add column if not exists email_response_domain text;
alter table notification_log add column if not exists aws_message_id text;
alter table record_event add column if not exists ses_message_id text;
create unique index if not exists record_event_ses_message_id_idx on record_event(ses_message_id) where ses_message_id is not null;

-- Referenced cases: denormalised index of records a user owns (case) or can reference (read only)
CREATE SEQUENCE IF NOT EXISTS record_user_id_seq START 1;
ALTER SEQUENCE record_user_id_seq OWNER TO ws;
CREATE TABLE IF NOT EXISTS record_user (
	id bigint NOT NULL DEFAULT nextval('record_user_id_seq') PRIMARY KEY,
	assignee integer REFERENCES users(id) ON DELETE CASCADE,
	assignee_ident text NOT NULL,
	group_survey_ident text NOT NULL,
	survey_ident text,
	thread text NOT NULL,
	access text NOT NULL DEFAULT 'owner',	-- owner || reference
	read_only boolean NOT NULL DEFAULT false,
	created_by text,
	created_at timestamp with time zone DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS record_user_unique ON record_user(assignee_ident, group_survey_ident, thread);
CREATE INDEX IF NOT EXISTS record_user_assignee ON record_user(assignee_ident);
CREATE INDEX IF NOT EXISTS record_user_thread ON record_user(group_survey_ident, thread);
ALTER TABLE record_user OWNER TO ws;
-- One off backfill of record_user owner rows from _assigned, run by the forward subscriber on startup
alter table server add column if not exists record_user_backfilled boolean default false;

-- Operations Monitor per-organisation settings (stale interval, RAG thresholds, trend window)
CREATE TABLE IF NOT EXISTS ops_settings (
	o_id        integer PRIMARY KEY references organisation(id) on delete cascade,
	settings    jsonb,
	changed_by  text,
	changed_ts  timestamp with time zone
);
ALTER TABLE ops_settings OWNER TO ws;

-- Remove the unused per-user alert table (never populated; ops alerts now come from
-- cms_alert + case_alert_triggered, alert history lives in record_event)
DROP TABLE IF EXISTS alert CASCADE;
DROP SEQUENCE IF EXISTS alert_seq CASCADE;

-- Cap the number of records supplied by a survey when used as a reference file.
-- 0 (default) = unlimited; N = only the latest N records by _upload_time.
alter table survey add column if not exists max_reference_records integer default 0;
-- Record the cap a linked file was generated with so it can be regenerated when the cap changes.
alter table linked_forms add column if not exists max_records integer default 0;

-- Static pseudo-SQL filter restricting the reference data a survey bundle pulls from a source
-- survey.  Defined at the group level (like roles) per (linker group, source group) pair.
create sequence if not exists reference_filter_seq start 1;
alter sequence reference_filter_seq owner to ws;
create table if not exists reference_filter (
	id integer default nextval('reference_filter_seq') constraint pk_reference_filter primary key,
	linker_s_ident text not null,		-- requesting survey group ident
	linked_s_ident text not null,		-- source survey group ident
	filter text,
	enabled boolean default true
);
alter table reference_filter owner to ws;
create unique index if not exists reference_filter_idx on reference_filter(linker_s_ident, linked_s_ident);
-- Cap the number of records supplied over this connection.  0 (default) = unlimited.
-- Replaces the shortcut survey.max_reference_records (now dormant) with a per-connection cap.
alter table reference_filter add column if not exists max_records integer default 0;

-- Offline map layers (mbtiles) managed on the server and pushed to devices.
-- When ft_offline_maps is set the device cannot add or delete layers itself.
alter table organisation add column if not exists ft_offline_maps boolean default false;

create sequence if not exists offline_layer_seq start 1;
alter sequence offline_layer_seq owner to ws;
create table if not exists offline_layer (
	id integer default nextval('offline_layer_seq') constraint pk_offline_layer primary key,
	o_id integer references organisation(id) on delete cascade,
	name text not null,			-- name shown to the user, unique within the organisation
	file_name text not null,	-- name of the file on disk
	file_path text,				-- full path to the file on the server
	file_size bigint default 0,
	md5 text,					-- checksum, used by the device to skip unchanged files
	version integer default 1,	-- incremented whenever the file is replaced
	description text,
	changed_by text,
	changed_ts timestamp with time zone
);
alter table offline_layer owner to ws;
create unique index if not exists offline_layer_idx on offline_layer(o_id, name);

-- A layer is assigned to projects, everybody with access to one of those projects gets it
create sequence if not exists offline_layer_project_seq start 1;
alter sequence offline_layer_project_seq owner to ws;
create table if not exists offline_layer_project (
	id integer default nextval('offline_layer_project_seq') constraint pk_offline_layer_project primary key,
	layer_id integer references offline_layer(id) on delete cascade,
	p_id integer references project(id) on delete cascade
);
alter table offline_layer_project owner to ws;
create unique index if not exists offline_layer_project_idx on offline_layer_project(layer_id, p_id);
create index if not exists idx_olp_p on offline_layer_project(p_id);

-- Records which devices have reported that they hold a layer, so that an administrator can
-- see how a large layer is rolling out before field teams lose coverage
create sequence if not exists offline_layer_device_seq start 1;
alter sequence offline_layer_device_seq owner to ws;
create table if not exists offline_layer_device (
	id integer default nextval('offline_layer_device_seq') constraint pk_offline_layer_device primary key,
	layer_id integer references offline_layer(id) on delete cascade,
	u_id integer references users(id) on delete cascade,
	device_id text,
	layer_version integer,		-- version of the layer that the device holds
	downloaded_ts timestamp with time zone
);
alter table offline_layer_device owner to ws;
create unique index if not exists offline_layer_device_idx on offline_layer_device(layer_id, u_id, device_id);

-- Version 26.08 upload_event query performance
--
-- upload_event carries the whole submission history and is on the insert path for every
-- submission, so it ends up both very large and heavily indexed.  The indexes below replace
-- single column ones that could not serve the queries that actually run against the table:
-- the monitor totals had to sort the whole project history, and the submissions API read a
-- survey's entire history to return one page.
--
-- Builds are concurrent so a deploy does not lock out submissions, but on a large table they
-- take a long time and the deploy waits for them.  This is a one off cost per server.

-- A concurrent build that is interrupted leaves an invalid index behind.  "if not exists"
-- would then match that invalid index and silently skip the retry forever, so clear it first.
DO $$
DECLARE r record;
BEGIN
	FOR r IN
		SELECT c.relname
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		WHERE NOT i.indisvalid
		  AND c.relname IN ('idx_ue_p_status_ident', 'idx_ue_ident_ueid', 'idx_ue_db_status_rare')
	LOOP
		RAISE NOTICE 'dropping invalid index % left by an interrupted build', r.relname;
		EXECUTE format('DROP INDEX %I', r.relname);
	END LOOP;
END $$;

-- Monitor totals: p_id and db_status are equality filters so the index is already ordered by
-- ident, which removes the sort as well as the heap access.  upload_time is included so the
-- variants that restrict to the last 100 days stay index only.
create index concurrently if not exists idx_ue_p_status_ident on upload_event (p_id, db_status, ident, upload_time);

-- Submissions API and the per survey monitor tabs: with ident fixed the index is ordered by
-- ue_id, so "order by ue_id desc limit n" is a backward scan that stops after n rows instead
-- of reading every upload event for the survey and sorting them.
create index concurrently if not exists idx_ue_ident_ueid on upload_event (ident, ue_id);

-- Nearly every row is 'success', so an index over all values of db_status cannot help a query
-- looking for one.  Only the rare values are worth indexing, which also makes the index small.
create index concurrently if not exists idx_ue_db_status_rare on upload_event (db_status)
	where db_status is distinct from 'success';

-- Superseded by the indexes above, which have these as a leading column
drop index concurrently if exists idx_ue_p_id;
drop index concurrently if exists ue_survey_ident;
drop index concurrently if exists idx_ue_db_status;
-- Never selected by the planner: status has too few distinct values to narrow anything down
drop index concurrently if exists idx_ue_status;

-- Every row is inserted and then updated two or three times as the subscriber records progress,
-- so the default 20% threshold leaves this table unvacuumed for long periods.  Index only scans
-- need the visibility map to be current, so vacuum has to keep up for the indexes above to work.
-- On an existing large server run "vacuum (analyze) upload_event" once, in a maintenance window,
-- to populate the visibility map for the history that is already there.
alter table upload_event set (
	autovacuum_vacuum_scale_factor = 0.02,
	autovacuum_analyze_scale_factor = 0.01,
	autovacuum_vacuum_insert_scale_factor = 0.02
);

-- Query statistics, used to find which statements are actually costing time.  Needs the library
-- in shared_preload_libraries, which patchdb.sh sets for a local database and which has to be set
-- through a parameter group on RDS - see RDS_TUNING.md.  No-op until then.
DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_statements')
		AND current_setting('shared_preload_libraries') LIKE '%pg_stat_statements%' THEN
		CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
	END IF;
END $$;

-- Optional two factor authentication (TOTP).  Apache authenticates the password, the
-- application then requires a code before it will serve the console.
alter table users add column if not exists totp_secret text;			-- base32, null until the user enrols
alter table users add column if not exists totp_confirmed boolean default false;	-- true once a code has been verified
alter table users add column if not exists totp_enrolled timestamp with time zone;
alter table users add column if not exists totp_last_counter bigint;	-- last accepted time step, stops a code being replayed

-- Key used to sign the step up cookie.  Generated on first use, shared by every web app.
-- Rotating it invalidates every outstanding step up.
alter table server add column if not exists two_factor_key text;

-- API and device tokens.  api_key and app_key were UUIDs held in plaintext in users, one
-- of each per user, with no expiry, no scope, no record of use and no index - so every
-- token authenticated request also cost a sequential scan of users.
--
-- Only the sha256 of a token is stored now.  The existing values are hashed in place
-- below, so every client that holds one keeps working and nothing has to be reissued.
-- users.api_key and users.app_key are left in place for one release as a way back, and
-- are no longer read or written.
create sequence if not exists api_token_seq start 1;
alter sequence api_token_seq owner to ws;

create table if not exists api_token (
	id integer default nextval('api_token_seq') constraint pk_api_token primary key,
	u_id integer references users(id) on delete cascade,
	scope text not null,			-- 'api' server to server || 'app' fieldTask and other devices
	name text,						-- Label chosen by whoever created it
	token_hash text not null,		-- sha256 of the token value, hex
	prefix text not null,			-- Leading characters of the value, so a token can be named in the UI and the log
	created timestamp with time zone default now(),
	created_by text,
	expires timestamp with time zone,		-- Null means no expiry
	last_used timestamp with time zone,
	last_used_ip text,
	revoked timestamp with time zone,
	revoked_by text
);
alter table api_token owner to ws;
create unique index if not exists idx_api_token_hash on api_token(token_hash);
create index if not exists idx_api_token_u_scope on api_token(u_id, scope) where revoked is null;

-- Carry the existing keys over.  Safe to re-run: the unique index on token_hash means a
-- second run inserts nothing.
insert into api_token (u_id, scope, name, token_hash, prefix, created)
select id, 'api', 'migrated', encode(digest(api_key, 'sha256'), 'hex'), left(api_key, 8), now()
	from users where api_key is not null
on conflict (token_hash) do nothing;

insert into api_token (u_id, scope, name, token_hash, prefix, created)
select id, 'app', 'migrated', encode(digest(app_key, 'sha256'), 'hex'), left(app_key, 8), now()
	from users where app_key is not null
on conflict (token_hash) do nothing;

-- The dynamic user keys used by webform links and task assignments were also unindexed
create index if not exists idx_dynamic_users_key on dynamic_users(access_key);

-- Version 26.09 DHIS2 connections
-- Held per organisation rather than per server so that one tenant can never write into
-- another tenant's DHIS2.  A tenant may have more than one, for example staging and production
CREATE SEQUENCE IF NOT EXISTS dhis2_server_seq START 1;
ALTER SEQUENCE dhis2_server_seq OWNER TO ws;

CREATE TABLE IF NOT EXISTS dhis2_server (
	id integer DEFAULT nextval('dhis2_server_seq') NOT NULL PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	label text NOT NULL,					-- Shown when choosing a connection
	base_url text NOT NULL,					-- Root of the DHIS2 instance, no trailing /api
	api_token text,							-- DHIS2 personal access token, sent as "ApiToken <token>"
	api_version text,						-- Optional version pin, eg "42".  Null uses the default
	last_tested TIMESTAMP WITH TIME ZONE,
	last_test_result text,					-- Summary of the last connection test
	enabled boolean DEFAULT true
	);
CREATE INDEX IF NOT EXISTS dhis2_server_org_idx ON dhis2_server(o_id);
ALTER TABLE dhis2_server OWNER TO ws;

-- Version 26.09 DHIS2 reference data sync
-- Each row is one DHIS2 resource cached as an organisation level CSV, referenced in a form as
-- "dhis2_{smap_name}".  Mirrors sharepoint_list_map, which does the same job for SharePoint lists
CREATE SEQUENCE IF NOT EXISTS dhis2_map_seq START 1;
ALTER SEQUENCE dhis2_map_seq OWNER TO ws;

CREATE TABLE IF NOT EXISTS dhis2_map (
	id integer DEFAULT nextval('dhis2_map_seq') NOT NULL PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	dhis2_server_id integer REFERENCES dhis2_server(id) ON DELETE CASCADE,
	smap_name text NOT NULL,				-- Referenced in a form as "dhis2_{smap_name}"
	resource_type text NOT NULL,			-- orgunits | optionset | programs
	dhis2_ref text,							-- The uid or code of the object, for a single object type
	ou_filter text,							-- Optional org unit subtree, the uid of the root to sync
	refresh_minutes integer DEFAULT 1440,	-- Metadata changes slowly, a day is plenty
	last_sync TIMESTAMP WITH TIME ZONE,
	last_sync_result text,
	row_count integer,
	csv_table_id integer REFERENCES csvtable(id) ON DELETE SET NULL,
	enabled boolean DEFAULT true
	);
CREATE INDEX IF NOT EXISTS dhis2_map_org_idx ON dhis2_map(o_id);
CREATE UNIQUE INDEX IF NOT EXISTS dhis2_map_name_idx ON dhis2_map(o_id, smap_name);
ALTER TABLE dhis2_map OWNER TO ws;

-- One DHIS2 connection per organisation.  A test setup belongs in its own organisation rather
-- than as a second connection, which keeps every screen that follows free of a picker
CREATE UNIQUE INDEX IF NOT EXISTS dhis2_server_org_unique ON dhis2_server(o_id);

-- One DHIS2 connection per organisation, so a resource does not need to name one.  The
-- connection is found from the organisation, and a second copy of the link could only drift
alter table dhis2_map drop column if exists dhis2_server_id;

-- Version 26.09 DHIS2 metadata cache
-- Configuration time metadata, read by the mapping screens rather than by a form, so it is
-- cached here rather than in the csv schema.  The payload holds the object as DHIS2 returned
-- it, which avoids modelling DHIS2's own structure a second time
CREATE SEQUENCE IF NOT EXISTS dhis2_metadata_seq START 1;
ALTER SEQUENCE dhis2_metadata_seq OWNER TO ws;

CREATE TABLE IF NOT EXISTS dhis2_metadata (
	id integer DEFAULT nextval('dhis2_metadata_seq') NOT NULL PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	object_type text NOT NULL,				-- dataset | program
	uid text NOT NULL,						-- The DHIS2 identifier
	code text,
	name text,
	payload jsonb,							-- Null until the detail has been fetched
	last_fetched TIMESTAMP WITH TIME ZONE,	-- When the detail was last read, null if never
	last_listed TIMESTAMP WITH TIME ZONE
	);
CREATE INDEX IF NOT EXISTS dhis2_metadata_org_idx ON dhis2_metadata(o_id, object_type);
CREATE UNIQUE INDEX IF NOT EXISTS dhis2_metadata_uid_idx ON dhis2_metadata(o_id, object_type, uid);
ALTER TABLE dhis2_metadata OWNER TO ws;

-- Version 26.09 DHIS2 aggregate export
-- Held at bundle level, keyed on group_survey_ident, because surveys in a bundle share data
-- tables so a question name means the same thing across them.  Bound on question NAME rather
-- than question id, so a mapping survives an XLSForm replacement as every other Smap rule does
CREATE SEQUENCE IF NOT EXISTS dhis2_export_seq START 1;
ALTER SEQUENCE dhis2_export_seq OWNER TO ws;

CREATE TABLE IF NOT EXISTS dhis2_export (
	id integer DEFAULT nextval('dhis2_export_seq') NOT NULL PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	group_survey_ident text NOT NULL,		-- The bundle this export belongs to
	dataset_uid text NOT NULL,				-- The DHIS2 data set values are written to
	dataset_name text,						-- Held for display, refreshed with the metadata cache
	period_type text,						-- Monthly | Weekly | Quarterly | Yearly | Daily
	period_question text,					-- Question name supplying the period, null uses upload time
	orgunit_question text NOT NULL,			-- Question name holding the DHIS2 org unit code
	enabled boolean DEFAULT true,
	last_export TIMESTAMP WITH TIME ZONE,
	last_export_result text
	);
CREATE INDEX IF NOT EXISTS dhis2_export_org_idx ON dhis2_export(o_id);
CREATE UNIQUE INDEX IF NOT EXISTS dhis2_export_bundle_idx ON dhis2_export(o_id, group_survey_ident, dataset_uid);
ALTER TABLE dhis2_export OWNER TO ws;

CREATE SEQUENCE IF NOT EXISTS dhis2_export_item_seq START 1;
ALTER SEQUENCE dhis2_export_item_seq OWNER TO ws;

CREATE TABLE IF NOT EXISTS dhis2_export_item (
	id integer DEFAULT nextval('dhis2_export_item_seq') NOT NULL PRIMARY KEY,
	e_id integer REFERENCES dhis2_export(id) ON DELETE CASCADE,
	question_name text,						-- Null when counting records rather than a question
	aggregation text NOT NULL,				-- one | count | sum
	data_element text NOT NULL,				-- DHIS2 data element code
	category_option_combo text,				-- Optional, required where the element is disaggregated
	seq integer DEFAULT 0
	);
CREATE INDEX IF NOT EXISTS dhis2_export_item_idx ON dhis2_export_item(e_id);
ALTER TABLE dhis2_export_item OWNER TO ws;
