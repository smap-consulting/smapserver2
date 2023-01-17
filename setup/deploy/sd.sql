-- 
-- Apply upgrade patches to survey definitions database
--

-- Upgrade to 19.09+
alter table task_group add column complete_all boolean;

CREATE SEQUENCE style_seq START 1;
ALTER SEQUENCE style_seq OWNER TO ws;

create TABLE style (
	id integer default nextval('style_seq') constraint pk_style primary key,
	s_id integer REFERENCES survey(s_id) ON DELETE CASCADE,
	name text,
	style text	-- json
	);
ALTER TABLE style OWNER TO ws;

alter table question add column style_id integer default 0;
alter table question add column server_calculate text;

alter table last_refresh add column 	device_time TIMESTAMP WITH TIME ZONE;

CREATE SEQUENCE last_refresh_log_seq START 1;
ALTER SEQUENCE last_refresh_log_seq OWNER TO ws;

create TABLE last_refresh_log (
	id integer default nextval('last_refresh_log_seq') constraint pk_last_refresh_log primary key,
	o_id integer,
	user_ident text,
	refresh_time TIMESTAMP WITH TIME ZONE,
	device_time TIMESTAMP WITH TIME ZONE
	);
ALTER TABLE last_refresh_log OWNER TO ws;

alter table group_survey add column f_name text;

update question set source = null where qtype = 'server_calculate' and source is not null;

alter table organisation add column training text;

alter table users drop constraint users_o_id_fkey;

alter table organisation add column ft_prevent_disable_track boolean default false;

-- Duplicates are allowed
drop index record_event_key;

alter table task_group add column assign_auto boolean;
alter table tasks add column assign_auto boolean;

CREATE SEQUENCE task_rejected_seq START 1;
ALTER TABLE task_rejected_seq OWNER TO ws;

CREATE TABLE public.task_rejected (
	id integer DEFAULT nextval('task_rejected_seq') NOT NULL PRIMARY KEY,
	a_id integer REFERENCES assignments(id),    -- assignment id
	ident text,		 -- user identifier
	rejected_at timestamp with time zone
);
ALTER TABLE public.task_rejected OWNER TO ws;
CREATE UNIQUE INDEX taskRejected ON task_rejected(a_id, ident);

insert into groups(id,name) values(10,'view own data');
insert into groups(id,name) values(11,'manage tasks');

alter table forward add column update_survey text references survey(ident) on delete cascade;
alter table forward add column update_question text;
alter table forward add column update_value text;

alter table survey add column data_survey boolean default true;
alter table survey add column oversight_survey boolean default true;

SELECT AddGeometryColumn('last_refresh_log', 'geo_point', 4326, 'POINT', 2);

-- Opt In to emails
-- Default to true for existing email addresses
alter table people add column opted_in boolean;
alter table people add column opted_in_sent TIMESTAMP WITH TIME ZONE;
alter table people add column opted_in_count integer default 0;
alter table people add column opted_in_status text;
alter table people add column opted_in_status_msg text;
update people set opted_in = 'true' where opted_in is null;

CREATE SEQUENCE pending_message_seq START 1;
ALTER SEQUENCE pending_message_seq OWNER TO ws;

create TABLE pending_message (
	id integer DEFAULT NEXTVAL('pending_message_seq') CONSTRAINT pk_pending_message PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	email text,
	topic text,
	description text,
	data text,
	created_time TIMESTAMP WITH TIME ZONE,
	processed_time TIMESTAMP WITH TIME ZONE,
	status text
);
CREATE index pending_message_email ON pending_message(email);
ALTER TABLE pending_message OWNER TO ws;

alter table question add column set_value text;
alter table people add column name text;

create unique index idx_people on people(o_id, email);

alter table organisation add column send_optin boolean default true;

-- Mailout
CREATE SEQUENCE mailout_seq START 1;
ALTER SEQUENCE mailout_seq OWNER TO ws;

create TABLE mailout (
	id integer default nextval('mailout_seq') constraint pk_mailout primary key,
	survey_ident text,				-- Survey in mail out
	name text,						-- Name for the mail out
	content text,
	subject text,
	created TIMESTAMP WITH TIME ZONE,
	modified TIMESTAMP WITH TIME ZONE
	);
CREATE UNIQUE INDEX idx_mailout_name ON mailout(survey_ident, name);
ALTER TABLE mailout OWNER TO ws;

CREATE SEQUENCE mailout_people_seq START 1;
ALTER SEQUENCE mailout_people_seq OWNER TO ws;

create TABLE mailout_people (
	id integer default nextval('mailout_people_seq') constraint pk_mailout_people primary key,
	p_id integer references people(id) on delete cascade,		-- People ID
	m_id integer references mailout(id) on delete cascade,		-- Mailout Id,
	status text,		-- Mailout status
	status_details text,
	initial_data text,
	processed TIMESTAMP WITH TIME ZONE,		-- Time converted into a message
	status_updated TIMESTAMP WITH TIME ZONE	
	);
CREATE UNIQUE INDEX idx_mailout_people ON mailout_people(p_id, m_id);	
ALTER TABLE mailout_people OWNER TO ws;

-- Final status of temporary user
CREATE SEQUENCE temp_users_final_seq START 1;
ALTER SEQUENCE temp_users_final_seq OWNER TO ws;

CREATE TABLE temp_users_final (
	id INTEGER DEFAULT NEXTVAL('temp_users_final_seq') CONSTRAINT pk_temp_users_final PRIMARY KEY,
	ident text,
	status text,
	created timestamp with time zone
	);
CREATE UNIQUE INDEX idx_temp_users_final_ident ON temp_users_final(ident);
ALTER TABLE temp_users_final OWNER TO ws;

alter table mailout_people add column link text;

alter table tasks alter column deleted set default false;
alter table pending_message add column message_id integer;

CREATE INDEX idx_up_u ON user_project(u_id);

alter table project add column imported boolean default false;
alter table users add column imported boolean default false;
alter table upload_event add column temporary_user boolean default false;
update upload_event set temporary_user = 'false' where temporary_user is null;

-- Transcribe
CREATE SEQUENCE aws_async_jobs_seq START 1;
ALTER SEQUENCE aws_async_jobs_seq OWNER TO ws;

-- Aynchronous AWS jobs deposit the data in an S3 bucket
-- This S3 object is the definitive and full results and a link to it
-- will be retaine in the sync table
create TABLE aws_async_jobs (
	id integer DEFAULT NEXTVAL('aws_async_jobs_seq') CONSTRAINT pk_aws_async_jobs PRIMARY KEY,
	o_id integer,
	col_name text,			-- Question that initiated this request
	table_name text,		-- Table containing the data
	instanceid text,		-- Record identifier
	type text,				-- AUTO_UPDATE_AUDIO ||
	update_details text,	-- AutoUpdate object in JSON
	job text,				-- Unique AWS job identifier
	status text,			-- open || pending || complete || error
	results_link text,			-- URI to job results
	request_initiated TIMESTAMP WITH TIME ZONE,
	request_completed TIMESTAMP WITH TIME ZONE
);
ALTER TABLE aws_async_jobs OWNER TO ws;

alter table language add column code text;
alter table language add column rtl boolean default false; 
alter table log add column measure integer default 0;
alter table role add column imported boolean default false;
alter table organisation add column limits text;

CREATE SEQUENCE resource_usage_seq START 1;
ALTER SEQUENCE resource_usage_seq OWNER TO ws;

create TABLE resource_usage (
	id integer DEFAULT NEXTVAL('resource_usage_seq') CONSTRAINT pk_resource_usage PRIMARY KEY,
	o_id integer,
	period text,			-- year - month
	resource text,			-- Resource identifier
	usage integer			-- Amount of usage
);
ALTER TABLE resource_usage OWNER TO ws;

CREATE SEQUENCE language_codes_seq START 1;
ALTER SEQUENCE language_codes_seq OWNER TO ws;

create TABLE language_codes (
	id integer DEFAULT NEXTVAL('language_codes_seq') CONSTRAINT pk_language_codes PRIMARY KEY,
	code text,
	aws_translate boolean,			-- set yes if supported by translate
	aws_transcribe boolean,			-- set yes if supported by transcribe
	transcribe_default boolean		-- true if this is the default language to use for transcribe
);
ALTER TABLE language_codes OWNER TO ws;
create unique index idx_language_codes_code on language_codes(code);

alter table survey add column auto_translate boolean default false;

alter table form add column append boolean default false;

CREATE INDEX idx_question_param ON question (parameters) WHERE (parameters is not null);

alter table aws_async_jobs add column duration integer;
alter table aws_async_jobs add column locale text;

update organisation set limits = '{"transcribe":250,"submissions":0,"rekognition":100,"translate":5000}' where limits is null;
update organisation set limits = '{"transcribe":250,"submissions":0,"rekognition":100,"translate":5000}' where limits = '{"transcribe":500,"submissions":0,"rekognition":100,"translate":5000}';

create TABLE email_alerts (
	o_id integer,
	alert_type text,
	alert_recorded TIMESTAMP WITH TIME ZONE
);
ALTER TABLE email_alerts OWNER TO ws;

--update question set compressed = true where not qtype = 'select' and not qtype = 'rank'; 

alter table organisation add column ft_high_res_video text;
update organisation set ft_high_res_video = 'not set' where ft_high_res_video is null;

alter table organisation add column ft_guidance text;
update organisation set ft_guidance = 'not set' where ft_guidance is null;

alter table organisation add column ft_server_menu boolean default true;
update organisation set ft_server_menu = true where ft_server_menu is null;
alter table organisation add column ft_meta_menu boolean default true;
update organisation set ft_meta_menu = true where ft_meta_menu is null;

alter table aws_async_jobs add column medical boolean;
alter table language_codes add column transcribe_medical boolean;

CREATE SEQUENCE autoupdate_questions_seq START 1;
ALTER SEQUENCE autoupdate_questions_seq OWNER TO ws;

create TABLE autoupdate_questions (
	id integer DEFAULT NEXTVAL('autoupdate_questions_seq') CONSTRAINT pk_autoupdate_questions PRIMARY KEY,
	q_id integer references question(q_id) on delete cascade,
	s_id integer references survey(s_id) on delete cascade
);
ALTER TABLE autoupdate_questions OWNER TO ws;

insert into autoupdate_questions (q_id, s_id) 
     select q.q_id, f.s_id from question q, form f, survey s 
     where q.f_id = f.f_id and f.s_id = s.s_id 
     and not s.deleted and not s.blocked 
     and q.parameters is not null and q.parameters like '%source=%' 
     and (q.parameters like '%auto=yes%' or q.parameters like '%auto_annotate=yes%') 
     and q_id not in (select q_id from autoupdate_questions);
 
 -- Custom reports
CREATE SEQUENCE custom_report_type_seq START 1;
ALTER SEQUENCE custom_report_type_seq OWNER TO ws;

CREATE TABLE custom_report_type (
	id integer DEFAULT NEXTVAL('custom_report_type_seq') CONSTRAINT pk_custom_report_type PRIMARY KEY,
	name text,
	config text								-- Custom report columns as json object
	);
ALTER TABLE custom_report_type OWNER TO ws;

insert into custom_report_type(name, config) values('Daily', null);

CREATE SEQUENCE custom_report_seq START 1;
ALTER SEQUENCE custom_report_seq OWNER TO ws;

CREATE TABLE custom_report (
	id integer DEFAULT NEXTVAL('custom_report_seq') CONSTRAINT pk_custom_report PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	p_id integer REFERENCES project(id) ON DELETE CASCADE,
	survey_ident text REFERENCES survey(ident) ON DELETE CASCADE,
	name text,
	type_id integer	REFERENCES custom_report_type(id) ON DELETE CASCADE,
	config text								-- Custom report columns as json object
	);
ALTER TABLE custom_report OWNER TO ws;
CREATE UNIQUE INDEX custom_report_name ON custom_report(p_id, name);

alter table custom_report add column p_id integer REFERENCES project(id) ON DELETE CASCADE;
alter table custom_report drop column type;
alter table custom_report add column type_id integer REFERENCES custom_report_type(id) ON DELETE CASCADE;
alter table custom_report add column  survey_ident text REFERENCES survey(ident) ON DELETE CASCADE;
drop index custom_report_name;
CREATE UNIQUE INDEX custom_report_name ON custom_report(p_id, name);

alter table organisation add column refresh_rate integer default 0;
update organisation set refresh_rate = 0 where refresh_rate is null;

CREATE INDEX record_event_key ON record_event(key);
CREATE INDEX question_column_name_key ON question(column_name);

create index idx_ue_upload_time on upload_event (upload_time);
CREATE INDEX log_time_key ON log(log_time);

alter table organisation add column ft_enable_geofence boolean default false;
update organisation set ft_enable_geofence = true where ft_send_location is null or ft_send_location = 'not set' or ft_send_location = 'on';

-- Version 20.09
alter table dashboard_settings add column ds_inc_ro boolean default false;
alter table dashboard_settings add column ds_geom_questions text;

-- version 20.10
alter table survey add column default_logo text;
CREATE INDEX task_task_group ON tasks(tg_id);
CREATE INDEX assignments_status ON assignments(status);
CREATE INDEX form_s_id ON form(s_id);

-- version 20.11
alter table question add column flash integer;
alter table survey add column group_survey_ident text;
update survey n set group_survey_ident = (select s.ident from survey s where s.s_id = n.group_survey_id) where n.group_survey_id > 0 and n.group_survey_ident is null;
update survey set group_survey_ident = ident where group_survey_id = 0 and group_survey_ident is null;
CREATE INDEX group_survey_ident_idx ON survey(group_survey_ident);
DROP INDEX if exists survey_group_survey_key;

insert into groups(id,name) values(12,'dashboard');

-- version 21.01
alter table question add column trigger text;
alter table server add column css text;
alter table organisation add column css text;

alter table organisation add column navbar_text_color text;

-- version 21.02
alter table organisation add column ft_mark_finalized boolean default false;
update organisation set ft_mark_finalized = false where ft_mark_finalized is null;
alter table organisation add column owner integer default 0;
update organisation set owner = 0 where owner is null;

delete from survey_settings where u_id not in (select id from users);

-- version 21.03
alter table csvtable add column linked_sident text;
alter table csvtable add column chart_key text;
alter table csvtable add column linked_s_pd boolean;

delete from csvtable where survey;

alter table organisation add column ft_bg_stop_menu boolean default false;
update organisation set ft_exit_track_menu = false where ft_bg_stop_menu is null;

-- rate limiter
alter table organisation add column api_rate_limit integer default 0;
update organisation set api_rate_limit = 0 where api_rate_limit is null;

update translation set type = 'guidance' where type = 'guidance_hint';

alter table survey add column search_local_data boolean default false;

-- Rotating csv file names
CREATE SEQUENCE linked_files_seq START 1;
ALTER SEQUENCE linked_files_seq OWNER TO ws;

create TABLE linked_files (
	id integer DEFAULT NEXTVAL('linked_files_seq') CONSTRAINT pk_linked_files PRIMARY KEY,
	s_id integer references survey(s_id) on delete cascade,
	logical_path text,
	current_id integer
);
ALTER TABLE linked_files OWNER TO ws;

CREATE SEQUENCE linked_files_old_seq START 1;
ALTER SEQUENCE linked_files_old_seq OWNER TO ws;

create TABLE linked_files_old (
	id integer DEFAULT NEXTVAL('linked_files_old_seq') CONSTRAINT pk_linked_old_files PRIMARY KEY,
	file text,
	deleted_time TIMESTAMP WITH TIME ZONE
);
ALTER TABLE linked_files_old OWNER TO ws;

alter table linked_files_old add column erase_time timestamp with time zone;

-- Log table archive
-- Note when patching in log_archive the order of columns is different in order to match the patched order of log on most servers
create TABLE log_archive (
	id integer CONSTRAINT pk_log_archive PRIMARY KEY,
	log_time TIMESTAMP WITH TIME ZONE,
	s_id integer,
	o_id integer,
	user_ident text,
	event text,	
	note text,
	e_id integer,
	measure int default 0		-- In the case of translate this would be the number of characters
	);
ALTER TABLE log_archive OWNER TO ws;

alter table people alter column opted_in set default false;
update people set opted_in = false where opted_in is null;

alter table users add column one_time_password_sent timestamp with time zone;
alter table last_refresh add column deviceid text;
alter table last_refresh_log add column deviceid text;

alter table dashboard_settings add column ds_selected_geom_question text;
alter table log add column server text;
alter table log_archive add column server text;

alter table mailout add column multiple_submit boolean;
alter table mailout_people add column user_ident text;

CREATE SEQUENCE background_report_seq START 1;
ALTER SEQUENCE background_report_seq OWNER TO ws;

create TABLE background_report (
	id integer DEFAULT NEXTVAL('background_report_seq') CONSTRAINT pk_background_report PRIMARY KEY,
	o_id integer,
	p_id integer,
	u_id integer,		-- user
	share boolean,
	status text,        -- new || processing || completed || failed
	status_msg text,
	report_type text,	-- Type of report
	report_name text,	-- Name given to the report by a user
	filename text,
	tz text,
	language text,
	params text,		-- Report details in JSON	
	start_time TIMESTAMP WITH TIME ZONE,
	end_time TIMESTAMP WITH TIME ZONE
);
ALTER TABLE background_report OWNER TO ws;

alter table last_refresh add column appversion text;
alter table last_refresh_log add column appversion text;

alter table organisation add column dashboard_region text;
alter table organisation add column dashboard_arn text;
alter table organisation add column dashboard_session_name text;

-- upgrade to version 21.12
alter table survey add column compress_pdf boolean;

CREATE SEQUENCE st_seq START 1;
ALTER SEQUENCE st_seq OWNER TO ws;

CREATE TABLE survey_template (
	t_id integer DEFAULT NEXTVAL('st_seq') CONSTRAINT pk_survey_template PRIMARY KEY,
	ident text,										-- Survey containing this version
	name text,
	filepath text,
	not_available boolean default false,				-- Set to true if the template is not available for selection
	default_template boolean default false,
	template_type text,							-- pdf || word
	user_id integer,							-- Person who made the changes				
	updated_time TIMESTAMP WITH TIME ZONE		-- Time and date of change
	);
ALTER TABLE survey_template OWNER TO ws;

Alter table survey_template add column rule text;

alter table survey_template drop constraint survey_template_ident_fkey;  -- Some deployments will have this set

alter table server add column password_strength decimal default 0.0;
alter table organisation add column password_strength decimal default 0.0;
create index idx_refresh_time on last_refresh_log (refresh_time);

alter table organisation add column limit_type text default 'alltime';

-- improve performance of image processing

CREATE SEQUENCE s3upload_seq START 1;
ALTER SEQUENCE s3upload_seq OWNER TO ws;

CREATE TABLE s3upload (
	id integer DEFAULT NEXTVAL('s3upload_seq') CONSTRAINT pk_s3upload PRIMARY KEY,
	filepath text,
	status text,    -- new or failed
	reason text,	-- failure reason
	processed_time TIMESTAMP WITH TIME ZONE		-- Time of processing
	);
ALTER TABLE s3upload OWNER TO ws;

CREATE SEQUENCE cms_alert_seq START 1;
ALTER SEQUENCE cms_alert_seq OWNER TO ws;

CREATE TABLE cms_alert (
	id integer DEFAULT NEXTVAL('cms_alert_seq') CONSTRAINT pk_cms_alert PRIMARY KEY,
	o_id integer,
	group_survey_ident text,
	name text,
	period text,
	changed_by text,
	changed_ts TIMESTAMP WITH TIME ZONE	
	);
CREATE UNIQUE INDEX cms_unique_alert ON cms_alert(group_survey_ident, name);
ALTER TABLE cms_alert OWNER TO ws;

CREATE SEQUENCE cms_setting_seq START 1;
ALTER SEQUENCE cms_setting_seq OWNER TO ws;

CREATE TABLE cms_setting (
	id integer DEFAULT NEXTVAL('cms_setting_seq') CONSTRAINT pk_setting_alert PRIMARY KEY,
	o_id integer,
	group_survey_ident text,
	settings text,
	changed_by text,
	changed_ts TIMESTAMP WITH TIME ZONE	
	);
CREATE UNIQUE INDEX cms_unique_setting ON cms_setting(group_survey_ident);
ALTER TABLE cms_setting OWNER TO ws;

alter table forward add column alert_id integer;


-- improve performance of scanning linked_files_old for files ready to be deleted
create index idx_lfo_erase on linked_files_old (erase_time);
create index idx_assignments_task_id on assignments (task_id);
create index idx_tasks_del_auto on tasks (deleted, assign_auto);

create index idx_record_event_table_name on record_event (table_name);
create index assignments_assignee on assignments(assignee);
create index survey_change_s_id on survey_change(s_id);
create index form_downloads_form on form_downloads(form_ident);

-- manage automatic geopoint recording
alter table organisation add column ft_input_method text;
alter table organisation add column ft_im_ri integer;
alter table organisation add column ft_im_acc integer;
update organisation set ft_input_method = 'not set' where ft_input_method is null;
update organisation set ft_im_ri = 3 where ft_im_ri is null;
update organisation set ft_im_acc = 3 where ft_im_acc is null;

alter table dashboard_settings add column ds_qname text;
update dashboard_settings ds set ds_qname = (select q.qname from question q where q.q_id = ds.ds_q_id) where ds.ds_qname is null and ds.ds_q_id > 0;

alter table dashboard_settings add column ds_date_question_name text;
update dashboard_settings ds set ds_date_question_name = (select q.qname from question q where q.q_id = ds.ds_date_question_id) where ds.ds_date_question_name is null and ds.ds_date_question_id > 0;

alter table dashboard_settings add column ds_group_question_name text;
update dashboard_settings ds set ds_group_question_name = (select q.qname from question q where q.q_id = ds.ds_group_question_id) where ds.ds_group_question_name is null and ds.ds_group_question_id > 0;

alter table s3upload add column o_id integer default 0;
alter table s3upload add column is_media boolean default false;

alter table survey add column read_only_survey boolean default false;

-- Version 22.12
alter table cms_setting add column key text;
alter table cms_setting add column key_policy text;

-- Create a table to hold biometric, including fingerprint record linkage data
CREATE SEQUENCE linkage_seq START 1;
ALTER SEQUENCE linkage_seq OWNER TO ws;

CREATE TABLE linkage (
	id integer DEFAULT NEXTVAL('linkage_seq') CONSTRAINT pk_linkage PRIMARY KEY,
	o_id integer,
	survey_ident text,				-- Source
	instance_id text,
	col_name text,
									-- Image Fingerprint data
	fp_location text,				-- Location on the body: hand or foot or unknown
	fp_side text,					-- left or right
	fp_digit integer,				-- 0-5, 0 = thumb, 5 = palm
	fp_image text,					-- URL of image
	fp_native_template bytea,		-- Generated from the raw image using FP tools
	
	fp_iso_template text,			-- Fingerprint ISO data
	
	changed_by text,
	changed_ts TIMESTAMP WITH TIME ZONE	
	);
ALTER TABLE linkage OWNER TO ws;

insert into groups(id,name) values(13,'links');
alter table server add column rebuild_link_cache boolean default false;
