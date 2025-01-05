CREATE USER ws WITH PASSWORD 'ws1234';

DROP SEQUENCE IF EXISTS email_id CASCADE;
CREATE SEQUENCE email_id START 1;
ALTER SEQUENCE email_id OWNER TO ws;

DROP SEQUENCE IF EXISTS sc_seq CASCADE;
CREATE SEQUENCE sc_seq START 1;
ALTER SEQUENCE sc_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS st_seq CASCADE;
CREATE SEQUENCE st_seq START 1;
ALTER SEQUENCE st_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS re_seq CASCADE;
CREATE SEQUENCE re_seq START 1;
ALTER SEQUENCE re_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS s_seq CASCADE;
CREATE SEQUENCE s_seq START 1;
ALTER SEQUENCE s_seq OWNER TO ws;
 
DROP SEQUENCE IF EXISTS f_seq CASCADE;
CREATE SEQUENCE f_seq START 1;
ALTER SEQUENCE f_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS o_seq CASCADE;
CREATE SEQUENCE o_seq START 1;
ALTER SEQUENCE o_seq OWNER TO ws;	

DROP SEQUENCE IF EXISTS l_seq CASCADE;
CREATE SEQUENCE l_seq START 1;
ALTER SEQUENCE l_seq OWNER TO ws;		

DROP SEQUENCE IF EXISTS q_seq CASCADE;
CREATE SEQUENCE q_seq START 1;
ALTER SEQUENCE q_seq OWNER TO ws;	

DROP SEQUENCE IF EXISTS ssc_seq CASCADE;
CREATE SEQUENCE ssc_seq START 1;
ALTER SEQUENCE ssc_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS forward_seq CASCADE;
CREATE SEQUENCE forward_seq START 1;
ALTER SEQUENCE forward_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS reminder_seq CASCADE;
CREATE SEQUENCE reminder_seq START 1;
ALTER SEQUENCE reminder_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS notification_log_seq CASCADE;
CREATE SEQUENCE notification_log_seq START 1;
ALTER SEQUENCE notification_log_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS t_seq CASCADE;
CREATE SEQUENCE t_seq START 1;
ALTER SEQUENCE t_seq OWNER TO ws;	

DROP SEQUENCE IF EXISTS location_seq CASCADE;
CREATE SEQUENCE location_seq START 1;
ALTER SEQUENCE location_seq OWNER TO ws;	

DROP SEQUENCE IF EXISTS l_seq CASCADE;
CREATE SEQUENCE l_seq START 1;
ALTER SEQUENCE l_seq OWNER TO ws;	

DROP SEQUENCE IF EXISTS g_seq CASCADE;
CREATE SEQUENCE g_seq START 1;
ALTER SEQUENCE g_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS ue_seq CASCADE;
CREATE SEQUENCE ue_seq START 1;
ALTER SEQUENCE ue_seq OWNER TO ws;

--DROP SEQUENCE IF EXISTS se_seq CASCADE;
--CREATE SEQUENCE se_seq START 1;
--ALTER SEQUENCE se_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS dp_seq CASCADE;
CREATE SEQUENCE dp_seq START 1;
ALTER SEQUENCE dp_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS sc_seq CASCADE;
CREATE SEQUENCE sc_seq START 1;
ALTER SEQUENCE sc_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS custom_report_type_seq CASCADE;
CREATE SEQUENCE custom_report_type_seq START 1;
ALTER SEQUENCE custom_report_type_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS custom_report_seq CASCADE;
CREATE SEQUENCE custom_report_seq START 1;
ALTER SEQUENCE custom_report_seq OWNER TO ws;

-- User management
DROP SEQUENCE IF EXISTS project_seq CASCADE;
CREATE SEQUENCE project_seq START 10;
ALTER SEQUENCE project_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS regions_seq CASCADE;
CREATE SEQUENCE regions_seq START 10;
ALTER SEQUENCE regions_seq OWNER TO ws;

-- Server level defaults
DROP TABLE IF EXISTS server CASCADE;
create TABLE server (
	email_type text,	-- smtp || awssdk
	aws_region text,
	smtp_host text,
	email_domain text,
	email_user text,
	email_password text,
	email_port integer,
	version text,
	mapbox_default text,
	google_key text,
	maptiler_key text,
	sms_url text,
	document_sync boolean,
	doc_server text,
	doc_server_user text,
	doc_server_password text,
	keep_erased_days integer default 0,
	billing_enabled boolean default false,
	css text,
	password_strength decimal default 0.0,
	rebuild_link_cache boolean default false,
	password_expiry integer default 0,				-- password expiry in months
	disable_ref_role_filters boolean default false,	-- If set true role filters will not be used for reference data
	max_rate integer default 0,						-- Max API rate per minute, 0 means no limit
	vonage_application_id text,
	vonage_webhook_secret text
	);
ALTER TABLE server OWNER TO ws;

DROP SEQUENCE IF EXISTS enterprise_seq CASCADE;
CREATE SEQUENCE enterprise_seq START 1;
ALTER SEQUENCE enterprise_seq OWNER TO ws;

DROP TABLE IF EXISTS enterprise CASCADE;
create TABLE enterprise (
	id INTEGER DEFAULT NEXTVAL('enterprise_seq') CONSTRAINT pk_enterprise PRIMARY KEY,
	name text,
	changed_by text,
	billing_enabled boolean default false,
	changed_ts TIMESTAMP WITH TIME ZONE
	);
CREATE UNIQUE INDEX idx_enterprise ON enterprise(name);
ALTER TABLE enterprise OWNER TO ws;

DROP SEQUENCE IF EXISTS organisation_seq CASCADE;
CREATE SEQUENCE organisation_seq START 10;
ALTER SEQUENCE organisation_seq OWNER TO ws;

DROP TABLE IF EXISTS organisation CASCADE;
create TABLE organisation (
	id INTEGER DEFAULT NEXTVAL('organisation_seq') CONSTRAINT pk_organisation PRIMARY KEY,
	e_id integer references enterprise(id) on delete cascade,
	name text,
	company_name text,
	company_address text,
	company_phone text,
	company_email text,
	allow_email boolean,
	allow_facebook boolean,
	allow_twitter boolean,
	can_edit boolean,
	can_notify boolean default true,
	can_use_api boolean default true,
	can_submit boolean default true,
	can_sms boolean default false,
	send_optin boolean default true,
	set_as_theme boolean default false,
	email_task boolean,
	ft_delete text,
	ft_backward_navigation text,
	ft_high_res_video text,
	ft_navigation text,
	ft_guidance text,
	ft_image_size text,
	ft_send_location text,
	ft_input_method text,
	ft_im_ri integer,
	ft_im_acc integer,
	ft_sync_incomplete boolean,
	ft_odk_style_menus boolean default true,
	ft_specify_instancename boolean default false,
	ft_mark_finalized boolean default false,
	ft_prevent_disable_track boolean default false,
	ft_enable_geofence boolean default false,
	ft_admin_menu boolean default false,
	ft_server_menu boolean default true,
	ft_meta_menu boolean default true,
	ft_exit_track_menu boolean default false,
	ft_bg_stop_menu boolean default false,
	ft_review_final boolean default true,
	ft_force_token boolean default false,
	ft_send text,
	ft_pw_policy integer default -1,
	ft_number_tasks integer default 20,
	changed_by text,
	admin_email text,
	email_type text,
	email_type text,	-- smtp || awssdk
	aws_region text,
	smtp_host text,				-- Set if email is enabled
	email_domain text,
	email_user text,
	email_password text,
	email_port integer,
	default_email_content text,
	website text,
	locale text,					-- default locale for the organisation
	timezone text,				-- default timezone for the organisation
	billing_enabled boolean default false,
	server_description text,
	sensitive_data text,			-- Questions that should be stored more securely
	webform text,				-- Webform options
	navbar_color text,
	navbar_text_color text,
	training text,
	limits text,				-- JSON object with resource limits
	limit_type text,			-- alltime or monthly
	refresh_rate integer,
	css text,
	owner integer default 0,				-- User that owns this organisation
	dashboard_region text,
	dashboard_arn text,
	dashboard_session_name text,
	password_strength decimal default 0.0,
	map_source text,							-- default map source for static maps
	password_expiry integer default 0,			-- password expiry in months
	changed_ts TIMESTAMP WITH TIME ZONE
	);
CREATE UNIQUE INDEX idx_organisation ON organisation(name);
ALTER TABLE organisation OWNER TO ws;

DROP SEQUENCE IF EXISTS log_seq CASCADE;
CREATE SEQUENCE log_seq START 1;
ALTER SEQUENCE log_seq OWNER TO ws;

-- Log table
DROP TABLE IF EXISTS log CASCADE;
create TABLE log (
	id integer DEFAULT NEXTVAL('log_seq') CONSTRAINT pk_log PRIMARY KEY,
	log_time TIMESTAMP WITH TIME ZONE,
	s_id integer,
	o_id integer,
	e_id integer,
	user_ident text,
	event text,	
	note text,
	measure int default 0,		-- In the case of translate this would be the number of characters
	server text
	);
CREATE INDEX log_time_key ON log(log_time);
ALTER TABLE log OWNER TO ws;

-- Log table archive
DROP TABLE IF EXISTS log_archive CASCADE;
create TABLE log_archive (
	id integer CONSTRAINT pk_log_archive PRIMARY KEY,
	log_time TIMESTAMP WITH TIME ZONE,
	s_id integer,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	e_id integer,
	user_ident text,
	event text,	
	note text,
	measure int default 0,		-- In the case of translate this would be the number of characters
	server text
	);
ALTER TABLE log_archive OWNER TO ws;

DROP TABLE IF EXISTS project CASCADE;
create TABLE project (
	id INTEGER DEFAULT NEXTVAL('project_seq') CONSTRAINT pk_project PRIMARY KEY,
	o_id INTEGER REFERENCES organisation(id) ON DELETE CASCADE,
	name text,
	description text,
	tasks_only boolean default false,	-- Deprecated - Set per form instead as (hide_on_device). When true only tasks will be downloaded to fieldTask
	imported boolean default false,		-- If set true project was imported from a spreadsheet
	changed_by text,
	changed_ts TIMESTAMP WITH TIME ZONE
	);
CREATE UNIQUE INDEX idx_project ON project(o_id,name);
ALTER TABLE project OWNER TO ws;


DROP TABLE IF EXISTS regions CASCADE;
create TABLE regions (
	id INTEGER DEFAULT NEXTVAL('regions_seq') CONSTRAINT pk_regions PRIMARY KEY,
	o_id INTEGER REFERENCES organisation(id) ON DELETE CASCADE,
	table_name text,
	region_name text,
	geometry_column text
	);
ALTER TABLE regions OWNER TO ws;

DROP SEQUENCE IF EXISTS map_seq CASCADE;
CREATE SEQUENCE map_seq START 1;
ALTER SEQUENCE map_seq OWNER TO ws;

DROP TABLE IF EXISTS map CASCADE;
create TABLE map (
	id INTEGER DEFAULT NEXTVAL('map_seq') CONSTRAINT pk_maps PRIMARY KEY,
	o_id INTEGER REFERENCES organisation(id) ON DELETE CASCADE,
	name text,
	map_type text,			-- mapbox || geojson
	description text,
	config text,
	version integer
	);
ALTER TABLE map OWNER TO ws;

DROP TABLE IF EXISTS data_processing CASCADE;
create TABLE data_processing (
	id INTEGER DEFAULT NEXTVAL('dp_seq') CONSTRAINT pk_dp PRIMARY KEY,
	o_id INTEGER REFERENCES organisation(id) ON DELETE CASCADE,
	name text,
	type text,			-- lqas || manage
	description text,
	config text
	);
ALTER TABLE data_processing OWNER TO ws;

DROP SEQUENCE IF EXISTS users_seq CASCADE;
CREATE SEQUENCE users_seq START 2;
ALTER SEQUENCE users_seq OWNER TO ws;

DROP TABLE IF EXISTS users CASCADE;
CREATE TABLE users (
	id INTEGER DEFAULT NEXTVAL('users_seq') CONSTRAINT pk_users PRIMARY KEY,
	ident text,
	temporary boolean default false,			-- If true will not show in user management page
	imported boolean default false,				-- user was added using a bult import from a spreadsheet
	password text,
	basic_password text,						-- Begin migration to using basic instead of digest for authentication
	password_set timestamp with time zone,		-- Date and time password was set
	api_key text,								-- Key to use when acting as user and calling the API
	app_key text,								-- Key to use when conecting from fieldTask
	realm text,
	name text,
	settings text,				-- User configurable settings
	signature text,
	language varchar(10),
	location text,
	has_gps boolean,
	has_camera boolean,
	has_barcode boolean,
	has_data boolean,
	has_sms boolean,
	phone_number text,
	email text,
	device_id text,
	max_dist_km integer,
	timezone text,
	user_role text,
	current_project_id integer,		-- Set to the last project the user selected
	current_survey_id integer,		-- Set to the last survey the user selected - deprecate
	current_survey_ident text,		-- Set to the last survey the user selected
	current_task_group_id integer,	-- Set to the last task group the user selected
	one_time_password varchar(36),	-- For password reset
	one_time_password_expiry timestamp with time zone,		-- Time and date one time password expires
	one_time_password_sent timestamp with time zone,		-- Time and date one time password was sent
	password_reset boolean default false,	-- Set true if the user has reset their password
	o_id integer REFERENCES organisation(id),
	action_details text,			-- Details of a specific action the user can undertake
	lastalert text,					-- Time last alert sent to the user
	seen boolean,					-- True if the user has aknowledged the alert
	single_submission boolean default false,		-- Only one submission can be accepted by this user
	created timestamp with time zone,
	total_tasks integer default -1
	);
CREATE UNIQUE INDEX idx_users_ident ON users(ident);
ALTER TABLE users OWNER TO ws;

-- Store the final status of a temporary user here in case someone want to know
DROP SEQUENCE IF EXISTS temp_users_final_seq CASCADE;
CREATE SEQUENCE temp_users_final_seq START 1;
ALTER SEQUENCE temp_users_final_seq OWNER TO ws;

DROP TABLE IF EXISTS temp_users_final CASCADE;
CREATE TABLE temp_users_final (
	id INTEGER DEFAULT NEXTVAL('temp_users_final_seq') CONSTRAINT pk_temp_users_final PRIMARY KEY,
	ident text,
	status text,	-- complete, deleted, expired
	created timestamp with time zone
	);
CREATE UNIQUE INDEX idx_temp_users_final_ident ON temp_users_final(ident);
ALTER TABLE temp_users_final OWNER TO ws;

DROP SEQUENCE IF EXISTS dynamic_users_seq CASCADE;
CREATE SEQUENCE dynamic_users_seq START 1;
ALTER SEQUENCE dynamic_users_seq OWNER TO ws;

DROP TABLE IF EXISTS dynamic_users CASCADE;
CREATE TABLE dynamic_users (
	id INTEGER DEFAULT NEXTVAL('dynamic_users_seq') CONSTRAINT pk_dynamic_users PRIMARY KEY,
	u_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
	survey_ident text,
	access_key varchar(41),
	expiry timestamp with time zone
	);
ALTER TABLE dynamic_users OWNER TO ws;

DROP TABLE IF EXISTS groups CASCADE;
create TABLE groups (
	id INTEGER CONSTRAINT pk_groups PRIMARY KEY,
	name text
	);
CREATE UNIQUE INDEX idx_groups_name ON groups(name);
ALTER TABLE groups OWNER TO ws;
	
DROP SEQUENCE IF EXISTS user_group_seq CASCADE;
CREATE SEQUENCE user_group_seq START 1;
ALTER SEQUENCE user_group_seq OWNER TO ws;

DROP TABLE IF EXISTS user_group CASCADE;
create TABLE user_group (
	id INTEGER DEFAULT NEXTVAL('user_group_seq') CONSTRAINT pk_user_group PRIMARY KEY,
	u_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
	g_id INTEGER REFERENCES groups(id) ON DELETE CASCADE
	);
CREATE UNIQUE INDEX idx_user_group ON user_group(u_id,g_id);
ALTER TABLE user_group OWNER TO ws;
	
DROP SEQUENCE IF EXISTS user_project_seq CASCADE;
CREATE SEQUENCE user_project_seq START 1;
ALTER SEQUENCE user_project_seq OWNER TO ws;

DROP TABLE IF EXISTS user_project CASCADE;
create TABLE user_project (
	id INTEGER DEFAULT NEXTVAL('user_project_seq') CONSTRAINT pk_user_project PRIMARY KEY,
	u_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
	p_id INTEGER REFERENCES project(id) ON DELETE CASCADE
	);
CREATE INDEX idx_up_u ON user_project(u_id);
ALTER TABLE user_project OWNER TO ws;

DROP SEQUENCE IF EXISTS user_organisation_seq CASCADE;
CREATE SEQUENCE user_organisation_seq START 1;
ALTER SEQUENCE user_organisation_seq OWNER TO ws;

DROP TABLE IF EXISTS user_organisation CASCADE;
create TABLE user_organisation (
	id INTEGER DEFAULT NEXTVAL('user_organisation_seq') CONSTRAINT pk_user_organisation PRIMARY KEY,
	u_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
	o_id INTEGER REFERENCES organisation(id) ON DELETE CASCADE,
	settings text
	);
CREATE UNIQUE INDEX idx_user_organisation ON user_organisation(u_id,o_id);
ALTER TABLE user_organisation OWNER TO ws;

DROP SEQUENCE IF EXISTS role_seq CASCADE;
CREATE SEQUENCE role_seq START 1;
ALTER TABLE role_seq OWNER TO ws;

DROP TABLE IF EXISTS public.role CASCADE;
CREATE TABLE public.role (
	id integer DEFAULT nextval('role_seq') NOT NULL PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	name text,
	description text,
	imported boolean default false,
	changed_by text,
	changed_ts TIMESTAMP WITH TIME ZONE
);
ALTER TABLE public.role OWNER TO ws;
CREATE UNIQUE INDEX role_name_index ON public.role(o_id, name);

DROP SEQUENCE IF EXISTS user_role_seq CASCADE;
CREATE SEQUENCE user_role_seq START 1;
ALTER SEQUENCE user_role_seq OWNER TO ws;

DROP TABLE IF EXISTS user_role CASCADE;
create TABLE user_role (
	id INTEGER DEFAULT NEXTVAL('user_role_seq') CONSTRAINT pk_user_role PRIMARY KEY,
	u_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
	r_id INTEGER REFERENCES role(id) ON DELETE CASCADE
	);
ALTER TABLE user_role OWNER TO ws;

-- Create an administrator and set up default values
insert into enterprise(id, name, changed_by, changed_ts) values(1, 'Default', '', now());
-- Create an organisation with communal ownership
insert into organisation(id, name, e_id, can_edit, owner) values(1, 'Smap', 1, 'true', 0);

insert into users (id, ident, realm, password, basic_password, o_id, name, email) 
	values (1, 'admin', 'smap', '9f12895fe9898cc306c45c9d3fcbc3d6', '{SHA}0DPiKuNIrrVmD8IUCuw1hQxNqZc=', 1, 'Administrator', '');

insert into groups(id,name) values(1,'admin');
insert into groups(id,name) values(2,'analyst');
insert into groups(id,name) values(3,'enum');
insert into groups(id,name) values(4,'org admin');
insert into groups(id,name) values(5,'manage');
insert into groups(id,name) values(6,'security');
insert into groups(id,name) values(7,'view data');
insert into groups(id,name) values(8,'enterprise admin');
insert into groups(id,name) values(9,'server owner');
insert into groups(id,name) values(10,'view own data');
insert into groups(id,name) values(11,'manage tasks');
insert into groups(id,name) values(12,'dashboard');
--insert into groups(id,name) values(13,'links');
insert into groups(id,name) values(14,'console admin');

insert into user_group (u_id, g_id) values (1, 1);
insert into user_group (u_id, g_id) values (1, 2);
insert into user_group (u_id, g_id) values (1, 3);
insert into user_group (u_id, g_id) values (1, 4);
insert into user_group (u_id, g_id) values (1, 5);
insert into user_group (u_id, g_id) values (1, 6);
insert into user_group (u_id, g_id) values (1, 7);
insert into user_group (u_id, g_id) values (1, 8);
insert into user_group (u_id, g_id) values (1, 9);
insert into user_group (u_id, g_id) values (1, 10);
insert into user_group (u_id, g_id) values (1, 11);
insert into user_group (u_id, g_id) values (1, 12);
--insert into user_group (u_id, g_id) values (1, 13);

insert into project (id, o_id, name) values (1, 1, 'A project');

insert into user_project (u_id, p_id) values (1 , 1);

-- Monitoring tables
DROP TABLE IF EXISTS upload_event CASCADE;
CREATE TABLE upload_event (
	ue_id INTEGER DEFAULT NEXTVAL('ue_seq') CONSTRAINT pk_upload_event PRIMARY KEY,
	results_db_applied boolean default false,
	s_id INTEGER,
	ident text,	-- Identifier used by survey
	p_id integer,
	o_id integer default 0,	-- Record organisation at time of upload for billing purposes
	e_id integer default 0,	-- Record enterprise for billing
	upload_time TIMESTAMP WITH TIME ZONE,
	user_name text,
	file_name text,
	file_path text,
	audit_file_path text,
	survey_name text,
	imei text,
	orig_survey_ident text,
	update_id varchar(41),
	assignment_id INTEGER,
	instanceid varchar(41),
	status varchar(10),
	reason text,
	db_status text,						-- status of application to database
	db_reason text,
	location text,
	form_status text,
	notifications_applied boolean,		-- Set after notifications are sent
	incomplete boolean default false,	-- odk will set this if sending attachments in multiple posts
	server_name text,  		-- Stores the server used to upload the results.  The url's of all attachments will reference this address
	survey_notes text,		-- Notes added during completion of the task
	location_trigger text,	-- The trigger for the completion of the task
	start_time timestamp with time zone,
	end_time timestamp with time zone,
	scheduled_start timestamp with time zone,
	processed_time timestamp with time zone,
	instance_name text,
	temporary_user boolean default false,
	queue_name text,
	queued boolean default false,
	restore boolean default false,
	submission_type text,	-- SMS or Form (default)
	payload text			-- SMS details, in future XML submission details
	);
create index idx_ue_ident on upload_event(user_name);
create index idx_ue_applied on upload_event (status, incomplete, results_db_applied);
create index idx_ue_upload_time on upload_event (upload_time);
create index idx_ue_processed_time on upload_event (processed_time);
CREATE index ue_survey_ident ON upload_event(ident);
CREATE INDEX idx_ue_p_id ON upload_event(p_id);
ALTER TABLE upload_event OWNER TO ws;

DROP TABLE IF EXISTS option CASCADE;
DROP TABLE IF EXISTS question CASCADE;
DROP TABLE IF EXISTS ssc CASCADE;
DROP TABLE IF EXISTS form CASCADE;
DROP TABLE IF EXISTS survey CASCADE;
DROP TABLE IF EXISTS survey_change CASCADE;

DROP TABLE IF EXISTS survey CASCADE;
CREATE TABLE survey (
	s_id INTEGER DEFAULT NEXTVAL('s_seq') CONSTRAINT pk_survey PRIMARY KEY,
	ident text,										-- identifier used by survey clients
	version integer,								-- Version of the survey
	p_id INTEGER REFERENCES project(id),			-- Project id
	blocked boolean default false,					-- Blocked indicator, no uploads accepted if true
	deleted boolean default false,					-- Soft delete indicator
	display_name text not null,						-- This is the name. The old name column has beeen removed
	def_lang text,
	task_file boolean,								-- allow loading of tasks from a file
	timing_data boolean,								-- collect timing data on the phone
	audit_location_data boolean,						-- collect location data on the phone
	track_changes boolean,							-- collect location data on the phone
	class text,
	model text,										-- JSON model of the survey for thingsat
	manifest text,									-- JSON set of manifest information for the survey
	instance_name text,								-- The rule for naming a survey instance form its data
	last_updated_time DATE,
	managed_id integer,								-- Identifier of configuration for managing records
	auto_updates text,								-- Contains the auto updates that need to be applied for this survey
	loaded_from_xls boolean default false,			-- Set true if the survey was initially loaded from an XLS Form
	hrk text,										-- human readable key
	key_policy text,									-- Whether to discard, append, replace or merge duplicates of the HRK
	based_on text,									-- Survey and form this survey was based on
	group_survey_id integer default 0,				-- deprecate
	group_survey_ident text,						-- common ident linking grouped surveys
	pulldata text,									-- Settings to customise pulling data from another survey into a csv file
	exclude_empty boolean default false,			-- True if reports should not include empty data
	compress_pdf boolean default false,				-- True if PDFs of data should have images compressed
	created timestamp with time zone,				-- Date / Time the survey was created
	meta text,										-- Meta items to collect with this survey
	public_link text,
	hidden boolean default false,					-- Updated when a form is replaced
	original_ident text,								-- Updated when a form is replaced
	hide_on_device boolean,							-- Used when forms are launched from other forms or as tasks to hide the ad-hoc form
	search_local_data boolean,						-- Use when local unsubmitted data should be referenced
	pdf_template text,
	default_logo text,
	data_survey boolean default true,
	oversight_survey boolean default true,
	read_only_survey boolean default false,
	my_reference_data boolean default false,
	auto_translate boolean default false
	);
ALTER TABLE survey OWNER TO ws;
DROP INDEX IF EXISTS SurveyDisplayName;
DROP INDEX IF EXISTS SurveyKey;
CREATE UNIQUE INDEX SurveyKey ON survey(ident);
CREATE INDEX group_survey_ident_idx ON survey(group_survey_ident);

DROP TABLE IF EXISTS survey_change CASCADE;
CREATE TABLE survey_change (
	c_id integer DEFAULT NEXTVAL('sc_seq') CONSTRAINT pk_survey_changes PRIMARY KEY,
	s_id integer REFERENCES survey ON DELETE CASCADE,		-- Survey containing this version
	version integer,								-- Version of survey with these changes
	changes text,								-- Changes as json object
	apply_results boolean default false,			-- Set to true if the results tables need to be updated	
	success boolean default false,				-- Set true if the update was a success
	msg text,									-- Error messages
	user_id integer,								-- Person who made the changes
	visible boolean default true,				-- set false if the change should not be displayed 				
	updated_time TIMESTAMP WITH TIME ZONE		-- Time and date of change
	);
create index survey_change_s_id on survey_change(s_id);
ALTER TABLE survey_change OWNER TO ws;

DROP TABLE IF EXISTS survey_template CASCADE;
CREATE TABLE survey_template (
	t_id integer DEFAULT NEXTVAL('st_seq') CONSTRAINT pk_survey_template PRIMARY KEY,
	ident text,									-- Survey containing this version
	name text,
	filepath text,
	not_available boolean default false,		-- Set to true if the template is not available for selection
	default_template boolean default false,
	template_type text,							-- pdf || word
	rule text,									-- rule for automatically selecting a PDF template
	user_id integer,							-- Person who made the changes				
	updated_time TIMESTAMP WITH TIME ZONE		-- Time and date of change
	);
ALTER TABLE survey_template OWNER TO ws;

-- record events on data records by HRK or instanceid if HRK not set
-- Events include
--     submission (reference data in data table keyed on instanceid)
--     managed form update
--     Data cleaning (reference data in .......
--     Task status change  (reference data in tasks)
--     Task completed (reference data in tasks,   submission)
--     Message sent (reference data in messages and notifications)
--     Record assignment status changes
DROP TABLE IF EXISTS record_event CASCADE;
CREATE TABLE record_event (
	id integer DEFAULT NEXTVAL('re_seq') CONSTRAINT pk_record_changes PRIMARY KEY,
	table_name text,								-- Main table containing unique key	
	key text,									-- HRK of change or notification
	instanceid text,								-- instance of change or notification	
	event text,									-- created || change || task || reminder || deleted
	status text,									-- Status of event - determines how it is displayed
	changes text,								-- Details of the change as json object	
	task text,									-- Details of task changes as json object
	message text,								-- Details of message changes as json object
	notification text,							-- Details of notification as json object
	description text,
	success boolean default false,				-- Set true of the event was a success
	msg text,									-- Error messages
	changed_by integer,							-- Person who made a change	
	change_survey text,							-- Survey ident that applied the change
	change_survey_version integer,				-- Survey version that made the change	
	assignment_id integer,						-- Record if this is an task event	
	task_id integer,								-- Record if this is an task event	
	event_time TIMESTAMP WITH TIME ZONE			-- Time and date of event
	);
CREATE INDEX record_event_key ON record_event(key);
create index idx_record_event_table_name on record_event (table_name);
ALTER TABLE record_event OWNER TO ws;


DROP TABLE IF EXISTS custom_report_type CASCADE;
CREATE TABLE custom_report_type (
	id integer DEFAULT NEXTVAL('custom_report_type_seq') CONSTRAINT pk_custom_report_type PRIMARY KEY,
	name text,
	config text								-- Custom report columns as json object
	);
ALTER TABLE custom_report_type OWNER TO ws;

DROP TABLE IF EXISTS custom_report CASCADE;
CREATE TABLE custom_report (
	id integer DEFAULT NEXTVAL('custom_report_seq') CONSTRAINT pk_custom_report PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	p_id integer REFERENCES project(id) ON DELETE CASCADE,
	survey_ident text REFERENCES survey(ident) ON DELETE CASCADE,
	name text,
	type_id integer REFERENCES custom_report_type(id) ON DELETE CASCADE,
	config text	
	);
ALTER TABLE custom_report OWNER TO ws;
CREATE UNIQUE INDEX custom_report_name ON custom_report(p_id, name);

-- table name is used by "results databases" to store result data for this form
CREATE TABLE form (
	f_id INTEGER DEFAULT NEXTVAL('f_seq') CONSTRAINT pk_form PRIMARY KEY,
	s_id INTEGER REFERENCES survey ON DELETE CASCADE,
	name text,
	label text,
	table_name text,
	parentForm integer not null default 0,
	parentQuestion integer not null default 0,
	repeats text,
	path text,
	form_index int default -1,					-- Temporary data used by the online editor
	reference boolean default false,
	merge boolean default false,
	replace boolean default false,
	append boolean default false
	);
CREATE INDEX form_s_id ON form(s_id);
create index form_table_name on form(table_name);
ALTER TABLE form OWNER TO ws;

DROP TABLE IF EXISTS listname CASCADE;
CREATE TABLE listname (
	l_id INTEGER DEFAULT NEXTVAL('l_seq') CONSTRAINT pk_listname PRIMARY KEY,
	s_id integer references survey on delete cascade, 
	name text
	);
ALTER TABLE listname OWNER TO ws;
CREATE UNIQUE INDEX listname_name ON listname(s_id, name);

-- q_itext references the text string in the translations table
DROP TABLE IF EXISTS question CASCADE;
CREATE TABLE question (
	q_id INTEGER DEFAULT NEXTVAL('q_seq') CONSTRAINT pk_question PRIMARY KEY,
	f_id INTEGER REFERENCES form ON DELETE CASCADE,
	l_id integer default 0,
	style_id integer default 0,
	seq INTEGER,
	qName text NOT NULL,						-- Name that conforms to ODK restrictions
	column_name text,							-- Name of column in results table, qname with postgres constraints
	display_name text,							-- Name displayed to user
	column_name_applied boolean default false,	-- If set true column name has been added to results
	qType text,									-- Question type, select, begin repeat (also int, decimal for legacy reasons)
	dataType text,								-- Decimal, int etc
	question text,
	qtext_id text,
	defaultAnswer text,
	set_value text,
	info text,
	infotext_id text,
	visible BOOLEAN default true,
	source text,
	source_param text,
	readonly BOOLEAN default false,
	readonly_expression text,
	mandatory BOOLEAN default false,
	relevant text,
	calculate text,
	server_calculate text,				-- JSON
	qConstraint text,
	constraint_msg text,
	required_expression text,
	appearance text,
	parameters text,
	enabled BOOLEAN default true,
	path text,
	nodeset text,						-- the xpath to an itemset containing choices, includes filter defn
	nodeset_value text,					-- name of value column for choice list when stored as an itemset
	nodeset_label text,					-- name of label column for choice list when stored as an itemset
	cascade_instance text,				-- Identical to list name (deprecate)
	list_name text,						-- Name of a set of options common across multiple questions
	published boolean default false,		-- Set true when a survey has been published for data collection
										--  Once a survey has been published there are constraints on the
										--  changes that can be applied to question definitions
	soft_deleted boolean default false,	-- Set true if a question has been deleted and has also been published
										-- If the question hasn't been published then it can be removed from the survey
	autoplay text,
	accuracy text,						-- gps accuracy at which a reading is automatically accepted
	linked_target text,					-- Id of a survey whose hrk is populated here
	compressed boolean default false,	-- Will put all answers to select multiples into a single column
	external_choices text,				-- Set to yes if choices are external
	external_table text,				-- The table containing the external choices
	intent text,						-- ODK intent attribute
	flash integer,						-- Interval in literacy test before question flashes
	trigger text
	);
ALTER TABLE question OWNER TO ws;
CREATE INDEX qtext_id_sequence ON question(qtext_id);
CREATE INDEX infotext_id_sequence ON question(infotext_id);
CREATE UNIQUE INDEX qname_index ON question(f_id,qname) where soft_deleted = 'false';
CREATE INDEX q_f_id ON question(f_id);
CREATE INDEX idx_question_param ON question (parameters) WHERE (parameters is not null);
CREATE INDEX question_column_name_key ON question(column_name);
create index question_l_id_idx on question(l_id);
	
DROP TABLE IF EXISTS option CASCADE;
CREATE TABLE option (
	o_id INTEGER DEFAULT NEXTVAL('o_seq') CONSTRAINT pk_option PRIMARY KEY,
	q_id integer,
	l_id integer references listname on delete cascade,
	seq INTEGER,
	label text,
	label_id text,
	oValue text,
	column_name text,
	display_name text,
	selected BOOLEAN,
	cascade_filters text,
	published boolean default false,
	externalfile boolean default false
	);
ALTER TABLE option OWNER TO ws;
CREATE INDEX label_id_sequence ON option(label_id);
CREATE index o_l_id ON option(l_id);

-- Server side calculates
DROP TABLE IF EXISTS ssc;
CREATE TABLE ssc (
	id INTEGER DEFAULT NEXTVAL('ssc_seq') CONSTRAINT pk_ssc PRIMARY KEY,
	s_id INTEGER REFERENCES survey ON DELETE CASCADE,
	f_id INTEGER,
	name text,
	type text,
	units varchar(20),
	function text,
	parameters text
	);
ALTER TABLE ssc OWNER TO ws;
CREATE UNIQUE INDEX SscName ON ssc(s_id, name);

-- Survey Forwarding (All notifications are stored in here, forward is a legacy name)
DROP TABLE IF EXISTS forward;
CREATE TABLE forward (
	id INTEGER DEFAULT NEXTVAL('forward_seq') CONSTRAINT pk_forward PRIMARY KEY,
	s_id INTEGER,
	p_id integer,
	name text,
	enabled boolean,
	filter text,
	trigger text,						-- cm_alert || task_reminder || console_update || submission || periodic
	target text,
	remote_s_id text,
	remote_s_name text,
	remote_user text,
	remote_password text,
	remote_host text,
	notify_details text,				-- JSON string
	tg_id integer default 0,			-- Reminder notifications
	period text,						-- Task Reminder notifications
	update_survey text references survey(ident) on delete cascade,
	update_question text,				-- Update notifications
	update_value text,
	alert_id integer,					-- Set where the source is a case management reminder
	periodic_time time,					-- The time of day a periodic trigger is fired
	periodic_period text,				-- day || week || month || year
	periodic_day_of_week integer,		-- 0 to 6, Sunday to Saturday for weekly reports
	periodic_day_of_month integer,		-- Day of the month for monthly and yearly reports
	periodic_local_day_of_month integer,-- Original local day of the month as this cannot reliably be recreated from utc value
	periodic_month integer,				-- Month used for yearly reports
	periodic_local_month integer,		-- Original local month as this cannot reliably be recreated from utc value
	r_id integer,						-- report id
	updated boolean						-- Set true if the notification has been changed
	);
ALTER TABLE forward OWNER TO ws;
CREATE UNIQUE INDEX ForwardDest ON forward(s_id, remote_s_id, remote_host);

-- Log of all sent notifications (except for forwards which are recorded by the forward subscriber)
DROP TABLE IF EXISTS notification_log;
CREATE TABLE public.notification_log (
	id integer default nextval('notification_log_seq') not null PRIMARY KEY,
	o_id integer,
	p_id integer,
	s_id integer,
	notify_details text,
	status text,	
	status_details text,
	event_time TIMESTAMP WITH TIME ZONE,
	message_id integer,			-- Identifier from the message queue that triggered this notification
	type text					-- Notification type, submission || reminder || task
	);
ALTER TABLE notification_log OWNER TO ws;


-- form can be long, short, image, audio, video
DROP TABLE IF EXISTS translation;
CREATE TABLE translation (
	t_id INTEGER DEFAULT NEXTVAL('t_seq') CONSTRAINT pk_translation PRIMARY KEY,
	s_id INTEGER REFERENCES survey ON DELETE CASCADE,
	language text,
	text_id text,
	type text,
	value text,
	external boolean default false
	);
ALTER TABLE translation OWNER TO ws;
CREATE UNIQUE INDEX translation_index ON translation(s_id, language, text_id, type);
CREATE INDEX text_id_sequence ON translation(text_id);
CREATE INDEX language_sequence ON translation(language);
CREATE INDEX t_s_id_sequence ON translation(s_id);


DROP TABLE IF EXISTS language;
CREATE TABLE language (
	id INTEGER DEFAULT NEXTVAL('l_seq') CONSTRAINT pk_language PRIMARY KEY,
	s_id INTEGER REFERENCES survey ON DELETE CASCADE,
	seq int,
	language text,
	code text,		-- 2 character (or more) language code
	rtl boolean		-- Set true for right to lft languages
	);
ALTER TABLE language OWNER TO ws;

-- Tables to manage settings

DROP SEQUENCE IF EXISTS ds_seq CASCADE;
CREATE SEQUENCE ds_seq START 1;
ALTER SEQUENCE ds_seq OWNER TO ws;

DROP TABLE IF EXISTS dashboard_settings CASCADE;
CREATE TABLE dashboard_settings (
	ds_id INTEGER DEFAULT NEXTVAL('ds_seq') CONSTRAINT pk_dashboard_settings PRIMARY KEY,
	ds_user_ident text,
	ds_seq INTEGER,
	ds_state text,
	ds_title text,
	ds_s_name text,
	ds_s_id integer,
	ds_type text,
	ds_region text,
	ds_lang text,
	ds_qname text,      -- replaces ds_q_id
	ds_q_is_calc boolean default false,
	ds_date_question_name text,   -- replaces ds_date_question_id
	ds_question text,
	ds_fn text,
	ds_table text,
	ds_key_words text,
	ds_q1_function text,
	ds_group_question_name text,   -- replaces ds_group_question_id
	ds_group_question_text text,
	ds_group_type text,
	ds_layer_id integer,
	ds_time_group text,
	ds_from_date date,
	ds_to_date date,
	ds_filter text,
	ds_advanced_filter text,
	ds_subject_type text,
	ds_u_id integer,
	ds_inc_ro boolean default false,
	ds_geom_questions text,
	ds_selected_geom_question text
	);
alter table dashboard_settings add constraint ds_user_ident FOREIGN KEY (ds_user_ident)
	REFERENCES users (ident) MATCH SIMPLE
	ON UPDATE NO ACTION ON DELETE CASCADE;
ALTER TABLE dashboard_settings OWNER TO ws;


DROP SEQUENCE IF EXISTS set_seq CASCADE;
CREATE SEQUENCE set_seq START 1;
ALTER SEQUENCE set_seq OWNER TO ws;

DROP TABLE IF EXISTS general_settings CASCADE;
CREATE TABLE general_settings (
	id INTEGER DEFAULT NEXTVAL('set_seq') CONSTRAINT pk_settings PRIMARY KEY,
	u_id integer REFERENCES users(id) ON DELETE CASCADE,
	s_id integer REFERENCES survey(s_id) ON DELETE CASCADE,
	key text,			-- Identifies type of setting such as "mf" managed forms
	settings text		-- JSON

	);
ALTER TABLE general_settings OWNER TO ws;


--- Task Management -----------------------------------
-- Cleanup old tables  
DROP TABLE IF EXISTS public.tasks CASCADE;
DROP TABLE IF EXISTS public.task_rejected CASCADE;
DROP TABLE IF EXISTS public.assignments CASCADE;
DROP TABLE IF EXISTS public.task_group CASCADE;

DROP SEQUENCE IF EXISTS assignment_id_seq CASCADE;
CREATE SEQUENCE assignment_id_seq START 1;
ALTER TABLE assignment_id_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS task_id_seq CASCADE;
CREATE SEQUENCE task_id_seq START 1;
ALTER TABLE task_id_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS task_rejected_seq CASCADE;
CREATE SEQUENCE task_rejected_seq START 1;
ALTER TABLE task_rejected_seq OWNER TO ws;

DROP SEQUENCE IF EXISTS task_group_id_seq CASCADE;
CREATE SEQUENCE task_group_id_seq START 1;
ALTER TABLE task_group_id_seq OWNER TO ws;

CREATE TABLE public.task_group (
	tg_id integer NOT NULL DEFAULT nextval('task_group_id_seq') PRIMARY KEY,
	name text,
	p_id integer,
    address_params text,
    rule text,					-- The criteria for adding a new task to this group (JSON)
    source_s_id integer,			-- The source survey id for quick lookup from notifications engine
    target_s_id integer,
    email_details text,
    dl_dist integer,				-- Download distance, same value is in the rule, needed here for selects
    complete_all boolean	,		-- Set true if all assignements of a task must be completed
    assign_auto boolean		-- Set true if the user can assign themselves to an unassigned task
);
ALTER TABLE public.task_group OWNER TO ws;

CREATE TABLE public.tasks (
	id integer DEFAULT nextval('task_id_seq') NOT NULL PRIMARY KEY,
	tg_id integer,
	tg_name text,
	p_id integer,
	p_name text,
	title text,
	url text,
	survey_ident text,	
	survey_name text,
	created_at timestamp with time zone,
	schedule_at timestamp with time zone,
	schedule_finish timestamp with time zone,
	deleted_at timestamp with time zone,
	address text,
	update_id text,				-- Record to update
	initial_data_source text,	-- none || survey || task
	initial_data text,			-- Contains InstanceJson of data if data source is task
	repeat boolean,
	repeat_count integer default 0,
	guidance text,
	location_trigger text,
	location_group text,
	location_name text,
	deleted boolean default false,
	complete_all boolean default false,	-- Set true if all assignments associated to this task need to be completed
	assign_auto boolean default false,	-- Set true if users can assign themselvs to this task
	show_dist integer						-- Distance in meters at which task will be downloaded
);
SELECT AddGeometryColumn('tasks', 'geo_point', 4326, 'POINT', 2);
SELECT AddGeometryColumn('tasks', 'geo_point_actual', 4326, 'POINT', 2);
CREATE INDEX task_task_group ON tasks(tg_id);
create index idx_tasks_del_auto on tasks (deleted, assign_auto);
create index tasks_survey_idx on tasks(survey_ident);
ALTER TABLE public.tasks OWNER TO ws;

CREATE TABLE public.locations (
	id integer DEFAULT nextval('location_seq') NOT NULL PRIMARY KEY,
	o_id integer REFERENCES organisation ON DELETE CASCADE,
	locn_group text,
	locn_type text,
	name text,
	uid text
);
SELECT AddGeometryColumn('locations', 'the_geom', 4326, 'POINT', 2);
CREATE UNIQUE INDEX location_index ON locations(locn_group, name);
ALTER TABLE public.locations OWNER TO ws;

CREATE TABLE public.assignments (
	id integer NOT NULL DEFAULT nextval('assignment_id_seq') PRIMARY KEY,
	assignee integer,
	assignee_name text,			-- Name of assigned person
	email text,					-- Email to send the task to
	status text NOT NULL,		-- Current status: accepted || rejected || submitted || deleted || unsent || unsubscribed
	comment text,
	task_id integer REFERENCES tasks(id) ON DELETE CASCADE,
	action_link text,			-- Used with single shot web form tasks
	assigned_date timestamp with time zone,
	completed_date timestamp with time zone,		-- Date of submitted || rejected
	cancelled_date timestamp with time zone,
	deleted_date timestamp with time zone
);
CREATE INDEX assignments_status ON assignments(status);
create index idx_assignments_task_id on assignments (task_id);
create index assignments_assignee on assignments(assignee);
ALTER TABLE public.assignments OWNER TO ws;

CREATE TABLE public.task_rejected (
	id integer DEFAULT nextval('task_rejected_seq') NOT NULL PRIMARY KEY,
	a_id integer REFERENCES assignments(id) ON DELETE CASCADE,    -- assignment id
	ident text,		 -- user identifier
	rejected_at timestamp with time zone
);
CREATE UNIQUE INDEX taskRejected ON task_rejected(a_id, ident);
ALTER TABLE public.task_rejected OWNER TO ws;

-- Record sending of notification reminders
DROP TABLE IF EXISTS reminder;
CREATE TABLE reminder (
	id integer DEFAULT NEXTVAL('reminder_seq') CONSTRAINT pk_reminder PRIMARY KEY,
	n_id integer references forward(id) ON DELETE CASCADE,
	a_id integer references assignments(id) ON DELETE CASCADE,
	reminder_date timestamp with time zone
	);
ALTER TABLE reminder OWNER TO ws;

-- Table to manage state of user downloads of forms
DROP SEQUENCE IF EXISTS form_downloads_id_seq CASCADE;
CREATE SEQUENCE form_downloads_id_seq START 1;
ALTER TABLE form_downloads_id_seq OWNER TO ws;

DROP TABLE IF EXISTS public.form_downloads CASCADE;
CREATE TABLE public.form_downloads (
	id integer DEFAULT nextval('form_downloads_id_seq') NOT NULL PRIMARY KEY,
	u_id integer REFERENCES users(id) ON DELETE CASCADE,
	form_ident text,
	form_version text,
	device_id text,
	updated_time TIMESTAMP WITH TIME ZONE
);
create index form_downloads_form on form_downloads(form_ident);
ALTER TABLE public.form_downloads OWNER TO ws;

-- Tables to manage task completion and user location
-- Deprecate - use tasks
DROP SEQUENCE IF EXISTS task_completion_id_seq CASCADE;
CREATE SEQUENCE task_completion_id_seq START 1;
ALTER TABLE task_completion_id_seq OWNER TO ws;

-- deprecate use tasks
DROP TABLE IF EXISTS public.task_completion CASCADE;
CREATE TABLE public.task_completion (
	id integer DEFAULT nextval('task_completion_id_seq') NOT NULL PRIMARY KEY,
	u_id integer REFERENCES users(id) ON DELETE CASCADE,
	form_ident text,
	form_version int,
	device_id text,
	uuid text,		-- Unique identifier for the results
	completion_time TIMESTAMP WITH TIME ZONE
);
SELECT AddGeometryColumn('task_completion', 'the_geom', 4326, 'POINT', 2);
ALTER TABLE public.task_completion OWNER TO ws;

DROP SEQUENCE IF EXISTS user_trail_id_seq CASCADE;
CREATE SEQUENCE user_trail_id_seq START 1;
ALTER TABLE user_trail_id_seq OWNER TO ws;

DROP TABLE IF EXISTS public.user_trail CASCADE;
CREATE TABLE public.user_trail (
	id integer DEFAULT nextval('user_trail_id_seq') NOT NULL PRIMARY KEY,
	u_id integer REFERENCES users(id) ON DELETE CASCADE,
	device_id text,
	event_time TIMESTAMP WITH TIME ZONE
);
SELECT AddGeometryColumn('user_trail', 'the_geom', 4326, 'POINT', 2);
create index idx_user_trail_u_id on user_trail(u_id);
ALTER TABLE public.user_trail OWNER TO ws;

DROP SEQUENCE IF EXISTS log_report_seq CASCADE;
CREATE SEQUENCE log_report_seq START 1;
ALTER TABLE log_report_seq OWNER TO ws;

DROP TABLE IF EXISTS public.log_report CASCADE;
CREATE TABLE public.log_report (
	id integer DEFAULT nextval('log_report_seq') NOT NULL PRIMARY KEY,
	u_id integer REFERENCES users(id) ON DELETE CASCADE,
	device_id text,
	report text,
	upload_time TIMESTAMP WITH TIME ZONE
);
ALTER TABLE public.log_report OWNER TO ws;

DROP SEQUENCE IF EXISTS linked_forms_seq CASCADE;
CREATE SEQUENCE linked_forms_seq START 1;
ALTER TABLE linked_forms_seq OWNER TO ws;

DROP TABLE IF EXISTS public.linked_forms CASCADE;
CREATE TABLE public.linked_forms (
	id integer DEFAULT nextval('linked_forms_seq') NOT NULL PRIMARY KEY,
	Linked_s_id integer REFERENCES survey(s_id) ON DELETE CASCADE,
	linked_table text,			-- deprecate
	number_records integer,		-- deprecate
	linker_s_id integer REFERENCES survey(s_id) ON DELETE CASCADE,
	link_file text,
	user_ident text,
	download_time TIMESTAMP WITH TIME ZONE
);
ALTER TABLE public.linked_forms OWNER TO ws;

DROP SEQUENCE IF EXISTS form_dependencies_seq CASCADE;
CREATE SEQUENCE form_dependencies_seq START 1;
ALTER TABLE form_dependencies_seq OWNER TO ws;

DROP TABLE IF EXISTS public.form_dependencies CASCADE;
CREATE TABLE public.form_dependencies (
	id integer DEFAULT nextval('form_dependencies_seq') NOT NULL PRIMARY KEY,
	Linked_s_id integer REFERENCES survey(s_id) ON DELETE CASCADE,
	linker_s_id integer REFERENCES survey(s_id) ON DELETE CASCADE
);
ALTER TABLE public.form_dependencies OWNER TO ws;

DROP SEQUENCE IF EXISTS survey_role_seq CASCADE;
CREATE SEQUENCE survey_role_seq START 1;
ALTER SEQUENCE survey_role_seq OWNER TO ws;

DROP TABLE IF EXISTS survey_role CASCADE;
create TABLE survey_role (
	id integer DEFAULT NEXTVAL('survey_role_seq') CONSTRAINT pk_survey_role PRIMARY KEY,
	survey_ident text REFERENCES survey(ident) ON DELETE CASCADE,
	group_survey_ident text,
	r_id integer REFERENCES role(id) ON DELETE CASCADE,
	enabled boolean,
	column_filter text,
	row_filter text
	);
ALTER TABLE survey_role OWNER TO ws;
CREATE UNIQUE INDEX survey_role_ident_index ON public.survey_role(survey_ident, r_id);

DROP SEQUENCE IF EXISTS alert_seq CASCADE;
CREATE SEQUENCE alert_seq START 1;
ALTER SEQUENCE alert_seq OWNER TO ws;

DROP TABLE IF EXISTS alert CASCADE;
create TABLE alert (
	id integer DEFAULT NEXTVAL('alert_seq') CONSTRAINT pk_alert PRIMARY KEY,
	u_id integer REFERENCES users(id) ON DELETE CASCADE,
	status varchar(10),
	priority integer,
	updated_time TIMESTAMP WITH TIME ZONE,
	created_time TIMESTAMP WITH TIME ZONE,
	link text,
	message text,
	s_id integer,	-- Survey Id that the alert applies to
	m_id integer,	-- Managed form id that the alert applies to
	prikey integer	-- Primary key of survey for which the alert applies
);
ALTER TABLE alert OWNER TO ws;

DROP SEQUENCE IF EXISTS pending_message_seq CASCADE;
CREATE SEQUENCE pending_message_seq START 1;
ALTER SEQUENCE pending_message_seq OWNER TO ws;

DROP TABLE IF EXISTS pending_message CASCADE;
create TABLE pending_message (
	id integer DEFAULT NEXTVAL('pending_message_seq') CONSTRAINT pk_pending_message PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	email text,
	topic text,
	description text,
	data text,
	message_id integer,
	created_time TIMESTAMP WITH TIME ZONE,
	processed_time TIMESTAMP WITH TIME ZONE,
	status text
);
CREATE index pending_message_email ON pending_message(email);
ALTER TABLE pending_message OWNER TO ws;

DROP SEQUENCE IF EXISTS message_seq CASCADE;
CREATE SEQUENCE message_seq START 1;
ALTER SEQUENCE message_seq OWNER TO ws;

-- Very draft definition of Smap messaging
-- topic is
--    and email for direct (hack) notifications
--    form for a change to a form

DROP TABLE IF EXISTS message CASCADE;
create TABLE message (
	id integer DEFAULT NEXTVAL('message_seq') CONSTRAINT pk_message PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	topic text,
	description text,
	data text,
	outbound boolean,
	created_time TIMESTAMP WITH TIME ZONE,
	processed_time TIMESTAMP WITH TIME ZONE,
	status text,
	queue_name text,
	queued boolean default false
);
CREATE index msg_outbound ON message(outbound);
CREATE index msg_processing_time ON message(processed_time);
ALTER TABLE message OWNER TO ws;

DROP SEQUENCE IF EXISTS custom_query_seq CASCADE;
CREATE SEQUENCE custom_query_seq START 1;
ALTER SEQUENCE custom_query_seq OWNER TO ws;

DROP TABLE IF EXISTS custom_query CASCADE;
create TABLE custom_query (
	id integer DEFAULT NEXTVAL('custom_query_seq') CONSTRAINT pk_custom_query PRIMARY KEY,
	u_id integer REFERENCES users(id) ON DELETE CASCADE,
	name text,
	query text
	
);
ALTER TABLE custom_query OWNER TO ws;

DROP SEQUENCE IF EXISTS report_seq CASCADE;
CREATE SEQUENCE report_seq START 1;
ALTER SEQUENCE report_seq OWNER TO ws;

DROP TABLE IF EXISTS report CASCADE;
create TABLE report (
	id INTEGER DEFAULT NEXTVAL('report_seq') CONSTRAINT pk_report PRIMARY KEY,
	o_id integer REFERENCES organisation(id) ON DELETE CASCADE,
	name text,				-- Report Name
	s_id int	,				-- Replace with many to many relationship
	url text
	);
ALTER TABLE report OWNER TO ws;

DROP SEQUENCE IF EXISTS replacement_seq CASCADE;
CREATE SEQUENCE replacement_seq START 1;
ALTER SEQUENCE replacement_seq OWNER TO ws;

DROP TABLE IF EXISTS replacement CASCADE;
create TABLE replacement (
	id INTEGER DEFAULT NEXTVAL('replacement_seq') CONSTRAINT pk_replacement PRIMARY KEY,
	old_id integer REFERENCES survey(s_id) ON DELETE CASCADE,
	old_ident text,				-- Survey ident of the replaced survey
	new_ident text				-- Survey ident of the new survey
	);
ALTER TABLE replacement OWNER TO ws;

DROP SEQUENCE IF EXISTS csv_seq CASCADE;
CREATE SEQUENCE csv_seq START 1;
ALTER SEQUENCE csv_seq OWNER TO ws;

DROP TABLE IF EXISTS csvtable CASCADE;
create TABLE csvtable (
	id integer default nextval('csv_seq') constraint pk_csvtable primary key,
	o_id integer references organisation(id) on delete cascade,
	s_id integer,					-- Survey id may be 0 for organisation level csv hence do not reference
	filename text,					-- Name of the CSV file
	headers text,
	survey boolean default false,	-- Set true if the data actually comes from a survey
	user_ident text,						-- Survey data from a survey needs tohave access authenticated so RBAC can be applied
	chart boolean default false,		-- Set true if the data is for a chart
	non_unique_key boolean default false,	-- Set true if the data does not have a unique key
	linked_sident text,						-- The ident of the survey from which the data is coming
	chart_key text,							-- The chart key
	linked_s_pd boolean,
	sqldef text,						-- The sql definition			
	ts_initialised TIMESTAMP WITH TIME ZONE
	);
ALTER TABLE csvtable OWNER TO ws;

CREATE SCHEMA csv AUTHORIZATION ws;

DROP SEQUENCE IF EXISTS du_seq CASCADE;
CREATE SEQUENCE du_seq START 1;
ALTER SEQUENCE du_seq OWNER TO ws;

DROP TABLE IF EXISTS disk_usage CASCADE;
create TABLE disk_usage (
	id integer default nextval('du_seq') constraint  pk_diskusage primary key,
	e_id integer,
	o_id integer,
	total bigint,					-- Total disk usage
	upload bigint,					-- Disk used in upload directory
	media bigint,					-- Disk used in media directory
	template bigint,					-- Disk used in template directory
	attachments bigint,				-- Disk used in attachments directory
	when_measured TIMESTAMP WITH TIME ZONE
	);
ALTER TABLE disk_usage OWNER TO ws;

DROP SEQUENCE IF EXISTS people_seq CASCADE;
CREATE SEQUENCE people_seq START 1;
ALTER SEQUENCE people_seq OWNER TO ws;

DROP TABLE IF EXISTS people;
create TABLE people (
	id integer default nextval('people_seq') constraint pk_people primary key,
	o_id integer,
	email text,		
	name text,
	unsubscribed boolean default false,
	opted_in boolean default false,
	opted_in_sent TIMESTAMP WITH TIME ZONE,
	opted_in_count integer default 0,
	opted_in_status text,
	opted_in_status_msg text,
	uuid text,								-- Uniquely identify this person
	when_unsubscribed TIMESTAMP WITH TIME ZONE,
	when_subscribed TIMESTAMP WITH TIME ZONE,
	when_requested_subscribe TIMESTAMP WITH TIME ZONE		-- prevent spamming
	);
create unique index idx_people on people(o_id, email);
ALTER TABLE people OWNER TO ws;

DROP SEQUENCE IF EXISTS mailout_seq CASCADE;
CREATE SEQUENCE mailout_seq START 1;
ALTER SEQUENCE mailout_seq OWNER TO ws;

DROP TABLE IF EXISTS mailout cascade;
create TABLE mailout (
	id integer default nextval('mailout_seq') constraint pk_mailout primary key,
	survey_ident text,				-- Survey in mail out
	name text,						-- Name for the mail out
	content text,
	subject text,
	multiple_submit boolean,
	anonymous boolean,				-- Set true if submissions are anonymous
	created TIMESTAMP WITH TIME ZONE,
	modified TIMESTAMP WITH TIME ZONE
	);
CREATE UNIQUE INDEX idx_mailout_name ON mailout(survey_ident, name);
ALTER TABLE mailout OWNER TO ws;

DROP SEQUENCE IF EXISTS mailout_people_seq CASCADE;
CREATE SEQUENCE mailout_people_seq START 1;
ALTER SEQUENCE mailout_people_seq OWNER TO ws;

DROP TABLE IF EXISTS mailout_people;
create TABLE mailout_people (
	id integer default nextval('mailout_people_seq') constraint pk_mailout_people primary key,
	p_id integer references people(id) on delete cascade,		-- People ID
	m_id integer references mailout(id) on delete cascade,		-- Mailout Id,
	status text,		-- Mailout status
	status_details text,
	initial_data text,
	link text,
	user_ident text,
	processed TIMESTAMP WITH TIME ZONE,	-- Time converted into a message
	status_updated TIMESTAMP WITH TIME ZONE	
	);
CREATE UNIQUE INDEX idx_mailout_people ON mailout_people(p_id, m_id);
ALTER TABLE mailout_people OWNER TO ws;

DROP SEQUENCE IF EXISTS apply_foreign_keys_seq CASCADE;
CREATE SEQUENCE apply_foreign_keys_seq START 1;
ALTER SEQUENCE apply_foreign_keys_seq OWNER TO ws;

DROP TABLE IF EXISTS apply_foreign_keys;
create TABLE apply_foreign_keys (
	id integer default nextval('apply_foreign_keys_seq') constraint pk_apply_foreign_keys primary key,
	update_id text,
	s_id integer REFERENCES survey(s_id) ON DELETE CASCADE,
	qname text,
	instanceid text,
	prikey integer,
	table_name text,
	instanceIdLaunchingForm text,
	applied boolean default false,
	comment text,
	ts_created TIMESTAMP WITH TIME ZONE,
	ts_applied TIMESTAMP WITH TIME ZONE
	);
ALTER TABLE apply_foreign_keys OWNER TO ws;

-- billing
DROP SEQUENCE IF EXISTS bill_rates_seq CASCADE;
CREATE SEQUENCE bill_rates_seq START 1;
ALTER SEQUENCE bill_rates_seq OWNER TO ws;

DROP TABLE IF EXISTS bill_rates;
create TABLE bill_rates (
	id integer default nextval('bill_rates_seq') constraint pk_bill_rates primary key,
	o_id integer default 0,	-- If 0 then all organisations (In enterprise or server)
	e_id integer default 0,	-- If 0 then all enterprises (ie server level)
	rates text,				-- json object
	currency text,
	created_by text,
	ts_created TIMESTAMP WITH TIME ZONE,
	ts_applies_from TIMESTAMP WITH TIME ZONE
	);
create unique index idx_bill_rates on bill_rates(o_id, e_id, ts_applies_from);
ALTER TABLE bill_rates OWNER TO ws;

-- Audit
DROP SEQUENCE IF EXISTS last_refresh_seq CASCADE;
CREATE SEQUENCE last_refresh_seq START 1;
ALTER SEQUENCE last_refresh_seq OWNER TO ws;

DROP TABLE IF EXISTS last_refresh;
create TABLE last_refresh (
	id integer default nextval('last_refresh_seq') constraint pk_last_refresh primary key,
	o_id integer,
	user_ident text,
	refresh_time TIMESTAMP WITH TIME ZONE,
	device_time TIMESTAMP WITH TIME ZONE,
	deviceid text,
	appversion text
	);
SELECT AddGeometryColumn('last_refresh', 'geo_point', 4326, 'POINT', 2);
ALTER TABLE last_refresh OWNER TO ws;

DROP SEQUENCE IF EXISTS last_refresh_log_seq CASCADE;
CREATE SEQUENCE last_refresh_log_seq START 1;
ALTER SEQUENCE last_refresh_log_seq OWNER TO ws;

DROP TABLE IF EXISTS last_refresh_log;
create TABLE last_refresh_log (
	id integer default nextval('last_refresh_log_seq') constraint pk_last_refresh_log primary key,
	o_id integer,
	user_ident text,
	refresh_time TIMESTAMP WITH TIME ZONE,
	device_time TIMESTAMP WITH TIME ZONE,
	deviceid text,
	appversion text
	);
SELECT AddGeometryColumn('last_refresh_log', 'geo_point', 4326, 'POINT', 2);
create index idx_refresh_time on last_refresh_log (refresh_time);
ALTER TABLE last_refresh_log OWNER TO ws;

-- Group Surveys
DROP SEQUENCE IF EXISTS group_survey_seq CASCADE;
CREATE SEQUENCE group_survey_seq START 1;
ALTER SEQUENCE group_survey_seq OWNER TO ws;

DROP TABLE IF EXISTS group_survey;
create TABLE group_survey (
	id integer default nextval('group_survey_seq') constraint pk_group_survey primary key,
	u_ident text REFERENCES users(ident) ON DELETE CASCADE,
	s_id integer REFERENCES survey(s_id) ON DELETE CASCADE,
	group_ident text,	-- Oversight ident
	f_name text			-- Sib form name
	);
ALTER TABLE group_survey OWNER TO ws;

-- Survey Styles
DROP SEQUENCE IF EXISTS style_seq CASCADE;
CREATE SEQUENCE style_seq START 1;
ALTER SEQUENCE style_seq OWNER TO ws;

DROP TABLE IF EXISTS style;
create TABLE style (
	id integer default nextval('style_seq') constraint pk_style primary key,
	s_id integer REFERENCES survey(s_id) ON DELETE CASCADE,
	name text,
	style text	-- json
	);
ALTER TABLE style OWNER TO ws;

DROP SEQUENCE IF EXISTS survey_settings_seq CASCADE;
CREATE SEQUENCE survey_settings_seq START 1;
ALTER SEQUENCE survey_settings_seq OWNER TO ws;

DROP TABLE IF EXISTS survey_settings;
create TABLE survey_settings (
	id integer DEFAULT NEXTVAL('survey_settings_seq') CONSTRAINT pk_survey_settings PRIMARY KEY,
	s_ident text,		-- Survey ident
	u_id integer REFERENCES users(id) ON DELETE CASCADE,		-- User
	view text,			-- Overall view (json)
	map_view text,		-- Map view data
	chart_view text,		-- Chart view data
	columns text	
);
ALTER TABLE survey_settings OWNER TO ws;

DROP SEQUENCE IF EXISTS aws_async_jobs_seq CASCADE;
CREATE SEQUENCE aws_async_jobs_seq START 1;
ALTER SEQUENCE aws_async_jobs_seq OWNER TO ws;

-- Aynchronous AWS jobs deposit the data in an S3 bucket
-- This S3 object is the definitive and full results and a link to it
-- will be retaine in the sync table
DROP TABLE IF EXISTS aws_async_jobs;
create TABLE aws_async_jobs (
	id integer DEFAULT NEXTVAL('aws_async_jobs_seq') CONSTRAINT pk_aws_async_jobs PRIMARY KEY,
	o_id integer,
	col_name text,			-- Question that initiated this request
	table_name text,		-- Table containing the data
	instanceid text,		-- Record identifier
	type text,				-- AUTO_UPDATE_AUDIO
	medical boolean,		-- Used with transcribe jobs
	locale text,			-- Locale of organisation that submitted this job
	update_details text,	-- AutoUpdate object in JSON
	job text,				-- Unique AWS job identifier
	status text,			-- open || pending || complete || error
	results_link text,		-- URI to job results
	duration integer,		-- Duration of audio file in seconds
	request_initiated TIMESTAMP WITH TIME ZONE,
	request_completed TIMESTAMP WITH TIME ZONE
);
ALTER TABLE aws_async_jobs OWNER TO ws;

-- Usage counter
-- Manage usage of costly resources by organisation id
-- Generally billing will be handled through the log, this table will provide greater performance
DROP SEQUENCE IF EXISTS resource_usage_seq CASCADE;
CREATE SEQUENCE resource_usage_seq START 1;
ALTER SEQUENCE resource_usage_seq OWNER TO ws;

DROP TABLE IF EXISTS resource_usage;
create TABLE resource_usage (
	id integer DEFAULT NEXTVAL('resource_usage_seq') CONSTRAINT pk_resource_usage PRIMARY KEY,
	o_id integer,
	period text,			-- year - month
	resource text,			-- Resource identifier
	usage integer			-- Amount of usage
);
ALTER TABLE resource_usage OWNER TO ws;

DROP SEQUENCE IF EXISTS language_codes_seq CASCADE;
CREATE SEQUENCE language_codes_seq START 1;
ALTER SEQUENCE language_codes_seq OWNER TO ws;

DROP TABLE IF EXISTS language_codes;
create TABLE language_codes (
	id integer DEFAULT NEXTVAL('language_codes_seq') CONSTRAINT pk_language_codes PRIMARY KEY,
	code text,
	aws_translate boolean,			-- set yes if supported by translate
	aws_transcribe boolean,			-- set yes if supported by trancribe
	transcribe_default boolean,		-- true if this is the default language to use for transcribe
	transcribe_medical boolean		-- true if this language can be used with transcribe medical
);
ALTER TABLE language_codes OWNER TO ws;
create unique index idx_language_codes_code on language_codes(code);

DROP TABLE IF EXISTS email_alerts;
create TABLE email_alerts (
	o_id integer,
	alert_type text,
	alert_recorded TIMESTAMP WITH TIME ZONE
);
ALTER TABLE email_alerts OWNER TO ws;

DROP SEQUENCE IF EXISTS autoupdate_questions_seq CASCADE;
CREATE SEQUENCE autoupdate_questions_seq START 1;
ALTER SEQUENCE autoupdate_questions_seq OWNER TO ws;

DROP TABLE IF EXISTS autoupdate_questions;
create TABLE autoupdate_questions (
	id integer DEFAULT NEXTVAL('autoupdate_questions_seq') CONSTRAINT pk_autoupdate_questions PRIMARY KEY,
	q_id integer references question(q_id) on delete cascade,
	s_id integer references survey(s_id) on delete cascade
);
ALTER TABLE autoupdate_questions OWNER TO ws;

DROP SEQUENCE IF EXISTS linked_files_seq CASCADE;
CREATE SEQUENCE linked_files_seq START 1;
ALTER SEQUENCE linked_files_seq OWNER TO ws;

DROP TABLE IF EXISTS linked_files;
create TABLE linked_files (
	id integer DEFAULT NEXTVAL('linked_files_seq') CONSTRAINT pk_linked_files PRIMARY KEY,
	s_id integer references survey(s_id) on delete cascade,
	logical_path text,
	current_id integer
);
ALTER TABLE linked_files OWNER TO ws;

DROP SEQUENCE IF EXISTS linked_files_old_seq CASCADE;
CREATE SEQUENCE linked_files_old_seq START 1;
ALTER SEQUENCE linked_files_old_seq OWNER TO ws;

DROP TABLE IF EXISTS linked_files_old;
create TABLE linked_files_old (
	id integer DEFAULT NEXTVAL('linked_files_old_seq') CONSTRAINT pk_linked_old_files PRIMARY KEY,
	file text,
	deleted_time TIMESTAMP WITH TIME ZONE,
	erase_time TIMESTAMP WITH TIME ZONE
);
ALTER TABLE linked_files_old OWNER TO ws;
create index idx_lfo_erase on linked_files_old (erase_time);

DROP SEQUENCE IF EXISTS background_report_seq CASCADE;
CREATE SEQUENCE background_report_seq START 1;
ALTER SEQUENCE background_report_seq OWNER TO ws;

DROP TABLE IF EXISTS background_report;
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

DROP SEQUENCE IF EXISTS s3upload_seq CASCADE;
CREATE SEQUENCE s3upload_seq START 1;
ALTER SEQUENCE s3upload_seq OWNER TO ws;

DROP TABLE IF EXISTS s3upload;
CREATE TABLE s3upload (
	id integer DEFAULT NEXTVAL('s3upload_seq') CONSTRAINT pk_s3upload PRIMARY KEY,
	filepath text,
	status text,    -- new or failed
	reason text,	-- failure reason
	o_id integer default 0,
	is_media boolean default false,
	created_time timestamp with time zone,
	processed_time TIMESTAMP WITH TIME ZONE		-- Time of processing
	);
ALTER TABLE s3upload OWNER TO ws;

DROP SEQUENCE IF EXISTS subevent_queue_seq CASCADE;
CREATE SEQUENCE subevent_queue_seq START 1;
ALTER SEQUENCE subevent_queue_seq OWNER TO ws;

DROP TABLE IF EXISTS subevent_queue;
CREATE TABLE subevent_queue (
	id integer DEFAULT NEXTVAL('subevent_queue_seq') CONSTRAINT pk_subevent_queue PRIMARY KEY,
	ue_id integer,
	linkage_items text,    -- JSON
	status text,    -- new or failed
	reason text,	-- failure reason
	created_time TIMESTAMP WITH TIME ZONE,
	processed_time TIMESTAMP WITH TIME ZONE		-- Time of processing
	);
ALTER TABLE subevent_queue OWNER TO ws;

DROP SEQUENCE IF EXISTS cms_alert_seq CASCADE;
CREATE SEQUENCE cms_alert_seq START 1;
ALTER SEQUENCE cms_alert_seq OWNER TO ws;

DROP TABLE IF EXISTS cms_alert;
CREATE TABLE cms_alert (
	id integer DEFAULT NEXTVAL('cms_alert_seq') CONSTRAINT pk_cms_alert PRIMARY KEY,
	o_id integer,
	group_survey_ident text,
	name text,
	period text,
	filter text,
	changed_by text,
	changed_ts TIMESTAMP WITH TIME ZONE	
	);
CREATE UNIQUE INDEX cms_unique_alert ON cms_alert(group_survey_ident, name);
ALTER TABLE cms_alert OWNER TO ws;

DROP SEQUENCE IF EXISTS cms_setting_seq CASCADE;
CREATE SEQUENCE cms_setting_seq START 1;
ALTER SEQUENCE cms_setting_seq OWNER TO ws;

DROP TABLE IF EXISTS cms_setting;
CREATE TABLE cms_setting (
	id integer DEFAULT NEXTVAL('cms_setting_seq') CONSTRAINT pk_setting_alert PRIMARY KEY,
	group_survey_ident text,
	settings text,
	key text,
	key_policy text,
	changed_by text,
	changed_ts TIMESTAMP WITH TIME ZONE	
	);
CREATE UNIQUE INDEX cms_unique_setting ON cms_setting(group_survey_ident);
ALTER TABLE cms_setting OWNER TO ws;

-- Create a table to hold biometric, including fingerprint record linkage data
DROP SEQUENCE IF EXISTS linkage_seq CASCADE;
CREATE SEQUENCE linkage_seq START 1;
ALTER SEQUENCE linkage_seq OWNER TO ws;

DROP TABLE IF EXISTS linkage;
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

-- Create a table to hold a change history for shared resource files
DROP SEQUENCE IF EXISTS sr_history_seq CASCADE;
CREATE SEQUENCE sr_history_seq START 1;
ALTER SEQUENCE sr_history_seq OWNER TO ws;

DROP TABLE IF EXISTS sr_history;
CREATE TABLE sr_history (
	id integer DEFAULT NEXTVAL('sr_history_seq') CONSTRAINT pk_sr_history PRIMARY KEY,
	o_id integer,
	survey_ident text,						-- null if this is an organisational level shared resource
	resource_name text,						-- Name of the resource including extension
	file_name text,							-- Original file name
	file_path text,							-- The path to the stored file
	user_ident text,						-- User who uploaded the file
	uploaded_ts TIMESTAMP WITH TIME ZONE	-- When the file was uploaded	
	);
ALTER TABLE sr_history OWNER TO ws;

DROP TABLE IF EXISTS submission_queue;
CREATE UNLOGGED TABLE submission_queue
(
    element_identifier UUID PRIMARY KEY,
    time_inserted TIMESTAMP,
    ue_id integer,
    instanceid text,	-- Don't allow duplicates in the submission queue where they can be worked on in parallel
    restore boolean,
    payload JSON
);
ALTER TABLE submission_queue OWNER TO ws;

DROP TABLE IF EXISTS monitor_data;
CREATE UNLOGGED TABLE IF NOT EXISTS monitor_data
(
    recorded_at TIMESTAMP WITH TIME ZONE,
    payload JSON
);
ALTER TABLE monitor_data OWNER TO ws;

DROP TABLE IF EXISTS message_queue;
CREATE UNLOGGED TABLE IF NOT EXISTS message_queue (
    element_identifier UUID PRIMARY KEY,
    time_inserted TIMESTAMP,
    m_id integer,
    o_id integer,
    topic text,	
    description text,
    data text
);
ALTER TABLE message_queue OWNER TO ws;

DROP TABLE IF EXISTS key_queue;
CREATE UNLOGGED TABLE IF NOT EXISTS key_queue
(
    element_identifier UUID PRIMARY KEY,
    key text,
    group_survey_ident text
);
ALTER TABLE key_queue OWNER TO ws;

DROP TABLE IF EXISTS sms_number;
CREATE TABLE IF NOT EXISTS sms_number (
    element_identifier UUID PRIMARY KEY,
    o_id integer,				-- Organisation that the number is allocated to
    time_modified TIMESTAMP WITH TIME ZONE,
    our_number text,			-- Our number that sends or receives messages
    survey_ident text,
    their_number_question text, -- The question in the survey that holds the number of the counterpart
    message_question text,		-- The question name in the survey that holds the message details
    mc_msg,						-- Message to send if there is more than one case to update
    channel text,				-- sms or whatsapp
    description text
);
ALTER TABLE sms_number OWNER TO ws;
CREATE UNIQUE INDEX sms_number_to_idx ON sms_number(our_number);

-- Improve Timezone Performance
DROP TABLE IF EXISTS timezone;
CREATE TABLE timezone (
    name text,
    utc_offset text
);
ALTER TABLE timezone OWNER TO ws;
