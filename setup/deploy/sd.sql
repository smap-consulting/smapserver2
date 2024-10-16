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
