-- Checks to run on survey_definitions before upgrading to 26.11
-- Each query lists stored settings that the new version will treat differently.  No rows means no change.
-- Run with: psql -d survey_definitions -f upgrade_checks.sql
-- Nothing is changed.

\echo
\echo '1. Managed (oversight) reports in use'
\echo '   Informational: surveys with a managed report and the number of stored reports'
select (select count(*) from survey where managed_id > 0) as managed_surveys,
       (select count(*) from custom_report) as reports;

\echo
\echo '2. Managed report columns that will be ignored'
\echo '   Column, start or end names that are not plain names (letters, digits and _), or a type that is not a single word'
select cr.id as report_id, cr.name as report,
       coalesce(c->>'column_name', c->>'name') as column_name, c->>'startName' as start_name,
       c->>'endName' as end_name, c->>'type' as type
from custom_report cr,
     jsonb_array_elements(case when jsonb_typeof(cr.config::jsonb) = 'array' then cr.config::jsonb
                               else cr.config::jsonb->'columns' end) c
where coalesce(c->>'column_name', c->>'name', '') !~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$'
   or coalesce(c->>'startName', 'x') !~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$'
   or coalesce(c->>'endName', 'x') !~ '^[A-Za-z_][A-Za-z0-9_]{0,62}$'
   or coalesce(c->>'type', 'x') !~ '^[A-Za-z_]+$'
order by 1;

\echo
\echo '3. Managed report columns that will not be added to a new results table'
\echo '   Supported types are text, select_one, date, integer, decimal and boolean'
select cr.id as report_id, cr.name as report, coalesce(c->>'column_name', c->>'name') as column_name, c->>'type' as type
from custom_report cr,
     jsonb_array_elements(case when jsonb_typeof(cr.config::jsonb) = 'array' then cr.config::jsonb
                               else cr.config::jsonb->'columns' end) c
where c->>'type' is not null
  and c->>'type' not in ('calculate', 'text', 'select_one', 'date', 'integer', 'decimal', 'boolean')
order by 1;

\echo
\echo '4. Managed report calculations'
\echo '   Informational: stored calculations are no longer used.  They only ever applied to columns of type server_calculate'
select cr.id as report_id, cr.name as report, coalesce(c->>'column_name', c->>'name') as column_name, c->>'type' as type
from custom_report cr,
     jsonb_array_elements(case when jsonb_typeof(cr.config::jsonb) = 'array' then cr.config::jsonb
                               else cr.config::jsonb->'columns' end) c
where c->>'type' = 'server_calculate' and c->'calculation' is not null and c->'calculation' <> 'null'
order by 1;

\echo
\echo '5. Role row filters in the old json format'
\echo '   These will be refused and users with the role will get an error instead of data.  Re-enter the filter as an expression'
select sr.id, sr.survey_ident, s.display_name as survey, r.name as role, sr.row_filter
from survey_role sr
left join survey s on s.ident = sr.survey_ident
left join role r on r.id = sr.r_id
where sr.row_filter like '{%'
order by sr.survey_ident;

\echo
\echo '6. Filters and calculations with a {...} token that is not a number of days, an interval or a time'
\echo '   Valid forms are {7}, {7_days} (also day, hour(s), minute(s), second(s)) and {1:30:00}'
\echo '   Such a token used to be ignored, it is now an error'
with expr(source, id, expression) as (
	select 'role row filter', sr.id::text, sr.row_filter from survey_role sr where sr.row_filter not like '{%'
	union all
	select 'dashboard advanced filter', ds.ds_id::text, ds.ds_advanced_filter from dashboard_settings ds
	union all
	select 'notification filter', f.id::text, f.filter from forward f
	union all
	select 'case alert filter', a.id::text, a.filter from cms_alert a
	union all
	select 'reference filter', rf.id::text, rf.filter from reference_filter rf
	union all
	select 'task group filter', tg.tg_id::text, tg.rule::jsonb->'filter'->>'advanced' from task_group tg where tg.rule like '{%'
	union all
	select 'task group assign rule', tg.tg_id::text, tg.rule::jsonb->>'assign_data' from task_group tg where tg.rule like '{%'
	union all
	select 'server calculation', q.q_id::text, q.server_calculate::jsonb->>'expression' from question q where q.server_calculate like '{%'
	union all
	select 'server calculation condition', q.q_id::text, c->>'condition'
		from question q, jsonb_array_elements(coalesce(q.server_calculate::jsonb->'conditions', '[]'::jsonb)) c
		where q.server_calculate like '{%'
	union all
	select 'server calculation value', q.q_id::text, c->>'value'
		from question q, jsonb_array_elements(coalesce(q.server_calculate::jsonb->'conditions', '[]'::jsonb)) c
		where q.server_calculate like '{%'
),
tokens as (
	-- Remove quoted text and separate operators, as the expression parser does, then find {...} tokens
	select source, id, expression, t[1] as token
	from expr,
	     regexp_matches(regexp_replace(regexp_replace(coalesce(expression, ''), '''[^'']*''', ' ', 'g'), '[=+\-><*/(),]', ' ', 'g'),
	                    '(?:^|\s)(\{[^\s]*\})(?=\s|$)', 'g') t
)
select source, id, token, expression
from tokens
where token !~* '^\{([0-9]+|[0-9]+_(day|days|hour|hours|minute|minutes|second|seconds)|[0-9]+:[0-9]+:[0-9]+)\}$'
order by source, id;

\echo
\echo '7. Case management status and criticality questions that are not plain names'
\echo '   These settings will not save until the question is chosen again.  A status question that is not a column of the table is no longer used to hide closed cases'
select cs.id, cs.group_survey_ident, cs.settings::jsonb->>'statusQuestion' as status_question,
       cs.settings::jsonb->>'criticalityQuestion' as criticality_question
from cms_setting cs
where cs.settings is not null
  and (coalesce(nullif(cs.settings::jsonb->>'statusQuestion', ''), 'x') !~ '^[A-Za-z_][A-Za-z0-9_\-\.]*$'
    or coalesce(nullif(cs.settings::jsonb->>'criticalityQuestion', ''), 'x') !~ '^[A-Za-z_][A-Za-z0-9_\-\.]*$')
order by 2;
