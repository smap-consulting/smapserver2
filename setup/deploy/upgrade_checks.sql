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

\echo
\echo '8. Question column names that are not plain names'
\echo '   Column names are set by the server when a question is created and can no longer be changed as a question property.  Any listed here were changed directly'
select s.s_id, s.display_name as survey, f.name as form, q.qname, q.column_name
from question q
join form f on f.f_id = q.f_id
join survey s on s.s_id = f.s_id
where q.column_name ~ '[\x01-\x2f\x3a-\x40\x5b-\x5e\x60\x7b-\x7f]' or q.column_name ~ '^[0-9]'
order by 1, 4;

\echo
\echo '9. Saved chart queries whose key is not one of their columns'
\echo '   These are rebuilt when next used, and the chart then reports the key as not found in the form'
select ct.id, ct.s_id, s.display_name as survey, ct.filename, ct.chart_key
from csvtable ct
left join survey s on s.s_id = ct.s_id
where ct.survey and ct.chart_key is not null
  and not coalesce((ct.sqldef::jsonb -> 'colNames') ? ct.chart_key, false)
order by 2;

\echo
\echo '10. Choice and csv file column names with characters other than letters, digits and _'
\echo '   New names replace these characters with x.  Existing columns keep their names, a csv file listed here should be uploaded again'
select 'choice' as source, l.s_id::text as id, l.name as list, o.ovalue as name, o.column_name
from option o join listname l on l.l_id = o.l_id
where o.column_name ~ '[\x01-\x2f\x3a-\x40\x5b-\x5e\x60\x7b-\x7f]'
union all
select 'csv file', ct.id::text, ct.filename, h->>'fName', h->>'tName'
from csvtable ct, jsonb_array_elements(case when ct.headers is not null and ct.headers like '[%' then ct.headers::jsonb else '[]'::jsonb end) h
where h->>'tName' ~ '[\x01-\x2f\x3a-\x40\x5b-\x5e\x60\x7b-\x7f]'
order by 1, 2;

\echo
\echo '11. Form table names with characters other than letters, digits and _'
\echo '   Repeats renamed before this version could get a table name with . or -.  Informational, existing tables keep their names'
select f.s_id, s.display_name as survey, f.name as form, f.table_name
from form f
left join survey s on s.s_id = f.s_id
where f.table_name ~ '[\x01-\x2f\x3a-\x40\x5b-\x5e\x60\x7b-\x7f]'
order by 1;

\echo
\echo '12. Email templates with markup that will be removed when the email is sent'
\echo '   Email content is now sanitised: style blocks, center, buttons, forms, scripts, embedded content, data: and javascript: links,'
\echo '   event handlers, bgcolor / background attributes and classes other than on span and div are removed.  Use inline styles'
\echo '   and a styled link instead of a button.  The email is still sent, only its formatting changes'
with tmpl(source, id, name, content) as (
	select 'notification', f.id::text, f.name, f.notify_details::jsonb->>'content'
		from forward f where f.target = 'email' and f.notify_details like '{%'
	union all
	select 'task group email', tg.tg_id::text, tg.name, tg.email_details::jsonb->>'content'
		from task_group tg where tg.email_details like '{%'
	union all
	select 'mailout', m.id::text, m.name, m.content from mailout m
	union all
	select 'organisation default email', o.id::text, o.name, o.default_email_content from organisation o
)
select source, id, name,
	concat_ws(', ',
		case when content ~* '<\s*/?\s*(style|center|button|script|iframe|form|input|select|textarea|link|meta|object|embed|svg|html|head|body|title|base)\M' then 'removed elements' end,
		case when content ~* '(src|href)\s*=\s*["'']?\s*(data|javascript):' then 'data: or javascript: link' end,
		case when content ~* '\son[a-z]+\s*=' then 'event handler' end,
		case when content ~* '\s(bgcolor|background)\s*=' then 'bgcolor or background attribute' end,
		case when content ~* '<(?!span\M|div\M)[a-z][a-z0-9]*\M[^>]*\sclass\s*=' then 'class not on span or div' end
	) as reason
from tmpl
where content ~* '<\s*/?\s*(style|center|button|script|iframe|form|input|select|textarea|link|meta|object|embed|svg|html|head|body|title|base)\M'
   or content ~* '(src|href)\s*=\s*["'']?\s*(data|javascript):'
   or content ~* '\son[a-z]+\s*='
   or content ~* '\s(bgcolor|background)\s*='
   or content ~* '<(?!span\M|div\M)[a-z][a-z0-9]*\M[^>]*\sclass\s*='
order by 1, 2;
