# MCP server

Smap can act as a Model Context Protocol server, so a person can drive it from an AI client such as
Claude Code. This file is the running record of what is exposed, what is deliberately not, and how
to switch it on.

Protocol revision implemented: **2026-07-28**. Earlier revisions back to 2024-11-25 are answered
too, because clients in the field still open with the `initialize` handshake that 2026-07-28
removed, and that revision gives them a twelve month window.

## Switching it on

Off on every server, new and existing. A **server owner** turns it on in Settings, and only then can
a server owner grant the `mcp access` group. Nobody else can see or grant that group, whatever else
they hold.

Turning it off is a kill switch: the flag is read on every request, so live tokens stop working at
once rather than when they would have expired.

## What a client can do

A client acts as one user and can never do more than that person could do in the console. Effective
permission is the user's security groups intersected with the scopes on their token.

| Scope | Covers | Advertised |
| --- | --- | --- |
| `smap:read` | Everything the user can see, read only | yes |
| `smap:write` | Surveys, data, tasks, cases, mailout preparation | yes |
| `smap:admin` | Users, projects, organisation records | by step up |
| `smap:access` | Who can reach what | by step up, never remembered |
| `smap:server` | Server settings | by step up |
| `smap:privacy` | Subject access exports | by step up |

Advertised means named in `/.well-known/oauth-protected-resource`, which is where a client looks to
find out what it may ask for. A tool needing a scope the token lacks answers `403` naming it, and the
client re-authorises for that as well as what it already had.

The two have to agree, and at first they did not. Only `smap:read` was advertised, on the reasoning
that a client should never hold a permission it has not yet needed. But a client sent by a `403`
naming `smap:write` goes to that same document to learn how to ask for it, finds the scope missing,
asks again for the read token it already had, is refused again and gives up. Every write tool was
unreachable from any client that discovers scopes instead of being handed an authorisation URL by
hand, which is how the write tools came to be tested without this showing up. A scope a challenge can
name must be a scope the metadata offers.

So least privilege is enforced where it can be rather than assumed: the consent form gives each scope
its own checkbox, so a person grants only what they mean to, and every call intersects the token with
the user's security groups. Scopes join the advertised set as their increments land, so nothing is
offered that no tool yet uses - `smap:admin` and below stay out until 5.8, and will need this decided
again, because step up on its own does not appear to work with any client shipping today.

Which is why `tools/list` is filtered by the caller's groups and not by their token's scopes. The two
refusals are not alike: a group is a property of the person, and no amount of re-authorising gives an
enumerator an administrator's rights, so a tool they can never run is better never seen. A scope is a
property of the token and is meant to be escalated. Filtering the listing by scope as well made that
impossible - a tool the client cannot see is one it never calls, so it never receives the challenge
that tells it what to ask for, and a session could sit on a read-only token with no way to find out
that writing was available. The listing says what the person may do; the call decides what this token
may do.

## Tools

| Tool | Scope | Groups | Status |
| --- | --- | --- | --- |
| `whoami` | read | any | done |
| `server_info` | read | any | done |
| `event_list` | read | admin, owner | done |
| `ops_status` | read | admin, manage, owner | done |
| `usage_report` | read | admin, owner | done |
| `project_list` | read | analyst, admin, view data, manage | done |
| `survey_list` | read | analyst, admin, view data, manage | done |
| `survey_submission_counts` | read | analyst, admin, view data | done |
| `data_query` | read | analyst, admin, view data | done |
| `data_get_record` | read | analyst, admin, view data | done |
| `data_count` | read | analyst, admin, view data | done |
| `data_aggregate` | read | analyst, admin, view data | done |
| `data_attachments` | read | analyst, admin, view data | done |
| `data_audit` | read | analyst, admin, view data | done |
| `data_delete_record` | write | analyst, admin | done |
| `data_restore_record` | write | analyst, admin | done |
| `data_submit` | write | analyst, admin | done |
| `data_update_record` | write | analyst, admin | done |
| `data_bulk_update` | write | analyst, admin | done |
| `data_bulk_undo` | write | analyst, admin | done |
| `survey_get` | read | analyst, admin, manage | done |
| `survey_questions` | read | analyst, admin, manage | done |
| `survey_options` | read | analyst, admin, manage | done |
| `survey_history` | read | analyst, admin, manage | done |
| `survey_media_list` | read | analyst, admin, manage | done |
| `reference_filter_list` | read | analyst, admin, manage | done |
| `survey_check_type_change` | read | analyst, admin | done |
| `survey_create` | write | analyst, admin | done |
| `survey_set_settings` | write | analyst, admin | done |
| `survey_delete` | write | analyst, admin | done |
| `survey_undelete` | write | analyst, admin | done |
| `reference_filter_set` | write | analyst, admin | done |
| `survey_add_question` | write | analyst, admin | done |
| `survey_delete_question` | write | analyst, admin | done |
| `notification_list` | read | admin, manage, manage tasks | done |
| `notification_create` | write | admin, manage | done |
| `notification_delete` | write | admin, manage | done |
| `notification_enable` | write | admin, manage | done |
| `mailout_list` | read | admin, manage | done |
| `case_settings` | read | analyst, admin, manage | done |
| `case_settings_set` | write | admin, manage | done |
| `case_assign` | write | admin, manage, manage tasks | done |
| `workflow_list` | read | admin, manage, manage tasks | done |
| `task_group_list` | read | analyst, admin, manage, manage tasks | done |
| `task_group_create` | write | admin, manage, manage tasks | done |
| `task_list` | read | analyst, admin, manage, manage tasks | done |
| `task_create` | write | admin, manage, manage tasks | done |
| `task_action` | write | admin, manage, manage tasks | done |
| `topic_list` | read | analyst, admin, view data, manage | done |

## Resources

| URI | What |
| --- | --- |
| `smap://docs/tools` | What this connection can do, generated from the registry |
| `smap://docs/tasks` | The order to use tools in for jobs that take more than one, and what cannot be done at all |
| `smap://survey/{ident}/definition` | One survey's questions, options and settings |
| `smap://record/{ident}/{instanceId}` | One submitted record and its repeating groups |
| `smap://attachment/{ident}/{file}` | A photo, audio or other file on a record, returned as bytes |

A caller's own surveys are also listed individually, by name, so a client shows them without having
to expand a template. That stops above a hundred surveys: at that size the list is no longer a menu
a model can choose from, and some clients put the whole thing in front of it, which costs more
context than the work. Above the threshold nothing is listed and the template plus completion are
the way in, which `smap://docs/tools` says explicitly so a model knows which it is dealing with.

Survey idents complete through `completion/complete`, so a client can offer them rather than having
the model guess.

Attachments are served here rather than linked, because the URLs in survey data point at
`/app/attachments`, which is behind form authentication and cannot be fetched by a client holding a
bearer token. The uri is the attachment URL with everything up to and including `/attachments/`
replaced by `smap://attachment/`, which is what a client already has in its hands after reading
data. The first segment is the survey ident and that is what authorises the read: the survey has to
be one the caller could have listed, which makes this stricter than `/app/attachments`, which
authenticates a caller but does not check they may see the survey. The file part is checked for
traversal and the resolved path confirmed to be inside that survey's own directory. Anything over
5MB is reported with a URL rather than returned, because base64 costs a third again and it all has
to fit in a model's context.

## Coverage against the console

Filled in as each functional increment lands. An area with no tools yet is not a decision, only work
not done.

| Console area | Tools | State |
| --- | --- | --- |
| Surveys, list and structure | `survey_list`, `survey_submission_counts`, `survey_history` | read only |
| Data | `data_query`, `data_get_record`, `data_count`, `data_attachments`, `data_audit`, `data_delete_record`, `data_restore_record`, `data_submit`, `data_update_record`, `data_bulk_update`, `data_bulk_undo` | read and write |
| Analysis | `data_aggregate` | read only |
| Projects | `project_list` | read only |
| Topics / bundles | `topic_list` | read only |
| Reference data | `reference_filter_list`, `reference_filter_set` | read and write |
| Survey design | `survey_get`, `survey_questions`, `survey_options`, `survey_history`, `survey_media_list`, `survey_check_type_change`, `survey_create`, `survey_delete`, `survey_undelete`, `survey_add_question`, `survey_delete_question` | read and write |
| Tasks and assignments | `task_group_list`, `task_group_create`, `task_list`, `task_create`, `task_action` | read and write |
| Cases and workflow | `case_settings`, `case_settings_set`, `case_assign`, `workflow_list` | read and write |
| Notifications and messaging | `notification_list`, `notification_create`, `notification_enable`, `notification_delete`, `mailout_list` | read and write, but nothing sends |
| Reporting and monitoring | `event_list`, `ops_status`, `usage_report` | read only |
| Users, roles and access | — | not started |
| Server administration | — | not started |

## Deliberately not exposed

These are decisions, not gaps. The rule is that no MCP operation may irrecoverably destroy data or
configuration, and that anything which cannot be undone is not something an agent does on a
person's behalf.

| Not exposed | Why |
| --- | --- |
| Right to be forgotten erase | Legally required to be irreversible |
| Hard erase of a survey | MCP soft deletes; the subscriber erases after 100 days |
| Cleanup, hard delete of a user, project or organisation | Irreversible |
| Media and attachment deletion | Irreversible |
| Sending a mailout, SMS or notification | Cannot be recalled. MCP prepares one, a person sends it |
| Switching organisation | Reloads the user's security groups, so it is an escalation path. A grant is bound to one organisation |
| Changing the acting user's own permissions | An agent must not widen its own lane |
| Question type change on a published question holding data | Cannot be reversed without data loss |

## Two access questions, not one

Whether a caller may see a survey and whether they may see a record inside it are separate
questions. A role can restrict a user to their own submissions within a survey they otherwise have
full access to, so answering only the first is not enough.

Every data path here answers both. The survey has to be one the caller could have listed, and the
record has to pass the role row filters, checked the same way the console checks them. Data reads go
through `TableDataManager`, which applies those filters as part of the query; single records go
through the hierarchy view, which does not - it reads the survey as an administrator - so the check
is made before it is called rather than left to it.

`smap://attachment/{ident}/{file}` is checked at the survey level only, because the stored path
holds the survey ident and the file name and does not say which record owns the file. The file name
carries the rest: attachments are saved as a random UUID
(`GeneralUtilityMethods.processAttachment`), so a name cannot be guessed or counted to, and the only
way to learn one is to be given it in a record. Every path here that hands out record content is row
filtered, so a user restricted to their own rows is never shown a name belonging to a record they
cannot see.

That makes the name the capability, which is the same model `/app/attachments` already relies on, and
this endpoint is the stricter of the two because it also checks the caller could have listed the
survey. Where it is weaker than a per-record check: a name learnt while access was held still works
after the access is removed, and names travel outside MCP in notification emails and exports. Both
are properties of the existing attachment URLs rather than of this resource. A per-record check would
need the results table searched for the file name; worth doing if attachment names ever start being
shared more widely than the records that carry them.

## Attachments are listed with the uri that fetches them

`data_attachments` returns the `smap://attachment/...` uri alongside the browser URL, rather than
leaving a model to build one. Turning a stored URL into a readable resource means stripping
everything up to `/attachments/` and prefixing the scheme, and a rule a model has to be told is a
rule it can get wrong.

It also supplies what the attachment resource cannot know by itself. A stored path names the survey
and the file and not the record, which is why that resource is authorised at survey level; this tool
reads through the row filtered path, so the owning record is known and only files on records the
caller may see are listed.

Which questions hold files is decided by the question type, not by whether a value looks like a
path, so a text answer that happens to resemble one is not offered as a file.

## Asking before acting

A tool that needs a person's agreement returns an input_required result carrying an elicitation, and
the client puts the question to somebody and calls again with the answer. There is no session, so the
second call is not a continuation of the first: it is a fresh call carrying the same arguments, the
answer, and a handle.

The handle is a row rather than a signed blob. The specification allows either, and encoding the
state into the handle is the more idiomatic choice, but what this handle decides is whether something
irrevocable happens, which is exactly the case the specification says must be protected from the
client. Signing that would mean a key to generate, store and rotate; a row needs none, and it can be
spent. The specification notes that signing bounds the replay window without making a state single
use, and an approval to send two emails must not be redeemable twice - deleting the row on use is the
whole of that guarantee.

The row holds only a digest of the call. The arguments come back on the retry and are checked against
it, so a confirmation shown for one record cannot be redeemed against another, and tampering with a
row causes a refusal and nothing worse. It is consumed whatever the verdict, so a client cannot try
one handle against varying arguments until something matches.

Only an explicit yes counts. A client can return a decline, a cancel, or an accept with the box
unticked, and none of those is agreement.

### When the client cannot be asked

Asking mid-request needs the 2026-07-28 protocol, and almost nothing speaks it yet: the clients in
the field still open with the handshake that revision removed, and they send no capabilities per
request at all. Refusing outright would mean no survey that sends anything could ever be submitted
to, which is most of the ones worth submitting to.

So `data_submit` accepts an `acknowledge` argument carrying the counts the caller says it has been
shown. They are checked against what the server has just worked out, so they cannot be guessed or
carried over from a survey that has since changed, and restating them is what puts the consequence
into the conversation where the person approving the call can read it. Counts rather than a token,
because a token would prove only that the tool had been called twice.

Weaker than being asked, and only used when asking is impossible: as clients adopt 2026-07-28 the
elicitation takes over, with no change here. `whoami` reports which of the two applies.

### What asks, and what does not

Scoped to what cannot be taken back, not to everything that changes. Deleting one record is confined,
audited and undoable, so it does not ask. If every field edit asked, nobody would read any of them,
and the prompt that matters - this will send two emails that cannot be recalled - would be waved
through with the rest. Rarity is what makes the question worth answering.

Each tool declares which it is, and the registry refuses at startup both a mutating tool that does not
say how it is undone and a read only tool that asks for confirmation. The second is the inverse
mistake and worth catching for the same reason.

A client that has not declared elicitation is refused rather than acted for. The specification forbids
sending it a question it cannot ask, and proceeding unasked would be worse than refusing.

## What a submission sets off, before it is made

Adding a record is not only a row. `SubmissionEventManager` hands every new record to
`NotificationManager` and then to `TaskManager`, so it can send email and SMS, call a webhook and
create tasks. The row can be undone; a sent message cannot. `survey_submission_effects` reports what
a survey will set off, so that anything submitting on a person's behalf can say what it is about to
do before it does it.

Every figure is the most that can happen. A notification carries a filter and a task group carries a
rule, both evaluated against the finished record, so before it exists there is nothing to test them
against. Recipients addressed from the record - whoever answered a question, whoever a case is
assigned to - count as one each, so the total is never lower than what is actually sent.

Recipients are counted rather than notifications, because one notification addressed to forty people
is forty emails, and forty is the number somebody approving needs. Each notification says where its
recipients come from, so the total can be checked against the console rather than believed. That is
not decoration: the first version counted a recipient for `emailQuestionName` whenever it was not
empty, and the value is `"-1"` when nobody chose a question, so a notification with two addresses
reported three emails. Over-counting is the safe direction for an approval, but a number that cannot
be reconciled with what the console shows is one that gets clicked through, which defeats the point
of asking.

## Ask for the questions you want

`data_query` takes `select`, and it narrows what is read rather than what is returned: the questions
nobody asked for are never fetched and their attachment URLs never built. A survey with fifty
questions costs fifty columns per record otherwise, and an agent pays that twice, once in the
database and again in the context the answer has to fit into.

Three things survive whatever is selected, because dropping them would break the request rather than
narrow it. The primary key is what paging follows. The instance id is how every other tool names a
record. And a column named by `filter` or `sort` has to be there, because the read path validates the
filter against the same list and refuses a name it cannot find, so selecting one question while
filtering on another would fail rather than answer.

## How caller input reaches SQL

Values are bound as parameters. Filters go through `SqlFrag`, the parser the rest of Smap uses,
which validates every question name against the survey before any of it becomes SQL.

Three things cannot be bound, because they are identifiers rather than values: the table name, and
the columns `data_aggregate` groups and summarises by. The table name comes from the database and
never from the caller. The two column names are resolved against the caller's own column list and
the stored name is what reaches the query, never the string that arrived; each is then confirmed to
exist with `hasColumn`, a parameterised lookup against `information_schema`, so a name carrying SQL
does not match a column and never gets that far. That makes this path stricter than the read path,
which trusts the stored name without the second check.

`sort` is matched against the known columns and falls back to the primary key when it matches none,
so an unrecognised sort orders by key rather than becoming part of the query.

`GeneralUtilityMethods.getDateRange` used to concatenate the column name it was given, and was safe
only because all nine of its callers happened to check that column existed first. That was a property
of the callers rather than of the method, so one that forgot would have written whatever it was given
into a query with nothing to show for it. It now quotes the name itself, doubling any embedded quote,
which is the whole of the escape a quoted identifier needs in Postgres. Quoted rather than checked
against a pattern, because `cleanName` strips a list of punctuation and lowercases the rest, so a
column name can legitimately hold any other unicode letter and a rule strict enough to be safe would
refuse real names.

## A bulk change is one thing, and can be undone as one

Every record a bulk update touches records the same change set id, so what happened is a single
action rather than a pile of edits that share a timestamp. `data_bulk_undo` takes that id and puts
each record back to the value its own history says it held, and records a change set of its own, so
an undo can be undone.

The id is generated before the first record is written. A run that stops half way still leaves the
part that happened undoable, which is when it matters most.

The undo checks access to each record again rather than trusting the change set. Knowing what was
done is not permission to do it again: a record that has moved out of the caller's reach is left
alone and counted.

Above twenty records the update asks first. Below that a mistake is a nuisance to undo and above it
undoing is a project even with the tool, which is a reason to be sure rather than a reason not to ask.

## Who changed a record, and what with

A record changed through MCP records both the person and the application. `changed_by` is the person,
who for an agent is whoever approved it, and `agent` is the program. Either alone leaves a question
that cannot be settled later: the person's name cannot say whether they typed it or approved it, and
the program's cannot say who let it.

`data_audit` returns the application's registered name, with `agentId` beside it. The name is how
somebody recognises it; the id is what the trail is anchored to and survives the client being renamed
or removed. Both are absent on a change a person made directly, which is most of them.

The console shows none of this yet. `/surveyKPI/api/data/changes` returns it, so the record history
panel has it and drops it - see the console section of the plan.

## A record can have more than one row

Smap never removes a row. It marks it `_bad`, and that covers two different things: a record somebody
deleted, and an earlier version of a record that has since been updated. An update writes a new row
with a new instance id and marks the old one with a reason naming its replacement, such as
"Merged with 11". Both rows stay, and both belong to the same thread.

So `data_count`, `data_query` and `data_aggregate` all leave `_bad` rows out unless
`include_deleted` says otherwise, which is what the console shows and what makes an updated record
count once rather than twice. `data_count` says so in its answer, and says superseded rather than
deleted, because a reader told a record was deleted will go looking for data that was never lost.

`survey_submission_counts` answers a different question again: it counts `upload_event` rows, so it
reports every submission the server ever accepted, including the ones later superseded. A survey
with one updated record therefore reports one more submission than it has records, and both numbers
are true.

This is why `data_audit` is keyed on the thread. The history of a record outlives the row that
carried it, so asking about the current instance still returns the original submission and every
change since, with the values before and after.

## A change says it came from MCP

`survey_change.source` was `editor` or `file`; it now also takes **`mcp`**. Without it a question
added through a tool recorded itself as having been typed into the editor, which is the one thing the
change log exists not to do.

The value goes on the **change item**, not only the change set. `ChangeElement`, which is what is
serialised into the log, reads `source` from the item, so setting it on the set alone is dropped
without complaint and the change quietly claims the editor made it. Nothing reads `source` to decide
behaviour, on the server or in the console, so the new value is safe to add and is there to be read
by a person.

That is separate from `agent`, and both are wanted. `source` says what kind of thing made the change;
`agent` says which application, by name. A change with `source` mcp and no `agent` would be a
console-minted token acting with no registered client.

## What happened, and how things stand

`event_list` is the organisation's log - surveys created and changed, users added, errors, refused
access - and it answers "what happened last week", which is a question an agent is good at and a
person usually has to go looking for.

It is scoped to the caller's organisation and no wider. `getLogEntries` takes a flag for entries
belonging to no organisation at all, which are server level things, and that flag is false here: an
organisation administrator is not a server administrator, and that is exactly the distinction lost
when one tool answers both questions. When a filter is given it asks for more rows than it returns,
because the filter is applied after the query - fetching exactly the limit and then discarding most
of it would return a handful of matches and call them all of them.

`ops_status` is the operations overview in words: what is open, what is late, which teams are behind,
what is shouting. **It deliberately does not use the cache.** `OpsMonitorManager` keeps a copy per
user for the page, which is right when somebody is clicking between tabs and wrong here - an agent
asked how things stand is asking now, and a number from an earlier minute answers a question nobody
put.

Alerts come back sorted by priority. An overview whose most urgent line is fourth has to be read in
full before it can be used.

`usage_report` counts submissions and the metered services month by month. Submissions come from
`upload_event` and the services from the log's measures, which answer two different questions - what
arrived, and what was spent doing something with it - so they are reported side by side and never
added together, a total across them being a number of nothing in particular.

**Nothing here is money, and there is deliberately no billing tool.** What a month cost depends on
the plan, and a tool that multiplied a count by a rate it had guessed would produce a figure somebody
might repeat.

## Preparing, and never sending

Email, SMS and webhooks cannot be recalled, so nothing here sends anything and nothing here ever
will. There is no `mailout_send`, no way to fire a notification by hand, and no SMS out. A mailout is
prepared and a person sends it from the console.

What is offered instead is the ability to see and to stop. `notification_list` shows the rules -
what each reacts to, what it sends, and whether it is switched on - and lists the disabled ones too,
because "why did nobody get an email" is answered by a rule that exists and is off far more often
than by one that was never made. `mailout_list` counts a campaign's people by state, since the
number waiting to be sent to is the number that would leave if somebody pressed send.

`notification_enable` is the one write, and it sits squarely inside the rule. Switching a
notification off stops things going out and is what somebody reaches for in a hurry; switching one on
sends nothing either, though it means the next matching submission will, and the answer says so. Off
does not recall what is already queued, and says that too.

**A notification is created switched off, and that is not a default the caller can override.** A rule
that started sending the moment it was made would be a send in everything but name: nobody would have
read the subject, checked the addresses or seen the filter before mail began arriving. Made off, it
is a draft - somebody reads it and turns it on, which is a separate call that a separate person can
refuse. Two steps rather than one, on purpose.

`notification_create` makes **only** an email, **only** on submission, **only** to addresses given in
the call. A notification can also fire on a timer, send SMS, call a webhook, escalate a case, write to
SharePoint, or take its recipients from an answer in the form, and each of those goes wrong
differently. One set of arguments covering all of them is how a plausible-looking call sends the
wrong thing to the wrong people. The rest are built in the console, where whoever is choosing can see
what each option means.

Addresses are checked one at a time and the bad ones named. An address wrong in a list of six is not
found by being told the list is wrong.

`notification_delete` is a real delete - the row goes, unlike a survey or a record, which are kept
and marked. So it returns what the notification was in enough detail to rebuild it, because that is
the only form its undo can take. It also says what deleting does not do: mail already sent stays
sent, anything queued still goes, and switching off is what somebody usually wants instead.

Two traps in the enable tool, both the kind that look like working code. `getNotification` will fetch any
notification on the server given its number, so the one being switched is found in the caller's own
projects rather than by id alone. And `updateNotification` writes the whole row, so the notification
is read first and written back with one flag changed - a half filled object would quietly empty
everything the caller did not know to supply.

## A case is a record, so most of it needs no tools

A case lives in the survey's own table with `_assigned` and `_case_closed` beside it. `data_query`
and `data_get_record` already read cases; `data_update_record` already closes one, because the
closing date is written by the server when the update touches the question the case settings name as
its status. There is no `case_list` or `case_get` or `case_close` here, and their absence is the
design rather than a gap - each would be a second way to read or write rows the data tools already
handle.

What could not be known from outside is which question closing depends on, and what value closes it,
because that is configuration rather than data. `case_settings` answers exactly that, and names the
alerts watching. After it, the ordinary data tools do the rest.

**Turning a survey into a case survey** is `case_settings_set`: name the question that holds a case's
status and the answer that means it is finished. Nothing about the records changes - `_assigned` and
`_case_closed` are already beside them. What changes is that the server now knows which answer to
watch, and writes the closing date itself when an update sets it.

Which is why that tool checks harder than most. A status question that does not exist, or a final
status no answer can ever hold, leaves a survey that looks like it manages cases and has no case that
ever closes, and nothing complains, because every part of it is individually plausible. A person
doing this in the console picks from lists and cannot make either mistake; a model can, so the
question is checked against the survey and the final status against that question's own choices.

`case_assign` exists because assigning is not an ordinary update: it asks the two access questions,
and `CaseManager.assignRecord` asks neither - the checking lives in the endpoints that call it, so a
tool calling it directly has to do the same or it would be the one way into a case that asks nothing.

`workflow_list` is read only, deliberately. A workflow is not a thing in Smap; it is what you get
when the notifications, task groups and case rules are read together and the arrows drawn between
them. Setting one means creating those, which are their own tools in their own increments - a tool
claiming to "set a workflow" would be one that quietly did several other things. The positions of the
boxes are not reported either: they are where somebody dragged them on a screen and say nothing about
what the server does.

## Who may be given work

**The person authorising the assignment must be able to see the record; the assignee needs only to be
a member of the project.**

It used to be both. An assignee had to pass the record's row filters as well, and that was the wrong
test: a filter limiting an enumerator to their own submissions is exactly the case where assigning
them the work is the point, because the record is not theirs yet - which is why somebody is giving it
to them. The rule made those assignments impossible in the console and silently dropped them where
tasks were generated automatically, while buying nothing, since the record reaches the assignee
through the task either way.

The rule now lives in `RoleManager.assignmentAllowed` and the assignment paths call it - creating a
task, assigning a case from the operations monitor, assigning one from managed forms.

Adding a **reference** to a record deliberately keeps the stricter test. A reference is read access
with no work attached, so it must not hand over a record the filters were hiding; an assignment gives
somebody a record in order that they work on it. Same-looking check, opposite justification.

`applyBulkAction` never asked the question at all - the single task path asked and the bulk path did
not - so `task_action` asks it before assigning. It also builds the task and assignment pairs from
what the server returns rather than from what the caller sent, because that call acts on the pairs it
is handed and an invented pair would act on somebody else's assignment.

`task_action` covers assigning, accepting and cancelling, because underneath they are one call with a
different word in it, and a list of one task is how a single task is dealt with. The manager's
"status" action only ever sets accepted, so it is offered as `accept` rather than as a status setter
it is not.

## A task belongs to a project, not to a survey

Which is a different access question from everything above it. The survey rule - a survey the caller
could not have listed does not exist - does not answer this one: somebody can be in a project and see
its tasks without being able to read every record those tasks point at. So the task tools ask about
project membership, and where a task is made from a record the record question is asked separately.

`getTasks` takes a task group as a number and says in its own comment that it assumes the check has
already happened, so the check happens in the tool. Asking without a group asks by **organisation**,
which is wider than a person, so the answer is filtered back to the projects the caller is in.

A task and an assignment are not the same thing: the task is the work and the assignment is that work
given to a particular person, so one task can carry several. They are reported together rather than
as two tools, because a task with no assignment and an assignment with no task are both meaningless.
The assignment id comes back beside the task id, and it is the assignment id the write tools take.
`mine` narrows the list to the caller's own work, which is why there is no separate tool for that
either.

## A survey's project is one of its settings

`survey_set_settings` changes everything about a survey that is not its questions, and the project is
one of those things. Moving a survey between projects is one field on the survey, the same kind of
act as renaming it, and a separate tool for it would suggest otherwise. The save re-points the upload
events afterwards, so the monitor still shows the survey's history where the survey now is.

Only the settings named are changed. The underlying save writes the whole row, so everything
unmentioned is read first and written back as it was - a tool that passed a half filled object would
quietly clear every setting the caller did not think to mention. For the same reason a boolean is
changed only when the caller actually named it: absent and false are different things here, and
treating them alike would turn off every option not mentioned.

The before and after of each setting changed goes to the change log and comes back in the answer, so
any of it can be set back.

**This is where `saveSettings` came from.** It lived only in the console's endpoint, some 380 lines of
it, and was extracted into `SurveyManager` so MCP and the survey editor change a survey the same way.
The endpoint now parses, authorises, and calls it.

## Knowing how to do a job, not just what a tool does

`smap://docs/tools` is generated from the registry, so it cannot drift from what is offered.
`smap://docs/tasks` is written by hand and says the thing a list of tools cannot: which order to use
them in, and which jobs need three of them.

It names what is **not** possible as plainly as what is - creating a project, giving someone access to
one, uploading an XLSForm, changing a question's type - so a model does not spend a conversation
hunting for a tool that was never built. A reorganisation needing a new project is therefore honest
about its shape: a person makes the project and adds the people in the console, and the surveys are
moved from here.

## Making a survey, and unmaking one

`survey_create` makes a survey empty or as a copy of one that exists; in Smap those are one call and
the difference is only whether a survey to copy is named, so they are one tool rather than two
wrapping the same manager with the same arguments. A copy takes the design and none of the answers.

**The XLSForm path is deliberately not exposed.** An agent cannot produce a spreadsheet it has never
seen, so a tool taking one would be a tool nothing can call. The way a model builds a form here is
`survey_create` and then `survey_add_question`, which is what the online editor does. Importing an
XLSForm remains something a person does in the console, where they have the file.

`survey_delete` is always the soft delete. The survey and everything submitted to it are kept, the
subscriber erases them after the server's retention period, and until then `survey_undelete` brings
both back. Deleting the data along with the survey is not offered, and neither is the hard erase.
Both tools tell the devices, as the console does, so a form leaves and returns to the phones that
have it rather than lingering until somebody notices.

`survey_undelete` is the one place in this package that asks for deleted surveys. Everywhere else a
deleted survey is not in the caller's list and so does not exist; here it has to be reachable
precisely because it is deleted, and the access rule is otherwise unchanged.

## Two kinds of file, and two kinds of nothing

`survey_media_list` reports the files a survey was published with - images, audio and video used in
questions, and the csv a question reads its choices from. `data_attachments` reports what people
photographed and recorded while answering. Both are files on a survey and they are not the same
thing: one is part of the form and identical for everyone filling it in, the other belongs to a
record and is different every time.

`reference_filter_list` lists **every** connection a survey has to another survey's data, not only
the filtered ones. A source with no filter hands over everything it holds, and that is what somebody
asking this question most needs to see - listing only the filters would show an empty list for a
survey pulling an entire register of people and look reassuring. For the same reason a cap of zero is
reported as "no cap" rather than as the number nought, which reads as the opposite.

Filters belong to the survey group, as roles do, so the group ident is looked up rather than read off
the listed survey: `getSurveys` is a list query and leaves what a list does not need unset, and a
blank ident here would report a survey as having no connections at all.

## Changing a question's type is investigated here and done elsewhere

`survey_check_type_change` reads and never writes. The change itself belongs to a person in the
console, and this exists so the person deciding has the numbers: how many stored answers would not
survive, and which ones. Working that out is what an agent is good at; the act that cannot be undone
stays with whoever is accountable for it.

Two facts get confused and the report keeps them apart.

**A question with no column can change type freely.** That is the exemption, and it is decided by
the column rather than by the published flag.

`published` means the column exists, and it exists for speed: it saves the editor a look at the
results table on every change. The two should always agree. This tool is a read-only diagnostic
making a single `information_schema` lookup, so the speed the flag protects is not in play and the
column can be asked directly - which also makes it the one place that reads both and can say when
they have come apart. A question marked published whose column is merely pending has nothing to lose
either, and a flag that has fallen out of step must never be able to report a column full of answers
as safe to convert.

**Smap does not convert the column.** There is no `ALTER COLUMN ... TYPE` anywhere in Smap, only
`add column`. A type change on a published question updates the definition and leaves the results
column exactly as it was, holding exactly what it held. The type change on a published question is
deliberately permitted - the constraint is commented out in `SurveyManager` - so this succeeds
quietly.

**The damage is in the two directions that follow.** Answers already stored may not survive if
anyone ever does convert that column, which is the count and the examples. And answers the *new*
type produces may not fit the column that is still there - which is the one that bites first, and
the one nobody expects, because it fails at submission time on somebody's device long after the
change appeared to work. A text column takes anything; any other is a column that will start
refusing what it is given.

The conversion test is `pg_input_is_valid`, which answers exactly this question without raising. It
arrived in PostgreSQL 16, so on an older server the tool says the count cannot be given rather than
offering a guess dressed as a number.

Every answer ends the same way: MCP will not make the change, and the alternative that loses nothing
is to add a new question of the wanted type and leave the old one holding its answers.

## Adding a question

`survey_add_question` builds the change set the online editor sends and hands it to
`applyChangeSetArray`, so the question added is the one the editor would have added: the same
sanitising of labels, the same reordering, the same log entry.

The tool's own work is refusing before it starts. The editor has a person looking at the form who can
see that a name is taken or that a choice list does not exist; a model cannot, so the name, the type,
the form and the choice list are checked here and answered in words rather than surfaced as a
constraint violation.

Three things it does not do. It will not add a group or a repeat, because those change the shape of
the form and the tables beneath it and a tool that cannot also place the matching end can leave a
form that will not open. It appends by default rather than inserting, because any other position
renumbers questions the caller did not mention. And it does not stop to ask, because adding a
question can be undone.

Deleting is where the two outcomes differ and the answer says which happened. A question that has
collected answers is soft deleted: the row stays, the results column stays, and the answers stay.
Adding a question of the same name back to the same form **reuses that column**, so the data comes
back with it. A question that never collected anything is removed outright, and nothing is lost that
was not only a definition. A group is refused, because deleting one takes everything inside it.

## A survey is read in parts, not whole

`smap://survey/{ident}/definition` returns everything, which is right for a client that wants the
form in one piece. The tools deliberately do not: `survey_get` answers what shape is this,
`survey_questions` what does it ask, `survey_options` what may be answered, `survey_history` what has
been done to it. A survey of any size spends a model's context quickly, and most questions asked
about a form need one of those four and not the other three.

All of them read through the same access rule as the data tools - a survey the caller could not have
listed does not exist - and with `superUser` false, so an administrator's view is never what comes
back.

Each reads only its own part, through a reader in `McpData` that asks for that part and no more:
`outline` for the settings, languages and forms, `optionLists` for the choices, `questions` for the
design, `SurveyManager.getChangeLog` for the history. There is no call here that loads a whole
survey, and that is deliberate - the coarse one was removed rather than left beside them, because
leaving it there is an invitation to load a form to answer a question about part of it.

The underlying functions are still Smap's own. Where a reader did not exist it was **extracted from
the method that had it inline, and that method now calls the extraction**, so the console and MCP run
one implementation rather than two that drift. That matters more here than the saving: a second way
of reading a design, disagreeing about soft deleted questions or external choices, is a bug waiting
for the day the two are compared.

The shape being corrected is worth naming. Smap's managers were built for a person moving through
screens, so they answer at the granularity of a screenful - `getById` returns forms, questions,
options, styles and labels together because the survey editor needs all of it at once. A model
usually wants one fact, and the coarse call makes it pay for the rest in the context it has to think
in. The same correction was already made on the data side, where the tools go to
`TableDataManager.getPreparedStatement` rather than `DataManager.getDataRecords`.

Deleted questions are not returned. A published question that has been deleted still owns its
results column, so it is a real thing a designer sometimes needs to see, but showing it beside live
questions is a distinction that has to be unmistakable to be safe, and these tools read a form as it
stands.

## Every change says which application made it

A change made through MCP is attributed twice: to the person it was made for, and to the application
that made it. `record_event` carries the agent for a submitted record, `survey_change` for a survey's
design, and both are shown in the console - the record history panel and the changes page - as the
user's name with the application beneath it.

Null means a person did it themselves in the console, which is what most changes are, so the absence
is a fact rather than a gap. The name is resolved through `oauth_client` and falls back to the raw
client id, so withdrawing an application's access does not erase what it did.

`data_audit` and `survey_history` are the same question asked of a record and of a form, and they are
how an agent's work is reviewed without opening the console.

Reversing a survey change is deliberately not part of this. Undo for survey definitions is a future
feature covering the online editor and XLSForm uploads as well as MCP, rather than something built
for MCP alone; what MCP owes in the meantime is a complete and honest record of what it did.

## Repeating groups can be read but not written

`data_query` and `data_get_record` both reach a repeating group: the first through `form`, naming
the group, the second by nesting its rows inside the record. Nothing writes one. `data_submit`
builds a flat instance out of the main form's questions, and `data_update_record` passes 0 as the
sub form key and null as the group form, which is to say the top level record and the main form.

The asymmetry is worth stating because it does not announce itself. A caller who has just read an
answer out of a repeat, and asks to change it, is told the survey has no question of that name -
true of the main form, and misleading about the survey. Addressing a repeat needs a row key as well
as a question name, since the whole point of a repeat is that the same question holds several
answers, and that argument does not exist on these tools yet.

Untested rather than decided: no survey reachable during the write tests had a repeat at all, so
this is what the code says, not what a run showed.

## History is per thread, not per submission

`data_audit` returns a record's history: the submission, every later change with old and new values,
who made it, and any task or notification that touched it. Correcting a record writes a new instance
into the same thread, so asking about any instance returns the whole story rather than that
instance's part in it. The console reaches the same history through a survey level check; here the
row filters are applied first, because the history of a record is the record.

## Counting and grouping happen in the database

Smap has no server side aggregation over results: the console fetches rows and adds them up in the
browser. That is fine for a browser and useless for an agent, which would have to read every record
into a model's context to count them. `data_count` and `data_aggregate` do the arithmetic in
Postgres and return the answer.

They do not share the read path's query builder. `TableDataManager.getPreparedStatement` assembles
its SELECT list and its WHERE clause in one pass, so reusing it for a different SELECT would have
meant restructuring a method the console and the REST API both depend on, to add something that is
off by default. `DataAggregateManager` instead builds its restriction from the same primitives that
method uses - `RoleManager` row filters, `SqlFrag` for the caller's own filter, `getDateRange`, the
`_bad` clause - so the two agree because they are made of the same parts. The assembly is
duplicated; the access logic is not.

Column names cannot be bound as parameters, so a name that arrives from a caller is resolved against
the caller's own column list and the stored name is used, never the string that was sent. A name that
does not resolve is refused. The column list is the role filtered one, so a question a role hides is
not groupable either.

Two kinds of column resolve but still cannot be grouped, and both say so rather than failing oddly.
Some are computed when data is read and have no column behind them - survey duration is one - so
grouping by them names something the database has never heard of. Geometry is stored as PostGIS
binary, so grouping by it returns unreadable hex, and every location is distinct anyway, so it would
not summarise anything.

An answer that was never given comes back as one group with an empty name, because null and empty
mean the same thing in survey results and a null would otherwise reach the reader as a count
attached to no group at all.

Both are top level form only for now. Aggregating across a repeating group needs the join tree
`QueryManager` builds, which is worth doing when something asks for it.

## Limits are MCP's own

`mcp_max_rows` on the server bounds how many rows one tool call returns. It is a separate setting
from `api_max_records` and never falls back to it, because the two answer different questions: the
API limit bounds what a program will page through, and is reasonably left unset since a program can
be trusted to ask again, whereas this one bounds what goes into a model's context in a single reply,
where an unbounded answer is not a large answer but a failed one. Zero means the built in default of
1000, not "no limit", so there is no configuration in which a tool is unbounded.

Reaching the limit is always visible. `data_query` returns `next_cursor` when the rest can be paged
to and `truncated` when it cannot.

It bounds **data**, and deliberately not a survey's design. `survey_questions`, `survey_options` and
`survey_history` return everything, because the two are not the same kind of thing. The number of
records in a survey is unbounded and a caller asking for all of them rarely means it; a form and its
history are bounded by the form itself. A half read form is worse than none - the reader has no way
to tell that a question they cannot see is missing rather than absent, and a partial history hides
exactly the change someone went looking for.

## The API suspension switch does not apply

A user whose API access is suspended can still use MCP. The suspension governs the v1 and v2 REST
API, which is a different surface with different credentials, and MCP grants are withdrawn on their
own terms: by removing the `mcp access` group, by revoking the token at **AI access**, or by turning
the server setting off, which stops live tokens on the next request. Data reads go through
`TableDataManager` directly and so never pass the API's check.

## Records are addressed by instance id

Never by `prikey`. That is sequential and can be guessed by counting, so accepting one would let a
caller walk a table they were never shown. `data_query` pages with an opaque `next_cursor` that is
derived from the key, which reveals nothing: it only moves forward through rows the row filters have
already allowed.

## Managing access

A user reviews and withdraws the applications they have authorised at **AI access** in the profile
menu, and can mint a token there for something with no browser to authorise with. A security
manager, organisation administrator or server owner sees and can withdraw access across their
organisation.
