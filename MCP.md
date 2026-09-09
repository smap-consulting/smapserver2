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
| `topic_list` | read | analyst, admin, view data, manage | done |

## Resources

| URI | What |
| --- | --- |
| `smap://docs/tools` | What this connection can do, generated from the registry |
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
| Surveys, list and structure | `survey_list`, `survey_submission_counts` | read only |
| Data | `data_query`, `data_get_record`, `data_count`, `data_attachments`, `data_audit`, `data_delete_record`, `data_restore_record`, `data_submit`, `data_update_record`, `data_bulk_update`, `data_bulk_undo` | read and write |
| Analysis | `data_aggregate` | read only |
| Projects | `project_list` | read only |
| Topics / bundles | `topic_list` | read only |
| Survey design | — | not started |
| Tasks and assignments | — | not started |
| Cases and workflow | — | not started |
| Notifications and messaging | — | not started |
| Reporting and monitoring | — | not started |
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
