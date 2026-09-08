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
| `smap:write` | Surveys, data, tasks, cases, mailout preparation | by step up |
| `smap:admin` | Users, projects, organisation records | by step up |
| `smap:access` | Who can reach what | by step up, never remembered |
| `smap:server` | Server settings | by step up |
| `smap:privacy` | Subject access exports | by step up |

Only `smap:read` is advertised. A tool needing more answers `403` naming the scope, and the client
re-authorises for that as well as what it already had, so it never holds a permission it has not
needed.

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
| Data | `data_query`, `data_get_record`, `data_count` | read only |
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
