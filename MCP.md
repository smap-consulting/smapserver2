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
| `survey_data` | read | analyst, admin, view data | done |
| `topic_list` | read | analyst, admin, view data, manage | done |

## Resources

| URI | What |
| --- | --- |
| `smap://docs/tools` | What this connection can do, generated from the registry |
| `smap://survey/{ident}/definition` | One survey's questions, options and settings |
| `smap://record/{ident}/{instanceId}` | One submitted record and its repeating groups |
| `smap://attachment/{ident}/{instanceId}/{question}` | A photo, audio or other file on a record, returned as bytes |

Survey idents complete through `completion/complete`, so a client can offer them rather than having
the model guess.

Attachments are served here rather than linked, because the URLs in survey data point at
`/app/attachments`, which is behind form authentication and cannot be fetched by a client holding a
bearer token. The client names the survey, the record and the question and never supplies a path,
so there is nothing to traverse with; the server looks the stored filename up itself and checks it
resolves inside the attachments directory. This is stricter than `/app/attachments`, which
authenticates the caller but does not check they may see the record. Anything over 5MB is reported
with a URL rather than returned, because base64 costs a third again and it all has to fit in a
model's context.

## Coverage against the console

Filled in as each functional increment lands. An area with no tools yet is not a decision, only work
not done.

| Console area | Tools | State |
| --- | --- | --- |
| Surveys, list and structure | `survey_list`, `survey_data`, `survey_submission_counts` | read only |
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

## Records are addressed by instance id

Never by `prikey`. That is sequential and can be guessed by counting, so accepting one would let a
caller walk a table they were never shown.

## Managing access

A user reviews and withdraws the applications they have authorised at **AI access** in the profile
menu, and can mint a token there for something with no browser to authorise with. A security
manager, organisation administrator or server owner sees and can withdraw access across their
organisation.
