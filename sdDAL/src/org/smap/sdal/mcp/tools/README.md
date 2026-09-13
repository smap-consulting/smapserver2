# Writing an MCP tool

Rules this code learned by getting them wrong. Each one cost a defect, most of them silent, and
several were found only by running the tool against a real server rather than by reading it.

## 1. An "update" in Smap replaces the lists attached to the thing

The console's save methods were written for screens that show everything and submit everything back.
So `updateProject` removes every member before re-adding whoever was listed, `updateUser` rewrites
groups, projects and roles from the object it was handed, `setUsersForRole` deletes every holder
before it looks at the list it was given - **and does that even when the list is null**.

Hand any of them a partly filled object and it does not leave the rest alone. It writes what the
object happens to hold, which for an unset boolean is `false` and for an absent list is nothing.

Correcting a misspelt surname would have removed somebody's access to everything. Renaming a role
would have taken it from every holder. Both would have reported success.

**So: do not call the console's save from a tool.** Add a narrow manager method that writes the
columns you named, next to the one you are avoiding, with a comment saying why. There are six:

| Manager | Method | Instead of |
| --- | --- | --- |
| `ProjectManager` | `updateProject` | the save that empties membership |
| `UserManager` | `updateUserDetails` | `updateUser` |
| `UserManager` | `setUserGroups` / `setUserProjects` / `setUserRoles` | one list at a time |
| `OrganisationManager` | `updateOrganisationDetails` | 37 columns from one object |
| `RoleManager` | `updateRoleDetails` | the update that empties the role |
| `ServerManager` | `updateOperationalLimits` | the save that rewrites every credential |

Where the underlying function already handles one list at a time - `insertUserGroupsProjects` leaves
a null list alone - use it rather than writing a second implementation of the rules.

## 2. Gson drops null fields

A null field is **absent** from the JSON, not null in it. An absent key does not read as "there
isn't one", it reads as "this tool does not report that", and the next question gets asked somewhere
else.

Four separate defects: a survey's `project` missing so `task_list` looked fine and `task_action`
broke, `website` missing from an organisation so the stated reversal could not restore it, and
others. **Emit `""` rather than null** for a string you are reporting.

## 3. An empty collection may mean "not loaded"

The opposite direction of the same problem, and worse because it looks like an answer.

`getUserList` builds an empty roles list and fills it **only** when told the caller is an
organisation administrator or a security manager. Ask any other way and every user comes back holding
no roles. A tool that trusted that removed a real role during testing and reported `previous: []`.

**Query the thing you are about to replace**, directly, rather than trusting a model object that was
populated for a different purpose.

## 4. Read back what you wrote

Report what the database holds, not what the request asked for. Rules below you - group hierarchies,
row filters, triggers - may have kept, skipped or changed part of it. `user_set_groups` re-reads
`user_group` after writing for exactly this reason.

## 5. Check before you call, if the manager skips silently

`insertUserGroupsProjects` ignores a group the caller may not grant, and ignores the role list
entirely for anybody who is not an organisation administrator or a security manager. Passing a list
straight through would report success on a change that was half applied - somebody believing a
permission was given when it was not.

Check every element first and refuse the whole call. Partly applied is the one outcome worth
avoiding.

## 6. Say what changed, not that it worked

Every mutating tool returns what it replaced, and `getReversal()` says how to put it back. That is
what makes the reversal claim true rather than decorative - and it only works if `previous` is
complete, which is rules 2 and 3 again.

## 7. Transactions belong to the manager

`deleteProjects` assumed its caller had opened one. With autocommit on, each statement committed by
itself, so a delete that failed half way had already removed what it got to while reporting that
nothing happened. A manager that needs a transaction opens and commits its own, and restores
autocommit in a `finally`.

The pool does not reset connection state on return: leaving one in manual-commit mode hands it to the
next request that way.

## 8. Nothing sends

No email, no SMS, no webhook, no mailout. A notification can be created switched off, enabled,
disabled or deleted; sending is a person's act in the console. If a tool could cause a message to go
out, it says so and asks first - `survey_submission_effects` exists to be asked before `data_submit`.

## 9. Asking needs a fallback

`ask()` uses elicitation, which the client declares. **The client most people use does not declare
it**, so `ask()` returns a refusal rather than a question, and a tool gated behind it is unreachable.

Offer `acknowledge` - the caller restates the specifics - and say plainly in the answer that it is
weaker: evidence the call was meant, not evidence somebody agreed.

## 10. Identifiers in SQL come from the schema

Table and column names cannot be bound, so they must come from the survey definition and be checked
with `hasColumn`, never from the caller's string. Values are always bound. Caller filters go through
`SqlFrag`, which parses to placeholders and validates every column against the survey.

And do not `prepareStatement` in a loop - one statement per form, not per column.

## 11. Scope and group are different refusals

A missing **group** is permanent: the tool is reported unknown, consistent with never being listed.
A missing **scope** is recoverable: a 403 challenge names it and the client steps up.

But a scope this server will never issue is neither - it is hidden, because a tool that can never be
run is better never seen.

## 12. The dispatcher holds the invariants

A tool that changes somebody's access names the argument carrying whose, via
`getSelfProtectedArgument()`, and the dispatcher refuses when that names the caller. It is there
rather than in each tool because the one place it must not be is optional.

## 13. Descriptions are context, on every call

Keep them to what changes what the agent does: what the tool returns, the warnings, and which tool to
use instead. Procedure belongs in a prompt (`McpPrompts`), argument detail in the argument's own
description. The catalogue is about 3,400 tokens; it was 5,200 before that rule was applied.
