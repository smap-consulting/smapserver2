# Workflow Page Design Document

## 1. Overview

**Approach**
This page will be implemented incrementally in multiple steps.  These steps will implement the partially specification and
may update it.

The styling should be similar to n8n.

**What is this page for?**
This page has 2 purposes:
1. Allow a user to specify the steps and their sequence to complete a piece of work
2. Show the status of work, specifically the number of jobs that are in each workflow item

**Users / Roles**
The page can be accessed by users with security groups:
- ADMIN
- ANALYST

All workflow items will be specified at the survey, project or bundle level. The user will see the items that
they have can access according to the projects and roles they have.  As a bundle consists of a list of surveys then
if the user can access any of those surveys they will be able to see and modify bundle work items.

---

## 2. User Stories

1. As the supervisor of a team of people processing data records, I can select the workflow menu to view each step in the workflows that I have access to and how these steps are linked so that I can easily understand what is required to process each incoming record

---

## 3. Page Layout & UI

**Where does this page live?**
- URL: `/app/tasks/workflow.html`
- Module: `prop-smapserver/tasks`
- Entry file: `WebContent/js/workflow.js` → bundled to `build/js/workflow.bundle.js`

**Navigation changes**
- `taskManagement.html` — "Workflow" link added to navbar
- `notifications.html` — "Workflow" link added to navbar
- The workflow page navbar has direct links to Tasks and Notifications pages
- Full system-wide menu replacement (tasks→workflow in modules dropdown) deferred to a later iteration

**Layout description**
- Navbar: brand "Workflow" + modules dropdown + links to Tasks, Notifications, Highlight dropdown, Reset, Refresh, Help, Profile
- Main area: full-viewport dark canvas (`background: #1a1a2e`) **[TODO: not yet applied in workflow.html]** with:
  - SVG overlay (`id="wf-arrows"`) for bezier connection arrows
  - Node container (`id="wf-nodes"`) for absolutely-positioned cards

**Key UI components**
| Component | Type | Purpose |
|-----------|------|---------|
| Trigger node card | Absolutely positioned div | Shows trigger type + survey name |
| Action node card | Absolutely positioned div | Shows action type + details |
| SVG bezier arrow | `<path>` in SVG overlay | Connects trigger to action |

**Node layout (iteration 2)**

Node positions are computed server-side in `WorkflowManager.applyLayout()` and returned as `x`/`y` pixel values on each `WorkflowItem`. The client uses these values directly.

*Default layout algorithm (server)*
- All nodes start at column 0.
- For each link `from → to`, the destination column is set to `max(current column, source column + 1)`. This is iterated until stable, giving a left-to-right topological ordering.
- Within each column, nodes are stacked vertically in insertion order.
- Pixel coordinates: `x = column × X_SPACING`, `y = Y_OFFSET + row × Y_SPACING` where `X_SPACING = 320px` (card width 240px + 80px gap), `Y_SPACING = 150px`, `Y_OFFSET = 40px`.
- Result: a chain `form → decision → task` lands at x = 0, 320, 640px. A direct link `form → task` (no decision) lands at x = 0, 320px.

*User-saved positions*
- Users can drag any node to a new position. Positions are saved per user per organisation in the `workflow_node_positions` table (`user_ident`, `o_id`, `positions jsonb`).
- On load, the server merges saved positions over the default layout before returning items to the client.
- Saving all positions at once (not per-node) means orphaned entries for deleted nodes are automatically removed on the next save.
- Positions are **auto-saved** after each drag (`PUT /surveyKPI/workflow/positions` with the full positions map).
- A **Reset** nav-link sends `DELETE /surveyKPI/workflow/positions`, deleting the saved row and reverting to the server-computed defaults.

*Client drag behaviour*
- Each node card is draggable via `mousedown`/`mousemove`/`mouseup` on the document.
- SVG arrows are redrawn live during drag.
- The SVG dimensions are updated after each drag to cover the full node extent.

---

## 4. Data Model

Smap already supports workflows.  These are constructed by specifying notifications which can assign tasks, assign cases send emails and
so on based on an event.  This new feature is a new view of that existing functionality.  Hence there will be few new tables.

### workitem

workitems are the visual components shown on the workflow page.  They are connected together to form a workflow.

**WorkItem Types**

| Type | Role   | Name Source    | Edit / Delete / Create                                                                                                                               |
|------|--------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| form | form   | Survey Name    | No. Created automatically when a survey with `data_survey=true, hide_on_device=false` is used as a source. |
| task | form   | Survey Name    | TaskGroup(s) that link another workitem of type form to this one. If there are more than one then select the one to edit/delete based on the filter. |
| case | form   | Survey Name    | Notifications(s) that link another workitem of type form to this one. Again multiple notifications will be distinguished by the filter.              |
| decision | decision | None           | No                                                                                                                                                   |
| periodic | trigger | Notification Name | Ignore for now                                                                                                                                       |
| reminder | trigger | Notification Name | Ignore for now                                                                                                                                       |
| email | notification | Notification Name | Notification that links another workitem to this email                                                                                               |
| sms | notification | Notification Name | Notification that links another workitem to this sms                                                                                                 |

**Sources**

workitems to be displayed on the workflow page should be obtained
1.  The existing notifications table contains the specification of most workitems.
Notifications are stored in the table **forward**
2. The existing **task_group** table.  This contains submission work item types (source_s_id) and task work item types (target_s_id)

Notifications commonly contain two workitems that will form the core workflow functionality
 - a trigger (such as submission of data from a form)
 - an action (such as create a task or case and assign it).

**forward table key columns (discovered)**
- `id` — primary key
- `s_id` — survey id (nullable if bundle or project level)
- `bundle`, `bundle_ident` — bundle association
- `p_id` — project id
- `trigger` — trigger type: `submission`, `periodic`, `reminder`, `server_calc`, etc.
- `target` — action type: `task`, `escalate` (case), `email`, `sms`, `server_calc`, `forward`, etc.
- `name` — notification name
- `enabled` — boolean
- `notify_details` — JSON details of the action; for `escalate` targets, `notify_details->>'survey_case'` is the case survey ident
- `filter` — submission filter expression
- `remote_user` — assignee for task/case targets: a username, role name, comma-separated emails, `_submitter` (person who submitted), or `_data` (taken from a form question)
- `remote_host`, `remote_password` — for forward targets
- `wf_prev_node_id` *(iteration 2)* — the node ID of the predecessor workflow step that this record was created from (e.g. `form:s:123`, `task:s:456:a:user`). Set by the Add Step dialog. NULL for records not created via the workflow canvas; those fall back to the legacy inference logic in WorkflowManager.

**task_group table key columns**
- `tg_id` — primary key
- `source_s_id` — survey whose submissions trigger this task group
- `target_s_id` — survey used as the task form
- `rule` — JSON (`AssignFromSurvey`); key sub-fields: `assign_data` (From Data), `emails`, `role_id`, `user_id`, `filter` (decision rule)
- `p_id` — project id
- `wf_prev_node_id` *(iteration 2)* — the node ID of the predecessor workflow step (same semantics as `forward.wf_prev_node_id`)

**Assignee display** (task and case workitems)

A unique task or case workitem is identified by the triple **(type, survey, assignee)**. Two items with the same survey and assignee are shown as one node. The assignee is derived as follows:

| Source | Raw value | Displayed as |
|--------|-----------|--------------|
| `forward.remote_user` | `_submitter` | Submitter |
| `forward.remote_user` | `_data` | From Data |
| `forward.remote_user` | anything else | the raw value (username, role, emails) |
| `task_group.rule.assign_data` | non-empty | From Data |
| `task_group.rule.emails` | non-empty | the email string |
| `task_group.rule.role_id` | > 0 | role name (DB lookup) |
| `task_group.rule.user_id` | > 0 | user ident (DB lookup) |

The assignee is shown below the survey name in the workitem card.

**Iteration 1: no new tables.** The `forward` table provides all needed data.

**Iteration 2: explicit sequence storage.** Two columns are added to store the user-defined workflow sequence directly in the backing tables (see §13).

**Future iterations may add:**
- Monitoring counts per workflow step (see §14)

**decision work items**
 Decision nodes appear between two other non decision nodes.  For example a workitem node of type "form" and a workitem node of type "task".  A decision node is required if there is a rule that          
determines whether the notification is fired or the task is created. In the forward table the rule is in the column "filter".  In the task_group table the rule is in a JSON object in the column "rule". 
The task group rule can be found in the "filter" property of that JSON object. The Java class AssignFromSurvey defines the json. 
If there is no rule in the notification or task group that links two nodes then no        
decision node is shown. If there is a rule then the decision node is shown.

**task_group chaining and source node rules**

A task workitem can itself be the trigger for further task_groups (completing a task produces a submission that starts the next step). `WorkflowManager` handles this with a two-sub-pass approach for `task_group` records:

- **Sub-pass i** — create all task destination nodes and record them in `surveyItemKeys[targetSId]`. No links are created here.
- **Sub-pass ii** — for each task_group, resolve its source nodes and create links. The source survey's `data_survey` and `hide_on_device` columns (from the `survey` table) drive which nodes appear as sources:

**Source node resolution rules (sub-pass ii)**

| Condition | Source node(s) created / used |
|-----------|-------------------------------|
| Existing `task:s:{sourceSId}:a:*` nodes in itemMap | Link from each of those task nodes |
| Source survey has `data_survey=true` AND `hide_on_device=false` | Also create (or reuse) a `form:s:{sourceSId}` node and link from it |
| Neither of the above | Create a fallback `form:s:{sourceSId}` node and link from it |

The `data_survey=true, hide_on_device=false` condition identifies a visible, standalone data-collection form — one that users fill in ad-hoc as well as potentially completing as a task. Such a form warrants its own Form workitem on the canvas in addition to any task node. Surveys that are hidden (`hide_on_device=true`) or non-data (`data_survey=false`) are oversight-only forms and do not get a standalone Form workitem.

Multiple source nodes produce multiple arrows converging on the same destination (or decision) node. This also ensures that an alphabetical ordering of task_groups cannot cause a spurious duplicate form node.

**bundle notifications**
A notification applies to a bundle when `forward.bundle = true`. Processing is two-pass:

- **Pass 1** — all non-bundle `forward` records and `task_group` records are processed first (with the sub-pass approach above for task_groups). As each node is created, its key is recorded in a `surveyItemKeys` map (`sId → [node keys]`), covering both source and destination sides of each notification chain.
- **Pass 2** — bundle notifications are processed after pass 1. For each bundle member survey (identified by `survey.group_survey_ident = forward.bundle_ident`), all existing nodes recorded in `surveyItemKeys` for that survey are used as source nodes. If a member survey has no existing nodes it gets a fresh `form:s:<id>` node. All source nodes connect independently to the same downstream decision/action node(s), so multiple arrows converge on those nodes. If no accessible member surveys are found at all, a single fallback node labelled with the bundle ident is shown.

A survey that is a bundle member may also have its own survey-specific notifications; those are rendered as ordinary single-source connections and share the same canvas nodes, which are then also wired as sources for any bundle notification.
---

## 5. REST API

| Method | Path | Auth roles | Description |
|--------|------|------------|-------------|
| GET | `/surveyKPI/workflow/items` | ANALYST, ADMIN | Returns `WorkflowData` (items + links) with server-computed x/y merged with any user-saved positions |
| PUT | `/surveyKPI/workflow/positions` | ANALYST, ADMIN | Saves full positions map `{nodeId: {x,y}}` for current user+org |
| DELETE | `/surveyKPI/workflow/positions` | ANALYST, ADMIN | Deletes saved positions for current user+org, reverting to defaults |
| GET | `/surveyKPI/workflow/edit/surveys` | ANALYST, ADMIN | Lists surveys accessible to the user (for dropdowns) |
| GET | `/surveyKPI/workflow/edit/notifications?ids=1,2` | ANALYST, ADMIN | Returns simplified notification records for the edit drawer |
| PUT | `/surveyKPI/workflow/edit/notifications` | ANALYST, ADMIN | Batch-saves notification edits (shared fields + per-connection filters) |
| DELETE | `/surveyKPI/workflow/edit/notification/{id}` | ANALYST, ADMIN | Deletes a forward record |
| POST | `/surveyKPI/workflow/edit/notification` | ANALYST, ADMIN | Creates a new forward record. Accepts optional `wfPrevNodeId` field — stored in `forward.wf_prev_node_id` |
| GET | `/surveyKPI/workflow/edit/taskgroups?ids=1,2` | ANALYST, ADMIN | Returns simplified task group records for the edit drawer |
| PUT | `/surveyKPI/workflow/edit/taskgroups` | ANALYST, ADMIN | Batch-saves task group edits (name + per-connection filters) |
| DELETE | `/surveyKPI/workflow/edit/taskgroup/{id}` | ANALYST, ADMIN | Deletes a task group and any linked forward records |
| POST | `/surveyKPI/workflow/edit/taskgroup` | ANALYST, ADMIN | Creates a new task group. Accepts optional `wfPrevNodeId` field — stored in `task_group.wf_prev_node_id` |

---

## 6. Backend Logic

**`WorkflowManager.java`** (`sdDAL`) — core logic:
- `getWorkflowItems(sd, user)` — queries `forward` and `task_group`, builds nodes and links, calls `applyLayout()` then `mergeUserPositions()`.
- `applyLayout()` — computes default x/y positions using topological column assignment (see §3 Node layout).
- `mergeUserPositions()` — loads saved positions from `workflow_node_positions` for the user's current org and overwrites x/y on matching nodes.
- `savePositions(sd, user, positions)` — upserts the full positions map as JSONB.
- `resetPositions(sd, user)` — deletes the saved row.

**DAG construction priority (iteration 2)**

When building nodes and links, `WorkflowManager` follows this priority:

1. **Explicit links** — if a `forward` or `task_group` record has `wf_prev_node_id` set, use it directly to create the link from that predecessor node to the destination node. The predecessor node is looked up in the already-built `itemMap`; if not yet present (e.g. the record was created before its predecessor), a placeholder form node is created and reconciled during layout.
2. **Inferred links** (legacy fallback) — if `wf_prev_node_id` is NULL, apply the existing multi-pass inference logic (source/target survey matching, `surveyItemKeys` map, bundle pass). This preserves backwards compatibility for pre-existing notifications and task groups.

This means records created via the workflow canvas (which always set `wf_prev_node_id`) will render their sequence exactly as the user designed it. Legacy records continue to render as before.

**`Workflow.java`** (`surveyKPI`) — REST endpoints delegating to `WorkflowManager`.

**Access scoping** — nodes are filtered to projects/surveys/bundles the user has access to (via `user_project` join), consistent with the existing notifications scoping.

---

## 7. Frontend

**Technology**
- ES modules compiled via webpack 5
- jQuery 3.5.1 + Bootstrap 5.3.8
- Vanilla `fetch` API for REST calls
- SVG for connection arrows (no external graph library in iteration 1)

**Files created / modified**
| File | Change |
|------|--------|
| `tasks/WebContent/workflow.html` | New — workflow page |
| `tasks/WebContent/js/workflow.js` | New — page JS entry point |
| `tasks/webpack.config.js` | Added `workflow` entry |
| `tasks/WebContent/taskManagement.html` | Added Workflow nav link |
| `fieldManagerClient/WebContent/notifications.html` | Added Workflow nav link |

**State management**
- `gData` — current `WorkflowData` (items + links)
- `gPositions` — `{id: {x,y}}` map updated live during drag

**API call sequence**
1. Page load → `getLoggedInUser(loadWorkflow, ...)` — sets up user/locale
2. `loadWorkflow()` → `fetch /surveyKPI/workflow/items`
3. Response → `renderWorkflow(data)` — clears and redraws canvas

**Layout**
See §3 Node layout for the full description. The client uses `item.x` and `item.y` from the server response directly — no client-side layout calculation.

**Drag and save**
- `gPositions` — module-level map `{id: {x, y}}` updated live during drag.
- `makeDraggable(el)` — attaches drag handlers; calls `drawArrows()` on each move.
- `saveLayout()` — `PUT /surveyKPI/workflow/positions` with `gPositions`.
- `resetLayout()` — `DELETE /surveyKPI/workflow/positions` then reloads.
---

## 8. Notifications / Events

Not applicable for iteration 1 (read-only view).

---

## 9. Permissions & Security

- Page accessible to ADMIN and ANALYST roles (same as notifications page)
- Data scoped to projects the user has access to (enforced by existing `NotificationList` endpoint)
- No new permission requirements in iteration 1

---

## 10. Migration / Deployment

**New table** (added to `setup/deploy/sd.sql`):
```sql
CREATE TABLE IF NOT EXISTS workflow_node_positions (
    user_ident   text,
    o_id         integer references organisation(id) on delete cascade,
    positions    jsonb,
    PRIMARY KEY (user_ident, o_id)
);
```

**Iteration 2 schema changes** (added to `setup/deploy/sd.sql`):
```sql
ALTER TABLE forward      ADD COLUMN IF NOT EXISTS wf_prev_node_id text;
ALTER TABLE task_group   ADD COLUMN IF NOT EXISTS wf_prev_node_id text;
```

No data migration required. Existing records with `wf_prev_node_id = NULL` continue to use the legacy inference path in WorkflowManager.

Build step required:
```bash
cd /path/to/prop-smapserver/tasks
npm run build
```
Generates `WebContent/build/js/workflow.bundle.js`.

---

## 11. Edit / Create / Delete

**UX model**

- Each editable node card (task, case, email, sms) shows a **✏ Edit** icon button in the card header.
- Clicking a **decision node** follows its outgoing link and opens the downstream target node's drawer.
- A floating **+** button (bottom-right of canvas) opens the Add Step dialog.

**Edit drawer** (right-side slide-in panel, 360px)
- Header: type icon + type label + × close.
- Body: type-specific fields (see table below).
- Condition section (amber background matching decision card): one filter input per backing record, labelled "From: [source survey name]" when multiple sources exist.
- Footer: **Save** | **Delete** | **Advanced ↗** (links to `/app/fieldManager/notifications.html` for forward-backed nodes, `/app/tasks/taskManagement.html` for task-group-backed nodes).

**Edit behaviour with multiple backing records**

When a node is backed by N notifications/task groups (same target, different sources), edits to shared fields (name, assignee, enabled) are applied to all N records. The filter/condition is per-record.

**Add Step dialog** (Bootstrap modal)

The dialog adds a **target** (action) element. The **trigger** is derived from the currently selected workflow item on the canvas — the user does not choose a trigger manually.

When creating the backing `forward` or `task_group` record, all trigger-side columns (e.g. `s_id`, `bundle`, `bundle_ident`, `p_id`, `trigger`) are pre-populated from the first backing record of the selected workflow item. If the selected item has more than one backing record, the first record is used.

*(Iteration 2)* The **node ID of the selected trigger item** is also passed as `wfPrevNodeId` in the POST body and stored in `forward.wf_prev_node_id` / `task_group.wf_prev_node_id`. This records the explicit sequence position and is used by WorkflowManager to build the DAG (see §6).

1. Ensure a workflow item is selected on the canvas (its node is highlighted) — this becomes the trigger.
2. Choose action type: Task | Case | Email | SMS
3. Fill in type-specific fields (assignee, email addresses, etc.) + optional condition
4. **Create** → `POST /surveyKPI/workflow/edit/notification` (or `/taskgroup`) with `wfPrevNodeId` set → canvas reloads

**Delete**

Delete button in the drawer shows a confirmation modal listing the number of records to be removed. Confirming deletes all backing forward/task-group records. The canvas reloads.

**Drag-to-connect** deferred.

**Field sets per node type**

| Type | Editable in drawer | Read-only in drawer |
|------|--------------------|---------------------|
| task | Label, Assign to, Enabled | — |
| case | Label, Assign to, Enabled | — |
| email | Label, To, Subject, Message, Enabled | — |
| sms | Label, To, Message, Enabled | — |
| task (task_group backed) | Label | Assignee (use Advanced) |
| form | — (no drawer) | — |
| decision | — (opens target drawer) | — |

**WorkflowItem model additions**

```java
public List<Integer> fwdIds = new ArrayList<>();  // forward record IDs backing this node
public List<Integer> tgIds  = new ArrayList<>();  // task_group record IDs backing this node
```

---

---

## 13. Explicit Sequence Storage (Iteration 2)

### Problem

The workflow diagram is currently built by inferring the DAG from `task_group` and `forward` table relationships (source/target survey IDs, trigger types). This works for simple cases but has two limitations:

1. If survey A has two forward records — one assigning to person Y and one to person Z — both are inferred as parallel branches from A. There is no way to express the intended chain A → Y → Z.
2. Without a stored link, there is no way to know which workflow step a running task belongs to (needed for future monitoring).

### Solution

Add `wf_prev_node_id text` to both `forward` and `task_group`. This column stores the node ID of the predecessor workflow step and is set whenever a record is created via the workflow canvas "Add Step" dialog.

**Node ID format** (stable, reconstructible):

| Record source | Node ID format |
|---------------|----------------|
| Form (workflow_start) | `form:s:{sId}` |
| Task from task_group | `task:s:{targetSId}:a:{assigneeKey}` |
| Case from forward | `case:s:{caseSurveyIdent}:a:{assigneeKey}` |
| Email from forward | `email:f:{fId}` |
| SMS from forward | `sms:f:{fId}` |
| SharePoint from forward | `sharepoint_list:f:{fId}` |

### WorkflowManager DAG construction changes

When processing a `forward` or `task_group` record that has `wf_prev_node_id` set:

1. Look up the predecessor node in `itemMap` by its stored ID.
2. If found: create the link directly (`prevNode → thisNode`) instead of running the inference logic for that record.
3. If not found (predecessor not yet in map): create the link anyway — the predecessor node will be added when its own record is processed. The topological pass in `applyLayout()` will correctly position it.
4. If `wf_prev_node_id` is NULL: run the existing inference logic unchanged (backwards compatible).

Records created via the workflow canvas will always have `wf_prev_node_id` set (to `gSelectedNode.dataset.id` at creation time). Legacy records without the column set continue to render as before.

### Frontend changes

`submitAddStep()` in `workflow.js` — add `wfPrevNodeId: gSelectedNode ? gSelectedNode.dataset.id : null` to every POST payload (both notification and taskgroup paths). No other frontend changes required.

---

## 14. Workflow Monitoring (Deferred — Iteration 3)

The second purpose of the workflow page is to show how many active tasks and cases are at each step. This is deferred to a later iteration.

**Planned approach (high level):**

- New table `workflow_step_counts (o_id, node_id, active_count, reset_count)` — per-org, per-node count, maintained incrementally.
- `tasks` table gains `workflow_node_id text` — set at task creation time using the same node-key formula as `wf_prev_node_id` above. Links each task instance to its workflow step.
- The same code paths that call `UserManager.incrementTotalTasks()` / `decrementTotalTasks()` also call equivalent methods on `workflow_step_counts`.
- `reset_count=true` triggers a full recalculation on next read (mirroring the `users.reset_total_tasks` pattern). Recalculation is org-scoped and only scans users with `total_tasks > 0`.
- `GET /workflow/items` merges step counts into `WorkflowItem.activeCount`.
- A "Monitor" nav-link on the workflow page toggles count badges on node cards, auto-refreshing every 30 seconds.

---

## 12. Open Questions

- Should the modules dropdown in all pages replace "Tasks" with "Workflow" as the primary entry point? (deferred — large change across many pages)
- Should enabled/disabled notifications be visually distinguished on the canvas?
- Should bundles and project-level notifications have a different visual grouping than survey-level ones?
- Should the Add Step dialog support creating task_group records (full task assignment) in addition to forward records? Currently creates forward records only; task groups require the full task management editor.
