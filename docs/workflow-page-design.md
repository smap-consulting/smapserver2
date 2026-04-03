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
- Navbar: brand "Workflow" + inline project selector + links to Tasks, Notifications, Help, Profile
- Main area: full-viewport dark canvas (`background: #1a1a2e`) with:
  - SVG overlay (`id="wf-arrows"`) for bezier connection arrows
  - Node container (`id="wf-nodes"`) for absolutely-positioned cards

**Key UI components**
| Component | Type | Purpose |
|-----------|------|---------|
| Project selector | `<select class="project_list">` in navbar | Filter workflow by project |
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
- A **Save** button sends `PUT /surveyKPI/workflow/positions` with the full positions map.
- A **Reset** button sends `DELETE /surveyKPI/workflow/positions`, deleting the saved row and reverting to the server-computed defaults.

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

| Type | Role   | Name Source       |
|------|--------|-------------------|
| form | form   | Survey Name       |
| task | form   | Survey Name       |
| case | form   | Survey Name       |
| decision | decision | None              |
| periodic | trigger | Notification Name |
| reminder | trigger | Notification Name |
| email | notification | Notification Name |
| sms | notification | Notification Name |

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
- `target` — action type: `task`, `case`, `email`, `sms`, `server_calc`, `forward`, etc.
- `name` — notification name
- `enabled` — boolean
- `notify_details` — JSON details of the action
- `filter` — submission filter expression
- `remote_user`, `remote_host`, `remote_password` — for forward targets

**Iteration 1: no new tables.** The `forward` table provides all needed data.

**Future iterations may add:**
- `work_item` table for storing canvas layout positions (x, y per notification id)

**decision work items**
 Decision nodes appear between two other non decision nodes.  For example a workitem node of type "form" and a workitem node of type "task".  A decision node is required if there is a rule that          
determines whether the notification is fired or the task is created. In the forward table the rule is in the column "filter".  In the task_group table the rule is in a JSON object in the column "rule". 
The task group rule can be found in the "filter" property of that JSON object. The Java class AssignFromSurvey defines the json. 
If there is no rule in the notification or task group that links two nodes then no        
decision node is shown. If there is a rule then the decision node is shown.

**bundle notifications**
A notification applies to a bundle when `forward.bundle = true`. Processing is two-pass:

- **Pass 1** — all non-bundle `forward` records and `task_group` records are processed first. As each node is created, its key is recorded in a `surveyItemKeys` map (`sId → [node keys]`), covering both source and destination sides of each notification chain.
- **Pass 2** — bundle notifications are processed after pass 1. For each bundle member survey (identified by `survey.group_survey_ident = forward.bundle_ident`), all existing nodes recorded in `surveyItemKeys` for that survey are used as source nodes. If a member survey has no existing nodes it gets a fresh `form:s:<id>` node. All source nodes connect independently to the same downstream decision/action node(s), so multiple arrows converge on those nodes. If no accessible member surveys are found at all, a single fallback node labelled with the bundle ident is shown.

A survey that is a bundle member may also have its own survey-specific notifications; those are rendered as ordinary single-source connections and share the same canvas nodes, which are then also wired as sources for any bundle notification.
---

## 5. REST API

| Method | Path | Auth roles | Description |
|--------|------|------------|-------------|
| GET | `/surveyKPI/workflow/items` | ANALYST, ADMIN | Returns `WorkflowData` (items + links) with server-computed x/y merged with any user-saved positions |
| PUT | `/surveyKPI/workflow/positions` | ANALYST, ADMIN | Saves full positions map `{nodeId: {x,y}}` for current user+org |
| DELETE | `/surveyKPI/workflow/positions` | ANALYST, ADMIN | Deletes saved positions for current user+org, reverting to defaults |

---

## 6. Backend Logic

**`WorkflowManager.java`** (`sdDAL`) — core logic:
- `getWorkflowItems(sd, user)` — queries `forward` and `task_group`, builds nodes and links, calls `applyLayout()` then `mergeUserPositions()`.
- `applyLayout()` — computes default x/y positions using topological column assignment (see §3 Node layout).
- `mergeUserPositions()` — loads saved positions from `workflow_node_positions` for the user's current org and overwrites x/y on matching nodes.
- `savePositions(sd, user, positions)` — upserts the full positions map as JSONB.
- `resetPositions(sd, user)` — deletes the saved row.

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
- `globals.gCurrentProject` holds selected project id
- Project list populated by `getLoggedInUser()` from common.js
- On project change: re-fetch and re-render workflow canvas

**API call sequence**
1. Page load → `getLoggedInUser(projectChanged, ...)` — populates `.project_list` selects
2. `projectChanged()` → `fetch /surveyKPI/notifications/{projectId}`
3. Response → `renderWorkflow(notifications)` — clears and redraws canvas

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

Build step required:
```bash
cd /path/to/prop-smapserver/tasks
npm run build
```
Generates `WebContent/build/js/workflow.bundle.js`.

---

## 11. Open Questions

- Should the modules dropdown in all pages replace "Tasks" with "Workflow" as the primary entry point? (deferred — large change across many pages)
- Should enabled/disabled notifications be visually distinguished on the canvas?
- Should bundles and project-level notifications have a different visual grouping than survey-level ones?
