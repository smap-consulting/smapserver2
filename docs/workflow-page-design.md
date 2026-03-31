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

**Node layout (iteration 1)**
- Trigger nodes: x=60px, y=index×130px+20px
- Action nodes: x=380px, y=index×130px+20px
- Node width: 280px
- Arrow: bezier curve from right edge of trigger to left edge of action, stroke `#4a9eff`

**Node card colors**
| Work Item Type | Color |
|------|------|-------|
| submission | #e06c00 |
| periodic | #7c3aed |
| reminder | #ca8a04 |
| task | #2563eb |
| case | #7c3aed |
| email | #16a34a |
| sms | #0891b2 |
| server_calc | #0e7490 |
| forward | #e06c00 |
| default | #6b7280 |

---

## 4. Data Model

Smap already supports workflows.  These are constructed by specifying notifications which can assign tasks, assign cases send emails and
so on based on an event.  This new feature is a new view of that existing functionality.  Hence there will be few new tables.

### workitem

workitems are the visual components shown on the workflow page.  They are connected together to form a workflow.

**WorkItem Types**

| Type | Role |
|------|------|
| submission | trigger |
| periodic | trigger |
| reminder | trigger |
| task | action |
| case | action |
| email | action |
| sms | action |
| server_calc | action |
| forward | action |
| default | either |

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

---

## 5. REST API

**Existing endpoint used (iteration 1)**
| Method | Path | Auth roles | Response |
|--------|------|------------|---------|
| GET | `/surveyKPI/notifications/{projectId}` | ANALYST, ADMIN | `ArrayList<Notification>` JSON |

The `Notification` model fields used: `id`, `name`, `trigger`, `target`, `s_name`, `bundle_name`, `enabled`

**New endpoints** — none in iteration 1. Future iterations may add:
- `GET /surveyKPI/workflow/{projectId}` — workflow with layout positions
- `PUT /surveyKPI/workflow/layout` — save canvas positions

---

## 6. Backend Logic

No backend changes in iteration 1. Existing `NotificationList.java` (`surveyKPI`) serves the data.

The `getNotifications` method in `NotificationManager.java` returns notifications scoped to:
- Project directly (`f.p_id = ?`)
- Surveys in the project (`f.s_id in (select s_id from survey where p_id = ?)`)
- Bundles where any member survey is in the project

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

**Iteration 1: none.** No database changes, no config changes.

Build step required:
```bash
cd /path/to/prop-smapserver/tasks
npm run build
```
Generates `WebContent/build/js/workflow.bundle.js`.

---

## 11. Open Questions

- Should the modules dropdown in all pages replace "Tasks" with "Workflow" as the primary entry point? (deferred — large change across many pages)
- What canvas positions should be used when there are many notifications (current linear layout won't scale)?
- Should enabled/disabled notifications be visually distinguished on the canvas?
- Should bundles and project-level notifications have a different visual grouping than survey-level ones?
