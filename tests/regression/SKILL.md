---
name: regression-test
description: Run the Smap regression tests against the server the smap MCP connection points at, using the MCP tools to check or create each test survey and Chrome to run it in the webform. Invoke with an optional test name e.g. "regression-test regex-constraints", or no argument for all tests.
argument-hint: [test name]
---

# Smap Regression Tests

Arguments: **$ARGUMENTS** (a test name to run only that test, empty for all)

Tests live in `~/.claude/skills/regression-test/tests/*.json`, one test per file.  The browser
scripts that run them are in `~/.claude/skills/regression-test/harness/`, one per test `kind`.

## Before starting

1. Call `mcp__smap__whoami` and `mcp__smap__server_info`.  Report the user and version.
2. The server's web address is `https://dev.smap.com.au` unless the user says otherwise.  The smap
   MCP connection must point at the same server: check with
   `python3 -c "import json,os;print(json.load(open(os.path.expanduser('~/.claude.json')))['projects'][os.getcwd()]['mcpServers'])"`
   or ask if unsure.
3. Load the Chrome tools in one ToolSearch call, create a tab, and open `{server}/` .  If the page
   shows a Login link the browser is not logged in: ask the user to log in in that tab and wait.
   Never enter a password.

## For each test

1. Read the test file.  Skip it if an argument was given and does not match `name`.
2. **Survey.** Only for a test that has a `survey`.  Find `survey.name` in `mcp__smap__survey_list`.
   - Missing: create it with `survey_create` in the project named `survey.project` (from
     `project_list`), then add each of `survey.questions` in order with `survey_add_question`.
     Say that it was created.
   - Present: compare `survey_questions` with `survey.questions` (name, type, constraint,
     relevant, calculation).  If they differ, report the difference and do not run the test - the
     survey has been edited and the result would not mean anything.  Do not change it.
   - Get the ident with `survey_get`.
3. **Run.** By `kind`:
   - `webform-constraint`: open `{server}/app/myWork/webForm/{ident}?nc={timestamp}` (the query
     string stops a cached copy of the form being used; `/webForm/` needs basic auth and shows an
     error page in Chrome).  Read `harness/webform-constraint.js`, replace `__CASES__` with the
     test's `cases` as JSON, and run it with `javascript_tool`.
   - `settings-secrets`: open `{server}{page}`.  The user must be a server owner.  Read
     `harness/settings-secrets.js`, replace `__CONFIG__` with the test's `config` as JSON, and run
     it with `javascript_tool`.  The read checks change nothing.  The write checks save the server
     settings, so before setting `write` to true for the run, ask the user and wait for a clear yes,
     every run.  They use `wa_app_secret` or `vonage_webhook_secret`, whichever is not set, and
     clear it afterwards.
   - `xss`: needs the test's survey.  When creating it, make each list in `survey.choices` with
     `survey_options_edit` before adding the questions.  The server sanitises choice labels, so
     compare a label by its text before the first `<` only.
     **Records**, each kept for later runs; call `survey_submission_effects` first and stop if it
     reports anything would be sent.  For `via: mcp` look with `data_query` for a record whose
     answers equal `answers`, and submit it with `data_submit` if there is none.  For `via: browser`
     look for a record with that `instanceID`, and if there is none run `harness/xss-setup.js` with
     `__STEP__` = `{"do":"submit","survey":<ident>,"instanceID","deviceID","answers"}` - the device
     and instance ids cannot be set through the MCP connection.  For a record's `task`, look with
     `task_list` for a task of that name in the survey and if there is none create it with
     `task_create` on the record, assigned to the user from `whoami`.
     **Pages:** for each of `pages`, open `{server}{url}` with `{sId}` replaced by the survey id, `{ident}`
     by its ident and `{ts}` by the time, in the order listed: the console page saves the survey as the
     user's current survey, and the translation and change history pages open the current survey.
     The labels are sent with entities (`&lt;`) on purpose: the server stores them decoded, which is
     how a raw `</textarea>` or `onerror` gets into a label.  For a page with `panels`, first run
     `harness/xss-setup.js` with `{"do":"addPanels","project":<project id>,"sId","title":<survey name>,
     "question","types"}` and reload, and afterwards run it with `{"do":"removePanels",...}`, so the
     user's analysis page is left as it was.  Read `harness/xss.js`, replace `__PAGE__` with the page
     entry as JSON (placeholders replaced), and run it with `javascript_tool`.  The payloads record
     themselves in `window.__xss` and never open a dialog.
   - `dashboard-map`: needs the test's survey, made as for `xss`.  Open `{server}/app/fieldAnalysis/index.html`.
     **Records** are all `via: browser`, each kept: submit the missing ones as for `xss`, with
     `harness/xss-setup.js` - only a browser submission records the geopoint as the upload's
     location, which is where a User Activity map takes its points from.  **Trail:** run
     `harness/dashboard-map-setup.js` with `{"do":"trail","trail":<trail>}`; it gives the logged in
     user the trail through `/surveyKPI/myassignments/key/{key}`, as FieldTask does, once, and keeps it.
     **Panels:** run it with `{"do":"addPanels","project":<project id>,"sId","title":<panels.title>,
     "graphQuestion","geomQuestion","lineQuestion"}`, reload, run `harness/dashboard-map.js` with `__CONFIG__` =
     `{"part":"map","title","trail","expect","recordCount":<number of records>}` (the map part also checks the
     time series map's cluster colour) and then with `"part":"settings"` (each part is one javascript_tool call),
     and finally run the setup with `{"do":"removePanels",...}` so the dashboard is left as it was.
     The settings checks expect an English user.
   - `dashboard-region`: needs the test's survey, made as for `xss` (on a server the smap MCP connection
     does not reach, upload it as an XForm through `/surveyKPI/upload/surveytemplate`).  Open
     `{server}/app/fieldAnalysis/index.html`.  **Records** are all `via: browser`, submitted as for
     `xss` with `harness/xss-setup.js`, and kept.  **Bundle:** run `harness/dashboard-region-setup.js` with
     `__STEP__` = `{"do":"bundle","sId","project","location":<panels.location>,"bundle":<bundle>}`; it uploads the
     test's polygon survey into the test survey's bundle as a small xlsx (an XForm cannot join a bundle) and
     submits its polygon record, once each, and returns the polygon survey's ident.  Run
     `harness/dashboard-region.js` with `__CONFIG__` = `{"part":"api","sId","project","grid","panels","bundle",
     "bundleIdent","otherSurvey","expect"}`: it creates the test's grid once and keeps it, then adds its five
     panels.  Reload and run it again with `"part":"map"`, which removes them.  `otherSurvey` is the
     dashboard-maps survey, used as a survey outside the bundle.
   - `sqli`: SQL injection probes.  Needs the test's survey: make it with `survey_create` and
     `survey_add_question` as for `xss`, or on a server the smap MCP connection does not reach run
     `harness/sqli-setup.js` with `__STEP__` = `{"do":"survey","project","survey"}`.  For a test with a `csv`,
     run the setup with `{"do":"csv","sId","csv"}` (kept).  For a test with `records`, look for them with
     `data_query` (by answers) and submit the missing ones with `data_submit` after `survey_submission_effects`,
     or where MCP does not reach run the setup with `{"do":"records","sId","ident","records"}` (kept).  Run the
     setup with `{"do":"ids","sId"}` for the form and question ids.  A survey whose questions have `parameters`
     is always made with the setup's survey step (xlsx), as `survey_add_question` cannot set them.  A survey with
     `groups` has each added with `survey_add_group` after its questions; compare them with `survey_get` when present.  For a test with
     `clear`, set that question to "" on the test's record with `data_update_record` before the probes, and put
     the record's instance id (from `data_query`) in place of `{record}` in the probes.  Then run `harness/sqli.js` with
     `__CONFIG__` = `{"ident","sId","ids","probes"}`.  A probe passes when a real name or value still works and
     SQL in one is refused or returns nothing.  GET probes only read; POST probes (`form`) write to the test's own
     survey only, as the test's description says.  A test with `uses` runs on that test's survey.  A test with `role` needs that role: make it with `role_create` if
     `role_list` does not have it (kept), and pass `"role"` to the setup's ids step for `{roleId}`.  The settings page's CSP blocks eval, so
     load a stored harness as a `blob:` script, and return only pass/fail text: output with query strings is
     withheld by the browser tool.
   - `mcp-totals`: totals of the `mcp__smap__submission_events` tool.  Needs the test's survey, made as for
     `xss`.  **Records** are all `via: browser`, submitted as for `xss` with `harness/xss-setup.js` and kept.
     **Duplicate:** if `submission_events` with the survey's `survey_id` shows no duplicate, submit the test's
     `duplicate` once more with the setup's submit step (kept).  Uploads are applied by a background process,
     so call `submission_events` for the survey until `not_loaded` is 0, every 10 seconds for up to 2 minutes.
     Then call it with `survey_id`, with `project_id` (the survey's project) and with neither, each with
     `ignore_old` false, and for each state call it with `survey_id`, `states` [state] and `limit` 200 and
     count the uploads listed (follow `next_cursor` if there is one).  Open `{server}/app/fieldManager/monitor.html`
     and run `harness/mcp-totals.js` with `__CONFIG__` = `{"pId","ident","expect","mcp":{"survey","project",
     "all"},"rows"}`, the totals being each call's `totals`.  Uploads made between the calls and the harness
     can make the project and all project totals differ, so run it again once before reporting a failure there.
     A `warn` line is reported as ⚠️ and does not fail the test.
   - `update-conflict`: saves of a record made from a version somebody else has since replaced.  Needs the
     test's survey and its `bundle`.  **Survey:** make each list in `survey.choices` with `survey_options_edit`,
     create the survey with `survey_create`, add `survey.questions`, then each of `survey.groups` with
     `survey_add_group` (`repeating`) and its questions moved into it with `survey_move_question` `into`.
     **Bundle:** find `bundle.name` in `survey_list`; if missing create it with `survey_create` and
     `same_record_as_survey_id` naming the test survey (it starts as a copy), then add `bundle.questions`.  Check
     with `survey_get` that both report the same bundle.  Call `survey_submission_effects` for both and stop if
     anything would be sent.  Open `{server}/app/myWork/index.html` and for each of `scenarios`, one
     javascript_tool call each, run `harness/update-conflict.js` with `__CONFIG__` = `{"sId": <test survey id>,
     "identA": <its ident>, "identB": <bundle survey ident>, "run": <the time in ms, the same for every scenario>,
     "scenario": <the scenario>}`.  Saves are applied by a background process, so a scenario takes up to 30
     seconds.  The console step changes the scenario's own record in the console and the resolve step reviews its
     conflict.
4. Do not submit forms.  Tests leave no data, apart from the settings-secrets write checks, which
   put the setting they use back as it was, the records and tasks the xss test keeps in its own
   survey, the records and trail the dashboard-map test keeps, the records and grid the
   dashboard-region test keeps, the records the sqli-filter and sqli-autoupdate tests keep, and the
   records and duplicate upload the submission-totals test keeps, and the records the update-conflicts
   test adds to its own surveys, one per scenario each run.

## Report

One table per test: for webform-constraint question, value, expected, actual; for settings-secrets
check and detail; for xss page, check and detail; for sqli probe and detail; for mcp-totals check and detail; for update-conflict scenario, check and detail; each with ✅/❌.  Then a summary line per test,
PASS or FAIL with the number of failures.  For a failure include the `constraints` the harness
returned, as the constraint as the page received it is usually the clue.  Close the Chrome tab.

## Adding a test

Add a JSON file to `tests/` with `name`, `description` (what it guards, with commit ids), `kind`,
`survey` (name, project, questions as survey_add_question takes them) and `cases`, or for a test
that needs no survey whatever its harness takes, such as `page` and `config`.  A new kind
needs a harness script in `harness/` and a line under Run above.
