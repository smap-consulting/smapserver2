// Case states test, run with javascript_tool on any console page.  Replace the placeholder on the CONFIG line:
//   {"part": "notification", "project": <project id>, "sId": <survey id>, "test": <the test file>}
//       Creates the test's case_reopened notification once (kept) and returns its id.  It sends to a question whose
//       answers are not addresses, so the attempt is recorded in the record's history and nothing is sent.
//   {"part": "alert", "sId", "group": <bundle ident>, "test"}
//       Creates the test's case alert once (kept).  It has no notifications, so it only writes to the history.
//   {"part": "import", "sId", "test"}
//       Imports the test's one closed case from a csv file (the survey needs task_file on).
//   {"part": "ops", "group", "test"}
//       The reopened count of each of the test's cases in the Operations record lists.
//   {"part": "record", "sId", "instanceid"}
//       The record's case columns as the console reads them: _case_closed, _case_reopened, _assigned.
const CONFIG = __CONFIG__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const getJson = url => fetch(url, { headers: H, cache: 'no-store' })
	.then(r => { if (!r.ok) throw new Error(url.split('?')[0] + ' ' + r.status); return r.json(); });
const result = { part: CONFIG.part };

if (CONFIG.part === 'notification') {
	const N = CONFIG.test.notification;
	const list = () => getJson('/surveyKPI/notifications/' + CONFIG.project + '?tz=UTC');
	let n = (await list()).find(x => x.name === N.name);
	if (!n) {
		const body = { name: N.name, trigger: N.trigger, target: 'email', enabled: true, filter: '', bundle: false,
			s_id: CONFIG.sId, notifyDetails: { emails: [], emailQuestionName: N.emailQuestion, emailAssigned: false,
				emailMeta: '', subject: N.subject, content: 'Regression test', attach: 'none',
				include_references: false, launched_only: false } };
		const r = await fetch('/surveyKPI/notifications/add?tz=UTC', { method: 'POST',
			headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' },
			body: new URLSearchParams({ notification: JSON.stringify(body) }) });
		result.created = r.ok ? true : r.status + ' ' + (await r.text()).slice(0, 200);
		n = (await list()).find(x => x.name === N.name);
	}
	result.nId = n ? n.id : null;
	result.trigger = n ? n.trigger : null;
} else if (CONFIG.part === 'alert') {
	const A = CONFIG.test.alert;
	let cms = await getJson('/surveyKPI/cases/settings/' + CONFIG.sId);
	let found = (cms.alerts || []).find(a => a.name === A.name);
	if (!found) {
		const r = await fetch('/surveyKPI/cases/settings/alert', { method: 'POST',
			headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' },
			body: new URLSearchParams({ alert: JSON.stringify({ id: -1, group_survey_ident: CONFIG.group, name: A.name,
				period: A.period, filter: '' }) }) });
		result.created = r.ok ? true : r.status + ' ' + (await r.text()).slice(0, 200);
		cms = await getJson('/surveyKPI/cases/settings/' + CONFIG.sId);
		found = (cms.alerts || []).find(a => a.name === A.name);
	}
	result.alert = found ? { id: found.id, period: found.period } : null;
} else if (CONFIG.part === 'import') {
	// One closed case loaded from a file, as the console's import does
	const fd = new FormData();
	fd.append('survey', String(CONFIG.sId));
	fd.append('import_source', 'file');
	fd.append('file', new Blob([CONFIG.test.import.csv], { type: 'text/csv' }), 'case_states_import.csv');
	const r = await fetch('/surveyKPI/assignments/load', { method: 'POST', headers: H, body: fd });
	result.status = r.status;
	result.text = (await r.text()).slice(0, 200);
} else if (CONFIG.part === 'ops') {
	// How many times each of the test's cases has reopened, as the Operations record lists show it
	result.items = [];
	for (const type of CONFIG.test.ops.types) {
		const d = await getJson('/surveyKPI/ops/items?type=' + type);
		for (const it of (Array.isArray(d) ? d : (d.items || []))) {
			if (it.gsi === CONFIG.group) result.items.push({ type, thread: it.thread, reopened: it.reopened });
		}
	}
} else if (CONFIG.part === 'record') {
	const meta = await getJson('/surveyKPI/survey/' + CONFIG.sId + '/getMeta');
	const top = meta.forms.find(f => f.name === meta.top_table) || meta.forms[0];
	const d = await getJson('/surveyKPI/items/' + top.f_id + '?rec_limit=200&mustHaveGeom=no&inc_bad=no&tz=UTC');
	const row = (d.features || []).map(f => f.properties).find(p => p.instanceid === CONFIG.instanceid);
	result.found = !!row;
	if (row) {
		result.closed = row._case_closed || null;
		result.reopened = row._case_reopened || null;
		result.assigned = row._assigned || null;
	}
}
return JSON.stringify(result);
