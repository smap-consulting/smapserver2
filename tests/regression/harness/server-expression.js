// Server expression test, run with javascript_tool.  Replace the placeholder on the CONFIG line with one part:
//   {"part": "setup", "project": <project id>, "sId": <survey id>, "test": <the test file>}
//       On any console page.  Creates the test's notification once (kept), with a filter on the question the
//       test later removes, which must be accepted while the question exists.  Returns the notification id.
//   {"part": "refuse", "project", "sId", "fId": <main form id>, "group": <bundle ident>, "test"}
//       On any console page.  Saving notifications and case alerts with the test's broken filters must be
//       refused with a message that says why, its good filters must be accepted (checked, not saved), and a
//       console filter must get the same message.  Nothing is saved.
//   {"part": "taskGroup", "project", "sId", "test"}
//       On any console page.  Creates the test's task group once (kept), with a condition no record meets,
//       so no task is ever made.  Returns its id.
//   {"part": "assignKept", "tgId", "test"}
//       After task_group_update has set it to assign from the test's question.  The workflow page sends the
//       rule to show, and saving the step there, renamed and back, keeps the rule.
//   {"part": "broken", "project", "nId", "test"}
//       On any console page, after the removed question has been deleted.  The issues report lists the
//       notification, saving it unchanged works, and changing it to another broken filter is refused.
const CONFIG = __CONFIG__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const getJson = url => fetch(url, { headers: H, cache: 'no-store' })
	.then(r => { if (!r.ok) throw new Error(url.split('?')[0] + ' ' + r.status); return r.json(); });
const lines = [];
const check = (name, ok, detail) => lines.push({ check: name, ok: !!ok, detail: detail === undefined ? '' : String(detail) });
const result = { part: CONFIG.part };
const N = CONFIG.test.notification;
const list = () => getJson('/surveyKPI/notifications/' + CONFIG.project + '?tz=UTC');
const postForm = (url, field, value) => fetch(url, { method: 'POST',
	headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' },
	body: new URLSearchParams({ [field]: JSON.stringify(value) }) });
const notification = filter => ({
	name: N.name, trigger: 'submission', target: 'email', enabled: true, filter: filter, bundle: false, s_id: CONFIG.sId,
	notifyDetails: { emails: [], emailQuestionName: N.emailQuestion, emailAssigned: false, emailMeta: '',
		subject: N.subject, content: 'Regression test', attach: 'none', include_references: false, launched_only: false }
});
// A refusal must not have saved anything, and must say what is wrong
const refusedWith = async (r, expect) => {
	const text = await r.text();
	return { ok: !r.ok && text.indexOf(expect) >= 0, detail: r.status + ' ' + text.slice(0, 160) };
};

if (CONFIG.part === 'setup') {
	let n = (await list()).find(x => x.name === N.name);
	if (!n) {
		const r = await postForm('/surveyKPI/notifications/add?tz=UTC', 'notification', notification(N.filter));
		check('notification with a working filter is accepted', r.ok, r.ok ? '' : r.status + ' ' + await r.text());
		n = (await list()).find(x => x.name === N.name);
	}
	check('notification has the filter', n && n.filter === N.filter, n ? n.filter : 'missing');
	result.nId = n ? n.id : null;

} else if (CONFIG.part === 'refuse') {
	const before = (await list()).length;
	for (const p of CONFIG.test.refused) {
		const temp = notification(p.filter);
		temp.name = 'Server expression test - refused';
		let c = await refusedWith(await postForm('/surveyKPI/notifications/add?tz=UTC', 'notification', temp), p.expect);
		check('notification refused: ' + p.name, c.ok, c.detail);

		c = await refusedWith(await postForm('/surveyKPI/cases/settings/alert', 'alert',
			{ id: 0, name: 'Server expression test - refused', period: '1 day', filter: p.filter, group_survey_ident: CONFIG.group }), p.expect);
		check('case alert refused: ' + p.name, c.ok, c.detail);
	}
	check('nothing saved', (await list()).length === before, (await list()).length + ' notifications, was ' + before);

	// A console filter that will not work gets the same message as a save, in place of the database's
	const bad = CONFIG.test.refused[1];
	const r = await fetch('/surveyKPI/items/' + CONFIG.fId + '?rec_limit=5&mustHaveGeom=no&advanced_filter='
		+ encodeURIComponent(bad.filter), { headers: H, cache: 'no-store' });
	const text = await r.text();
	check('console filter message', text.indexOf(bad.expect) >= 0 && text.indexOf('Error in filter') >= 0, r.status + ' ' + text.slice(0, 160));

	// Good filters run in the console, the same check that saving uses
	for (const f of CONFIG.test.accepted) {
		const g = await fetch('/surveyKPI/items/' + CONFIG.fId + '?rec_limit=5&mustHaveGeom=no&advanced_filter='
			+ encodeURIComponent(f), { headers: H, cache: 'no-store' });
		const t = await g.text();
		check('accepted: ' + f, g.ok && t.indexOf('Error in filter') < 0, g.status + ' ' + t.slice(0, 120));
	}

} else if (CONFIG.part === 'taskGroup') {
	const T = CONFIG.test.taskGroup;
	let tg = (await getJson('/surveyKPI/tasks/taskgroups/' + CONFIG.project)).find(x => x.name === T.name);
	let id = tg ? (tg.tg_id || tg.id) : 0;
	if (!id) {
		const r = await fetch('/surveyKPI/workflow/edit/taskgroup', { method: 'POST',
			headers: { ...H, 'Content-Type': 'application/json' },
			body: JSON.stringify({ name: T.name, sourceSurveyId: CONFIG.sId, targetSurveyId: CONFIG.sId, filter: T.filter }) });
		const t = await r.text();
		check('task group created', r.ok, r.ok ? '' : r.status + ' ' + t.slice(0, 160));
		id = r.ok ? JSON.parse(t).tgId : 0;
	}
	check('task group found', id > 0, id);
	result.tgId = id;

} else if (CONFIG.part === 'assignKept') {
	const T = CONFIG.test.taskGroup;
	const get = async () => (await getJson('/surveyKPI/workflow/edit/taskgroups?ids=' + CONFIG.tgId))[0];
	const put = tgs => fetch('/surveyKPI/workflow/edit/taskgroups', { method: 'PUT',
		headers: { ...H, 'Content-Type': 'application/json' }, body: JSON.stringify(tgs) });
	let tg = await get();
	check('workflow page is sent the rule', tg && tg.remoteUser === '_data' && tg.assignData === T.assignData,
		tg ? tg.remoteUser + ' ' + tg.assignData : 'missing');
	if (tg) {
		// The page saves the step as it was, with only the name changed
		let r = await put([Object.assign({}, tg, { name: T.name + ' renamed', assignData: undefined })]);
		check('renamed in the workflow page', r.ok, r.ok ? '' : r.status + ' ' + (await r.text()).slice(0, 160));
		r = await put([Object.assign({}, tg, { assignData: undefined })]);
		check('renamed back', r.ok, r.ok ? '' : r.status + ' ' + (await r.text()).slice(0, 160));
		tg = await get();
		check('rule kept', tg && tg.assignData === T.assignData && tg.name === T.name,
			tg ? tg.name + ' ' + tg.assignData : 'missing');
	}

} else if (CONFIG.part === 'broken') {
	const issues = await getJson('/surveyKPI/issues?tz=UTC');
	const issue = issues.find(i => i.type === 'notification_filter' && i.item === N.name);
	check('issues report lists the broken filter', issue && issue.detail.indexOf(CONFIG.test.removed) >= 0,
		issue ? issue.detail : 'not listed');
	check('its fix link opens the notification', issue && issue.fixUrl && issue.fixUrl.indexOf('fwd_id=' + CONFIG.nId) >= 0,
		issue ? issue.fixUrl : '');

	const stored = (await list()).find(x => x.id === CONFIG.nId);
	if (!stored) {
		check('notification kept', false, 'missing');
	} else {
		let r = await postForm('/surveyKPI/notifications/update?tz=UTC', 'notification', stored);
		check('saved unchanged', r.ok, r.ok ? '' : r.status + ' ' + await r.text());

		const changed = Object.assign({}, stored, { filter: N.changedTo });
		const c = await refusedWith(await postForm('/surveyKPI/notifications/update?tz=UTC', 'notification', changed),
			'no question called');
		check('changed to another broken filter is refused', c.ok, c.detail);
		const after = (await list()).find(x => x.id === CONFIG.nId);
		check('filter left as it was', after && after.filter === N.filter, after ? after.filter : 'missing');
	}
}

result.lines = lines;
return JSON.stringify(result);
