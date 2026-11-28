// Conflict notification test, run with javascript_tool on any console page of the server.  A call times out after
// 45 seconds, so the test is run in parts.  Replace the placeholder on the CONFIG line with:
//   {"part": "setup", "project": <project id>, "a": {"id", "ident", "name"}, "b": {"id", "ident", "name"},
//    "bundleIdent": <the bundle's ident>, "test": <the test file>}
//       Creates each of the test's notifications that is missing, by name, and checks they are enabled conflict
//       notifications on the right survey or bundle.  Kept.  Returns their ids by name.
//   {"part": "save", "a", "b", "run": <the time in ms>, "scenario": <one scenario>}
//       Makes a conflict whose last save is by the scenario's saver: v1 saved by stage 1, v2 by the other stage
//       from v1, v3 by the saver from v1, both changing the status.  Returns the latest instance id.
//   {"part": "check", "sId": <stage 1 id>, "instanceid": <from save>, "scenario": <the scenario>}
//       Waits up to 35 seconds for the notifications the scenario expects to be recorded in the record's history,
//       then checks none it does not expect were.  Returns "pending" while one has not arrived; call it again.
//   {"part": "workflow", "ids": <setup's ids>, "a", "b", "bundleName": <the bundle's name>, "test": <the test file>}
//       Checks the workflow page's items for each notification, and that its edit drawer can load it.
const CONFIG = __CONFIG__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const getJson = url => fetch(url, { headers: H, cache: 'no-store' })
	.then(r => { if (!r.ok) throw new Error(url.split('?')[0] + ' ' + r.status); return r.json(); });
const xmlEsc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const lines = [];
const check = (name, ok, detail) => lines.push((ok ? 'PASS ' : 'FAIL ') + name + (detail === undefined || detail === '' ? '' : ' - ' + detail));
const result = { part: CONFIG.part };

if (CONFIG.part === 'setup') {
	const T = CONFIG.test;
	const list = async () => getJson('/surveyKPI/notifications/' + CONFIG.project + '?tz=UTC');
	let have = await list();
	for (const n of T.notifications) {
		if (have.some(x => x.name === n.name)) continue;
		const notification = {
			name: n.name, trigger: 'conflict', target: 'email', enabled: true, filter: '',
			bundle: n.on === 'bundle',
			notifyDetails: { emails: [], emailQuestionName: T.emailQuestion, emailAssigned: false, emailMeta: '',
				subject: n.subject, content: 'Regression test conflict notification', attach: 'none',
				include_references: false, launched_only: false }
		};
		if (n.on === 'bundle') notification.bundle_ident = CONFIG.bundleIdent;
		else notification.s_id = CONFIG[n.on].id;
		const r = await fetch('/surveyKPI/notifications/add?tz=UTC', { method: 'POST',
			headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' },
			body: new URLSearchParams({ notification: JSON.stringify(notification) }) });
		check('created ' + n.name, r.ok, r.ok ? '' : r.status + ' ' + (await r.text()).slice(0, 100));
	}
	have = await list();
	result.ids = {};
	for (const n of T.notifications) {
		const found = have.filter(x => x.name === n.name);
		const x = found[0];
		result.ids[n.name] = x ? x.id : null;
		const where = n.on === 'bundle' ? (x && x.bundle && x.bundle_ident === CONFIG.bundleIdent)
			: (x && !x.bundle && Number(x.s_id) === Number(CONFIG[n.on].id));
		check(n.name + ': one, enabled conflict email', found.length === 1 && x.enabled && x.trigger === 'conflict' && x.target === 'email',
			found.length + ' found' + (x ? ', ' + x.trigger + ' ' + x.target + (x.enabled ? '' : ' disabled') : ''));
		check(n.name + ': on ' + (n.on === 'bundle' ? 'the bundle' : 'stage ' + (n.on === 'a' ? 1 : 2)), where,
			x ? (x.bundle ? 'bundle ' + x.bundle_ident : 'survey ' + x.s_id) : '');
	}
} else if (CONFIG.part === 'save') {
	const sc = CONFIG.scenario;
	const other = sc.saver === 'a' ? 'b' : 'a';
	const key = (await getJson('/surveyKPI/login/key?form=user')).key;
	const inst = label => 'uuid:cn-' + CONFIG.run + '-' + sc.saver + '-' + label;
	const rowsOf = r => Array.isArray(r) ? r : (r && (r.data || r.records)) || [];
	const applied = async id => {
		for (let i = 0; i < 15; i++) {
			const r = rowsOf(await getJson('/surveyKPI/api/data/' + CONFIG.b.ident + '?meta=yes&instanceid=' + encodeURIComponent(id)));
			if (r.length) return true;
			await sleep(2000);
		}
		return false;
	};
	const save = async (label, stage, opened, status) => {
		const id = inst(label);
		const xml = `<?xml version="1.0" encoding="UTF-8"?><main id="${CONFIG[stage].ident}"><meta><instanceID>${xmlEsc(id)}</instanceID></meta>`
			+ `<header>h0</header><status>${status}</status><a_note>a0</a_note><visits><visit_note>x</visit_note><_row_id>r1</_row_id></visits></main>`;
		const fd = new FormData();
		fd.append('xml_submission_file', new Blob([xml], { type: 'text/xml' }), 'submission.xml');
		let url = '/submission/key/' + encodeURIComponent(key);
		if (opened) url += '/' + encodeURIComponent(inst(opened));
		const r = await fetch(url + '?deviceID=conflict-notification-test', { method: 'POST', body: fd });
		const ok = r.status === 201 && await applied(id);
		check('save ' + label + ' by stage ' + (stage === 'a' ? 1 : 2), ok, r.status === 201 ? (ok ? '' : 'not applied within 30s') : 'status ' + r.status);
		return ok;
	};
	if (await save('v1', 'a', null, 'received')
			&& await save('v2', other, 'v1', 'with_pp')
			&& await save('v3', sc.saver, 'v1', 'closed')) {
		result.instanceid = inst('v3');
	}
} else if (CONFIG.part === 'check') {
	const sc = CONFIG.scenario;
	const until = Date.now() + 35000;
	let names = [], conflicts = 0;
	for (;;) {
		const events = await getJson('/surveyKPI/api/data/changes/' + CONFIG.sId + '/' + encodeURIComponent(CONFIG.instanceid) + '?tz=UTC');
		conflicts = events.filter(e => e.event === 'conflict').length;
		names = events.filter(e => e.event === 'notification' && e.notification).map(e => e.notification.notificationName);
		if (sc.fired.every(n => names.includes(n)) || Date.now() > until) break;
		await sleep(5000);
	}
	result.pending = !sc.fired.every(n => names.includes(n));
	if (!result.pending) {
		await sleep(5000);		// Give a notification that should not fire the time to show up
		const events = await getJson('/surveyKPI/api/data/changes/' + CONFIG.sId + '/' + encodeURIComponent(CONFIG.instanceid) + '?tz=UTC');
		names = events.filter(e => e.event === 'notification' && e.notification).map(e => e.notification.notificationName);
		check('conflict recorded', conflicts === 1, conflicts + ' conflicts');
		for (const n of sc.fired) check('fired: ' + n, names.filter(x => x === n).length === 1, names.filter(x => x === n).length + ' times');
		for (const n of sc.notFired) check('not fired: ' + n, !names.includes(n), names.includes(n) ? 'fired' : '');
		// Their recipient question holds no address, so nothing is sent and the history says so
		const statuses = events.filter(e => e.event === 'notification' && e.notification && sc.fired.includes(e.notification.notificationName)).map(e => e.status);
		check('nothing sent: status no_recipients', statuses.length > 0 && statuses.every(x => x === 'no_recipients'), statuses.join(','));
	} else {
		result.seen = names;
	}
} else if (CONFIG.part === 'workflow') {
	const T = CONFIG.test;
	const data = await getJson('/surveyKPI/workflow/items');
	const items = data.items || [], links = data.links || [];
	const byId = id => items.find(i => i.id === id);
	// The steps a node leads to, through a decision if there is one
	const next = id => links.filter(l => l.from === id).flatMap(l => l.to.indexOf('decision:') === 0 ? next(l.to) : [l.to]);
	for (const n of T.notifications) {
		const id = CONFIG.ids[n.name];
		const trig = byId('conflict:f:' + id);
		const expect = n.on === 'bundle' ? CONFIG.bundleName : CONFIG[n.on].name;
		check(n.name + ': conflict trigger', trig && trig.type === 'conflict' && trig.role === 'trigger', trig ? trig.type + '/' + trig.role : 'no node conflict:f:' + id);
		if (!trig) continue;
		check(n.name + ': named after ' + (n.on === 'bundle' ? 'the bundle' : 'the survey'), trig.name === expect, trig.name);
		check(n.name + ': trigger carries the notification', (trig.fwdIds || []).includes(id), JSON.stringify(trig.fwdIds));
		const edit = await getJson('/surveyKPI/workflow/edit/notifications?ids=' + id + '&tz=UTC');
		check(n.name + ': loads for editing', edit.length === 1 && edit[0].name === n.name && edit[0].emailSubject === n.subject,
			edit.length ? edit[0].name + ' / ' + edit[0].emailSubject : 'not returned');
		const email = byId('email:f:' + id);
		check(n.name + ': leads to its email', next(trig.id).includes('email:f:' + id), next(trig.id).join(','));
		if (email) {
			check(n.name + ': email subject', email.subject === n.subject, email.subject);
			check(n.name + ': email recipients', JSON.stringify(email.recipients) === JSON.stringify(['${' + T.emailQuestion + '}']), JSON.stringify(email.recipients));
			const from = links.filter(l => l.to === email.id || (l.to.indexOf('decision:') === 0 && next(l.to).includes(email.id))).map(l => l.from);
			check(n.name + ': only the trigger leads to it', from.length === 1 && from[0] === trig.id, from.join(','));
		}
	}
}
result.pass = lines.length > 0 && lines.every(l => l.indexOf('PASS') === 0) && !result.pending;
result.checks = lines;
JSON.stringify(result);
