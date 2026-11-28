// Operational issues report test, run with javascript_tool.  Replace the placeholder on the CONFIG line with one part:
//   {"part": "setup", "project": <project id>, "sId": <case-alerts survey id>, "ident": <its ident>, "run": <time in ms>,
//    "test": <the test file>}
//       On any console page.  Creates the test's notification once (kept) and submits the test's record, which
//       its filter fails on.  Returns the notification id.
//   {"part": "check", "nId": <setup's>, "project", "sId", "test"}
//       On any console page.  Reads /surveyKPI/issues and checks the notification's two issues and a conflict.
//       Returns "pending" until the notification error has been logged; call it again.  Returns the fix links.
//   {"part": "page"}
//       On /app/tasks/issues.html.  Checks the page shows the issues and its filters.
//   {"part": "fixNotification", "nId", "test"}
//       On the notification's fix link.  Checks the notification is open for editing.
//   {"part": "fixConflict", "sId": <conflict's survey id>, "instanceid": <from its fix link>}
//       On the conflict's fix link.  Checks the console opened that record.
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
	const N = CONFIG.test.notification;
	const list = () => getJson('/surveyKPI/notifications/' + CONFIG.project + '?tz=UTC');
	let n = (await list()).find(x => x.name === N.name);
	if (!n) {
		const notification = {
			name: N.name, trigger: 'submission', target: 'email', enabled: true, filter: N.filter, bundle: false, s_id: CONFIG.sId,
			notifyDetails: { emails: [], emailQuestionName: N.emailQuestion, emailAssigned: false, emailMeta: '',
				subject: N.subject, content: 'Regression test', attach: 'none', include_references: false, launched_only: false }
		};
		const r = await fetch('/surveyKPI/notifications/add?tz=UTC', { method: 'POST',
			headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' },
			body: new URLSearchParams({ notification: JSON.stringify(notification) }) });
		check('notification created', r.ok, r.ok ? '' : r.status);
		n = (await list()).find(x => x.name === N.name);
	}
	check('notification enabled with the bad filter', n && n.enabled && n.filter === N.filter, n ? n.filter : 'missing');
	result.nId = n ? n.id : null;

	const key = (await getJson('/surveyKPI/login/key?form=user')).key;
	const answers = Object.entries(CONFIG.test.record.answers).map(([q, v]) => `<${q}>${xmlEsc(v)}</${q}>`).join('');
	const xml = `<?xml version="1.0" encoding="UTF-8"?><main id="${CONFIG.ident}"><meta><instanceID>uuid:iss-${CONFIG.run}</instanceID></meta>${answers}</main>`;
	const fd = new FormData();
	fd.append('xml_submission_file', new Blob([xml], { type: 'text/xml' }), 'submission.xml');
	const r = await fetch('/submission/key/' + encodeURIComponent(key) + '?deviceID=issues-test', { method: 'POST', body: fd });
	check('record submitted', r.status === 201, r.status);
} else if (CONFIG.part === 'check') {
	const N = CONFIG.test.notification;
	const fwd = 'fwd_id=' + CONFIG.nId + '&project=' + CONFIG.project;
	const until = Date.now() + 35000;
	let issues = [];
	for (;;) {
		issues = await getJson('/surveyKPI/issues?tz=UTC');
		if (issues.some(i => i.type === 'notification_error' && i.item === N.name) || Date.now() > until) break;
		await sleep(5000);
	}
	const err = issues.find(i => i.type === 'notification_error' && i.item === N.name);
	result.pending = !err;
	if (!result.pending) {
		const filt = issues.find(i => i.type === 'notification_filter' && i.item === N.name);
		check('filter that does not work', filt && filt.detail.indexOf('ca_gone') >= 0, filt ? filt.detail : 'not listed');
		check('filter issue fixes the notification', filt && filt.fixUrl.endsWith(fwd), filt && filt.fixUrl);
		check('notification error recorded against the notification', err.count >= 1 && err.fixUrl.endsWith(fwd),
			err.count + ' times, ' + err.fixUrl);
		check('error labelled', err.typeLabel && err.typeLabel !== err.type, err.typeLabel);
		const conflict = issues.find(i => i.type === 'conflict' && i.survey === CONFIG.test.conflictSurvey);
		check('unreviewed conflict listed', conflict, conflict ? conflict.item : 'none for ' + CONFIG.test.conflictSurvey);
		if (conflict) {
			const m = conflict.fixUrl.match(/managed_forms\.html\?id=(\d+)&instanceid=(.+)$/);
			check('conflict fixed in the console', m, conflict.fixUrl);
			result.conflict = m ? { sId: Number(m[1]), instanceid: decodeURIComponent(m[2]), url: conflict.fixUrl } : null;
		}
		check('fix links stay on this server', issues.every(i => !i.fixUrl || (i.fixUrl[0] === '/' && i.fixUrl[1] !== '/')));
		const x = await fetch('/surveyKPI/issues/xlsx?tz=UTC', { headers: H, cache: 'no-store' });
		const bytes = new Uint8Array(await x.arrayBuffer());
		check('xlsx download', x.ok && bytes[0] === 0x50 && bytes[1] === 0x4b, x.status + ', ' + bytes.length + ' bytes');
		result.notificationUrl = filt ? filt.fixUrl : null;
		result.counts = issues.reduce((a, i) => { a[i.type] = (a[i.type] || 0) + 1; return a; }, {});
	}
} else if (CONFIG.part === 'page') {
	for (let i = 0; i < 60 && !document.querySelector('#iss_table tbody tr'); i++) await sleep(250);
	const issues = await getJson('/surveyKPI/issues?tz=UTC');
	const rows = () => document.querySelectorAll('#iss_table tbody tr').length;
	check('all issues shown', rows() === issues.length, rows() + ' rows, ' + issues.length + ' issues');
	check('fix buttons', document.querySelectorAll('#iss_table tbody a.btn').length === issues.filter(i => i.fixUrl).length);
	check('counts by type', document.querySelectorAll('#iss_summary .iss_pick').length === new Set(issues.map(i => i.type)).size);
	const $ = window.jQuery;
	$('#iss_type').val('conflict').trigger('change');
	check('type filter', rows() === issues.filter(i => i.type === 'conflict').length, rows() + ' conflict rows');
	$('#iss_type').val('').trigger('change');
	$('#iss_search').val('ca_gone').trigger('input');
	check('search', rows() >= 1 && rows() < issues.length, rows() + ' rows');
	$('#iss_search').val('').trigger('input');
	check('nothing injected', document.querySelectorAll('#iss_table tbody script, #iss_table tbody img').length === 0);
} else if (CONFIG.part === 'fixNotification') {
	const N = CONFIG.test.notification;
	for (let i = 0; i < 60 && !(document.querySelector('#addNotificationPopup.show')); i++) await sleep(250);
	const open = document.querySelector('#addNotificationPopup.show');
	check('notification open for editing', open && document.querySelector('#name').value === N.name,
		open ? document.querySelector('#name').value : 'dialog not open');
	check('its filter shown', document.querySelector('#not_filter') && document.querySelector('#not_filter').value === N.filter);
	if (open && window.bootstrap) bootstrap.Modal.getOrCreateInstance(open).hide();
} else if (CONFIG.part === 'fixConflict') {
	for (let i = 0; i < 60 && !(window.globals && globals.gCurrentInstance); i++) await sleep(250);
	check('console opened the record', window.globals && globals.gCurrentInstance === CONFIG.instanceid, window.globals && globals.gCurrentInstance);
	check('console on the survey', window.globals && String(globals.gCurrentSurvey) === String(CONFIG.sId), window.globals && globals.gCurrentSurvey);
}
result.pass = lines.length > 0 && lines.every(l => l.indexOf('PASS') === 0) && !result.pending;
result.checks = lines;
JSON.stringify(result);
