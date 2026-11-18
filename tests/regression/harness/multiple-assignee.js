// Multiple assignee test, run with javascript_tool on /app/tasks/managed_forms.html.  One part per call, as a call
// times out after 45 seconds.  Replace the placeholder on the CONFIG line with:
//   {"part": "setup", "project": <project id>, "test": <the test file>}
//       Makes the survey from an xlsx if it is missing, sets its case settings and creates its two escalation
//       notifications if missing (kept).  Returns sId and ident.
//   {"part": "single" | "multiple" | "escalateAdd" | "escalateReplace" | "close" | "switchBack",
//    "sId", "ident", "run": <the time in ms, the same for every part>, "test"}
//       single: one assignee: assigning replaces, a role is refused.  Makes record r1.
//       multiple: turns the setting on; add, role, remove, lock, release and unassign all on r1.
//       escalateAdd: record r2 escalated to the first notification's user, then a person added in the console.
//       escalateReplace: r2 updated so the second notification escalates; the first one's user goes, the console's
//                        stays, everyone selected is added.
//       close: r2 closed, everyone taken off.
//       switchBack: the setting turned off, r1 keeps its two owners.
//   Parts that wait for the background processes return "pending"; call them again.
const CONFIG = __CONFIG__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const getJson = url => fetch(url, { headers: H, cache: 'no-store' })
	.then(r => { if (!r.ok) throw new Error(url.split('?')[0] + ' ' + r.status); return r.json(); });
const xmlEsc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const lines = [];
const check = (name, ok, detail) => lines.push((ok ? 'PASS ' : 'FAIL ') + name + (detail === undefined || detail === '' ? '' : ' - ' + detail));
const result = { part: CONFIG.part };
const T = CONFIG.test, U = T.users;
const post = (url, data) => fetch(url, { method: 'POST', headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' },
	body: new URLSearchParams(data) });
const inst = label => 'uuid:ma-' + CONFIG.run + '-' + label;
const sorted = a => a.slice().sort().join(',');

// The people a record is assigned to, by any of its versions
async function owners(instanceId) {
	const r = await getJson('/surveyKPI/api/data/' + CONFIG.ident + '?meta=yes&instanceid=' + encodeURIComponent(instanceId));
	const rows = Array.isArray(r) ? r : (r && (r.data || r.records)) || [];
	const rec = rows[0];
	if (!rec) return null;
	const k = Object.keys(rec).find(x => x.toLowerCase() === '_assigned');
	const v = k ? rec[k] : '';
	return (v || '').split(',').map(s => s.trim()).filter(s => s);
}
// The latest version of a record's thread
async function latest(thread) {
	const r = await getJson('/surveyKPI/api/data/' + CONFIG.ident + '?meta=yes');
	const rows = Array.isArray(r) ? r : (r && (r.data || r.records)) || [];
	const pick = (o, n) => o[Object.keys(o).find(x => x.toLowerCase() === n)];
	const found = rows.find(x => pick(x, '_thread') === thread);
	return found ? pick(found, 'instanceid') || pick(found, 'uuid') : null;
}
async function submit(id, answers, opened) {
	const key = (await getJson('/surveyKPI/login/key?form=user')).key;
	const xml = `<?xml version="1.0" encoding="UTF-8"?><main id="${CONFIG.ident}"><meta><instanceID>${xmlEsc(id)}</instanceID></meta>`
		+ Object.entries(answers).map(([q, v]) => `<${q}>${xmlEsc(v)}</${q}>`).join('') + `</main>`;
	const fd = new FormData();
	fd.append('xml_submission_file', new Blob([xml], { type: 'text/xml' }), 'submission.xml');
	let url = '/submission/key/' + encodeURIComponent(key);
	if (opened) url += '/' + encodeURIComponent(opened);
	const r = await fetch(url + '?deviceID=multiple-assignee-test', { method: 'POST', body: fd });
	if (r.status !== 201) return false;
	for (let i = 0; i < 15; i++) {
		if (await owners(id) !== null) return true;
		await sleep(2000);
	}
	return false;
}
async function setMultiple(on) {
	const cms = await getJson('/surveyKPI/cases/settings/' + CONFIG.sId);
	const s = { ...(cms.settings || {}), ...T.caseSettings, multipleAssignees: on };
	const r = await post('/surveyKPI/cases/settings/' + encodeURIComponent(cms.group_survey_ident || CONFIG.ident), { settings: JSON.stringify(s) });
	return r.ok;
}
const assign = (record, user) => post('/surveyKPI/managed/assign/' + CONFIG.sId + '/' + encodeURIComponent(user), { record });
const unassign = (record, user) => post('/surveyKPI/managed/unassign/' + CONFIG.sId, { record, user });
async function waitOwners(id, want, ms) {
	const until = Date.now() + ms;
	let o;
	for (;;) {
		o = await owners(id);
		if ((o && sorted(o) === sorted(want)) || Date.now() > until) return o;
		await sleep(4000);
	}
}

if (CONFIG.part === 'setup') {
	// A small xlsx with a survey and a choices sheet
	const enc = new TextEncoder(), crcTable = [];
	for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1; crcTable[n] = c >>> 0; }
	const crc = d => { let c = 0xFFFFFFFF; for (const b of d) c = crcTable[(c ^ b) & 0xFF] ^ (c >>> 8); return (c ^ 0xFFFFFFFF) >>> 0; };
	const zip = files => {
		const parts = [], central = []; let offset = 0;
		for (const [name, text] of Object.entries(files)) {
			const n = enc.encode(name), d = enc.encode(text), c = crc(d);
			const h = new DataView(new ArrayBuffer(30));
			h.setUint32(0, 0x04034b50, true); h.setUint16(4, 20, true); h.setUint32(14, c, true);
			h.setUint32(18, d.length, true); h.setUint32(22, d.length, true); h.setUint16(26, n.length, true);
			parts.push(new Uint8Array(h.buffer), n, d);
			const e = new DataView(new ArrayBuffer(46));
			e.setUint32(0, 0x02014b50, true); e.setUint16(4, 20, true); e.setUint16(6, 20, true); e.setUint32(16, c, true);
			e.setUint32(20, d.length, true); e.setUint32(24, d.length, true); e.setUint16(28, n.length, true); e.setUint32(42, offset, true);
			central.push(new Uint8Array(e.buffer), n);
			offset += 30 + n.length + d.length;
		}
		const size = central.reduce((a, p) => a + p.length, 0);
		const end = new DataView(new ArrayBuffer(22));
		end.setUint32(0, 0x06054b50, true); end.setUint16(8, Object.keys(files).length, true); end.setUint16(10, Object.keys(files).length, true);
		end.setUint32(12, size, true); end.setUint32(16, offset, true);
		return new Blob([...parts, ...central, new Uint8Array(end.buffer)], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
	};
	const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
	const sheet = rows => '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
		+ rows.map((r, i) => '<row r="' + (i + 1) + '">' + r.map((v, j) => '<c r="' + String.fromCharCode(65 + j) + (i + 1) + '" t="inlineStr"><is><t>' + esc(v) + '</t></is></c>').join('') + '</row>').join('')
		+ '</sheetData></worksheet>';
	const choiceRows = [['list_name', 'name', 'label']];
	for (const [list, names] of Object.entries(T.survey.choices)) names.forEach(n => choiceRows.push([list, n, n]));
	const ns = 'http://schemas.openxmlformats.org/';
	const xlsx = zip({
		'[Content_Types].xml': `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="${ns}package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>`,
		'_rels/.rels': `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="${ns}package/2006/relationships"><Relationship Id="rId1" Type="${ns}officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`,
		'xl/workbook.xml': `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="${ns}spreadsheetml/2006/main" xmlns:r="${ns}officeDocument/2006/relationships"><sheets><sheet name="survey" sheetId="1" r:id="rId1"/><sheet name="choices" sheetId="2" r:id="rId2"/></sheets></workbook>`,
		'xl/_rels/workbook.xml.rels': `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="${ns}package/2006/relationships"><Relationship Id="rId1" Type="${ns}officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="${ns}officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/></Relationships>`,
		'xl/worksheets/sheet1.xml': sheet([['type', 'name', 'label'], ...T.survey.questions.map(q => [q.type, q.name, q.label])]),
		'xl/worksheets/sheet2.xml': sheet(choiceRows)
	});
	const find = async () => (await getJson('/surveyKPI/surveys?projectId=' + CONFIG.project + '&blocked=true')).find(s => s.displayName === T.survey.name);
	let s = await find();
	if (!s) {
		const fd = new FormData();
		fd.append('templateName', T.survey.name);
		fd.append('projectId', String(CONFIG.project));
		fd.append('file', xlsx, 'multiple_assignee_test.xlsx');
		const r = await fetch('/surveyKPI/upload/surveytemplate', { method: 'POST', headers: H, body: fd });
		s = await find();
		check('survey created', s, 'upload ' + r.status + (s ? '' : ' ' + (await r.text()).slice(0, 120)));
	}
	if (s) {
		result.sId = s.id; result.ident = s.ident;
		CONFIG.sId = s.id; CONFIG.ident = s.ident;
		check('case settings, one assignee', await setMultiple(false));
		const have = await getJson('/surveyKPI/notifications/' + CONFIG.project + '?tz=UTC');
		for (const n of T.notifications) {
			if (have.some(x => x.name === n.name)) continue;
			const notification = { name: n.name, trigger: 'submission', target: 'escalate', enabled: true, filter: n.filter,
				bundle: false, s_id: s.id, remote_user: n.assign,
				notifyDetails: { survey_case: s.ident, assign_question: n.question || '', emails: [], emailAssigned: false } };
			const r = await post('/surveyKPI/notifications/add?tz=UTC', { notification: JSON.stringify(notification) });
			check('created ' + n.name, r.ok, r.ok ? '' : r.status);
		}
		const now = await getJson('/surveyKPI/notifications/' + CONFIG.project + '?tz=UTC');
		for (const n of T.notifications) {
			const x = now.find(y => y.name === n.name);
			check(n.name + ': enabled escalation', x && x.enabled && x.target === 'escalate' && x.filter === n.filter, x ? x.target + ' ' + x.filter : 'missing');
		}
	}
} else if (CONFIG.part === 'single') {
	check('one assignee', await setMultiple(false));
	const r1 = inst('r1');
	check('record r1 made', await submit(r1, { m_status: 'open' }));
	await assign(r1, U.a);
	check('assign sets the one assignee', sorted(await owners(r1)) === U.a, (await owners(r1)).join(','));
	await assign(r1, U.b);
	check('assign again replaces', sorted(await owners(r1)) === U.b, (await owners(r1)).join(','));
	const roles = await getJson('/surveyKPI/role/roles');
	const role = roles.find(x => x.name === T.role);
	const rr = role ? await post('/surveyKPI/managed/assignrole/' + CONFIG.sId + '/' + role.id, { record: r1 }) : null;
	check('a role is refused', rr && rr.status === 400, rr ? rr.status : 'no role ' + T.role);
} else if (CONFIG.part === 'multiple') {
	const r1 = inst('r1');
	check('multiple assignees on', await setMultiple(true));
	const start = await owners(r1);
	await assign(r1, U.a);
	check('assign adds', sorted(await owners(r1)) === sorted([...start, U.a]), (await owners(r1)).join(','));
	const roles = await getJson('/surveyKPI/role/roles');
	const role = roles.find(x => x.name === T.role);
	const before = await owners(r1);
	const rr = await post('/surveyKPI/managed/assignrole/' + CONFIG.sId + '/' + role.id, { record: r1 });
	const res = rr.ok ? await rr.json() : {};
	const afterRole = await owners(r1);
	check('a role adds its members', rr.ok && afterRole.length === new Set([...before, ...afterRole]).size && before.every(u => afterRole.includes(u)),
		rr.status + ', ' + res.assigned + ' assigned, ' + res.notAllowed + ' not allowed, now ' + afterRole.join(','));
	await unassign(r1, U.a);
	check('an administrator takes one person off', !(await owners(r1)).includes(U.a), (await owners(r1)).join(','));
	await post('/surveyKPI/managed/lock/' + CONFIG.sId, { record: r1 });
	check('lock adds me', (await owners(r1)).includes(U.me), (await owners(r1)).join(','));
	const others = (await owners(r1)).filter(u => u !== U.me);
	await post('/surveyKPI/managed/release/' + CONFIG.sId, { record: r1 });
	check('release takes only me off', sorted(await owners(r1)) === sorted(others), (await owners(r1)).join(','));
	await unassign(r1, '_all');
	check('unassign all', (await owners(r1)).length === 0, (await owners(r1)).join(','));
	await assign(r1, U.a);
	await assign(r1, U.b);
	check('two assigned for later', sorted(await owners(r1)) === sorted([U.a, U.b]), (await owners(r1)).join(','));
	const ev = await getJson('/surveyKPI/api/data/changes/' + CONFIG.sId + '/' + encodeURIComponent(r1) + '?tz=UTC');
	check('history records each change', ev.filter(e => e.event === 'assigned').length >= 8, ev.filter(e => e.event === 'assigned').length + ' entries');
} else if (CONFIG.part === 'escalateAdd') {
	const r2 = inst('r2');
	if (!(await owners(r2))) {
		check('record r2 made, escalated to ' + T.notifications[0].assign, await submit(r2, { m_status: 'open', m_esc: 'a' }));
	}
	const o = await waitOwners(r2, [T.notifications[0].assign], 35000);
	result.pending = !(o && sorted(o) === T.notifications[0].assign);
	if (!result.pending) {
		check('escalation assigned its user', true, o.join(','));
		await assign(r2, U.b);
		check('a person added in the console', sorted(await owners(r2)) === sorted([U.a, U.b]), (await owners(r2)).join(','));
	} else {
		result.seen = o;
	}
} else if (CONFIG.part === 'escalateReplace') {
	const r2 = inst('r2'), r2b = inst('r2b');
	if (!(await owners(r2b))) {
		const thread = r2;		// The first version names the thread
		const opened = await latest(thread) || r2;
		check('r2 updated to escalate from data', await submit(r2b, { m_status: 'open', m_esc: 'b', m_users: U.me + ' ' + U.b }, opened));
	}
	const want = [U.me, U.b];
	const o = await waitOwners(r2b, want, 35000);
	result.pending = !(o && sorted(o) === sorted(want));
	if (!result.pending) {
		check('the earlier escalation\'s user taken off, the console\'s kept, everyone selected added', true, o.join(','));
	} else {
		result.seen = o;
	}
} else if (CONFIG.part === 'close') {
	const r2b = inst('r2b'), r2c = inst('r2c');
	if (!(await owners(r2c))) {
		const opened = await latest(inst('r2')) || r2b;
		check('r2 closed', await submit(r2c, { m_status: T.caseSettings.finalStatus, m_esc: 'b', m_users: U.me + ' ' + U.b }, opened));
	}
	const o = await waitOwners(r2c, [], 30000);
	result.pending = !(o && o.length === 0);
	if (!result.pending) check('closing takes everybody off', true);
	else result.seen = o;
} else if (CONFIG.part === 'switchBack') {
	const r1 = inst('r1');
	check('one assignee again', await setMultiple(false));
	check('owners kept until changed', sorted(await owners(r1)) === sorted([U.a, U.b]), (await owners(r1)).join(','));
	const m = await getJson('/surveyKPI/cases/multiple/' + CONFIG.sId);
	check('setting reported off', m.multipleAssignees === false, JSON.stringify(m));
}
result.pass = lines.length > 0 && lines.every(l => l.indexOf('PASS') === 0) && !result.pending;
result.checks = lines;
JSON.stringify(result);
