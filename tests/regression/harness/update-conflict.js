// Update conflict test, run with javascript_tool on any page of the server.  One scenario per call,
// as a call times out after 45 seconds.  Replace the placeholder on the CONFIG line with:
//   {"sId": <stage 1 survey id>, "identA": <stage 1 ident>, "identB": <stage 2 ident>,
//    "run": <a number unique to this run>, "scenario": <one scenario from the test file>}
//
// Each save step posts a submission the way a device does, so the version it opened can be chosen:
//   {"save": "<label>", "survey": "a" | "b", "opened": "<label of the version opened>" | absent for a new record,
//    "answers": {...}, "rows": [{"visit_note": ..., "rowId": ...}], "noRowIds": true}
// A console step changes the record in the console, as a person would, and labels the new version:
//   {"console": "<label>", "opened": "<label>", "updates": [{"name": ..., "value": ...}]}
// A resolve step reviews the latest conflict on the record:
//   {"resolve": {"0": "theirs"}}
// An expect step checks the latest version of the record:
//   {"expect": {"answers": {...}, "rows": [...visit notes in order...], "conflicts": n, "unresolved": n,
//    "items": [{"name", "base", "theirs", "mine"}]}}
const CONFIG = __CONFIG__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const getJson = url => fetch(url, { headers: H, cache: 'no-store' })
	.then(r => { if (!r.ok) throw new Error(url.split('?')[0] + ' ' + r.status); return r.json(); });
const xmlEsc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const lines = [];
const check = (name, ok, detail) => lines.push({ check: name, ok: !!ok, detail: detail === undefined ? '' : String(detail) });

const sc = CONFIG.scenario;
const versions = {};		// label -> instance id
// Instance ids are held in a column of 41 characters, so the scenario is named by its initials
const short = sc.name.split('-').map(w => w[0]).join('');
const inst = label => 'uuid:uc-' + CONFIG.run + '-' + short + '-' + label;
const key = (await getJson('/surveyKPI/login/key?form=user')).key;

const pick = (obj, name) => {
	if (!obj) return undefined;
	const want = name.toLowerCase();
	const k = Object.keys(obj).find(x => x.toLowerCase() === want)
		|| Object.keys(obj).find(x => x.toLowerCase().replace(/^_/, '') === want.replace(/^_/, ''));
	return k === undefined ? undefined : obj[k];
};
const rowsOf = r => Array.isArray(r) ? r : (r && (r.data || r.records)) || [];

// The record as the console reads it, the latest version only.  Read through the stage 2 survey, which
// has every question of the record, and returns the values of select questions rather than their labels
async function getRecord(instanceId) {
	const r = await getJson('/surveyKPI/api/data/' + CONFIG.identB + '?meta=yes&instanceid=' + encodeURIComponent(instanceId));
	return rowsOf(r)[0];
}
async function getRows(prikey) {
	const r = await getJson('/surveyKPI/api/data/' + CONFIG.identB + '?meta=yes&form=visits&parkey=' + prikey);
	return rowsOf(r);
}
// Wait for the background process to apply a save
async function applied(instanceId) {
	for (let i = 0; i < 15; i++) {
		const rec = await getRecord(instanceId);
		if (rec) return rec;
		await sleep(2000);
	}
	return null;
}

let latest = null;
for (const step of sc.steps) {
	if (step.save) {
		const ident = step.survey === 'b' ? CONFIG.identB : CONFIG.identA;
		const id = inst(step.save);
		const answers = Object.entries(step.answers || {}).map(([q, v]) => `<${q}>${xmlEsc(v)}</${q}>`).join('');
		const rows = (step.rows || []).map(r => '<visits><visit_note>' + xmlEsc(r.visit_note) + '</visit_note>'
			+ (step.noRowIds || !r.rowId ? '' : '<_row_id>' + xmlEsc(r.rowId) + '</_row_id>') + '</visits>').join('');
		const xml = `<?xml version="1.0" encoding="UTF-8"?><main id="${ident}"><meta><instanceID>${xmlEsc(id)}</instanceID></meta>${answers}${rows}</main>`;
		const fd = new FormData();
		fd.append('xml_submission_file', new Blob([xml], { type: 'text/xml' }), 'submission.xml');
		let url = '/submission/key/' + encodeURIComponent(key);
		if (step.opened) url += '/' + encodeURIComponent(versions[step.opened]);
		const r = await fetch(url + '?deviceID=update-conflict-test', { method: 'POST', body: fd });
		if (r.status !== 201) { check('save ' + step.save, false, 'status ' + r.status); break; }
		versions[step.save] = id;
		latest = id;
		const rec = await applied(id);
		check('save ' + step.save + ' applied', rec, rec ? '' : 'not applied within 30s');
		if (!rec) break;
	} else if (step.console) {
		const rec = await getRecord(latest);
		const body = new URLSearchParams({
			updates: JSON.stringify(step.updates.map(u => ({ name: u.name, value: u.value, currentValue: pick(rec, u.name), displayName: u.name }))),
			instanceid: versions[step.opened],
			prikey: '0',
			tz: 'UTC'
		});
		const r = await fetch('/surveyKPI/managed/update_gs/' + CONFIG.sId + '/' + CONFIG.identA,
			{ method: 'POST', headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' }, body });
		const text = await r.text();
		let v = null;
		try { v = JSON.parse(text); } catch (e) { v = null; }
		check('console ' + step.console, r.ok && v && v.instanceid, r.ok ? '' : 'status ' + r.status);
		if (!(r.ok && v && v.instanceid)) break;
		check('console made a new version', v.instanceid !== versions[step.opened], v.instanceid === versions[step.opened] ? 'changed in place' : '');
		versions[step.console] = v.instanceid;
		latest = v.instanceid;
	} else if (step.resolve) {
		const events = await getJson('/surveyKPI/api/data/changes/' + CONFIG.sId + '/' + encodeURIComponent(latest) + '?tz=UTC');
		const c = events.find(e => e.event === 'conflict' && e.status === 'unresolved');
		if (!c) { check('resolve', false, 'no unresolved conflict'); break; }
		const r = await fetch('/surveyKPI/managed/conflict/' + CONFIG.sId + '/' + c.id,
			{ method: 'POST', headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' },
			  body: new URLSearchParams({ choices: JSON.stringify(step.resolve), tz: 'UTC' }) });
		const text = await r.text();
		let v = null;
		try { v = JSON.parse(text); } catch (e) { v = null; }
		check('resolve', r.ok, r.ok ? '' : 'status ' + r.status);
		const hasVersion = !!(v && v.instanceid && v.prikey);
		check('resolve returned the new version', hasVersion, hasVersion ? '' : 'got ' + text.slice(0, 80));
		if (v && v.instanceid) latest = v.instanceid;
	} else if (step.expect) {
		// A version made on the server, by the console or a review, is found by the record's thread
		if (!(await getRecord(latest))) {
			const thread = versions[sc.steps.find(x => x.save).save];
			const all = rowsOf(await getJson('/surveyKPI/api/data/' + CONFIG.identB + '?meta=yes'));
			const found = all.find(x => pick(x, '_thread') === thread);
			if (found) latest = pick(found, 'uuid');
		}
		const ex = step.expect;
		const rec = await getRecord(latest);
		if (!rec) { check('record', false, 'latest version not found'); break; }
		for (const [q, v] of Object.entries(ex.answers || {})) {
			const actual = pick(rec, q);
			check(q + ' = ' + v, String(actual === null || actual === undefined ? '' : actual) === String(v), 'got ' + actual);
		}
		if (ex.rows) {
			const rows = (await getRows(pick(rec, 'prikey'))).map(r => pick(r, 'visit_note'));
			check('rows ' + ex.rows.join(', '), JSON.stringify(rows) === JSON.stringify(ex.rows), 'got ' + rows.join(', '));
		}
		if (ex.conflicts !== undefined || ex.items || ex.unresolved !== undefined) {
			const events = await getJson('/surveyKPI/api/data/changes/' + CONFIG.sId + '/' + encodeURIComponent(pick(rec, 'uuid')) + '?tz=UTC');
			const conflicts = events.filter(e => e.event === 'conflict');
			if (ex.conflicts !== undefined) {
				check('conflicts ' + ex.conflicts, conflicts.length === ex.conflicts, 'got ' + conflicts.length);
			}
			if (ex.unresolved !== undefined) {
				const u = conflicts.filter(e => e.status === 'unresolved').length;
				check('unresolved ' + ex.unresolved, u === ex.unresolved, 'got ' + u);
			}
			if (ex.items) {
				const items = conflicts.length ? (conflicts[0].conflict.items || []) : [];
				for (const want of ex.items) {
					const got = items.find(i => (i.name || '') === (want.name || '') && (want.kind ? i.kind === want.kind : true));
					const ok = got && ['base', 'theirs', 'mine'].every(f => want[f] === undefined || String(got[f]) === String(want[f]));
					check('conflict ' + (want.name || want.kind), ok, got ? 'base ' + got.base + ', theirs ' + got.theirs + ', mine ' + got.mine : 'missing');
				}
			}
		}
	}
}
JSON.stringify({ scenario: sc.name, lines });
