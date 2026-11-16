// Case alert test, run with javascript_tool.  A call times out after 45 seconds, so the test is run in parts.
// Replace the placeholder on the CONFIG line with:
//   {"part": "setup", "project": <project id>, "run": <the time in ms>, "test": <the test file>}
//       On any console page.  Makes the test survey from a small xlsx if there is none with its name, sets its
//       case settings and alert when they differ, and submits the test's records with new instance ids.
//       Returns the survey id and the records' instance ids.
//   {"part": "check", "sId": <survey id>, "records": [{"label", "instanceid", "alerts"}], "alert": <alert name>}
//       On any console page.  Waits up to 35 seconds for each record to have the number of alert events it
//       expects.  Returns "pending" while an alert that should fire has not, so call it again.
//   {"part": "render", "sId": <survey id>, "instanceid": <the open record>, "alert": <alert name>}
//       On /app/tasks/managed_forms.html.  Shows the record's history with the console's own code and checks
//       the alert row and the Alerts switch.  The page's history is put back as it was.
const CONFIG = __CONFIG__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const getJson = url => fetch(url, { headers: H, cache: 'no-store' })
	.then(r => { if (!r.ok) throw new Error(url.split('?')[0] + ' ' + r.status); return r.json(); });
const xmlEsc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const sleep = ms => new Promise(r => setTimeout(r, ms));
const lines = [];
const check = (name, ok, detail) => lines.push({ check: name, ok: !!ok, detail: detail === undefined ? '' : String(detail) });
const events = (sId, inst) => getJson('/surveyKPI/api/data/changes/' + sId + '/' + encodeURIComponent(inst) + '?tz=UTC');
const post = (url, field, value) => fetch(url, { method: 'POST', headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' },
	body: field + '=' + encodeURIComponent(JSON.stringify(value)) });

// A zip of stored (uncompressed) entries, all an xlsx needs
function zip(files) {
	const enc = new TextEncoder(), crcTable = [];
	for (let n = 0; n < 256; n++) { let c = n; for (let k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1; crcTable[n] = c >>> 0; }
	const crc = d => { let c = 0xFFFFFFFF; for (const b of d) c = crcTable[(c ^ b) & 0xFF] ^ (c >>> 8); return (c ^ 0xFFFFFFFF) >>> 0; };
	const parts = [], central = [];
	let offset = 0;
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
}
const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const sheet = rows => '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>' +
	rows.map((r, i) => '<row r="' + (i + 1) + '">' + r.map((v, j) => '<c r="' + String.fromCharCode(65 + j) + (i + 1) + '" t="inlineStr"><is><t>' + esc(v) + '</t></is></c>').join('') + '</row>').join('') +
	'</sheetData></worksheet>';
const xlsx = questions => zip({
	'[Content_Types].xml': '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>',
	'_rels/.rels': '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>',
	'xl/workbook.xml': '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="survey" sheetId="1" r:id="rId1"/></sheets></workbook>',
	'xl/_rels/workbook.xml.rels': '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>',
	'xl/worksheets/sheet1.xml': sheet([['type', 'name', 'label'], ...questions.map(q => [q.type, q.name, q.label])])
});

const result = { part: CONFIG.part };
if (CONFIG.part === 'setup') {
	const T = CONFIG.test;
	const find = async () => (await getJson('/surveyKPI/surveys?projectId=' + CONFIG.project + '&blocked=true')).find(s => s.displayName === T.survey.name);
	let s = await find();
	if (!s) {
		const fd = new FormData();
		fd.append('templateName', T.survey.name);
		fd.append('projectId', String(CONFIG.project));
		fd.append('file', xlsx(T.survey.questions), 'case_alert_test.xlsx');
		const r = await fetch('/surveyKPI/upload/surveytemplate', { method: 'POST', headers: H, body: fd });
		s = await find();
		check('survey created', s, 'upload ' + r.status);
		result.created = true;
	}
	if (s) {
		result.sId = s.id;
		result.ident = s.ident;
		let cms = await getJson('/surveyKPI/cases/settings/' + s.id);
		const bundle = cms.group_survey_ident || s.ident;
		const set = cms.settings || {};
		if (set.statusQuestion !== T.caseSettings.statusQuestion || set.finalStatus !== T.caseSettings.finalStatus) {
			const r = await post('/surveyKPI/cases/settings/' + encodeURIComponent(bundle), 'settings', { ...set, ...T.caseSettings });
			check('case settings saved', r.ok, r.ok ? '' : r.status + ' ' + (await r.text()).slice(0, 100));
		}
		const alerts = (cms.alerts || []).filter(a => a.name === T.alert.name);
		if (alerts.length === 0 || alerts[0].period !== T.alert.period || alerts[0].filter) {
			const a = { id: alerts.length ? alerts[0].id : -1, group_survey_ident: bundle, name: T.alert.name, period: T.alert.period, filter: '' };
			const r = await post('/surveyKPI/cases/settings/alert', 'alert', a);
			check('alert saved', r.ok, r.ok ? '' : r.status + ' ' + (await r.text()).slice(0, 100));
		}
		cms = await getJson('/surveyKPI/cases/settings/' + s.id);
		check('case settings', cms.settings && cms.settings.statusQuestion === T.caseSettings.statusQuestion
			&& cms.settings.finalStatus === T.caseSettings.finalStatus, JSON.stringify(cms.settings));
		check('one alert named ' + T.alert.name, (cms.alerts || []).filter(a => a.name === T.alert.name && a.period === T.alert.period).length === 1);

		// The records, posted as a device posts them
		result.records = [];
		for (const rec of T.records) {
			const inst = 'uuid:ca-' + CONFIG.run + '-' + rec.label;
			const key = (await getJson('/surveyKPI/login/key?form=user')).key;
			const answers = Object.entries(rec.answers).map(([q, v]) => `<${q}>${xmlEsc(v)}</${q}>`).join('');
			const xml = `<?xml version="1.0" encoding="UTF-8"?><main id="${s.ident}"><meta><instanceID>${inst}</instanceID></meta>${answers}</main>`;
			const fd = new FormData();
			fd.append('xml_submission_file', new Blob([xml], { type: 'text/xml' }), 'submission.xml');
			const r = await fetch('/submission/key/' + encodeURIComponent(key) + '?deviceID=case-alert-test', { method: 'POST', body: fd });
			check('submit ' + rec.label, r.ok, r.status);
			result.records.push({ label: rec.label, instanceid: inst, alerts: rec.alerts });
		}
	}
} else if (CONFIG.part === 'check') {
	const until = Date.now() + 35000;
	let counts;
	for (;;) {
		counts = {};
		for (const rec of CONFIG.records) {
			let ev = [];
			try { ev = await events(CONFIG.sId, rec.instanceid); } catch (e) { ev = []; }
			counts[rec.label] = ev.filter(e => e.event === 'alert');
		}
		const fired = CONFIG.records.every(r => r.alerts === 0 || counts[r.label].length >= r.alerts);
		if (fired || Date.now() > until) {
			result.pending = !fired;
			break;
		}
		await sleep(5000);
	}
	if (!result.pending) {
		for (const rec of CONFIG.records) {
			const a = counts[rec.label];
			check(rec.label + ': alert events', a.length === rec.alerts, a.length + ' (expected ' + rec.alerts + ')');
			for (const e of a) {
				check(rec.label + ': alert names the alert', (e.description || '').indexOf(CONFIG.alert) >= 0, e.description);
				check(rec.label + ': alert status', e.status === 'success', e.status);
			}
		}
	}
} else if (CONFIG.part === 'render') {
	const $ = window.jQuery;
	check('console page', $ && $('#changes').length && $('#er_show_alerts').length, location.pathname);
	if ($ && $('#er_show_alerts').length) {
		const saved = window.gChanges;
		const ev = await events(CONFIG.sId, CONFIG.instanceid);
		window.gChanges = ev;
		const $sw = $('#er_show_alerts');
		const wasOn = $sw.prop('checked');
		const rowsOf = () => $('#changes tbody tr').filter(function () { return $(this).find('.fa-bell').length > 0; });
		$sw.prop('checked', true).trigger('change');
		const rows = rowsOf();
		check('alert row shown', rows.length === 1, rows.length + ' rows with a bell');
		const cells = rows.first().children('td').map((i, c) => $(c).text().trim()).get();
		check('event label', cells.includes(window.localise ? window.localise.set['alert'] : 'Alert') || cells.includes('Alert'), cells.join(' | '));
		check('details name the alert', cells.some(c => c.indexOf(CONFIG.alert) >= 0), cells.join(' | '));
		$sw.prop('checked', false).trigger('change');
		check('Alerts switch hides it', rowsOf().length === 0, rowsOf().length + ' rows');
		check('other events still shown', $('#changes tbody tr').length > 0, $('#changes tbody tr').length + ' rows');
		$sw.prop('checked', wasOn);
		window.gChanges = saved;
		$sw.trigger('change');
	}
}
result.pass = lines.length > 0 && lines.every(l => l.ok) && !result.pending;
result.checks = lines.map(l => (l.ok ? 'PASS ' : 'FAIL ') + l.check + (l.detail ? ' - ' + l.detail : ''));
JSON.stringify(result);
