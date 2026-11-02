// Setup for an sqli test, run with javascript_tool on any console page of the server.
// Replace the placeholder on the STEP line with one step:
//   {"do": "survey", "project": <project id>, "survey": <the test's survey>}
//       Make the test survey from a small xlsx if there is none with its name, for a server the smap MCP
//       connection does not reach, or whose questions have parameters, which survey_add_question cannot set
//       (otherwise make it with survey_create).  Returns its id and ident.
//   {"do": "ids", "sId": <survey id>, "role": <optional role name>}
//       The survey's top form id and table and its question ids by name, for the probes' {fId}, {table}
//       and {q:name}, and the role's id for {roleId}.
//   {"do": "records", "sId": <survey id>, "ident": <survey ident>, "records": <the test's records>}
//       Submit each record whose instanceID is not in the survey yet, as the webform does.  Kept.
//   {"do": "csv", "sId": <survey id>, "csv": <the test's csv>}
//       Attach the test's csv file to the survey, once.  Kept for later runs.
const STEP = __STEP__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const getJson = url => fetch(url, { headers: H, cache: 'no-store' }).then(r => { if (!r.ok) throw new Error(url + ' ' + r.status); return r.json(); });

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
function xlsx(questions) {
	return zip({
		'[Content_Types].xml': '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>',
		'_rels/.rels': '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>',
		'xl/workbook.xml': '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="survey" sheetId="1" r:id="rId1"/></sheets></workbook>',
		'xl/_rels/workbook.xml.rels': '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>',
		'xl/worksheets/sheet1.xml': sheet(questions.some(q => q.parameters)
			? [['type', 'name', 'label', 'parameters'], ...questions.map(q => [q.type, q.name, q.label, q.parameters || ''])]
			: [['type', 'name', 'label'], ...questions.map(q => [q.type, q.name, q.label])])
	});
}

const result = {};
if (STEP.do === 'survey') {
	const find = async () => (await getJson('/surveyKPI/surveys?projectId=' + STEP.project + '&blocked=true')).find(s => s.displayName === STEP.survey.name);
	let s = await find();
	if (!s) {
		const fd = new FormData();
		fd.append('templateName', STEP.survey.name);
		fd.append('projectId', String(STEP.project));
		fd.append('file', xlsx(STEP.survey.questions.map(q => ({ type: q.type === 'string' ? 'text' : q.type === 'int' ? 'integer' : q.type, name: q.name, label: q.label, parameters: q.parameters }))), 'sqli_test.xlsx');
		const r = await fetch('/surveyKPI/upload/surveytemplate', { method: 'POST', headers: H, body: fd });
		result.upload = r.status;
		s = await find();
		result.created = !!s;
	}
	if (s) {
		result.id = s.id;
		result.ident = s.ident;
	}
} else if (STEP.do === 'ids') {
	const meta = await getJson('/surveyKPI/survey/' + STEP.sId + '/getMeta');
	const top = meta.forms.find(f => f.name === meta.top_table) || meta.forms[0];
	result.fId = top.f_id;
	result.table = top.name;
	result.q = {};
	for (const q of await getJson('/surveyKPI/questionList/' + STEP.sId + '/language?exc_read_only=false')) {
		result.q[q.name] = Number(q.id);
	}
	// Groups and repeats are not in the question list; the editor's survey has them
	const def = await getJson('/surveyKPI/surveys/' + STEP.sId + '?tz=UTC');
	for (const f of def.forms || []) {
		for (const q of f.questions || []) {
			if (result.q[q.name] === undefined) result.q[q.name] = Number(q.id);
		}
	}
	if (STEP.role) {
		const role = (await getJson('/surveyKPI/role/roles')).find(r => r.name === STEP.role);
		result.roleId = role ? role.id : null;
	}
} else if (STEP.do === 'records') {
	const meta = await getJson('/surveyKPI/survey/' + STEP.sId + '/getMeta');
	const top = meta.forms.find(f => f.name === meta.top_table) || meta.forms[0];
	const have = (await getJson('/surveyKPI/items/' + top.f_id + '?rec_limit=500')).features || [];
	const ids = new Set(have.map(f => f.properties && f.properties.instanceid));
	const xmlEsc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
	result.submitted = [];
	for (const rec of STEP.records) {
		if (ids.has(rec.instanceID)) continue;
		const key = (await getJson('/surveyKPI/login/key?form=user')).key;
		const answers = Object.entries(rec.answers).map(([q, v]) => `<${q}>${xmlEsc(v)}</${q}>`).join('');
		const xml = `<?xml version="1.0" encoding="UTF-8"?><main id="${STEP.ident}"><meta><instanceID>${xmlEsc(rec.instanceID)}</instanceID></meta>${answers}</main>`;
		const fd = new FormData();
		fd.append('xml_submission_file', new Blob([xml], { type: 'text/xml' }), 'submission.xml');
		const r = await fetch('/submission/key/' + encodeURIComponent(key) + '?deviceID=sqli-test', { method: 'POST', body: fd });
		result.submitted.push(rec.instanceID + ' ' + r.status);
	}
} else if (STEP.do === 'csv') {
	const fileName = STEP.csv.name + '.csv';
	const have = await getJson('/surveyKPI/upload/media?survey_id=' + STEP.sId);
	const files = (have.files || have || []).map(f => f.name || f.fileName || '');
	if (files.some(n => n === fileName || n === STEP.csv.name)) {
		result.existing = true;
	} else {
		const fd = new FormData();
		fd.append('surveyId', String(STEP.sId));
		fd.append('file', new Blob([STEP.csv.content], { type: 'text/csv' }), fileName);
		const r = await fetch('/surveyKPI/upload/media', { method: 'POST', headers: H, body: fd });
		result.uploaded = r.status;
	}
}
JSON.stringify(result);
