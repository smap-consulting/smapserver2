// Setup for a dashboard-region test, run with javascript_tool on the analysis page of the server.
// Replace the placeholder on the STEP line with
//   {"do": "bundle", "sId": <test survey id>, "project": <project id>, "bundle": <the test's bundle>}
// Makes the test's polygon survey in the test survey's bundle, once, and submits its polygon record, once.
// A survey can only join a bundle when it is uploaded as a spreadsheet, so a small xlsx is built here.
// Both are kept for later runs.  Returns the polygon survey's id and ident.
const STEP = __STEP__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const getJson = url => fetch(url, { headers: H, cache: 'no-store' }).then(r => { if (!r.ok) throw new Error(url + ' ' + r.status); return r.json(); });
const sleep = ms => new Promise(r => setTimeout(r, ms));
const B = STEP.bundle;

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
		'xl/worksheets/sheet1.xml': sheet([['type', 'name', 'label'], ...questions.map(q => [q.type, q.name, q.label])])
	});
}

const result = {};
const findSurvey = async () => (await getJson('/surveyKPI/surveys?projectId=' + STEP.project + '&blocked=true')).find(s => s.displayName === B.name);
let poly = await findSurvey();
if (!poly) {
	const fd = new FormData();
	fd.append('templateName', B.name);
	fd.append('projectId', String(STEP.project));
	fd.append('surveyId', String(STEP.sId));		// Add it to the test survey's bundle
	fd.append('action', 'add');
	fd.append('file', xlsx(B.questions), 'region_polygons.xlsx');
	const r = await fetch('/surveyKPI/upload/surveytemplate', { method: 'POST', headers: H, body: fd });
	result.upload = r.status + ' ' + (await r.text()).slice(0, 120);
	poly = await findSurvey();
	result.created = !!poly;
}
if (poly) {
	result.id = poly.id;
	result.ident = poly.ident;
	const have = await fetch('/surveyKPI/results/' + STEP.sId + '?qId=-1&fn=none&lang=x&dateId=-100&selected_geom_question=' + encodeURIComponent(STEP.location) +
		'&regionSurvey=' + encodeURIComponent(poly.ident) + '&regionShape=' + encodeURIComponent(B.shape), { headers: H, cache: 'no-store' });
	const features = have.ok ? (await have.json())[0].features : [];
	if (features.length === 0) {
		const key = (await getJson('/surveyKPI/login/key?form=user')).key;
		const answers = Object.entries(B.record.answers).map(([k, v]) => `<${k}>${esc(v)}</${k}>`).join('');
		const xml = `<?xml version="1.0" encoding="UTF-8"?><main id="${poly.ident}"><meta><instanceID>${esc(B.record.instanceID)}</instanceID></meta>${answers}</main>`;
		const fd = new FormData();
		fd.append('xml_submission_file', new Blob([xml], { type: 'text/xml' }), 'submission.xml');
		const r = await fetch('/submission/key/' + encodeURIComponent(key) + '?deviceID=' + encodeURIComponent(B.record.deviceID), { method: 'POST', body: fd });
		result.submitted = r.status;
	} else {
		result.existing = features.length;
	}
}
JSON.stringify(result);
