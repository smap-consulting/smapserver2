// Setup steps for an xss test that need the browser session, run with javascript_tool on any page of
// the server.  Replace the placeholder on the STEP line with one step as JSON:
//   {"do": "submit", "survey": "<survey ident>", "instanceID": ..., "deviceID": ..., "answers": {...}}
//       Submit a record the way the webform does, with a key from /surveyKPI/login/key, so the device
//       id and the instance id can be chosen.  They cannot be set through the MCP connection.
//   {"do": "addPanels", "project": <project id>, "sId": <survey id>, "title": ..., "question": <name>,
//    "types": ["graph", "table"]}
//       Remove any analysis panels with this title, then add one of each type for the question.  Only
//       panels with this title are touched, the user's other panels are left as they are.
//   {"do": "removePanels", "project": <project id>, "title": ...}
//       Remove the analysis panels with this title.
const STEP = __STEP__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const getJson = url => fetch(url, { headers: H, cache: 'no-store' })
	.then(r => { if (!r.ok) throw new Error(url + ' ' + r.status); return r.json(); });
const post = (url, body) => fetch(url, { method: 'POST', headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' }, body })
	.then(r => { if (!r.ok) throw new Error(url + ' ' + r.status); return r; });
const xmlEsc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

async function removePanels(project, title) {
	const views = await getJson('/surveyKPI/dashboard/' + project);
	const mine = views.filter(v => v.title === title);
	for (const v of mine) {
		await post('/surveyKPI/dashboard/state', 'state=' + encodeURIComponent(JSON.stringify({ id: v.id, state: 'deleted' })));
	}
	return mine.length;
}

let result;
if (STEP.do === 'submit') {
	const key = (await getJson('/surveyKPI/login/key?form=user')).key;
	const answers = Object.entries(STEP.answers).map(([q, v]) => `<${q}>${xmlEsc(v)}</${q}>`).join('');
	const xml = `<?xml version="1.0" encoding="UTF-8"?><main id="${STEP.survey}"><meta><instanceID>${xmlEsc(STEP.instanceID)}</instanceID></meta>${answers}</main>`;
	const fd = new FormData();
	fd.append('xml_submission_file', new Blob([xml], { type: 'text/xml' }), 'submission.xml');
	const r = await fetch('/submission/key/' + encodeURIComponent(key) + '?deviceID=' + encodeURIComponent(STEP.deviceID), { method: 'POST', body: fd });
	result = { submitted: r.status === 201, status: r.status };
} else if (STEP.do === 'addPanels') {
	const removed = await removePanels(STEP.project, STEP.title);
	const questions = await getJson('/surveyKPI/questionList/' + STEP.sId + '/language?exc_read_only=false');
	const q = questions.find(x => x.name === STEP.question);
	if (!q) throw new Error('No question ' + STEP.question);
	const views = STEP.types.map((type, i) => ({
		id: -1, seq: 90 + i, state: 'shown', title: STEP.title, pId: 0, sId: STEP.sId, sName: STEP.title, type,
		layerId: -1, region: 'none', lang: 'language', subject_type: 'survey', qId: Number(q.id), qId_is_calc: false,
		dateQuestionId: -1, question: STEP.question, table: '', groupQuestionId: -1, groupQuestionText: '',
		groupType: 'normal', timeGroup: 'none', advanced_filter: '', inc_ro: false, chartType: 'bar',
		tShowMeta: false, tWrapText: true
	}));
	await post('/surveyKPI/dashboard/', 'settings=' + encodeURIComponent(JSON.stringify(views)));
	result = { removed, added: (await getJson('/surveyKPI/dashboard/' + STEP.project)).filter(v => v.title === STEP.title).length };
} else if (STEP.do === 'removePanels') {
	result = { removed: await removePanels(STEP.project, STEP.title) };
}
JSON.stringify(result);
