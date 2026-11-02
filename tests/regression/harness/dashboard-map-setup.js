// Setup steps for a dashboard-map test, run with javascript_tool on the analysis page of the server.
// Replace the placeholder on the STEP line with one step as JSON:
//   {"do": "trail", "trail": <the test's trail>}
//       Give the logged in user the test's trail, the way FieldTask uploads one, unless the trail's
//       day already has at least that many points.  The trail is kept for later runs.
//   {"do": "addPanels", "project": <project id>, "sId": <survey id>, "title": ..., "graphQuestion": <name>,
//    "geomQuestion": <name>, "lineQuestion": <name>}
//       Remove any panels whose title starts with this title, then add a User Activity map for the
//       logged in user, a Data map of each location question, a daily time series map of the graph
//       question, a Data table of the survey and a graph of the question.  The user's other panels
//       are left as they are.
//   {"do": "removePanels", "project": <project id>, "title": ...}
//       Remove the panels whose title starts with this title.
const STEP = __STEP__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const getJson = url => fetch(url, { headers: H, cache: 'no-store' })
	.then(r => { if (!r.ok) throw new Error(url + ' ' + r.status); return r.json(); });
const post = (url, body) => fetch(url, { method: 'POST', headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' }, body })
	.then(r => { if (!r.ok) throw new Error(url + ' ' + r.status); return r; });
const myId = async () => (await getJson('/surveyKPI/userList')).find(u => u.ident === globals.gLoggedInUser.ident).id;

async function removePanels(project, title) {
	const mine = (await getJson('/surveyKPI/dashboard/' + project)).filter(v => v.title.indexOf(title) === 0);
	for (const v of mine) {
		await post('/surveyKPI/dashboard/state', 'state=' + encodeURIComponent(JSON.stringify({ id: v.id, state: 'deleted' })));
	}
	return mine.length;
}

let result;
if (STEP.do === 'trail') {
	const t = STEP.trail, uId = await myId();
	const dayStart = new Date(t.day + 'T00:00:00').getTime(), dayEnd = dayStart + 86400000;
	const have = await getJson(`/surveyKPI/usertrail/trail?userId=${uId}&startDate=${dayStart}&endDate=${dayEnd}&mps=0`);
	const count = have.features.reduce((a, f) => a + f.points.length, 0);
	if (count >= t.points.length) {
		result = { planted: 0, existing: count };
	} else {
		const start = new Date(t.start).getTime();
		const userTrail = t.points.map((p, i) => ({ lat: p[0], lon: p[1], time: start + i * t.stepMinutes * 60000 }));
		// The key path, /surveyKPI/myassignments itself is behind device basic auth and hangs the page
		const key = (await getJson('/surveyKPI/login/key?form=user')).key;
		await post('/surveyKPI/myassignments/key/' + encodeURIComponent(key), 'assignInput=' + encodeURIComponent(JSON.stringify({ deviceId: t.deviceId, taskAssignments: [], userTrail })));
		result = { planted: userTrail.length, existing: count };
	}
} else if (STEP.do === 'addPanels') {
	const removed = await removePanels(STEP.project, STEP.title);
	const uId = await myId();
	const langs = await getJson('/surveyKPI/languages/' + STEP.sId);
	const lang = typeof langs[0] === 'string' ? langs[0] : langs[0].name;		// The survey's first language
	const q = (await getJson('/surveyKPI/questionList/' + STEP.sId + '/' + encodeURIComponent(lang) + '?exc_read_only=false')).find(x => x.name === STEP.graphQuestion);
	if (!q) throw new Error('No question ' + STEP.graphQuestion);
	const base = {
		id: -1, state: 'shown', pId: 0, sId: STEP.sId, uId: 0, sName: STEP.title, layerId: -1, region: 'none',
		lang, subject_type: 'survey', qId: -1, qId_is_calc: false, dateQuestionId: -1, question: '',
		table: '', groupQuestionId: -1, groupQuestionText: '', groupType: 'normal', timeGroup: 'none',
		advanced_filter: '', inc_ro: false, chartType: 'bar', tShowMeta: false, tWrapText: true
	};
	const views = [
		{ ...base, seq: 90, title: STEP.title + ' - user map', type: 'map', subject_type: 'user', sId: -1, uId, dateQuestionId: 1 },
		{ ...base, seq: 91, title: STEP.title + ' - survey map', type: 'map',		// The dialog always sets a location question
			geomFormQuestions: [{ form: 'main', question: STEP.geomQuestion }], selectedGeomQuestion: STEP.geomQuestion },
		{ ...base, seq: 94, title: STEP.title + ' - line map', type: 'map',
			geomFormQuestions: [{ form: 'main', question: STEP.lineQuestion }], selectedGeomQuestion: STEP.lineQuestion },
		{ ...base, seq: 95, title: STEP.title + ' - time map', type: 'map', qId: Number(q.id), question: STEP.graphQuestion,
			fn: 'percent', timeGroup: 'day', dateQuestionId: -100,		// -100 is upload time
			geomFormQuestions: [{ form: 'main', question: STEP.geomQuestion }], selectedGeomQuestion: STEP.geomQuestion },
		{ ...base, seq: 92, title: STEP.title + ' - graph', type: 'graph', qId: Number(q.id), question: STEP.graphQuestion },
		{ ...base, seq: 93, title: STEP.title + ' - table', type: 'table' }
	];
	await post('/surveyKPI/dashboard/', 'settings=' + encodeURIComponent(JSON.stringify(views)));
	result = { removed, added: (await getJson('/surveyKPI/dashboard/' + STEP.project)).filter(v => v.title.indexOf(STEP.title) === 0).length };
} else if (STEP.do === 'removePanels') {
	result = { removed: await removePanels(STEP.project, STEP.title) };
}
JSON.stringify(result);
