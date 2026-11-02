// Checks the totals of the MCP submission_events tool, run with javascript_tool on any console page of the
// server.  Replace the placeholder on the CONFIG line with
//   {"pId": <project id>, "ident": <survey ident>, "expect": <the test's expect>,
//    "mcp": {"survey": <totals with survey_id>, "project": <totals with project_id>, "all": <totals with neither>},
//    "rows": {<state>: <number of uploads the tool lists for the survey with states [state]>, ...}}
// The monitor page's totals (/surveyKPI/eventList/.../totals) are fetched and summed over their rows.  Its
// success total ignores ignore_old_issues, so neither side leaves out old uploads here.  Nothing is changed.
const CONFIG = __CONFIG__;
const lines = [];
const check = (name, pass, detail) => lines.push(`${name}|${pass === 'warn' ? 'warn' : pass ? 'ok' : 'FAIL'}|${detail || ''}`);
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const STATES = ['success', 'error', 'duplicate', 'merged', 'not_loaded', 'upload_error'];
// The monitor's name for each of the tool's states
const MONITOR = { success: 'success', error: 'errors', duplicate: 'duplicates', merged: 'merged',
	not_loaded: 'not_loaded', upload_error: 'upload_errors' };
const show = t => STATES.map(s => s + ' ' + t[s]).join(', ');

async function monitor(url) {
	const r = await fetch(url, { headers: H, cache: 'no-store' });
	if (!r.ok) throw new Error(url + ' ' + r.status);
	const d = await r.json();
	const t = {};
	for (const s of STATES) {
		t[s] = (d.features || []).reduce((n, f) => n + (Number(f.properties[MONITOR[s]]) || 0), 0);
	}
	return t;
}

function compare(name, tool, other, otherName, notLoadedWarn) {
	const diff = STATES.filter(s => Number(tool[s]) !== Number(other[s]));
	if (diff.length === 0) {
		check(name, true, show(tool));
	} else {
		check(name, notLoadedWarn && diff.length === 1 && diff[0] === 'not_loaded' ? 'warn' : false,
			'tool: ' + show(tool) + ' / ' + otherName + ': ' + show(other));
	}
}

try {
	compare('survey: expected uploads', CONFIG.mcp.survey, CONFIG.expect, 'expected');
	compare('survey: listed uploads per state', CONFIG.mcp.survey, CONFIG.rows, 'listed');

	const survey = await monitor('/surveyKPI/eventList/' + CONFIG.pId + '/' + encodeURIComponent(CONFIG.ident) + '/totals');
	compare('survey: monitor totals', CONFIG.mcp.survey, survey, 'monitor');

	const project = await monitor('/surveyKPI/eventList/' + CONFIG.pId + '/_all/totals');
	compare('project: monitor totals', CONFIG.mcp.project, project, 'monitor', true);

	const all = await monitor('/surveyKPI/eventList/0/_all/totals');
	compare('all projects: monitor totals', CONFIG.mcp.all, all, 'monitor', true);
} catch (e) {
	check('run', false, e.message);
}
JSON.stringify({ lines });
