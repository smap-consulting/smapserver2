// XForm upload test, run with javascript_tool on any console page.  Replace the placeholder on the CONFIG line:
//   {"part": "valid", "project": <project id>, "test": <the test file>}
//       Only when the test's survey is missing.  Uploads the valid XForm, which must succeed with the warning.
//   {"part": "refused", "project", "test"}
//       Each broken XForm must be refused with its message, and no survey made.  Messages are decoded as the
//       upload page decodes them, so the check sees the text a person sees.
const CONFIG = __CONFIG__;
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const lines = [];
const check = (name, ok, detail) => lines.push({ check: name, ok: !!ok, detail: detail === undefined ? '' : String(detail) });
const decode = t => new DOMParser().parseFromString(t, 'text/html').documentElement.textContent;
const form = (title, binds) => '<?xml version="1.0"?>'
	+ '<h:html xmlns="http://www.w3.org/2002/xforms" xmlns:h="http://www.w3.org/1999/xhtml" xmlns:jr="http://openrosa.org/javarosa">'
	+ '<h:head><h:title>' + title + '</h:title><model>'
	+ '<instance><data id="xform_upload_test"><xa/><xb/><xc/><xs/><meta><instanceID/></meta></data></instance>'
	+ '<bind nodeset="/data/xa" type="string" ' + (binds.xa || '') + '/>'
	+ '<bind nodeset="/data/xb" type="int" ' + (binds.xb || '') + '/>'
	+ '<bind nodeset="/data/xc" type="string" ' + (binds.xc || '') + '/>'
	+ '<bind nodeset="/data/xs" type="string"/>'
	+ '<bind nodeset="/data/meta/instanceID" type="string" readonly="true()" jr:preload="uid"/>'
	+ '</model></h:head><h:body>'
	+ '<input ref="/data/xa"><label>A</label></input><input ref="/data/xb"><label>B</label></input>'
	+ '<input ref="/data/xc"><label>C</label></input>'
	+ '<select1 ref="/data/xs"><label>S</label><item><label>Yes</label><value>yes</value></item>'
	+ '<item><label>No</label><value>no</value></item></select1>'
	+ '</h:body></h:html>';
const upload = async (name, binds) => {
	const fd = new FormData();
	fd.append('templateName', name);
	fd.append('projectId', String(CONFIG.project));
	fd.append('file', new Blob([form(name, binds)], { type: 'text/xml' }), 'xform_upload_test.xml');
	const r = await fetch('/surveyKPI/upload/surveytemplate', { method: 'POST', headers: H, body: fd });
	const text = await r.text();
	let j = {};
	try { j = JSON.parse(text); } catch (e) { j = { status: 'http ' + r.status, message: text }; }
	return { status: j.status, message: decode(j.message || '') };
};
const surveys = async () => (await (await fetch('/surveyKPI/surveys?projectId=' + CONFIG.project + '&blocked=true',
	{ headers: H, cache: 'no-store' })).json()).map(s => s.displayName);

if (CONFIG.part === 'valid') {
	const r = await upload(CONFIG.test.survey.name, CONFIG.test.valid.binds);
	check('valid XForm uploaded with a warning', r.status === 'warning' && r.message.indexOf(CONFIG.test.valid.warning) >= 0,
		r.status + ' ' + r.message.slice(0, 200));
} else if (CONFIG.part === 'refused') {
	for (const c of CONFIG.test.refused) {
		const r = await upload(CONFIG.test.refusedName, c.binds);
		check('refused: ' + c.name, r.status === 'error' && r.message.indexOf(c.expect) >= 0
			&& r.message.indexOf('&quot;') < 0, r.status + ' ' + r.message.slice(0, 200));
	}
	const names = await surveys();
	check('no survey made', names.indexOf(CONFIG.test.refusedName) < 0, names.indexOf(CONFIG.test.refusedName) < 0 ? '' : 'made');
}
return JSON.stringify({ part: CONFIG.part, lines });
