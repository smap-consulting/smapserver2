// SQL injection probes for an sqli test, run with javascript_tool on any console page of the server.
// Replace the placeholder on the CONFIG line with
//   {"ident": <survey ident>, "sId": <survey id>, "ids": <the setup's ids step result>, "probes": <the test's probes>}
// Each probe is a GET of probe.url with {ident}, {sId}, {fId} and {q:name} (a question id) replaced.
// A probe's filter {"q": name, "value"} is added as the filter parameter {"qId", "value"}.
// A probe with "form" is a POST of those form fields; an object field is sent as JSON, and "{q:name}" and
// "{table}" (the top form's table) and {fId} inside it are replaced.  POST probes change data, so they only go to
// the test's own survey; "method" sends them as a PUT instead.  Expectations:
//   status          the response status must equal this
//   choices         the response must be a JSON array of this many entries
//   records         the response must hold this many records (GeoJSON features)
//   notStatus       the response status must not be this (a save that should be refused)
//   fields          the response must be JSON whose fields have these values, e.g. {"failed": 1}
//   contains        the response text must contain this ({sId} replaced)
//   notContains     the response text must not contain this ({sId} replaced)
//   filled          {instanceID, column, waitSecs}: the GeoJSON record with that instance id must have a value
//                   in that column; the url is fetched again every 10 seconds until it does or waitSecs pass.
//                   For a column a background process fills in.
//   refusedOrEmpty  the request must be refused (status >= 400) or return nothing: an empty array or
//                   object, or no rows.  A query that SQL in a name widened returns rows instead.
// Nothing is changed by the probes.
const CONFIG = __CONFIG__;
const lines = [];
const check = (name, pass, detail) => lines.push(`${name}|${pass ? 'ok' : 'FAIL'}|${detail || ''}`);
const H = { 'X-Requested-With': 'XMLHttpRequest' };

// Records in a GeoJSON response: a feature collection, or the array of them /results returns
const records = d => d && Array.isArray(d.features) ? d.features.length
	: Array.isArray(d) && d.length && d.every(c => c && Array.isArray(c.features)) ? d.reduce((n, c) => n + c.features.length, 0) : null;
const isEmpty = d => d == null || records(d) === 0 || (Array.isArray(d) && d.length === 0)
	|| (d && typeof d === 'object' && typeof d.message === 'string' && records(d) === null)	// /items reports an error as 200 with a message
	|| (typeof d === 'object' && !Array.isArray(d) && Object.keys(d).length === 0)
	|| (typeof d === 'object' && Array.isArray(d.rows) && d.rows.length === 0);

for (const p of CONFIG.probes) {
	const ids = CONFIG.ids || { q: {} };
	let url = p.url.split('{ident}').join(encodeURIComponent(CONFIG.ident || ''))
		.split('{sId}').join(String(CONFIG.sId)).split('{fId}').join(String(ids.fId))
		.split('{roleId}').join(String(ids.roleId))
		.replace(/\{q:(\w+)\}/g, (m, n) => String(ids.q[n]));
	if (p.filter) {
		url += (url.includes('?') ? '&' : '?') + 'filter=' + encodeURIComponent(JSON.stringify({ qId: ids.q[p.filter.q], value: p.filter.value }));
	}
	let init = { headers: H, cache: 'no-store' };
	if (p.form) {
		const body = new URLSearchParams();
		for (const [k, v] of Object.entries(p.form)) {
			const json = typeof v === 'string' ? v : JSON.stringify(v);
			body.append(k, json.replace(/"\{q:(\w+)\}"/g, (m, n) => String(ids.q[n])).split('{table}').join(String(ids.table)).split('{fId}').join(String(ids.fId)).split('"{roleId}"').join(String(ids.roleId)));
		}
		init = { method: p.method || 'POST', headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' }, body };
	}
	let status = 0, data = null, text = '';
	const features = d => d && Array.isArray(d.features) ? d.features : Array.isArray(d) ? d.flatMap(c => (c && c.features) || []) : [];
	const value = () => { const f = features(data).find(f => f.properties && f.properties.instanceid === p.expect.filled.instanceID); return f ? f.properties[p.expect.filled.column] : undefined; };
	const until = Date.now() + (p.expect.filled ? (p.expect.filled.waitSecs || 0) * 1000 : 0);
	for (;;) {
		try {
			const r = await fetch(url, init);
			status = r.status;
			text = await r.text();
			try { data = JSON.parse(text); } catch (e) { data = undefined; }
		} catch (e) {
			text = e.message;
		}
		if (!p.expect.filled || (value() != null && value() !== '') || Date.now() > until) break;
		await new Promise(res => setTimeout(res, 10000));
	}
	if (p.expect.filled) {
		const v = value();
		check(p.name, status === 200 && v != null && v !== '', status + ' ' + (v == null || v === '' ? 'empty' : 'filled'));
		continue;
	}
	const shown = records(data) !== null ? records(data) + ' records'
		: Array.isArray(data) ? data.length + ' entries'
		: (text || '').replace(/\s+/g, ' ').slice(0, 60);
	if (p.expect.notStatus !== undefined) {
		check(p.name, status !== p.expect.notStatus, status + ' ' + shown);
	} else if (p.expect.refusedOrEmpty) {
		check(p.name, status >= 400 || (status === 200 && isEmpty(data)), status + ' ' + shown);
	} else {
		let pass = p.expect.status === undefined || status === p.expect.status;
		if (p.expect.choices !== undefined) {
			pass = pass && Array.isArray(data) && data.length === p.expect.choices;
		}
		if (p.expect.records !== undefined) {
			pass = pass && records(data) === p.expect.records;
		}
		const sub = t => t.split('{sId}').join(String(CONFIG.sId));
		if (p.expect.contains !== undefined) {
			pass = pass && text.includes(sub(p.expect.contains));
		}
		if (p.expect.notContains !== undefined) {
			pass = pass && !text.includes(sub(p.expect.notContains));
		}
		if (p.expect.fields !== undefined) {
			pass = pass && data != null && typeof data === 'object' && Object.entries(p.expect.fields).every(([k, v]) => data[k] === v);
		}
		check(p.name, pass, status + ' ' + shown);
	}
}
JSON.stringify({ lines });
