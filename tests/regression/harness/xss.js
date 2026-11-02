// Run with javascript_tool on a page opened for one entry of an xss test's "pages".  Replace the
// placeholder on the PAGE line with that entry as JSON, with {sId}, {ident} and {ts} in it replaced by
// the survey id, the survey ident and the time.
//
// Waits until every marker in page.expect is in the page, including hidden tabs, inputs, textareas
// and link or image addresses, so a pass means the data was there.  Then runs the actions, waits for
// the markers again, and checks:
//   no payload ran         window.__xss stays empty
//   no injected elements   nothing has data-xss, which a payload would give an element it made, or an
//                          element whose attribute it broke out of
//                          checked after every action too, as a view that is replaced can hide one
//   absent                 page.absent selectors match nothing (for payloads the server sanitises to
//                          a harmless tag, which would then have no data-xss)
//   present                page.present selectors each match something (markup that must still work)
//   field holds            some input or textarea holds each of page.fields exactly, so nothing was cut
//                          off or decoded
//   no pending changes     page.noPending, a selector for the count of unsaved changes, is empty or 0,
//                          so viewing did not change anything
//   shown as text          each of page.literal is in the text exactly as entered
//   not encoded twice      no &lt; &quot; etc. in visible text or a visible field, which a record edit would
//                          also save back.  Skipped when page.entitiesExpected is true
// Actions:
//   selectRow:TEXT         click the first table cell in the row containing TEXT
//   click:SELECTOR         wait for the element and click it
//   select:SELECTOR=VALUE  wait for the option, choose it and fire change
const PAGE = __PAGE__;
const lines = [];
const check = (name, pass, detail) => lines.push(`${PAGE.name}|${name}|${pass ? 'ok' : 'FAIL'}|${detail || ''}`);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const injectedSeen = new Set();
const noteInjected = () => document.querySelectorAll('[data-xss]').forEach(e => injectedSeen.add(e.tagName.toLowerCase() + '#' + e.getAttribute('data-xss')));
const present = () => [document.body.textContent,
	...[...document.querySelectorAll('input, textarea')].map(e => e.value),
	...[...document.querySelectorAll('[href], [src], [data-href]')].map(e => [e.getAttribute('href'), e.getAttribute('src'), e.getAttribute('data-href')].join(' '))].join('\n');
async function waitFor(markers) {
	let missing = markers;
	for (let i = 0; i < 80 && missing.length; i++) {
		await sleep(250);
		const text = present();
		missing = markers.filter(m => !text.includes(m));
	}
	return missing;
}

for (const action of PAGE.actions || []) {
	const verb = action.slice(0, action.indexOf(':')), arg = action.slice(action.indexOf(':') + 1);
	if (verb === 'selectRow') {
		const findRow = () => [...document.querySelectorAll('table tbody tr')].find(tr => tr.innerText.includes(arg));
		for (let i = 0; i < 80 && !findRow(); i++) await sleep(250);
		const row = findRow();
		if (row) row.querySelector('td').click(); else check(action, false, 'no row contains ' + arg);
	} else if (verb === 'click') {
		for (let i = 0; i < 80 && !document.querySelector(arg); i++) await sleep(250);
		const el = document.querySelector(arg);
		if (el) el.click(); else check(action, false, 'not found');
	} else if (verb === 'select') {
		const [sel, value] = [arg.slice(0, arg.indexOf('=')), arg.slice(arg.indexOf('=') + 1)];
		for (let i = 0; i < 40 && !document.querySelector(`${sel} option[value="${value}"]`); i++) await sleep(250);
		const el = document.querySelector(sel);
		if (el && document.querySelector(`${sel} option[value="${value}"]`)) {
			el.value = value;
			el.dispatchEvent(new Event('change', { bubbles: true }));
		} else {
			check(action, false, 'no option ' + value);
		}
	}
	await sleep(1000);
	noteInjected();
}
const missing = await waitFor(PAGE.expect);
await sleep(1500);		// Let any image errors and focus events fire

check('data shown', missing.length === 0, missing.length ? 'missing ' + missing.join(',') : PAGE.expect.length + ' markers');
const ran = window.__xss || [];
check('no payload ran', ran.length === 0, ran.length ? 'ran ' + [...new Set(ran)].join(',') : '');
noteInjected();
const injected = [...injectedSeen];
check('no injected elements', injected.length === 0, injected.join(','));
for (const sel of PAGE.present || []) {
	const n = document.querySelectorAll(sel).length;
	check('present ' + sel, n > 0, n ? '' : 'none found');
}
const values = [...document.querySelectorAll('input, textarea')].map(e => e.value);
for (const f of PAGE.fields || []) {
	check('field holds: ' + f.slice(0, 12), values.includes(f), values.includes(f) ? '' : 'no field holds it in full');
}
if (PAGE.noPending) {
	const p = ((document.querySelector(PAGE.noPending) || {}).textContent || '').trim();
	check('no pending changes', p === '' || p === '0', p);
}
for (const sel of PAGE.absent || []) {
	const n = document.querySelectorAll(sel).length;
	check('absent ' + sel, n === 0, n ? n + ' found' : '');
}
const text = document.body.textContent;
for (const lit of PAGE.literal || []) {
	check('shown as text: ' + lit.slice(0, 12), text.includes(lit), text.includes(lit) ? '' : 'not found as text');
}
const shown = [document.body.innerText, ...[...document.querySelectorAll('input, textarea')].filter(e => e.offsetParent !== null).map(e => e.value)].join('\n');
const twice = ['&lt;', '&gt;', '&quot;', '&amp;', '&#39;'].filter(e => shown.includes(e));
if (PAGE.entitiesExpected) {		// The page holds text with entities in it on purpose, page.fields checks it instead
	check('not encoded twice', true, 'skipped, entities expected');
} else {
	check('not encoded twice', twice.length === 0, twice.join(' '));
}
JSON.stringify({ lines });
