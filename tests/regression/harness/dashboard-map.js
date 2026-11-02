// Run with javascript_tool on the analysis page, reloaded after the dashboard-map setup added its
// panels.  Replace the placeholder on the CONFIG line with
//   {"part": "map" | "settings", "title": <panels title>, "trail": <the test's trail>, "expect": <the test's expect>,
//    "recordCount": <number of the test's records>}
// Each part fits in one javascript_tool call.  Nothing here is saved: dates and the dashboard range
// are changed in the page only, and the settings dialog is cancelled.
//
// map:      trail drawn            the User Activity map has a line layer holding the trail
//           trail follows dates    with the panel set to the trail's day, the lines hold exactly
//                                  the trail's points
//           line colour            a geotrace on the Data line map is drawn blue, not OpenLayers'
//                                  default black
//           cluster labels short   a cluster of the user's records shows at most 3 record numbers,
//                                  needs a cluster of more than 3 to mean anything
//           time map clusters      the daily time series map clusters the records at one spot, the
//                                  first day holds them all as the test's records are uploaded together
//           time map window        moving the slider past the last day empties the map, moving it
//                                  back shows the cluster again
//           time map colour        the cluster is coloured by the share of its records with the
//                                  selected answer: none blue, all red, in quarters between
// settings: graph has no Include Read Only, a User Activity map has none, a Data map's Show on
//           defaults to expect.showOn, and a table switched to User then back to Data does not
//           show the map only location picker
const CONFIG = __CONFIG__;
const lines = [];
const check = (name, pass, detail) => lines.push(`${CONFIG.part}|${name}|${pass ? 'ok' : 'FAIL'}|${detail || ''}`);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const views = () => globals.gSelector.getViews().filter(v => v.state !== 'deleted');
const panel = suffix => views().find(v => v.title === CONFIG.title + ' - ' + suffix);
const mapOf = v => globals.gSelector.getMap(v.pId);
const linePoints = map => map.layers.filter(l => !l.isBaseLayer && l.features)
	.flatMap(l => l.features).filter(f => f.geometry && f.geometry.CLASS_NAME === 'OpenLayers.Geometry.LineString')
	.reduce((a, f) => a + f.geometry.components.length, 0);
async function waitFor(fn, ms) {
	for (let i = 0; i < ms / 250; i++) { if (fn()) return true; await sleep(250); }
	return !!fn();
}
async function refresh() { $('#refreshMenu').trigger('click'); await sleep(5000); }
await waitFor(() => window.globals && globals.gSelector && globals.gSelector.getViews() && panel('table'), 15000);

if (CONFIG.part === 'map') {
	const savedRange = globals.gDashboardDateRange;
	globals.gDashboardDateRange = 'all';
	const um = panel('user map'), lm = panel('line map'), tm = panel('time map');
	if (!um || !lm || !tm) {
		check('panels', false, 'test panels not found, run addPanels and reload');
	} else {
		await refresh();
		const umap = mapOf(um);
		await waitFor(() => linePoints(umap) > 0, 10000);
		const all = linePoints(umap);
		check('trail drawn', all >= CONFIG.trail.points.length, all + ' trail points');

		um.fromDate = CONFIG.trail.day; um.toDate = CONFIG.trail.day;
		await refresh();
		await waitFor(() => linePoints(umap) === CONFIG.trail.points.length, 6000);
		const dated = linePoints(umap);
		check('trail follows dates', dated === CONFIG.trail.points.length, dated + ' points on ' + CONFIG.trail.day + ', expected ' + CONFIG.trail.points.length);
		um.fromDate = undefined; um.toDate = undefined;

		const paths = [...document.querySelectorAll('#map_panel' + lm.pId + ' path, #map_panel' + lm.pId + ' polyline')];
		const strokes = [...new Set(paths.map(c => c.getAttribute('stroke')))];
		check('line colour', paths.length > 0 && strokes.every(s => s === 'blue'), paths.length ? 'stroke ' + strokes.join(',') : 'no lines drawn');

		const labels = [...document.querySelectorAll('#map_panel' + um.pId + ' text')].map(t => t.textContent).filter(Boolean);
		const records = l => l.split(', ').length + (/ \+(\d+)$/.test(l) ? Number(l.match(/ \+(\d+)$/)[1]) : 0);
		const big = labels.filter(l => records(l) > 3);
		const long = labels.filter(l => l.replace(/ \+\d+$/, '').split(', ').length > 3);
		check('cluster labels short', big.length > 0 && long.length === 0,
			big.length === 0 ? 'no cluster of more than 3 records to check' : (long.length ? 'longest: ' + long.sort((a, b) => b.length - a.length)[0].slice(0, 40) : big[0]));

		const tl = mapOf(tm).layers.find(l => !l.isBaseLayer && l.features && l.name.indexOf('SelectFeature') < 0);
		const $slider = $('#p' + tm.pId + ' .timecontrols .slider-range');
		const shown = () => tl ? tl.features.map(f => f.cluster ? f.cluster.length : 1) : [];
		await waitFor(() => shown().length > 0, 5000);
		const first = shown();
		check('time map clusters', Math.max(0, ...first) >= CONFIG.recordCount, first.length ? 'shown as ' + first.join(',') : 'nothing shown');
		$slider.val($slider.attr('max')).trigger('change'); await sleep(300);
		const past = shown();
		$slider.val(0).trigger('change'); await sleep(300);
		const back = shown();
		check('time map window', past.length === 0 && back.join() === first.join(), 'past last day ' + (past.join(',') || 'empty') + ', back ' + back.join(','));

		const cl = tl ? tl.features.find(f => f.cluster) : null;
		if (cl) {
			const matching = cl.cluster.filter(f => f.attributes.result > 0).length;
			const expected = Math.round(4 * matching / cl.cluster.length);
			const fills = ['blue', 'green', 'yellow', 'orange', 'red'];
			const fill = tl.styleMap.createSymbolizer(cl, 'default').fillColor;
			check('time map colour', cl.attributes.value === expected && fill === fills[expected],
				matching + ' of ' + cl.cluster.length + ' ' + cl.cluster[0].attributes.selected + ', ' + fill + ', expected ' + fills[expected]);
		} else {
			check('time map colour', false, 'no cluster');
		}
	}
	globals.gDashboardDateRange = savedRange;
} else if (CONFIG.part === 'settings') {
	const vis = sel => $(sel).is(':visible');
	const open = async v => { document.querySelector('#p' + v.pId + ' .pSettings').click(); await sleep(2500); };
	const close = async () => { $('#settings_cancel').trigger('click'); await sleep(800); };
	const g = panel('graph'), sm = panel('survey map'), um = panel('user map'), t = panel('table');
	if (!g || !sm || !um || !t) {
		check('panels', false, 'test panels not found, run addPanels and reload');
	} else {
		await open(g);
		check('graph: no Include Read Only', !vis('#settings_inc_ro'));
		await close();
		await open(sm);
		const showOn = $('#display_panel option[value="-1"]').text();
		check('data map: Include Read Only', vis('#settings_inc_ro'));
		check('data map: Show on default', showOn === CONFIG.expect.showOn, showOn);
		await close();
		await open(um);
		check('user map: no Include Read Only', !vis('#settings_inc_ro'));
		await close();
		await open(t);
		$('#subject_type').val('user').trigger('change'); await sleep(800);
		$('#subject_type').val('survey').trigger('change'); await sleep(800);
		check('table back to Data: no location picker', !vis('.geomselect_settings'));
		check('table back to Data: Include Read Only', vis('#settings_inc_ro'));
		await close();
	}
}
JSON.stringify({ lines });
