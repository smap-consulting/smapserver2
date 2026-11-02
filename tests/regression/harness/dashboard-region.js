// Run with javascript_tool on the analysis page of the server, after harness/dashboard-region-setup.js has made
// the bundle's polygon survey.  Replace the placeholder on the CONFIG line with
//   {"part": "api" | "map", "sId": <survey id>, "project": <project id>, "grid": <the test's grid>,
//    "panels": <the test's panels>, "bundle": <the test's bundle>, "bundleIdent": <polygon survey ident>,
//    "otherSurvey": <the test's otherSurvey>, "expect": <the test's expect>}
// Each part fits in one javascript_tool call.
//
// api:  grid kept         the test's grid is listed for the survey, created the first time and kept
//       shape listed      the survey's geoshape question is offered
//       bundle listed     the bundle survey's geoshape question is offered, with its survey's name
//       too many cells    a grid of more than 20,000 cells is refused
//       shape counts      grouped by the polygons and labelled by the label question, each polygon
//                         holds expect.shapes, the record outside both is left out
//       old format        a region sent without its survey, as panels saved before bundle polygons do,
//                         gives the same counts
//       bundle counts     grouped by the bundle survey's polygons, labelled by its label question
//       grid counts       grouped by the grid every feature is a polygon and they hold expect.gridRecords
//       record counts     without a question the regions hold a Records count, expect.countShapes
//       outside bundle    polygons from a survey that is not in the bundle are refused
//       unknown grid      a grid id that is not the bundle's, as a deleted grid leaves, gives the records
//                         ungrouped, the same as no grouping
//       bad shape         a question that is not a geoshape is refused
//       label injection   a label with SQL appended is not used: it is not a question, so the polygons are
//                         labelled by their keys, as when the label question is deleted
//       geoTable ignored  the removed geoTable parameter changes nothing
//       Then adds the test's map panels, titled panels.title + " - ...".  Reload before the map part.
// map:  grid drawn        the grid panel draws polygons with no cluster strategy
//       share colours     each polygon is coloured by the share of its records with the answer,
//                         expect.shapeColours
//       count colours     the panel without a question colours its regions by their record counts,
//                         relative to each other, expect.countColours
//       bundle colours    the bundle polygon panel, expect.bundleColours
//       time regions      a weekly time series grouped by polygons draws them unclustered, the slider
//                         past the last week empties the map and back shows them again
//       label offered     the settings dialog shows the label question for the shapes panel (cancelled)
//       drawer lists      the map drawer's Group by lists the grid, the survey's polygons and the bundle's
//       Then removes the test's panels.
const CONFIG = __CONFIG__;
const lines = [];
const check = (name, pass, detail) => lines.push(`${CONFIG.part}|${name}|${pass ? 'ok' : 'FAIL'}|${detail || ''}`);
const H = { 'X-Requested-With': 'XMLHttpRequest' };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const getJson = url => fetch(url, { headers: H, cache: 'no-store' }).then(r => { if (!r.ok) throw new Error(url + ' ' + r.status); return r.json(); });
const post = (url, body) => fetch(url, { method: 'POST', headers: { ...H, 'Content-Type': 'application/x-www-form-urlencoded' }, body });
async function waitFor(fn, ms) { for (let i = 0; i < ms / 250; i++) { try { if (fn()) return true; } catch (e) {} await sleep(250); } return false; }

const P = CONFIG.panels, B = CONFIG.bundle, E = CONFIG.expect;
const removePanels = async () => {
	const mine = (await getJson('/surveyKPI/dashboard/' + CONFIG.project)).filter(v => v.title.indexOf(P.title + ' - ') === 0);
	for (const v of mine) await post('/surveyKPI/dashboard/state', 'state=' + encodeURIComponent(JSON.stringify({ id: v.id, state: 'deleted' })));
	return mine.length;
};

if (CONFIG.part === 'api') {
	const langs = await getJson('/surveyKPI/languages/' + CONFIG.sId);
	const lang = typeof langs[0] === 'string' ? langs[0] : langs[0].name;
	const q = (await getJson('/surveyKPI/questionList/' + CONFIG.sId + '/' + encodeURIComponent(lang) + '?exc_read_only=false')).find(x => x.name === P.question);
	const base = '/surveyKPI/results/' + CONFIG.sId + '?dateId=-100&lang=' + encodeURIComponent(lang) + '&selected_geom_question=' + encodeURIComponent(P.location);
	const withQ = '&qId=' + q.id + '&fn=count';
	const results = async extra => { const r = await fetch(base + extra, { headers: H, cache: 'no-store' }); return r.ok ? { status: r.status, d: (await r.json())[0] } : { status: r.status }; };
	const counts = (res, keys) => { const got = {}; (res.d ? res.d.features : []).forEach(f => { got[f.properties.group_label] = Object.fromEntries(keys(f.properties.group_label).map(k => [k, f.properties[k]])); }); return got; };

	let regions = await getJson('/surveyKPI/regions?sId=' + CONFIG.sId);
	let grid = regions.grids.find(g => g.name === CONFIG.grid.name);
	let made = false;
	if (!grid) {
		const r = await post('/surveyKPI/regions', 'grid=' + encodeURIComponent(JSON.stringify({ sId: CONFIG.sId, ...CONFIG.grid })));
		made = r.ok;
		regions = await getJson('/surveyKPI/regions?sId=' + CONFIG.sId);
		grid = regions.grids.find(g => g.name === CONFIG.grid.name);
	}
	check('grid kept', !!grid, grid ? 'id ' + grid.id + (made ? ', created' : '') : 'not listed');
	check('shape listed', regions.shapes.some(s => s.question === P.shape && s.sId == CONFIG.sId), regions.shapes.map(s => s.survey + ':' + s.question).join(','));
	check('bundle listed', regions.shapes.some(s => s.question === B.shape && s.sIdent === CONFIG.bundleIdent && s.survey === B.name),
		regions.shapes.map(s => s.survey + ':' + s.question).join(','));

	const big = await post('/surveyKPI/regions', 'grid=' + encodeURIComponent(JSON.stringify({ sId: CONFIG.sId, name: 'Too big', cellSize: 50, extent: [150, -30, 155, -25] })));
	check('too many cells', big.status === 400, String(big.status));

	const ident = (await getJson('/surveyKPI/surveys?projectId=' + CONFIG.project + '&blocked=true')).find(s => s.id == CONFIG.sId).ident;
	const shapeNew = counts(await results(withQ + '&regionSurvey=' + ident + '&regionShape=' + P.shape + '&regionLabel=' + P.label), l => Object.keys(E.shapes[l] || {}));
	check('shape counts', JSON.stringify(shapeNew) === JSON.stringify(E.shapes), JSON.stringify(shapeNew));
	const shapeOld = counts(await results(withQ + '&regionShape=' + P.shape + '&regionLabel=' + P.label), l => Object.keys(E.shapes[l] || {}));
	check('old format', JSON.stringify(shapeOld) === JSON.stringify(shapeNew), JSON.stringify(shapeOld));
	const bundle = counts(await results(withQ + '&regionSurvey=' + encodeURIComponent(CONFIG.bundleIdent) + '&regionShape=' + B.shape + '&regionLabel=' + B.label), l => Object.keys(E.bundle[l] || {}));
	check('bundle counts', JSON.stringify(bundle) === JSON.stringify(E.bundle), JSON.stringify(bundle));

	if (grid) {
		const g = await results(withQ + '&regionId=' + grid.id);
		const feats = g.d ? g.d.features : [];
		const recs = feats.reduce((a, f) => a + f.properties.prikeys.length, 0);
		check('grid counts', feats.length > 0 && feats.every(f => f.geometry && f.geometry.type === 'Polygon') && recs === E.gridRecords, feats.length + ' cells, ' + recs + ' records');
	}
	const rc = await results('&qId=-1&fn=none&regionShape=' + P.shape + '&regionLabel=' + P.label);
	const recordCounts = {};
	(rc.d ? rc.d.features : []).forEach(f => { recordCounts[f.properties.group_label] = f.properties[rc.d.cols[0]]; });
	check('record counts', rc.d && rc.d.qtype === 'count' && JSON.stringify(recordCounts) === JSON.stringify(E.countShapes), JSON.stringify(recordCounts));

	const other = (await getJson('/surveyKPI/surveys?projectId=' + CONFIG.project + '&blocked=true')).find(s => s.displayName === CONFIG.otherSurvey);
	const outside = await results(withQ + '&regionSurvey=' + (other ? other.ident : 'none') + '&regionShape=' + P.shape);
	check('outside bundle', outside.status >= 400, String(outside.status));
	const plain = await results(withQ);
	const unknown = await results(withQ + '&regionId=999999');
	const ungrouped = r => r.d ? r.d.features.length + ' features, ' + r.d.features.filter(f => f.properties.group_label != null).length + ' labelled' : '-';
	check('unknown grid', unknown.status === 200 && plain.d && unknown.d && ungrouped(unknown) === ungrouped(plain),
		unknown.status + ', ' + ungrouped(unknown) + ' (no grouping: ' + ungrouped(plain) + ')');
	const badShape = await results(withQ + '&regionShape=' + encodeURIComponent(P.question));
	check('bad shape', badShape.status >= 400, String(badShape.status));
	const inj = await results(withQ + '&regionShape=' + P.shape + '&regionLabel=' + encodeURIComponent(P.label + '; drop table x'));
	const injLabels = inj.d ? inj.d.features.map(f => String(f.properties.group_label)).sort() : [];
	check('label injection', inj.status === 200 && injLabels.length === Object.keys(E.shapes).length && injLabels.every(l => /^\d+$/.test(l)),
		inj.status + ', labels ' + injLabels.join(','));
	const geo = await results(withQ + '&geoTable=' + encodeURIComponent('survey; --'));
	check('geoTable ignored', geo.status === 200 && plain.d && geo.d && geo.d.features.length === plain.d.features.length, geo.status + ', ' + (geo.d ? geo.d.features.length : '-') + ' features');

	await removePanels();
	const view = {
		id: -1, state: 'shown', pId: 0, sId: CONFIG.sId, uId: 0, sName: P.title, layerId: -1, lang, subject_type: 'survey',
		qId: Number(q.id), qId_is_calc: false, dateQuestionId: -100, question: P.question, fn: 'count', table: '',
		groupQuestionId: -1, groupQuestionText: 'Location', groupType: 'normal', timeGroup: 'none', advanced_filter: '',
		inc_ro: false, chartType: 'bar', tShowMeta: false, tWrapText: true, type: 'map',
		geomFormQuestions: [{ form: 'main', question: P.location }], selectedGeomQuestion: P.location
	};
	await post('/surveyKPI/dashboard/', 'settings=' + encodeURIComponent(JSON.stringify([
		{ ...view, seq: 95, title: P.title + ' - grid', region: 'grid:' + (grid ? grid.id : 0) },
		{ ...view, seq: 96, title: P.title + ' - shapes', region: 'shape:' + ident + ':' + P.shape + ':' + P.label },
		{ ...view, seq: 97, title: P.title + ' - count', qId: -1, question: '', fn: 'none', region: 'shape:' + ident + ':' + P.shape + ':' + P.label },
		{ ...view, seq: 98, title: P.title + ' - bundle', region: 'shape:' + CONFIG.bundleIdent + ':' + B.shape + ':' + B.label },
		{ ...view, seq: 99, title: P.title + ' - week', timeGroup: 'week', region: 'shape:' + ident + ':' + P.shape + ':' + P.label }
	])));

} else if (CONFIG.part === 'map') {
	const panel = s => (globals.gSelector.getViews() || []).find(v => v.state !== 'deleted' && v.title === P.title + ' - ' + s);
	const layerOf = v => globals.gSelector.getMap(v.pId).layers.find(l => !l.isBaseLayer && l.features && l.name.indexOf('SelectFeature') < 0);
	const names = ['grid', 'shapes', 'count', 'bundle', 'week'];
	await waitFor(() => names.every(n => panel(n) && layerOf(panel(n)).features.length > 0), 25000);
	if (!names.every(n => panel(n))) {
		check('panels', false, 'not found, run the api part and reload');
	} else {
		const colours = n => { const l = layerOf(panel(n)); return Object.fromEntries(l.features.map(f => [f.attributes.group_label, l.styleMap.createSymbolizer(f, 'default').fillColor])); };
		const gl = layerOf(panel('grid'));
		check('grid drawn', gl.strategies.length === 0 && gl.features.length > 0 && gl.features.every(f => f.geometry.CLASS_NAME.indexOf('Polygon') >= 0 && !f.cluster),
			gl.features.length + ' features, strategies ' + gl.strategies.map(s => s.CLASS_NAME).join(','));
		const share = colours('shapes'), count = colours('count'), bundle = colours('bundle');
		check('share colours', JSON.stringify(share) === JSON.stringify(E.shapeColours), JSON.stringify(share));
		check('count colours', JSON.stringify(count) === JSON.stringify(E.countColours), JSON.stringify(count));
		check('bundle colours', JSON.stringify(bundle) === JSON.stringify(E.bundleColours), JSON.stringify(bundle));

		const wp = panel('week'), wl = layerOf(wp), $slider = $('#p' + wp.pId + ' .timecontrols .slider-range');
		const shown = () => wl.features.map(f => f.attributes.group_label + (f.cluster ? '*' : '')).sort().join(',');
		const first = shown();
		$slider.val($slider.attr('max')).trigger('change'); await sleep(300);
		const past = shown();
		$slider.val(0).trigger('change'); await sleep(300);
		const back = shown();
		check('time regions', wl.strategies.length === 0 && first === Object.keys(E.shapes).sort().join(',') && past === '' && back === first,
			'first ' + first + ', past last week ' + (past || 'empty') + ', back ' + back);

		const sp = panel('shapes');
		document.querySelector('#p' + sp.pId + ' .pSettings').click();
		await waitFor(() => $('.region_label_row').is(':visible') && $('#settings_region_label').val() === P.label, 8000);
		check('label offered', ($('#settings_group').val() || '').endsWith(':' + P.shape) && $('#settings_region_label').val() === P.label,
			$('#settings_group').val() + ' / ' + $('#settings_region_label').val());
		$('#settings_cancel').trigger('click');
		await sleep(500);

		const opts = $('#mGrids' + sp.pId + ' .region_select option').map((i, o) => o.text).get();
		check('drawer lists', opts.some(t => t.indexOf(CONFIG.grid.name) === 0) && opts.includes(P.shape) && opts.includes(B.name + ' : ' + B.shape), opts.join(' | '));
	}
	check('panels removed', await removePanels() === 5);
}
JSON.stringify({ lines });
