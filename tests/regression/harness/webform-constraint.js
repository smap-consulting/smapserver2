// Run in the webform page with javascript_tool.  Replace the placeholder on the CASES line with the
// test's "cases" object as JSON.  For each question, sets each value, fires change, and reads whether Enketo marked the
// question invalid-constraint.  Returns JSON: per value expected, actual and pass.
const CASES = __CASES__;
// Wait for Enketo to finish building the form
for (let i = 0; i < 40 && !document.querySelector('form.or .question'); i++) {
  await new Promise(r => setTimeout(r, 250));
}
await new Promise(r => setTimeout(r, 1000));
const results = [];
for (const [q, c] of Object.entries(CASES)) {
  const inp = document.querySelector(`[name$="/${q}"]`);
  const expected = [...(c.accept || []).map(v => [v, 'accept']), ...(c.reject || []).map(v => [v, 'reject'])];
  if (!inp) {
    expected.forEach(([v, e]) => results.push({ q, value: v, expected: e, actual: 'question missing', pass: false }));
    continue;
  }
  const qd = inp.closest('.question');
  for (const [v, e] of expected) {
    inp.value = v;
    inp.dispatchEvent(new Event('change', { bubbles: true }));
    await new Promise(r => setTimeout(r, 300));
    const actual = qd.classList.contains('invalid-constraint') ? 'reject' : 'accept';
    results.push({ q, value: v, expected: e, actual, pass: actual === e });
  }
  inp.value = '';
  inp.dispatchEvent(new Event('change', { bubbles: true }));
}
const constraints = Object.fromEntries(Object.keys(CASES).map(q => {
  const el = document.querySelector(`[name$="/${q}"]`);
  return [q, el ? el.getAttribute('data-constraint') : null];
}));
// Compact, so a long test is not truncated by the tool: one line per value, constraints only for failures
const failed = [...new Set(results.filter(r => !r.pass).map(r => r.q))];
JSON.stringify({
  title: document.title,
  lines: results.map(r => `${r.q}|${r.value}|${r.expected}|${r.actual}|${r.pass ? 'ok' : 'FAIL'}`),
  failedConstraints: Object.fromEntries(failed.map(q => [q, constraints[q]]))
});
