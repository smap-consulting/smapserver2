// Run in any console page of the server, logged in as a server owner, with javascript_tool.  Replace
// the placeholder on the CONFIG line with the test's "config" object as JSON.
//
// Read checks, always run: the server settings, the organisation email settings and the organisation
// list never contain a secret, only null, empty or the placeholder.
//
// Write checks, only when config.write is true: uses a webhook secret, as it can be tested from
// outside - wa_app_secret signs messages to /sms/whatsapp/inbound and vonage_webhook_secret signs the
// JWT on /sms/vonage/inbound, and each is refused without a valid signature.  Uses whichever is not set,
// so a real connection is never touched, and is skipped if both are.  Sets a random secret, checks a
// request signed with it is accepted, saves the settings again unchanged (the placeholder comes back)
// and checks it is still accepted and a wrong one refused, then clears it and checks it is gone.
const CONFIG = __CONFIG__;
const P = CONFIG.placeholder;
const HDR = { 'X-Requested-With': 'XMLHttpRequest' };
const lines = [];
const check = (name, pass, detail) => lines.push(`${name}|${pass ? 'ok' : 'FAIL'}|${detail || ''}`);
const hidden = v => v === null || v === undefined || v === '' || v === P;
const getJson = url => fetch(url, { credentials: 'include', headers: HDR, cache: 'no-store' })
	.then(r => { if (!r.ok) throw new Error(url + ' ' + r.status); return r.json(); });

// Read checks
const server = await getJson('/surveyKPI/server');
for (const f of CONFIG.serverSecrets) {
	check(`server.${f} hidden`, hidden(server[f]), server[f] === undefined ? 'absent' : (server[f] === P ? 'placeholder' : (server[f] ? 'VALUE SENT' : 'not set')));
}
const email = await getJson('/surveyKPI/organisationList/email');
check('org email_password hidden', hidden(email.email_password), email.email_password === P ? 'placeholder' : (email.email_password ? 'VALUE SENT' : 'not set'));
const orgs = await getJson('/surveyKPI/organisationList');
const leaked = orgs.filter(o => !hidden(o.email_password)).map(o => o.id);
check('org list email_password hidden', leaked.length === 0, `${orgs.length} organisations` + (leaked.length ? `, sent for ${leaked.join(',')}` : ''));

// Write checks
async function save(data) {
	const r = await fetch('/surveyKPI/server', {
		method: 'POST', credentials: 'include',
		headers: { ...HDR, 'Content-Type': 'application/x-www-form-urlencoded' },
		body: 'settings=' + encodeURIComponent(JSON.stringify(data))
	});
	if (!r.ok) throw new Error('save ' + r.status + ' ' + await r.text());
}
const enc = t => new TextEncoder().encode(t);
const b64url = b => btoa(String.fromCharCode(...new Uint8Array(b))).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
async function hmac(secret, text) {
	const key = await crypto.subtle.importKey('raw', enc(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
	return crypto.subtle.sign('HMAC', key, enc(text));
}
// The status of an empty request to the setting's webhook, signed with secret.  200 means accepted
const ORACLES = {
	wa_app_secret: async secret => {
		const body = '{}';
		const sig = [...new Uint8Array(await hmac(secret, body))].map(b => b.toString(16).padStart(2, '0')).join('');
		return (await fetch('/sms/whatsapp/inbound', { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Hub-Signature-256': 'sha256=' + sig }, body })).status;
	},
	vonage_webhook_secret: async secret => {
		const head = b64url(enc(JSON.stringify({ alg: 'HS256', typ: 'JWT' })));
		const claims = b64url(enc(JSON.stringify({ iat: Math.floor(Date.now() / 1000) - 10 })));
		const jwt = head + '.' + claims + '.' + b64url(await hmac(secret, head + '.' + claims));
		return (await fetch('/sms/vonage/inbound', { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + jwt }, body: '{}' })).status;
	}
};
if (!CONFIG.write) {
	check('write checks', true, 'skipped, config.write is false');
} else if (!lines.every(l => l.includes('|ok|'))) {
	check('write checks', false, 'skipped, the read checks failed so this build may not keep secrets on save');
} else {
	const field = Object.keys(ORACLES).find(f => !server[f]);
	if (!field) {
		check('write checks', true, 'skipped, wa_app_secret and vonage_webhook_secret are both set on this server');
	} else {
		const accepted = ORACLES[field];
		const secret = 'regression-' + crypto.randomUUID();
		try {
			await save({ ...(await getJson('/surveyKPI/server')), [field]: secret });
			let s = await accepted(secret);
			check(`${field}: new secret saved and used`, s === 200, 'webhook status ' + s);
			const again = await getJson('/surveyKPI/server');
			check(`${field}: new secret sent as placeholder`, again[field] === P, String(again[field]));
			await save(again);
			s = await accepted(secret);
			check(`${field}: placeholder sent back keeps the secret`, s === 200, 'webhook status ' + s);
			s = await accepted('wrong-' + secret);
			check(`${field}: wrong secret refused`, s !== 200, 'webhook status ' + s);
		} finally {
			await save({ ...(await getJson('/surveyKPI/server')), [field]: '' });
			const after = await getJson('/surveyKPI/server');
			check(`${field}: empty value clears the secret`, !after[field], String(after[field]));
		}
	}
}
JSON.stringify({ lines });
