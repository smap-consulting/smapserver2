
let CACHE_NAME = 'v21';
let ASSIGNMENTS = '/surveyKPI/myassignments';
let WEBFORM = "/webForm";
let USER = "/surveyKPI/user?";
let ORG_CSS = "/custom/css/org/custom.css";
let PROJECT_LIST = "/myProjectList";
let CURRENT_PROJECT = "/currentproject";
let TIMEZONES = "/timezones";
let organisationId = 0;

// During the installation phase, you'll usually want to cache static assets.
self.addEventListener('install', function(e) {
	// Once the service worker is installed, go ahead and fetch the resources to make this work offline.
	e.waitUntil(
		caches.open(CACHE_NAME).then(function(cache) {
			return cache.addAll([
				'/',
				'/myWork/index.html',
				'/css/bootstrap.v4.5.min.css',
				'/css/fa.v5.15.1.all.min.css',
				'/build/css/theme-smap.css',
				'/build/css/theme-smap.print.css',
				'/build/css/webform.print.css',
				'/build/css/webform.css',
				'/fonts/OpenSans-Regular-webfont.woff',
				'/fonts/OpenSans-Bold-webfont.woff',
				'/fonts/fontawesome-webfont.woff',
				'/webfonts/fa-solid-900.woff',
				'/webfonts/fa-solid-900.woff2',
				'/webfonts/fa-solid-900.ttf',
				'/build/js/webform-bundle.min.js',
				'/js/libs/modernizr.js',
				'/js/libs/require.js',
				'/js/libs/jquery-2.1.1.js',
				'/js/libs/bootstrap.bundle.v4.5.min.js',
				'/js/app/custom.js',
				'/js/app/idbconfig.js',
				'/myWork/js/my_work.js',
				'/images/enketo_bare_150x56.png',
				'/images/smap_logo.png',
				'/images/ajax-loader.gif',
				'/favicon.ico'
			])
		})
	);
});

self.addEventListener('activate', function (event) {
	var cacheKeeplist = [CACHE_NAME];

	event.waitUntil(
		caches.keys().then(function (keyList) {
			return Promise.all(keyList.map(function (key) {
				if (cacheKeeplist.indexOf(key) === -1) {
					return caches.delete(key);
				}
			}));
		})
	);

});

// when the browser fetches a URL…
// PWA version
/*
self.addEventListener('fetch', function(event) {

	if (event.request.url.includes(ASSIGNMENTS)) {
		// response to request for forms and tasks. Cache Update Refresh strategy
		event.respondWith(caches.match(ASSIGNMENTS));
		event.waitUntil(update_assignments(event.request).then(refresh).then(precacheforms));

	} else if (event.request.url.includes(TIMEZONES)) {
		// Cache then if not found network then cache the response
		event.respondWith(
			caches
				.match(getCacheUrl(event.request)) // check if the request has already been cached
				.then(cached => cached || update(event.request)) // otherwise request network
		);

	} else if (event.request.url.includes(WEBFORM)
		|| event.request.url.includes(USER)
		|| event.request.url.includes(CURRENT_PROJECT)
		|| event.request.url.includes(PROJECT_LIST)) {

		// response to a webform/user request.  Network then cache strategy
		event.respondWith(
			fetch(event.request)
				.then(response => {
					if(response.status == 401) {
						fetch(event.request);
					} else if (response.status == 200) {
						return caches
							.open(CACHE_NAME)
							.then(cache => {
								cache.put(getCacheUrl(event.request), response.clone());
								return response;
							})
					} else {
						return caches.match(getCacheUrl(event.request))
							.then(cached => cached || response) // otherwise request network
					}
				}).catch(() => {
					return caches.match(getCacheUrl(event.request));
				})
		);

	} else {
		// Try cache then network - but do not cache missing files as there will be a lot of them
		event.respondWith(
			caches
				.match(getCacheUrl(event.request)) // check if the request has already been cached
				.then(cached => cached || fetch(event.request)) // otherwise request network
		);
	}


});
*/


// Temporary cache option
self.addEventListener('fetch', function(event) {

	if(event.request.url.includes('login.js')
		|| event.request.url.includes('@')
		|| event.request.url.includes('formList')) {   // do not use service worker

		return false;

	} else if(event.request.url.includes(USER)) {
		// Get organisation id
		event.respondWith(
			getOrgId(event.request)

		);
	} else if(event.request.url.includes(ORG_CSS)) {
		if(organisationId) {
			let url = ORG_CSS;
			url = url.replace("org", organisationId);
			var myRequest = new Request(url);

			event.respondWith(
				caches
					.match(getCacheUrl(myRequest)) // check if the request has already been cached
					.then(cached => cached || fetch(myRequest)) // otherwise request network
			);
		}
	} else if (event.request.url.includes(TIMEZONES)) {
		// Cache then if not found network then cache the response
		event.respondWith(
			caches
				.match(getCacheUrl(event.request)) // check if the request has already been cached
				.then(cached => cached || update(event.request)) // otherwise request network
		);

	} else {
		// Try cache then network - but do not cache missing files as there will be a lot of them
		event.respondWith(
			caches
				.match(getCacheUrl(event.request)) // check if the request has already been cached
				.then(cached => cached || fetch(event.request)) // otherwise request network
		);
	}


}, {passive: true});

function getOrgId(request) {
	return fetch(request).then(
		async response => {
			if (response.status == 200) {
				let responseData = await response.clone().json();
				organisationId = responseData.o_id;
			}
			return response;
		}
	);
}

function update(request) {
	return fetch(request).then(
		response => {
			if (response.status == 200) {
				return caches
					.open(CACHE_NAME)
					.then(cache => {
						cache.put(getCacheUrl(request), response.clone());
						return response;
					})
			}
			return response;
		}
	);
}

function refresh(response) {
	if (response.ok) {
		return response
			.json() // read and parse JSON response
			.then(jsonResponse => {
				self.clients.matchAll().then(clients => {
					clients.forEach(client => {
						// report and send new data to client
						client.postMessage(
							JSON.stringify({
								type: response.url,
								data: jsonResponse
							})
						);
					});
				});
				return jsonResponse; // resolve promise with new data
			});
	} else {
		return response;
	}
}



/*
 * Refresh assignments cache using data from the network
 */
function update_assignments(request) {
	return fetch(request.url).then(
		response => {

			if (response.status == 200) {
				return caches
					.open(CACHE_NAME)
					.then(cache => {
						cache.put(ASSIGNMENTS, response.clone());
						return response;
					});
			}
			return response;
		}

	);
}

function precacheforms(response) {

	if(response && response.forms) {
		for(let i = 0; i < response.forms.length; i++) {
			let url = '/myWork/webForm/' + response.forms[i].ident;
			fetch(new Request(url, {credentials: 'same-origin'})).then(function(response) {
				if (response.status == 200) {
					return caches
						.open(CACHE_NAME)
						.then(cache => {
							cache.put(url, response.clone());
							return response;
						});
				}
				return response;
			})
		}
	}
}

/*
 * Remove cache buster from URLs that can be cached
 */
function getCacheUrl(request) {
	let url = request.url;
	if(url.includes(USER) || url.includes(PROJECT_LIST)) {
		let parts = url.split('?');
		url = parts[0];
	}
	return url;
}
