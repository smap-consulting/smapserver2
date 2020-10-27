
var CACHE_NAME = 'v4';
var ASSIGNMENTS = '/surveyKPI/myassignments';
var WEBFORM = "/webForm";

// During the installation phase, you'll usually want to cache static assets.
self.addEventListener('install', function(e) {
	// Once the service worker is installed, go ahead and fetch the resources to make this work offline.
	e.waitUntil(
		caches.open(CACHE_NAME).then(function(cache) {
			return cache.addAll([
				'./index.html',
				'./css/bootstrap.v4.5.min.css',
				'./css/font-awesome.css',
				'./js/libs/modernizr.js',
				'./js/app/theme2.js',
				'./js/libs/jquery-2.1.1.js',
				'./js/libs/bootstrap.bundle.v4.5.min.js',
				'./js/app/custom.js'
			])
		})
	);
});

// when the browser fetches a URL…
self.addEventListener('fetch', function(event) {

	if (event.request.url.includes(ASSIGNMENTS)) {
		// response to request for forms and tasks. Cache Update Refresh strategy
		event.respondWith(caches.match(ASSIGNMENTS));
		event.waitUntil(update_assignments(event.request).then(refresh).then(precacheforms));

	} else if (event.request.url.includes(WEBFORM)) {
		// response to a webform request.  Network then cache strategy
		event.respondWith(
			fetch(event.request).then(function(response) {
				if (!response.ok || response.type === "error" || response.type === "opaque") {
					// An HTTP error response code (40x, 50x) won't cause the fetch() promise to reject.
					// We need to explicitly throw an exception to trigger the catch() clause.
					throw Error('response status ' + response.status);
				}

				return caches
					.open(CACHE_NAME)
					.then(cache => {
						cache.put(event.request, response.clone());
						return response;
					});
			}).catch( function () {
				return caches.match(event.request);
			})
		);

	} else {
		// Always fethc
		event.respondWith(
			fetch(event.request)
		);
	}


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

function cache(request, response) {
	if (response.type === "error" || response.type === "opaque") {
		return Promise.resolve(); // do not put in cache network errors
	}

	return caches
		.open(CACHE_NAME)
		.then(cache => cache.put(request, response.clone()));
}

function update(request) {
	return fetch(request.url).then(
		response =>
			cache(request, response) // we can put response in cache
				.then(() => response) // resolve promise with the Response object
	);
}

function refresh(response) {
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
}



/*
 * Refresh assignments cache using data from the network
 */
function update_assignments(request) {
	return fetch(request.url).then(
		response =>

			cache(ASSIGNMENTS, response) // we can put response in cache
				.then(() => response) // resolve promise with the Response object
	);
}

function precacheforms(response) {
	console.log("-------" + JSON.stringify(response));
	if(response && response.forms) {
		for(var i = 0; i < response.forms.length; i++) {
			console.log(response.forms[i].ident);
		}
	}
}