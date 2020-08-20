
// During the installation phase, you'll usually want to cache static assets.
self.addEventListener('install', function(e) {
	// Once the service worker is installed, go ahead and fetch the resources to make this work offline.
	e.waitUntil(
		caches.open('v2').then(function(cache) {
			return cache.addAll([
				'./myWork.html',
				'./css/bootstrap.v4.5.min.css',
				'./font-awesome/css/font-awesome.css',
				'./css/smap2.css',
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
	// … either respond with the cached object or go ahead and fetch the actual URL
	event.respondWith(
		caches.match(event.request).then(function(response) {
			caches.match(event.request).then(function (response) {
				return response || fetch(event.request);
			});
		})
	);
});

self.addEventListener('activate', function (event) {
	var cacheKeeplist = ['v2'];

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