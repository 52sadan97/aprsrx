const CACHE_NAME = 'aprsrx-cache-v2';
const urlsToCache = [
  '/manifest.json',
  '/static/icon.svg'
];

self.addEventListener('install', event => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(urlsToCache))
      .catch(err => console.log('Cache hatasi:', err))
  );
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.map(cacheName => {
          if (cacheName !== CACHE_NAME) {
            return caches.delete(cacheName);
          }
        })
      );
    }).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  // Canlı uygulama olduğu için her zaman önce Ağa (Network) gidilir.
  // Çevrimdışı durumlarda cache'e düşer.
  event.respondWith(
    fetch(event.request).catch(() => caches.match(event.request))
  );
});
