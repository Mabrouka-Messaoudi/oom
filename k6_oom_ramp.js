import http from 'k6/http';
import { check, sleep } from 'k6';

// Même correctif que k6_oom_baseline.js -- IP locale (192.168.100.113) remplacée
// par le hostname DuckDNS, surchargeable via -e BASE_URL=... -e KEYCLOAK_URL=...
const BASE_URL = __ENV.BASE_URL || 'http://nexshop-mab.duckdns.org:31018';
const KEYCLOAK = __ENV.KEYCLOAK_URL || 'http://nexshop-mab.duckdns.org:30930';
const REALM = 'spring-microservices-security-realm';

export const options = {
  stages: [
    { duration: '3m', target: 50 },
    { duration: '3m', target: 120 },
    { duration: '3m', target: 200 },
    { duration: '15m', target: 200 }, // plateau long, arrêté manuellement dès OOM
  ],
};

let tokenCache = ''; let tokenExpiry = 0;
function getToken() {
  const now = Date.now() / 1000;
  if (tokenCache && now < tokenExpiry - 30) return tokenCache;
  const res = http.post(`${KEYCLOAK}/realms/${REALM}/protocol/openid-connect/token`,
    { grant_type: 'password', client_id: 'angular-client', username: 'mab', password: '123mlk123', scope: 'openid' },
    { tags: { name: 'keycloak_login' } });
  if (res.status !== 200) return tokenCache;
  const body = JSON.parse(res.body);
  if (!body.access_token) return tokenCache;
  tokenCache = body.access_token; tokenExpiry = now + body.expires_in;
  return tokenCache;
}

export default function () {
  const headers = { 'Authorization': `Bearer ${getToken()}`, 'Content-Type': 'application/json' };

  // size=1000 -> Spring doit charger + sérialiser 1000 entités par requête.
  // Base confirmée avec >1000 produits, donc chaque page est pleine.
  // Alterne page 0 / page 1 pour éviter une désérialisation identique en boucle.
  const page = __ITER % 2;

  const res1 = http.get(`${BASE_URL}/api/product?page=${page}&size=400`, {
    headers, tags: { name: 'ListProductsLargePage' },
  });
  check(res1, { 'list product 1 200': (res) => res1.status === 200 });

  const res2 = http.get(`${BASE_URL}/api/product?page=${1 - page}&size=400`, {
    headers, tags: { name: 'ListProductsLargePage2' },
  });
  check(res2, { 'list product 2 200': (res) => res2.status === 200 });

  sleep(Math.random() * 0.2);
}
