import http from 'k6/http';
import { check, sleep } from 'k6';

// Avant : IP privée locale (192.168.100.113) -- reliquat d'un ancien déploiement
// hors GKE, plus du tout joignable depuis le cluster actuel. Remplacé par le
// hostname DuckDNS (stable, survit aux resize de node pool -- contrairement à l'IP
// externe brute du nœud) + surchageable via -e pour ne plus jamais coder un port en
// dur : ces NodePort peuvent changer si 02-services.yaml est un jour réappliqué.
//   k6 run -e BASE_URL=http://nexshop-mab.duckdns.org:31018 \
//          -e KEYCLOAK_URL=http://nexshop-mab.duckdns.org:30930 k6_oom_baseline.js
const BASE_URL = __ENV.BASE_URL || 'http://nexshop-mab.duckdns.org:31018';
const KEYCLOAK = __ENV.KEYCLOAK_URL || 'http://nexshop-mab.duckdns.org:30930';
const REALM = 'spring-microservices-security-realm';

export const options = { vus: 8, duration: '1m' };

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
  const res = http.get(`${BASE_URL}/api/product`, { headers });
  check(res, { 'product 200': (res) => res.status === 200 });
  sleep(2 + Math.random());
}
