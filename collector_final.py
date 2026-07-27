#!/usr/bin/env python3
"""collector_final.py — assemble toutes les sources en un CSV, une ligne par pod.

Changements v3 :
- profile / fault_type / fault_phase / output_csv / run_id / mem_request / mem_limit
  sont passés en CLI, plus de sed -i sur ce fichier.
- Plus de duration_minutes codé en dur : le cycle de vie est piloté par le bash
  parent (kill -INT). Boucle infinie par défaut, --duration reste possible
  pour des tests manuels.
"""
import argparse
import concurrent.futures
import urllib.request
import urllib.parse
import json
import time
import csv
import os
import re
import signal

PROM_URL = "http://localhost:30090"
LOKI_URL = "http://localhost:30003"
TEMPO_URL = "http://localhost:30622"
# Ingestion live de la plateforme AIOps -- même logique que PROM_URL/LOKI_URL/TEMPO_URL
# ci-dessus (NodePort exposé, atteint depuis le poste où tourne ce script, généralement
# via un tunnel/port-forward vers localhost). Surchargeable avec --platform-url si la
# plateforme est exposée autrement (LoadBalancer, Ingress, etc.).
PLATFORM_URL = "http://localhost:30080/ingest/tick"
NS = "nexshop"

SERVICES_WITH_TRACES = ["product-service", "order-service", "inventory-service",
                         "notification-service", "api-gateway"]

COLLECT_INTERVAL_SEC = 10

# ---------- Requêtes génériques ----------
def _http_json(url: str, timeout: int = 10):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.load(resp)
    except Exception as e:
        print(f"[WARN] Requête échouée: {url[:80]}... -> {e}")
        return None


def prom_query(promql: str, label: str = "pod") -> dict:
    url = f"{PROM_URL}/api/v1/query?" + urllib.parse.urlencode({"query": promql})
    data = _http_json(url)
    if not data or data.get("status") != "success":
        return {}
    out = {}
    for r in data["data"]["result"]:
        key = r["metric"].get(label)
        if key:
            out[key] = float(r["value"][1])
    return out


def loki_query(logql: str) -> dict:
    url = f"{LOKI_URL}/loki/api/v1/query?" + urllib.parse.urlencode({"query": logql})
    data = _http_json(url)
    if not data or data.get("status") != "success":
        return {}
    out = {}
    for r in data["data"]["result"]:
        pod = r["metric"].get("pod")
        if pod:
            out[pod] = float(r["value"][1])
    return out


# ---------- Catalogues ----------
PROM_QUERIES = {
    "cpu_usage_pct": (
        f'sum(rate(container_cpu_usage_seconds_total{{namespace="{NS}", '
        f'container!="", container!="POD"}}[2m])) by (pod) '
        f'/ sum(kube_pod_container_resource_limits{{namespace="{NS}", resource="cpu"}}) by (pod) * 100'
    ),
    "memory_usage_pct": (
        f'sum(container_memory_working_set_bytes{{namespace="{NS}", '
        f'container!="", container!="POD"}}) by (pod) '
        f'/ sum(kube_pod_container_resource_limits{{namespace="{NS}", resource="memory"}}) by (pod) * 100'
    ),
    "memory_working_set_bytes": (
        f'sum(container_memory_working_set_bytes{{namespace="{NS}", '
        f'container!="", container!="POD"}}) by (pod)'
    ),
    "restart_count_delta": (
        f'sum(increase(kube_pod_container_status_restarts_total{{namespace="{NS}"}}[5m])) by (pod)'
    ),
    "jvm_heap_used_pct": (
        f'sum(jvm_memory_used_bytes{{namespace="{NS}", area="heap"}}) by (pod) '
        f'/ sum(jvm_memory_max_bytes{{namespace="{NS}", area="heap"}}) by (pod) * 100'
    ),
    "gc_pause_total_sec": (
        f'sum(rate(jvm_gc_pause_seconds_sum{{namespace="{NS}"}}[2m])) by (pod)'
    ),
    "http_p95_latency_ms": (
        f'histogram_quantile(0.95, sum(rate(http_server_requests_seconds_bucket'
        f'{{namespace="{NS}"}}[2m])) by (le, pod)) * 1000'
    ),
    "http_error_rate_pct": (
        f'sum(rate(http_server_requests_seconds_count{{namespace="{NS}", status=~"5.."}}[5m])) by (pod) '
        f'/ sum(rate(http_server_requests_seconds_count{{namespace="{NS}"}}[5m])) by (pod) * 100'
    ),
    "db_active_connections_mysql": f'hikaricp_connections_active{{namespace="{NS}"}}',
    "db_active_connections_mongo": f'mongodb_driver_pool_checkedout{{namespace="{NS}"}}',
    "db_query_duration_avg_ms": (
        f'sum(rate(mongodb_driver_commands_seconds_sum{{namespace="{NS}"}}[5m])) by (pod) '
        f'/ sum(rate(mongodb_driver_commands_seconds_count{{namespace="{NS}"}}[5m])) by (pod) * 1000'
    ),
    "db_query_duration_max_ms": (
        f'max(max_over_time(mongodb_driver_commands_seconds_max{{namespace="{NS}"}}[5m])) by (pod) * 1000'
    ),
}

PROM_NODE_QUERIES = {
    "node_cpu_usage_pct": '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[2m])) * 100)',
    "node_load1": "node_load1",
    "node_memory_available_pct": "node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes * 100",
}

LOKI_QUERIES = {
    "log_error_count": f'sum(count_over_time({{namespace="{NS}"}} |~ "(?i)error" [5m])) by (pod)',
    "log_warn_count": f'sum(count_over_time({{namespace="{NS}"}} |~ "(?i)warn" [5m])) by (pod)',
    "log_volume_total": f'sum(count_over_time({{namespace="{NS}"}} [5m])) by (pod)',
}

EXCEPTION_CATEGORIES = [
    (re.compile(r"MemberIdRequired|NotCoordinator|Rebalance", re.I), "kafka_coordination_error"),
    (re.compile(r"EndOfStream|ConnectException|IOException|ZooKeeperServer not running", re.I), "connection_lost"),
    (re.compile(r"SessionExpired", re.I), "session_expired"),
    (re.compile(r"OutOfMemory|OOM", re.I), "oom"),
    (re.compile(r"IllegalState|Exception|Error", re.I), "client_error"),
]


def classify_exception(raw_line: str) -> str:
    for pattern, category in EXCEPTION_CATEGORIES:
        if pattern.search(raw_line):
            return category
    return "unclassified"


def get_exception_signatures(window_minutes: int = 5) -> dict:
    query = f'{{namespace="{NS}"}} |~ "(?i)exception"'
    now_ns = int(time.time() * 1e9)
    start_ns = now_ns - window_minutes * 60 * int(1e9)
    params = {"query": query, "start": str(start_ns), "end": str(now_ns), "limit": "200"}
    url = f"{LOKI_URL}/loki/api/v1/query_range?" + urllib.parse.urlencode(params)
    data = _http_json(url)
    if not data or data.get("status") != "success":
        return {}
    pod_categories = {}
    for stream in data["data"]["result"]:
        pod = stream["stream"].get("pod")
        if not pod:
            continue
        for _, line in stream["values"]:
            pod_categories.setdefault(pod, []).append(classify_exception(line))
    return {pod: max(set(cats), key=cats.count) for pod, cats in pod_categories.items()}


def get_waiting_reasons() -> dict:
    url = f"{PROM_URL}/api/v1/query?" + urllib.parse.urlencode(
        {"query": f'kube_pod_container_status_waiting_reason{{namespace="{NS}"}} == 1'}
    )
    data = _http_json(url)
    if not data or data.get("status") != "success":
        return {}
    return {r["metric"]["pod"]: r["metric"]["reason"] for r in data["data"]["result"] if "pod" in r["metric"]}


def get_last_terminated_reasons() -> dict:
    url = f"{PROM_URL}/api/v1/query?" + urllib.parse.urlencode(
        {"query": f'kube_pod_container_status_last_terminated_reason{{namespace="{NS}"}} == 1'}
    )
    data = _http_json(url)
    if not data or data.get("status") != "success":
        return {}
    return {r["metric"]["pod"]: r["metric"]["reason"] for r in data["data"]["result"] if "pod" in r["metric"]}


def tempo_search(service: str, status_error: bool = False, window_minutes: int = 5, limit: int = 200) -> list:
    now = int(time.time())
    start = now - window_minutes * 60
    if status_error:
        query = f'{{resource.service.name="{service}" && status=error}}'
    else:
        query = f'{{resource.service.name="{service}"}}'
    params = {"q": query, "start": str(start), "end": str(now), "limit": str(limit)}
    url = f"{TEMPO_URL}/api/search?" + urllib.parse.urlencode(params)
    data = _http_json(url)
    if not data:
        return []
    return [t.get("durationMs", 0) for t in data.get("traces", [])]


def percentile(values: list, p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (p / 100)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def get_trace_features(window_minutes: int = 5) -> dict:
    """Une requête Tempo par service ET par statut (normal/erreur) -- séquentiellement,
    ça fait 2 x len(SERVICES_WITH_TRACES) appels HTTP à la suite. Parallélisé pour
    que le pire cas soit "le plus lent des appels", pas leur somme -- vécu en
    pratique : sous congestion réseau (typiquement causée par la charge k6 elle-même
    empruntant le même chemin), un cycle de collecte entier peut prendre plusieurs
    minutes en séquentiel, faisant paraître le collecteur "arrêté" alors qu'il est
    juste très lent."""
    def _one_service(service: str):
        all_d = tempo_search(service, status_error=False, window_minutes=window_minutes)
        err_d = tempo_search(service, status_error=True, window_minutes=window_minutes)
        total = len(all_d)
        ratio = (len(err_d) / total) if total > 0 else 0.0
        return service, {
            "trace_error_span_ratio": ratio,
            "trace_p95_duration_ms": percentile(all_d, 95) if all_d else "",
        }

    out = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(len(SERVICES_WITH_TRACES), 1)) as ex:
        for service, result in ex.map(_one_service, SERVICES_WITH_TRACES):
            out[service] = result
    return out


def get_pod_node_mapping() -> dict:
    url = f"{PROM_URL}/api/v1/query?" + urllib.parse.urlencode({"query": f'kube_pod_info{{namespace="{NS}"}}'})
    data = _http_json(url)
    if not data or data.get("status") != "success":
        return {}
    return {r["metric"]["pod"]: r["metric"]["node"] for r in data["data"]["result"] if "pod" in r["metric"]}


def get_node_name_to_instance() -> dict:
    url = f"{PROM_URL}/api/v1/query?" + urllib.parse.urlencode({"query": "node_uname_info"})
    data = _http_json(url)
    if not data or data.get("status") != "success":
        return {}
    return {r["metric"]["nodename"]: r["metric"]["instance"] for r in data["data"]["result"]}


# ---------- Assemblage ----------
# NOTE : run_id / mem_request_mi / mem_limit_mi ajoutés pour tracer chaque
# expérience individuellement dans le CSV final (indispensable pour le
# pipeline multi-runs / multi-modèles).
FIELDNAMES = (
    ["timestamp", "run_id", "service_name", "node_name",
     "profile", "fault_type", "fault_phase",
     "mem_request_mi", "mem_limit_mi", "time_to_failure_sec"]
    + list(PROM_QUERIES.keys())
    + list(PROM_NODE_QUERIES.keys())
    + list(LOKI_QUERIES.keys())
    + ["exception_signature", "trace_error_span_ratio", "trace_p95_duration_ms",
       "waiting_reason", "last_terminated_reason"]
)

# Champs texte/catégoriels : jamais convertis en nombre lors du push JSON vers la
# plateforme. mem_request_mi/mem_limit_mi restent des chaînes ("300Mi") -- c'est le
# format que m3_prediction.py attend côté plateforme (il les parse lui-même).
_CATEGORICAL_FIELDS = {
    "run_id", "service_name", "node_name", "profile", "fault_type", "fault_phase",
    "mem_request_mi", "mem_limit_mi", "exception_signature", "waiting_reason",
    "last_terminated_reason",
}


def _row_to_json_safe(row: dict) -> dict:
    """Convertit une ligne CSV (valeurs parfois "" pour 'manquant', timestamp en
    epoch entier) vers le format attendu par la plateforme :
      - "" -> None (la plateforme sait déjà gérer une métrique manquante proprement,
        cf. m4_threshold.py -- statut 'missing_data' plutôt qu'un plantage ou une
        fausse valeur)
      - timestamp epoch (int) -> chaîne "YYYY-MM-DD HH:MM:SS", le format que
        pd.to_datetime() attend côté plateforme. Un entier brut serait interprété
        comme des NANOSECONDES depuis epoch par pandas -- une date en 1970, pas 2026.
      - champs numériques : conversion en float quand c'est un nombre exploitable.
    """
    out = {}
    for key, value in row.items():
        if key == "timestamp":
            out[key] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(value)))
            continue
        if key in _CATEGORICAL_FIELDS:
            out[key] = value if value != "" else None
            continue
        if value == "" or value is None:
            out[key] = None
        else:
            try:
                out[key] = float(value)
            except (TypeError, ValueError):
                out[key] = value  # champ texte inattendu -- transmis tel quel plutôt que planter
    return out


def push_tick_to_platform(rows: list, ctx: dict) -> bool:
    """Envoie les lignes fraîchement collectées à la plateforme AIOps (POST
    /ingest/tick). Ne lève JAMAIS d'exception -- une plateforme injoignable ne doit
    JAMAIS interrompre la collecte, qui reste la source de vérité (le CSV continue
    d'être écrit dans tous les cas). Retourne True/False juste pour le décompte
    d'échecs consécutifs (utilisé pour ne pas spammer les logs)."""
    if not ctx.get("live_push"):
        return True
    payload = json.dumps({"rows": [_row_to_json_safe(r) for r in rows]}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if ctx.get("platform_token"):
        headers["X-Ingest-Token"] = ctx["platform_token"]
    req = urllib.request.Request(ctx["platform_url"], data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=ctx.get("live_push_timeout", 3)) as resp:
            resp.read()
        return True
    except Exception as e:
        print(f"[WARN] Push live vers la plateforme échoué ({ctx['platform_url']}) -> {e}")
        return False


def collect_once(ctx: dict) -> list:
    ts = int(time.time())

    # Toutes ces requêtes sont indépendantes -- avant, séquentielles (~33 appels HTTP
    # d'affilée, jusqu'à 10s de timeout chacun = plusieurs minutes dans le pire cas
    # sous congestion réseau). Parallélisées : le pire cas devient "le plus lent des
    # appels individuels", pas leur somme.
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(PROM_QUERIES) + len(PROM_NODE_QUERIES) + len(LOKI_QUERIES) + 6) as ex:
        f_pod_metrics = {name: ex.submit(prom_query, q, "pod") for name, q in PROM_QUERIES.items()}
        f_node_metrics = {name: ex.submit(prom_query, q, "instance") for name, q in PROM_NODE_QUERIES.items()}
        f_log_metrics = {name: ex.submit(loki_query, q) for name, q in LOKI_QUERIES.items()}
        f_exceptions = ex.submit(get_exception_signatures)
        f_traces = ex.submit(get_trace_features)
        f_pod_to_node = ex.submit(get_pod_node_mapping)
        f_node_to_instance = ex.submit(get_node_name_to_instance)
        f_waiting_reasons = ex.submit(get_waiting_reasons)
        f_last_terminated_reasons = ex.submit(get_last_terminated_reasons)

        pod_metrics = {name: f.result() for name, f in f_pod_metrics.items()}
        node_metrics = {name: f.result() for name, f in f_node_metrics.items()}
        log_metrics = {name: f.result() for name, f in f_log_metrics.items()}
        exceptions = f_exceptions.result()
        traces = f_traces.result()
        pod_to_node = f_pod_to_node.result()
        node_to_instance = f_node_to_instance.result()
        waiting_reasons = f_waiting_reasons.result()
        last_terminated_reasons = f_last_terminated_reasons.result()

    all_pods = set()
    for d in pod_metrics.values():
        all_pods.update(d.keys())

    rows = []
    for pod in sorted(all_pods):
        node_name = pod_to_node.get(pod, "")
        instance = node_to_instance.get(node_name, "")

        row = {
            "timestamp": ts,
            "run_id": ctx["run_id"],
            "service_name": pod,
            "node_name": node_name,
            "profile": ctx["profile"],
            "fault_type": ctx["fault_type"],
            "fault_phase": ctx["fault_phase"],
            "mem_request_mi": ctx["mem_request"],
            "mem_limit_mi": ctx["mem_limit"],
            "time_to_failure_sec": "",
        }
        for name in PROM_QUERIES:
            row[name] = pod_metrics[name].get(pod, "")
        for name in PROM_NODE_QUERIES:
            row[name] = node_metrics[name].get(instance, "")
        for name in LOKI_QUERIES:
            row[name] = log_metrics[name].get(pod, 0.0)

        row["exception_signature"] = exceptions.get(pod, "none")
        row["waiting_reason"] = waiting_reasons.get(pod, "none")
        row["last_terminated_reason"] = last_terminated_reasons.get(pod, "none")

        service_key = next((s for s in SERVICES_WITH_TRACES if pod.startswith(s)), None)
        if service_key:
            row["trace_error_span_ratio"] = traces[service_key]["trace_error_span_ratio"]
            row["trace_p95_duration_ms"] = traces[service_key]["trace_p95_duration_ms"]
        else:
            row["trace_error_span_ratio"] = ""
            row["trace_p95_duration_ms"] = ""

        rows.append(row)

    return rows


def run(output_csv: str, ctx: dict, duration_minutes=None):
    """Boucle de collecte. duration_minutes=None -> tourne jusqu'à SIGINT (Ctrl+C
    ou kill -INT envoyé par le script bash parent)."""
    file_exists = os.path.isfile(output_csv)
    start_time = time.time()
    consecutive_push_failures = 0

    with open(output_csv, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()

        while True:
            rows = collect_once(ctx)
            for row in rows:
                writer.writerow(row)
            f.flush()
            print(f"[{time.strftime('%H:%M:%S')}] {len(rows)} lignes écrites -> {output_csv} "
                  f"(run_id={ctx['run_id']}, phase={ctx['fault_phase']})")

            # Le CSV ci-dessus est écrit dans TOUS les cas, que le push live réussisse
            # ou non -- c'est la garantie que --live-push ne peut jamais faire perdre
            # de données de collecte, seulement l'affichage en direct.
            if ctx.get("live_push") and rows:
                ok = push_tick_to_platform(rows, ctx)
                if ok:
                    if consecutive_push_failures > 0:
                        print(f"[{time.strftime('%H:%M:%S')}] Push live rétabli après {consecutive_push_failures} échec(s).")
                    consecutive_push_failures = 0
                else:
                    consecutive_push_failures += 1
                    # Rate-limite le bruit dans les logs : détail au 1er échec, puis
                    # un rappel toutes les ~10 tentatives (~1min40 à 10s/tick) au lieu
                    # de spammer à chaque tick si la plateforme reste injoignable.
                    if consecutive_push_failures > 1 and consecutive_push_failures % 10 != 0:
                        pass
                    elif consecutive_push_failures > 1:
                        print(f"[{time.strftime('%H:%M:%S')}] Push live toujours en échec "
                              f"({consecutive_push_failures} tentatives) -- la collecte CSV continue normalement.")

            if duration_minutes and (time.time() - start_time) / 60 >= duration_minutes:
                print("Durée de collecte atteinte, arrêt.")
                break
            time.sleep(COLLECT_INTERVAL_SEC)


def parse_args():
    p = argparse.ArgumentParser(description="Collecte de métriques nexshop -> CSV")
    p.add_argument("--profile", default="normal", help="normal | high | blackfriday")
    p.add_argument("--fault-type", default="none", help="none | oom | cpu_throttle | ...")
    p.add_argument("--fault-phase", default="pre_fault", help="pre_fault | during_fault | recovery | steady_state")
    p.add_argument("--run-id", default="run_default", help="identifiant unique du run (ex: oom_200mi_01)")
    p.add_argument("--mem-request", default="", help="valeur requests.memory appliquée pendant ce run")
    p.add_argument("--mem-limit", default="", help="valeur limits.memory appliquée pendant ce run")
    p.add_argument("--output", default="nexshop_dataset_oom.csv", help="fichier CSV de sortie")
    p.add_argument("--duration", type=float, default=None, help="durée max en minutes (défaut: infini, arrêté par SIGINT)")
    # ----- URLs Prometheus/Loki/Tempo -----
    # Par défaut sur localhost (valable si lancé DEPUIS un nœud du cluster, ou via
    # kubectl port-forward). Si le collecteur tourne sur un poste externe (ex: ton
    # PC, comme observé en pratique), passer les NodePort via l'IP externe d'un
    # nœud -- exactement comme --platform-url. Sans ça, toutes les requêtes
    # Prometheus échouent silencieusement (connexion refusée), le collecteur ne
    # trouve aucun pod, et rien n'est ni écrit ni poussé -- symptôme observé :
    # "0 ticks / 0 lignes" côté plateforme alors que le script tourne normalement.
    p.add_argument("--prom-url", default=PROM_URL,
                    help=f"URL de Prometheus (défaut: {PROM_URL})")
    p.add_argument("--loki-url", default=LOKI_URL,
                    help=f"URL de Loki (défaut: {LOKI_URL})")
    p.add_argument("--tempo-url", default=TEMPO_URL,
                    help=f"URL de Tempo (défaut: {TEMPO_URL})")
    # ----- Ingestion live vers la plateforme AIOps (Lot B) -----
    # Désactivé par défaut : aucun changement de comportement pour les scripts/runs
    # existants tant qu'on ne l'active pas explicitement.
    p.add_argument("--live-push", action="store_true",
                    help="pousse aussi chaque tick vers la plateforme AIOps (POST /ingest/tick), "
                         "en plus d'écrire le CSV comme d'habitude")
    p.add_argument("--platform-url", default=PLATFORM_URL,
                    help=f"URL de l'endpoint d'ingestion de la plateforme (défaut: {PLATFORM_URL})")
    p.add_argument("--platform-token", default=os.environ.get("PLATFORM_INGEST_TOKEN", ""),
                    help="jeton d'authentification si la plateforme en exige un (ou variable "
                         "d'env PLATFORM_INGEST_TOKEN)")
    p.add_argument("--live-push-timeout", type=float, default=3.0,
                    help="délai max (s) pour le push HTTP avant abandon de CE tick (défaut: 3s)")
    return p.parse_args()
def _handle_sigterm(signum, frame):
    raise KeyboardInterrupt()

signal.signal(signal.SIGTERM, _handle_sigterm)

if __name__ == "__main__":
    args = parse_args()

    # Applique les surcharges d'URL AVANT tout appel de collecte -- les fonctions
    # (prom_query, loki_query, etc.) lisent ces globales à l'exécution, donc les
    # réaffecter ici suffit, pas besoin de les passer en paramètre partout.
    PROM_URL = args.prom_url
    LOKI_URL = args.loki_url
    TEMPO_URL = args.tempo_url

    ctx = {
        "profile": args.profile,
        "fault_type": args.fault_type,
        "fault_phase": args.fault_phase,
        "run_id": args.run_id,
        "mem_request": args.mem_request,
        "mem_limit": args.mem_limit,
        "live_push": args.live_push,
        "platform_url": args.platform_url,
        "platform_token": args.platform_token,
        "live_push_timeout": args.live_push_timeout,
    }
    print(f"Démarrage collecte — run_id={ctx['run_id']} profile={ctx['profile']} "
          f"fault_type={ctx['fault_type']} phase={ctx['fault_phase']} -> {args.output}")
    print(f"Prometheus={PROM_URL}  Loki={LOKI_URL}  Tempo={TEMPO_URL}")
    if ctx["live_push"]:
        print(f"Push live activé -> {ctx['platform_url']} "
              f"(échecs tolérés, la collecte CSV n'est jamais interrompue)")
    print("Ctrl+C (ou SIGINT du parent) pour arrêter proprement.")
    try:
        run(args.output, ctx, duration_minutes=args.duration)
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
