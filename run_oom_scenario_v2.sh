#!/usr/bin/env bash
set -euxo pipefail
# ===== Paramètres du run (passés en argument, plus de valeurs codées en dur) =====
# Usage: ./run_oom_scenario_v2.sh <run_id> <fault_req> <fault_lim>
# Exemple: ./run_oom_scenario_v2.sh oom_200mi_01 200Mi 320Mi
RUN_ID="${1:?usage: $0 <run_id> <fault_req> <fault_lim>}"
FAULT_REQ="${2:?usage: $0 <run_id> <fault_req> <fault_lim>}"
FAULT_LIM="${3:?usage: $0 <run_id> <fault_req> <fault_lim>}"

# ===== Ingestion live vers la plateforme AIOps (optionnel) =====
# Ne change rien par défaut (LIVE_PUSH=0) : usage positionnel inchangé.
# Pour activer, définir les variables d'environnement AVANT l'appel, ex:
#   LIVE_PUSH=1 PLATFORM_URL="http://<ip-worker1>:30080/ingest/tick" \
#     ./run_oom_scenario_v2.sh oom_200mi_01 200Mi 320Mi
# Ces variables sont automatiquement héritées si c'est run_all_oom.sh qui appelle
# ce script (l'environnement se propage aux sous-scripts sans rien à changer là-bas).
LIVE_PUSH="${LIVE_PUSH:-0}"
PLATFORM_URL="${PLATFORM_URL:-http://localhost:30080/ingest/tick}"
PLATFORM_TOKEN="${PLATFORM_TOKEN:-}"

# ===== URLs Prometheus/Loki/Tempo (optionnel) =====
# Par défaut sur localhost (valable si ce script tourne DEPUIS un nœud du cluster,
# ou via kubectl port-forward actif). Si lancé depuis un poste externe (constaté en
# pratique), passer les NodePort via l'IP externe d'un nœud, ex :
#   PROM_URL="http://<ip-noeud>:30090" LOKI_URL="http://<ip-noeud>:30003" \
#     TEMPO_URL="http://<ip-noeud>:30622" ./run_oom_scenario_v2.sh oom_200mi_01 200Mi 320Mi
# Sans ça, le collecteur ne trouve aucun pod (requêtes Prometheus en échec silencieux)
# et n'écrit ni ne pousse aucune ligne -- symptôme déjà rencontré : script qui tourne
# normalement mais CSV et /ws/live tous les deux vides.
PROM_URL="${PROM_URL:-http://localhost:30090}"
LOKI_URL="${LOKI_URL:-http://localhost:30003}"
TEMPO_URL="${TEMPO_URL:-http://localhost:30622}"

NS="nexshop"
DEPLOY="product-service"
COLLECTOR="collector_final.py"
K6_BASELINE="k6_oom_baseline.js"
K6_FAULT="k6_oom_ramp.js"
POD_LABEL="app=product-service"

BASELINE_MIN=1
MAX_WAIT_OOM_MIN=15
POLL_SEC=10
RECOVERY_MIN=1

# Un CSV par run : plus simple à isoler/relancer/jeter individuellement.
# La fusion en un seul dataset se fait après coup avec merge_csvs.py.
mkdir -p runs
OUTPUT_CSV="runs/nexshop_dataset_oom_${RUN_ID}.csv"

start_collector() {
  # $1 = fault_phase, $2 = fault_type
  LIVE_PUSH_ARGS=()
  if [ "$LIVE_PUSH" = "1" ]; then
    LIVE_PUSH_ARGS+=(--live-push --platform-url "$PLATFORM_URL")
    if [ -n "$PLATFORM_TOKEN" ]; then
      LIVE_PUSH_ARGS+=(--platform-token "$PLATFORM_TOKEN")
    fi
  fi
  python3 "$COLLECTOR" \
    --profile "normal" \
    --fault-type "$2" \
    --fault-phase "$1" \
    --run-id "$RUN_ID" \
    --mem-request "$FAULT_REQ" \
    --mem-limit "$FAULT_LIM" \
    --output "$OUTPUT_CSV" \
    --prom-url "$PROM_URL" \
    --loki-url "$LOKI_URL" \
    --tempo-url "$TEMPO_URL" \
    "${LIVE_PUSH_ARGS[@]}" \
    > "collector_${RUN_ID}_${1}.log" 2>&1 &
  COLLECTOR_PID=$!
  echo "[$(date +%H:%M:%S)] collector démarré (pid=$COLLECTOR_PID, phase=$1, live_push=$LIVE_PUSH) -> collector_${RUN_ID}_${1}.log"
}

stop_collector() {
  if kill -0 "$COLLECTOR_PID" 2>/dev/null; then
    kill "$COLLECTOR_PID"
    wait "$COLLECTOR_PID" 2>/dev/null || true
    echo "[$(date +%H:%M:%S)] collector arrêté (pid=$COLLECTOR_PID)"
  fi
}

wait_pod_running() { kubectl_retry rollout status deployment/"$DEPLOY" -n "$NS" --timeout=300s; }

# kubectl_retry : vécu en pratique -- un simple "TLS handshake timeout" transitoire
# (coupure réseau de quelques secondes, rien à voir avec le cluster lui-même) a fait
# planter tout un run de 15 minutes, en laissant product-service coincé sur la
# mémoire réduite parce que l'étape 5 (repatch) n'a jamais été atteinte. La boucle
# de poll (toutes les 10s pendant jusqu'à 15min = ~90 appels kubectl) est
# statistiquement la plus exposée à ce genre de blip. Retry avec un court délai
# avant d'abandonner pour de bon -- si le cluster est VRAIMENT injoignable au-delà
# de quelques dizaines de secondes, le script s'arrête toujours, mais un simple
# aller-retour réseau raté ne doit plus coûter tout le run.
kubectl_retry() {
  local max_attempts=5 attempt=1 delay=4 output
  while [ "$attempt" -le "$max_attempts" ]; do
    if output=$(kubectl "$@" 2>&1); then
      echo "$output"
      return 0
    fi
    echo "[$(date +%H:%M:%S)] [WARN] kubectl $* a échoué (tentative $attempt/$max_attempts) -- $(echo "$output" | head -1)" >&2
    sleep "$delay"
    attempt=$((attempt + 1))
  done
  echo "[$(date +%H:%M:%S)] [ERREUR] kubectl $* a échoué après $max_attempts tentatives -- abandon." >&2
  echo "$output" >&2
  return 1
}

get_pod_name() {
    kubectl_retry get pods \
        -n "$NS" \
        -l "$POD_LABEL" \
        --sort-by=.metadata.creationTimestamp \
        -o jsonpath='{.items[-1:].metadata.name}'
}

# get_last_terminated_reason / get_restart_count : ATTENTION, ce pod a plusieurs
# conteneurs (product-service + le sidecar loki-proxy). L'ordre de
# status.containerStatuses[] NE SUIT PAS forcément l'ordre de spec.containers[] --
# vécu en pratique : containerStatuses[0] = loki-proxy, [1] = product-service. Un
# index [0] en dur vérifiait donc le sidecar (qui ne redémarre jamais), ratant
# systématiquement le vrai OOMKilled sur product-service. On filtre maintenant par
# NOM explicitement (via Python plutôt que le filtre jsonpath ?() de kubectl, plus
# fiable et cohérent avec le reste du projet).
get_last_terminated_reason() {
  kubectl_retry get pod "$1" -n "$NS" -o json 2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    for c in d.get('status', {}).get('containerStatuses', []):
        if c.get('name') == '$DEPLOY':
            print(c.get('lastState', {}).get('terminated', {}).get('reason', ''))
            break
except Exception:
    pass
" || echo ""
}
get_restart_count() {
  kubectl_retry get pod "$1" -n "$NS" -o json 2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    for c in d.get('status', {}).get('containerStatuses', []):
        if c.get('name') == '$DEPLOY':
            print(c.get('restartCount', 0))
            break
    else:
        print(0)
except Exception:
    print(0)
" || echo "0"
}

echo "=== RUN $RUN_ID : requests=$FAULT_REQ limits=$FAULT_LIM ==="
echo "[$(date +%H:%M:%S)] Prometheus=$PROM_URL  Loki=$LOKI_URL  Tempo=$TEMPO_URL"
if [ "$LIVE_PUSH" = "1" ]; then
  echo "[$(date +%H:%M:%S)] Ingestion live activée -> $PLATFORM_URL"
else
  echo "[$(date +%H:%M:%S)] Ingestion live désactivée (LIVE_PUSH=1 pour l'activer) -- CSV uniquement."
fi

# ===== ÉTAPE 0 : capturer les ressources actuelles =====
ORIG_REQ=$(kubectl_retry get deployment "$DEPLOY" -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].resources.requests.memory}')
ORIG_LIM=$(kubectl_retry get deployment "$DEPLOY" -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].resources.limits.memory}')
echo "[$(date +%H:%M:%S)] ressources initiales: requests=$ORIG_REQ limits=$ORIG_LIM"

POD_NAME=$(get_pod_name)
BASE_RESTARTS=$(get_restart_count "$POD_NAME")

# ===== ÉTAPE 1 : baseline sous charge légère (pre_fault) =====
k6 run "$K6_BASELINE" > "k6_baseline_${RUN_ID}.log" 2>&1 &
K6_BASE_PID=$!
start_collector "pre_fault" "none"
sleep $((BASELINE_MIN * 60))
stop_collector
kill "$K6_BASE_PID" 2>/dev/null || true
wait "$K6_BASE_PID" 2>/dev/null || true

# ===== ÉTAPE 2 : patch mémoire réduite (spécifique à ce run) =====
echo "[$(date +%H:%M:%S)] patch mémoire -> requests=$FAULT_REQ limits=$FAULT_LIM"
kubectl_retry patch deployment "$DEPLOY" -n "$NS" --type='json' -p="[
  {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/requests/memory\",\"value\":\"$FAULT_REQ\"},
  {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/limits/memory\",\"value\":\"$FAULT_LIM\"}
]"
wait_pod_running
POD_NAME=$(get_pod_name)
BASE_RESTARTS=$(get_restart_count "$POD_NAME")
echo "[$(date +%H:%M:%S)] patch appliqué, pod: $POD_NAME"

# ===== ÉTAPE 3 : charge progressive =====
k6 run "$K6_FAULT" > "k6_fault_${RUN_ID}.log" 2>&1 &
K6_FAULT_PID=$!

start_collector "during_fault" "oom"
kubectl get events -n "$NS" --field-selector involvedObject.name="$POD_NAME" --watch > "events_${RUN_ID}.log" 2>&1 &
EVENTS_PID=$!

# ===== ÉTAPE 4 : poll jusqu'à OOMKilled =====
echo "[$(date +%H:%M:%S)] attente OOM (poll ${POLL_SEC}s, max ${MAX_WAIT_OOM_MIN}min)..."
OOM_TIME=""
DEADLINE=$(( $(date +%s) + MAX_WAIT_OOM_MIN * 60 ))
while [ "$(date +%s)" -lt "$DEADLINE" ]; do

    CURRENT_POD=$(get_pod_name)

    REASON=$(get_last_terminated_reason "$CURRENT_POD")
    RESTARTS=$(get_restart_count "$CURRENT_POD")

    if [ "$REASON" = "OOMKilled" ] || [ "$RESTARTS" -gt "$BASE_RESTARTS" ]; then
        OOM_TIME=$(date +%s)
        POD_NAME="$CURRENT_POD"

        echo "[$(date +%H:%M:%S)] [OK] OOMKilled détecté sur $POD_NAME (reason=$REASON restart=$RESTARTS)"
        break
    fi

    sleep "$POLL_SEC"
done
[ -z "$OOM_TIME" ] && echo "[$(date +%H:%M:%S)] [WARN] pas d'OOM après ${MAX_WAIT_OOM_MIN}min."

stop_collector
kill "$EVENTS_PID" 2>/dev/null || true
kill "$K6_FAULT_PID" 2>/dev/null || true
wait "$K6_FAULT_PID" 2>/dev/null || true

# ===== ÉTAPE 5 : repatch aux valeurs d'origine =====
echo "[$(date +%H:%M:%S)] repatch -> requests=$ORIG_REQ limits=$ORIG_LIM"
if ! kubectl_retry patch deployment "$DEPLOY" -n "$NS" --type='json' -p="[
  {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/requests/memory\",\"value\":\"$ORIG_REQ\"},
  {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/limits/memory\",\"value\":\"$ORIG_LIM\"}
]"; then
  echo "############################################################"
  echo "# [ERREUR CRITIQUE] Le repatch final a échoué malgré les"
  echo "# tentatives -- $DEPLOY tourne probablement TOUJOURS avec la"
  echo "# mémoire réduite ($FAULT_REQ/$FAULT_LIM). Vérifie et corrige"
  echo "# manuellement AVANT de relancer un autre run :"
  echo "#   kubectl patch deployment $DEPLOY -n $NS --type=json -p='["
  echo "#     {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/requests/memory\",\"value\":\"$ORIG_REQ\"},"
  echo "#     {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/limits/memory\",\"value\":\"$ORIG_LIM\"}"
  echo "#   ]'"
  echo "############################################################"
  exit 1
fi
wait_pod_running

start_collector "recovery" "none"
sleep $((RECOVERY_MIN * 60))
stop_collector

# ===== ÉTAPE 6 : labelliser time_to_failure_sec pour ce run =====
if [ -n "$OOM_TIME" ]; then
  python3 label_ttf.py "$OUTPUT_CSV" "$OOM_TIME" "$POD_NAME" "$RUN_ID"
fi

echo "[$(date +%H:%M:%S)] Run $RUN_ID terminé."
