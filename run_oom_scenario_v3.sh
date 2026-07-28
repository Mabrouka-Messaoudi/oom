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
K6_FAULT="k6_oom_ramp.js"
POD_LABEL="app=product-service"

MAX_WAIT_OOM_MIN=15
POLL_SEC=10

# ===== DuckDNS (optionnel, activé par défaut) =====
# k6_oom_ramp.js cible nexshop-mab.duckdns.org par défaut -- si le cluster a été
# redimensionné entre deux runs, l'IP externe du nœud change mais DuckDNS peut
# encore pointer vers l'ancienne. Vécu en pratique : ~11 minutes d'un run de 24min
# perdues en "dial: i/o timeout" avant de s'en rendre compte. On met à jour
# systématiquement AVANT que k6 ne démarre, plutôt que de compter sur d'y penser
# manuellement à chaque fois.
DUCKDNS_UPDATE="${DUCKDNS_UPDATE:-1}"
DUCKDNS_DOMAIN="${DUCKDNS_DOMAIN:-nexshop-mab}"
DUCKDNS_TOKEN="${DUCKDNS_TOKEN:-743cbc45-8eb5-4c89-9848-fea68cbb0660}"

K6_BASE_URL=""
K6_KEYCLOAK_URL=""

update_duckdns() {
  # DuckDNS mis à jour quand même pour un accès humain pratique (navigateur, curl
  # manuel) -- mais k6 lui-même NE DÉPEND PLUS de cette résolution DNS du tout
  # (voir discover_k6_targets plus bas). Vécu en pratique : même après une mise à
  # jour DuckDNS confirmée par nslookup, les VUs k6 ont continué à viser
  # l'ancienne IP pendant plusieurs minutes -- cache DNS réparti sur plusieurs
  # résolveurs intermédiaires (Cloud Shell, metadata GCP, DuckDNS lui-même), dont
  # le délai de propagation réel est hors de notre contrôle.
  if [ "$DUCKDNS_UPDATE" != "1" ]; then
    echo "[$(date +%H:%M:%S)] Mise à jour DuckDNS désactivée (DUCKDNS_UPDATE=0)."
    return 0
  fi
  local node_ip response
  node_ip=$(kubectl_retry get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')
  if [ -z "$node_ip" ]; then
    echo "[$(date +%H:%M:%S)] [WARN] Impossible de récupérer une IP de nœud -- DuckDNS non mis à jour." >&2
    return 0
  fi
  response=$(curl -sk "https://www.duckdns.org/update?domains=${DUCKDNS_DOMAIN}&token=${DUCKDNS_TOKEN}&ip=${node_ip}")
  if [ "$response" = "OK" ]; then
    echo "[$(date +%H:%M:%S)] DuckDNS mis à jour: ${DUCKDNS_DOMAIN}.duckdns.org -> $node_ip"
  else
    echo "[$(date +%H:%M:%S)] [WARN] Échec de la mise à jour DuckDNS (réponse: $response) -- sans conséquence pour k6 (voir discover_k6_targets)." >&2
  fi
}

# discover_k6_targets : découvre l'IP d'un nœud + les NodePort réels d'api-gateway
# et keycloak, pour que k6 tape directement dessus via -e BASE_URL=.../-e
# KEYCLOAK_URL=... -- aucune résolution DNS en jeu pendant le run, donc plus aucun
# risque de cache DNS périmé côté k6, quel que soit le délai de propagation
# DuckDNS. Échec silencieux et non bloquant : si la découverte rate, k6 retombe
# sur son URL DuckDNS par défaut (comportement d'avant, pas pire qu'avant).
discover_k6_targets() {
  local node_ip gateway_port keycloak_port
  node_ip=$(kubectl_retry get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')
  if [ -z "$node_ip" ]; then
    echo "[$(date +%H:%M:%S)] [WARN] Pas d'IP de nœud -- k6 utilisera son URL DuckDNS par défaut." >&2
    return 0
  fi
  gateway_port=$(kubectl_retry get svc -n "$NS" api-gateway -o jsonpath='{.spec.ports[?(@.port==9000)].nodePort}')
  keycloak_port=$(kubectl_retry get svc -n "$NS" keycloak -o jsonpath='{.spec.ports[?(@.port==8080)].nodePort}')
  if [ -z "$gateway_port" ] || [ -z "$keycloak_port" ]; then
    echo "[$(date +%H:%M:%S)] [WARN] NodePort api-gateway/keycloak introuvable -- k6 utilisera son URL DuckDNS par défaut." >&2
    return 0
  fi
  K6_BASE_URL="http://${node_ip}:${gateway_port}"
  K6_KEYCLOAK_URL="http://${node_ip}:${keycloak_port}"
  echo "[$(date +%H:%M:%S)] k6 ciblera directement (sans DNS) : BASE_URL=$K6_BASE_URL  KEYCLOAK_URL=$K6_KEYCLOAK_URL"
}

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

update_duckdns
discover_k6_targets

# ===== ÉTAPE 0 : capturer les ressources actuelles =====
ORIG_REQ=$(kubectl_retry get deployment "$DEPLOY" -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].resources.requests.memory}')
ORIG_LIM=$(kubectl_retry get deployment "$DEPLOY" -n "$NS" -o jsonpath='{.spec.template.spec.containers[0].resources.limits.memory}')
echo "[$(date +%H:%M:%S)] ressources initiales: requests=$ORIG_REQ limits=$ORIG_LIM"

POD_NAME=$(get_pod_name)
BASE_RESTARTS=$(get_restart_count "$POD_NAME")

# ===== pre_fault et recovery retirés (à la demande) : le run démarre directement en
# during_fault, sans phase de baseline calme avant, et se termine dès le repatch,
# sans phase d'observation après. Voir la discussion sur le comportement attendu de
# la pipeline dans ce mode -- en résumé : aucun changement technique pour M1/M2/M3
# (modèles pré-entraînés, sans état persistant entre phases), et M4 "chauffe" de
# toute façon pendant ~2.5min à chaque during_fault (son historique est indexé par
# NOM DE POD EXACT, qui change à chaque patch mémoire -- pre_fault ou pas ne change
# rien à ce délai). Ce qui se perd : la démonstration visuelle "calme avant/après la
# panne" pour le jury, et ~2min de temps de run économisées (l'ancien BASELINE_MIN+RECOVERY_MIN).

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
K6_ENV_ARGS=()
[ -n "$K6_BASE_URL" ] && K6_ENV_ARGS+=(-e "BASE_URL=$K6_BASE_URL")
[ -n "$K6_KEYCLOAK_URL" ] && K6_ENV_ARGS+=(-e "KEYCLOAK_URL=$K6_KEYCLOAK_URL")
k6 run "${K6_ENV_ARGS[@]}" "$K6_FAULT" > "k6_fault_${RUN_ID}.log" 2>&1 &
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

# ===== ÉTAPE 6 : labelliser time_to_failure_sec pour ce run =====
if [ -n "$OOM_TIME" ]; then
  python3 label_ttf.py "$OUTPUT_CSV" "$OOM_TIME" "$POD_NAME" "$RUN_ID"
fi

echo "[$(date +%H:%M:%S)] Run $RUN_ID terminé."
