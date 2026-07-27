#!/usr/bin/env python3
"""
verify_metrics.py — Vérifie, un par un, que chaque source de données utilisée par
collector_final.py est vraiment joignable ET renvoie de vraies valeurs sur CE
cluster, avant de lancer un vrai scénario de panne.

Réutilise DIRECTEMENT les dictionnaires de requêtes de collector_final.py
(PROM_QUERIES, PROM_NODE_QUERIES, LOKI_QUERIES) -- aucune requête dupliquée à la
main, donc ce script ne peut jamais dériver silencieusement de ce que le vrai
collecteur exécute.

Deux niveaux de vérification, pour distinguer les causes :
  1. Le endpoint répond-il DU TOUT (santé de base) ?
  2. Chaque requête PromQL/LogQL renvoie-t-elle des séries, ou 0 (métrique
     structurellement absente -- exporter non configuré, service non scrapé, etc.) ?

Usage :
    python3 verify_metrics.py
    python3 verify_metrics.py --prom-url http://<IP>:30090 --loki-url http://<IP>:30003 \
                               --tempo-url http://<IP>:30622 --platform-url http://<IP>:30080

Si tu obtiens "Connection refused" ou "timed out" sur Prometheus/Loki/Tempo et que
tu lances ce script depuis ton poste (pas depuis un nœud du cluster), c'est presque
toujours parce que ces URL pointent vers "localhost" sans tunnel actif -- soit tu
passes l'IP externe d'un nœud (comme pour --platform-url), soit tu ouvres d'abord
un kubectl port-forward vers chaque service.
"""

import argparse
import importlib.util
import sys
import urllib.request
from pathlib import Path


def load_collector_module(path: str):
    """Importe collector_final.py comme un module, pour réutiliser EXACTEMENT ses
    dictionnaires de requêtes -- pas une copie qui pourrait diverger avec le temps."""
    spec = importlib.util.spec_from_file_location("collector_final", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_basic_health(name: str, url: str, timeout: int = 5) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            ok = 200 <= resp.status < 300
            print(f"  [{'OK' if ok else 'WARN'}] {name} ({url}) -> HTTP {resp.status}")
            return ok
    except Exception as e:
        print(f"  [ECHEC] {name} ({url}) -> {e}")
        return False


def check_prom_queries(cf, queries: dict, label: str):
    print(f"\n--- Requêtes Prometheus ({label}) ---")
    results = {}
    for name, promql in queries.items():
        data = cf.prom_query(promql, label="pod" if label == "pods" else "instance")
        n = len(data)
        status = "OK" if n > 0 else "VIDE"
        print(f"  [{status:4s}] {name:32s} -> {n} série(s){'' if n else '  <-- rien remonté, voir hypothèses ci-dessous'}")
        results[name] = n
    return results


def check_loki_queries(cf):
    print("\n--- Requêtes Loki ---")
    results = {}
    for name, logql in cf.LOKI_QUERIES.items():
        data = cf.loki_query(logql)
        n = len(data)
        status = "OK" if n > 0 else "VIDE"
        print(f"  [{status:4s}] {name:32s} -> {n} pod(s){'' if n else '  <-- aucun log ne matche sur les 5 dernières minutes'}")
        results[name] = n
    return results


def main():
    p = argparse.ArgumentParser(description="Diagnostic des sources de données de collector_final.py")
    p.add_argument("--collector-path", default="collector_final.py")
    p.add_argument("--prom-url", default=None, help="défaut: valeur dans collector_final.py (localhost:30090)")
    p.add_argument("--loki-url", default=None, help="défaut: valeur dans collector_final.py (localhost:30003)")
    p.add_argument("--tempo-url", default=None, help="défaut: valeur dans collector_final.py (localhost:30622)")
    p.add_argument("--platform-url", default=None, help="ex: http://<IP-noeud>:30080 -- pour tester /ingest/tick")
    args = p.parse_args()

    if not Path(args.collector_path).exists():
        print(f"ERREUR : {args.collector_path} introuvable. Lance ce script depuis le même dossier que collector_final.py,")
        print("ou précise --collector-path.")
        sys.exit(1)

    cf = load_collector_module(args.collector_path)

    if args.prom_url:
        cf.PROM_URL = args.prom_url
    if args.loki_url:
        cf.LOKI_URL = args.loki_url
    if args.tempo_url:
        cf.TEMPO_URL = args.tempo_url

    print("=" * 70)
    print("ÉTAPE 1 -- Les endpoints répondent-ils DU TOUT ?")
    print("=" * 70)
    prom_ok = check_basic_health("Prometheus", f"{cf.PROM_URL}/-/healthy")
    loki_ok = check_basic_health("Loki", f"{cf.LOKI_URL}/ready")
    tempo_ok = check_basic_health("Tempo", f"{cf.TEMPO_URL}/ready")
    if args.platform_url:
        check_basic_health("Plateforme AIOps", f"{args.platform_url}/health")

    if not (prom_ok or loki_ok or tempo_ok):
        print("\n[STOP] Aucun endpoint n'est joignable -- inutile de tester les requêtes individuelles.")
        print("Hypothèses les plus probables (dans l'ordre) :")
        print("  1. Ces URL utilisent 'localhost' (valeur par défaut de collector_final.py) --")
        print("     lancé depuis ton poste, ça ne pointe vers rien sans tunnel actif.")
        print("     -> soit un kubectl port-forward par service, soit relance ce script avec")
        print("        --prom-url http://<IP-externe-d-un-noeud>:30090 (etc.), comme on l'a fait")
        print("        pour la plateforme AIOps avec 34.155.37.20:30080.")
        print("  2. Le pare-feu GCP n'autorise pas ces NodePort précis depuis l'extérieur.")
        print("  3. Le cluster est arrêté (num-nodes=0) ou les pods Prometheus/Loki/Tempo sont")
        print("     encore Pending.")
        sys.exit(1)

    print("\n" + "=" * 70)
    print("ÉTAPE 2 -- Chaque métrique renvoie-t-elle vraiment des données ?")
    print("=" * 70)

    prom_pod_results = check_prom_queries(cf, cf.PROM_QUERIES, "pods") if prom_ok else {}
    prom_node_results = check_prom_queries(cf, cf.PROM_NODE_QUERIES, "nœuds") if prom_ok else {}
    loki_results = check_loki_queries(cf) if loki_ok else {}

    print("\n" + "=" * 70)
    print("RÉSUMÉ")
    print("=" * 70)
    all_results = {**prom_pod_results, **prom_node_results, **loki_results}
    empty = [name for name, n in all_results.items() if n == 0]
    ok = [name for name, n in all_results.items() if n > 0]
    print(f"Métriques avec données : {len(ok)}/{len(all_results)}")
    if empty:
        print(f"Métriques VIDES : {', '.join(empty)}")
        print("\nCes colonnes seront '' (vide) dans le CSV -- déjà géré côté plateforme (statut")
        print("'missing_data' pour M4, imputation à 0 pour M1/M2 -- voir m4_threshold.py/m1_anomaly.py),")
        print("donc pas bloquant en soi, mais vérifie si c'est attendu (ex: métrique JVM sur un")
        print("service Node.js, normal) ou un vrai souci de config Prometheus (exporter absent,")
        print("ServiceMonitor mal labellisé -- cf. le piège 'release:kps' déjà rencontré sur ce projet).")
    else:
        print("Toutes les métriques testées renvoient des données. ✅")


if __name__ == "__main__":
    main()
