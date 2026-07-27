#!/usr/bin/env python3

import sys
import csv


def main():
    if len(sys.argv) < 5:
        print("usage: label_ttf.py <csv_path> <oom_time> <pod_name> <run_id>")
        sys.exit(1)

    csv_path, oom_time_str, pod_name, run_id = (
        sys.argv[1],
        sys.argv[2],
        sys.argv[3],
        sys.argv[4],
    )

    oom_time = int(oom_time_str)

    # Lecture du CSV
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    updated = 0

    for row in rows:

        # On ne labellise que les lignes :
        # - du bon run
        # - pendant la panne
        # - du POD EXACT qui a réellement crashé (pas juste "un service qui
        #   commence par product-service") -- vécu en pratique : deux pods
        #   product-service ont coexisté brièvement pendant un RollingUpdate,
        #   un simple startswith() aurait aussi labellisé les lignes de l'ancien
        #   pod, qui n'a jamais été proche d'un vrai crash.
        if (
            row.get("run_id") == run_id
            and row.get("fault_phase") == "during_fault"
            and row.get("service_name") == pod_name
        ):

            ts = int(row["timestamp"])
            ttf = oom_time - ts

            # uniquement les mesures avant le crash
            if 0 <= ttf <= 3600:
                row["time_to_failure_sec"] = ttf
                updated += 1

    # Réécriture du CSV
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(
        f"{updated} lignes labellisées "
        f"(run_id={run_id}, OOM à t={oom_time})"
    )


if __name__ == "__main__":
    main()
