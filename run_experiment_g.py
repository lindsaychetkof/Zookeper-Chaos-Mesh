#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_experiment_g.py
===================
Standalone runner for Experiment G:
  5-node ZooKeeper — Leader-on-Minority Partition (3 runs)

What this does
--------------
Deploys the 5-node ZooKeeper ensemble (zookeeper5.yaml) if not already up,
runs 3 repetitions of the leader-minority partition experiment, then tears
down the 5-node ensemble.

All data is written to files that ALREADY EXIST in the repo (logs/, chaos_yamls/,
results_v2_live.csv).  The experiment appends to results_v2_live.csv and the
master log; per-run logs are NEW files that have never been written before.

Files written (never overwritten - safe to run once):
  logs/log_v2_leader_minority_partition_run1.txt
  logs/log_v2_leader_minority_partition_run2.txt
  logs/log_v2_leader_minority_partition_run3.txt
  logs/log_v2_leader_minority_stdout.txt        <- tee of all stdout
  chaos_yamls/chaos_v2_leader_minority_run{1-3}_dynamic.yaml
  logs/results_v2_live.csv                      <- appended, not overwritten

Prerequisites
-------------
  1. Docker Desktop running
  2. minikube running  (minikube start --memory=4096 --cpus=4)
  3. Chaos Mesh installed  (helm install chaos-mesh ...)
  4. Port-forward to zookeeper5 service in a separate terminal:
       kubectl port-forward svc/zookeeper5 2181:2181
  5. workload.py running in a separate terminal:
       python workload.py
  6. pip install kazoo  (for workload.py; not needed for this script itself)

Run
---
  python run_experiment_g.py
"""

import csv
import os
import sys
import time
import subprocess
from datetime import datetime

# ---------------------------------------------------------------------------
# Redirect stdout to both terminal AND a log file
# ---------------------------------------------------------------------------
STDOUT_LOG = os.path.join("logs", "log_v2_leader_minority_stdout.txt")
os.makedirs("logs", exist_ok=True)
os.makedirs("chaos_yamls", exist_ok=True)

class Tee:
    """Write to both stdout and a file."""
    def __init__(self, path):
        self._f = open(path, "w", buffering=1, encoding="utf-8")
        self._orig = sys.stdout
    def write(self, data):
        self._orig.write(data)
        self._f.write(data)
    def flush(self):
        self._orig.flush()
        self._f.flush()
    def reconfigure(self, **kwargs):
        # Forward reconfigure calls (Python 3.7+)
        pass

sys.stdout = Tee(STDOUT_LOG)

# ---------------------------------------------------------------------------
# Now import the experiment module (after Tee is installed so all prints go
# to the log file too)
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(__file__))
import run_advanced_experiments as m

# ---------------------------------------------------------------------------
# Guard: refuse to overwrite existing per-run logs
# ---------------------------------------------------------------------------
def _check_no_overwrite():
    for n in range(1, 4):
        path = os.path.join(m.LOG_DIR, f"log_v2_leader_minority_partition_run{n}.txt")
        if os.path.exists(path):
            print(f"[ABORT] Log file already exists: {path}")
            print("        Delete it first if you want to re-run.")
            sys.exit(1)

# ---------------------------------------------------------------------------
# Ensure results CSV has a header (append-safe)
# ---------------------------------------------------------------------------
def _ensure_csv():
    if not os.path.exists(m.RESULTS_LIVE) or os.path.getsize(m.RESULTS_LIVE) == 0:
        with open(m.RESULTS_LIVE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=m.RESULTS_LIVE_FIELDS).writeheader()
        print(f"[SETUP] Created fresh {m.RESULTS_LIVE}")
    else:
        print(f"[SETUP] Appending to existing {m.RESULTS_LIVE}")

# ---------------------------------------------------------------------------
# Ensure master log exists (append-safe)
# ---------------------------------------------------------------------------
def _ensure_master_log():
    with open(m.MASTER_LOG, "a", buffering=1) as mf:
        mf.write(f"\n{'=' * 70}\n")
        mf.write(f"EXPERIMENT G (leader_minority_partition) started: {m.ts()}\n")
        mf.write("=" * 70 + "\n\n")
    print(f"[SETUP] Master log: {m.MASTER_LOG}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    suite_start = datetime.now()
    print(f"[{m.ts()}] ====================================================")
    print(f"[{m.ts()}]  Experiment G: 5-Node Leader-Minority Partition")
    print(f"[{m.ts()}]  Partition: [leader + 1 follower] (minority 2/5)")
    print(f"[{m.ts()}]    vs [3 followers] (majority 3/5, elects new leader)")
    print(f"[{m.ts()}]  Runs: 3")
    print(f"[{m.ts()}]  Log dir: {m.LOG_DIR}/")
    print(f"[{m.ts()}]  YAML dir: {m.YAML_DIR}/")
    print(f"[{m.ts()}]  Stdout tee: {STDOUT_LOG}")
    print(f"[{m.ts()}] ====================================================")
    print()

    _check_no_overwrite()
    _ensure_csv()
    _ensure_master_log()

    # Deploy 5-node ZooKeeper if not already running
    pods_ok, n_running, _ = m.check_pods_running("app=zookeeper5")
    if pods_ok:
        print(f"[{m.ts()}] zookeeper5 ensemble already running ({n_running}/5 pods).")
        print(f"[{m.ts()}] Waiting 15s for leader election to settle ...")
        time.sleep(15)
    else:
        print(f"[{m.ts()}] zookeeper5 not running (found {n_running}/5 pods). Deploying ...")
        m.deploy_zookeeper5()
        print(f"[{m.ts()}] zookeeper5 deployed. Waiting 30s for stabilisation ...")
        time.sleep(30)

    try:
        for run in range(1, 4):
            print(f"\n[{m.ts()}] ----------------------------------------")
            print(f"[{m.ts()}] --- Experiment G  Run {run}/3 ---")
            print(f"[{m.ts()}] ----------------------------------------")
            m.run_5node_leader_minority_partition(run)

            if run < 3:
                print(f"[{m.ts()}] Run {run} complete. Waiting 90s for cluster stabilisation ...")
                time.sleep(90)

    except KeyboardInterrupt:
        print(f"\n[{m.ts()}] Interrupted by user. Cleaning up chaos resources ...")
        for n in range(1, 4):
            subprocess.run(
                ["kubectl", "delete", "NetworkChaos",
                 f"v2-leader-minority-r{n}", "--ignore-not-found=true", "-n", m.NAMESPACE],
                capture_output=True, timeout=15,
            )
        sys.exit(0)
    except Exception as exc:
        print(f"\n[{m.ts()}] UNHANDLED ERROR: {exc}")
        import traceback
        traceback.print_exc()
        for n in range(1, 4):
            subprocess.run(
                ["kubectl", "delete", "NetworkChaos",
                 f"v2-leader-minority-r{n}", "--ignore-not-found=true", "-n", m.NAMESPACE],
                capture_output=True, timeout=15,
            )
        raise
    finally:
        print(f"\n[{m.ts()}] Tearing down zookeeper5 ensemble ...")
        m.teardown_zookeeper5()

    # Summary
    suite_end = datetime.now()
    elapsed   = round((suite_end - suite_start).total_seconds(), 1)
    print()
    print(f"[{m.ts()}] ====================================================")
    print(f"[{m.ts()}]  EXPERIMENT G COMPLETE  ({elapsed}s total)")
    print(f"[{m.ts()}] ====================================================")
    print()
    print("Files written:")
    for n in range(1, 4):
        p = os.path.join(m.LOG_DIR, f"log_v2_leader_minority_partition_run{n}.txt")
        size = os.path.getsize(p) if os.path.exists(p) else 0
        print(f"  {p}  ({size:,} bytes)")
    for n in range(1, 4):
        p = os.path.join(m.YAML_DIR, f"chaos_v2_leader_minority_run{n}_dynamic.yaml")
        size = os.path.getsize(p) if os.path.exists(p) else 0
        print(f"  {p}  ({size:,} bytes)")
    print(f"  {m.RESULTS_LIVE}  (appended)")
    print(f"  {m.MASTER_LOG}  (appended)")
    print(f"  {STDOUT_LOG}  (full stdout tee)")


if __name__ == "__main__":
    main()
