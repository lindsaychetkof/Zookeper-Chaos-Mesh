#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_advanced_experiments.py
============================
ZooKeeper Advanced Chaos Engineering Suite - v2

EXPERIMENTS (3 runs each unless noted):
  A: Kill Leader           - POD killed via PodChaos; ZAB re-election timed
  B: Network Partition     - Leader isolated from both followers via NetworkChaos
  C: Cascading Failure     - Follower killed, then leader killed 10s later (no quorum)
  D: Three-Way Isolation   - All 3 nodes simultaneously isolated (3 NetworkChaos objects)
  E: 5-Node 3+2 Partition  - 5-node ensemble partitioned into 3-node majority + 2-node minority
  F: 5-Node 2+2+1 Partition- 5-node ensemble split into 3 groups; no group has majority

IMPROVEMENTS OVER v1 (run_final_experiments.py):
  - Polling interval: 1 second (was 5 seconds) using parallel ThreadPoolExecutor
  - Runs A/B/C repeated 3 times with finer granularity for cross-validation
  - Experiments D/E/F are entirely new, requiring new Chaos Mesh objects and a
    5-node ZooKeeper StatefulSet (deployed automatically, cleaned up after)
  - Every log file begins with a TEST PURPOSE block explaining what is being
    tested and why it is academically significant for distributed systems study

OUTPUT FILES (NEVER overwrites v1 files):
  log_v2_kill_leader_run{1-3}.txt
  log_v2_network_partition_run{1-3}.txt
  log_v2_cascading_failure_run{1-3}.txt
  log_v2_threeway_isolation_run{1-3}.txt
  log_v2_5node_majority_partition_run{1-3}.txt
  log_v2_5node_threeway_partition_run{1-3}.txt
  log_v2_all_experiments_master.txt
  results_v2_live.csv
  chaos_v2_*.yaml  (dynamic per-run, auto-generated)
"""

import concurrent.futures
import csv
import os
import subprocess
import sys
import time
from datetime import datetime

# Force UTF-8 on Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ZK3_PODS  = ["zookeeper-0", "zookeeper-1", "zookeeper-2"]
ZK5_PODS  = ["zookeeper5-0", "zookeeper5-1", "zookeeper5-2",
             "zookeeper5-3", "zookeeper5-4"]
ZK_BIN    = "/apache-zookeeper-3.9.3-bin/bin/zkServer.sh"
NAMESPACE = "default"

# Output directories (created at startup by main())
LOG_DIR  = "logs"
YAML_DIR = "chaos_yamls"

MASTER_LOG   = os.path.join(LOG_DIR, "log_v2_all_experiments_master.txt")
RESULTS_LIVE = os.path.join(LOG_DIR, "results_v2_live.csv")
RESULTS_LIVE_FIELDS = [
    "experiment", "run", "ensemble", "leader_before", "leader_after",
    "leadership_changed", "injection_time", "recovery_time",
    "recovery_seconds", "quorum_lost", "notes",
]

# Test purpose descriptions - logged at the top of every run log
TEST_PURPOSES = {
    "kill_leader": """\
TEST PURPOSE
------------
What: A ZooKeeper leader pod is forcibly killed via Chaos Mesh PodChaos.
Why:  This directly tests ZAB (ZooKeeper Atomic Broadcast) leader election.
      When the leader dies the two remaining followers must detect the loss
      (via heartbeat timeout) and elect a new leader among themselves.
      With 3 nodes, 2/3 constitutes majority; so 2 surviving nodes CAN
      elect a new leader. We measure time-to-election at 1-second resolution.
Academic significance:
  - Validates ZAB quorum requirement (>50% majority needed)
  - Measures leader election latency under pod failure
  - Tests Kubernetes StatefulSet self-healing alongside ZK session management
  - Expected: new leader elected within ~2-5 seconds; full recovery ~20-90s
""",
    "network_partition": """\
TEST PURPOSE
------------
What: A NetworkChaos partition isolates the ZooKeeper leader from both followers.
      The leader can no longer communicate with any other ensemble member.
Why:  This creates a classic split-brain scenario. The isolated leader has
      only 1/3 votes (minority). The two followers have 2/3 (majority) and
      MUST elect a new leader among themselves. The original leader should
      step down to LOOKING state once it cannot reach a quorum.
Academic significance:
  - Tests ZAB's protection against split-brain (two simultaneous leaders)
  - Measures whether Chaos Mesh duration field auto-expires chaos objects
  - Captures the window where both sides may briefly report "leader"
  - Expected: new leader elected on follower side within ~5-15 seconds
""",
    "cascading_failure": """\
TEST PURPOSE
------------
What: A follower is killed first; 10 seconds later the leader is also killed.
      Only 1 of 3 nodes remains alive, which cannot reach quorum (1/3 < 50%).
Why:  This tests ZooKeeper's behavior when quorum is permanently lost.
      The single surviving node must recognize it cannot form a quorum and
      refuse to accept writes or serve as leader/follower.
Academic significance:
  - Validates quorum loss detection (surviving pod should go to LOOKING/error)
  - Tests Kubernetes pod restart speed vs ZK session timeout interaction
  - Cascading failures are common in real outage scenarios (first one node,
    then operators/runbooks kill more, losing quorum)
  - Note: PodChaos triggers instant K8s restart; pods may rejoin before ZK
    detects quorum loss if restart is faster than ZK session timeout (~30s)
""",
    "threeway_isolation": """\
TEST PURPOSE
------------
What: Three simultaneous NetworkChaos objects each isolate one of the 3
      ZooKeeper nodes from the other two (1+1+1 partition).
Why:  With ALL nodes isolated, no node has any peers visible. No node can
      reach the 2/3 majority needed to become or remain leader. The entire
      ensemble should be in LOOKING/error state simultaneously.
Academic significance:
  - Extreme case: total network fragmentation with no possible quorum
  - Unlike a simple leader partition (B), here EVERY node is isolated
  - Tests whether ZooKeeper correctly refuses to serve ANY requests when
    the entire cluster is partitioned (no split-brain possible because no
    group can form quorum)
  - With only 1 node visible to itself, majority requires 2/3 - impossible
  - Expected: all 3 nodes go to not_running/error/looking; writes rejected
""",
    "5node_majority_partition": """\
TEST PURPOSE
------------
What: A 5-node ZooKeeper ensemble is partitioned into two groups:
      Group A: {zk5-0, zk5-1, zk5-2} (3 nodes - has 3/5 = 60% majority)
      Group B: {zk5-3, zk5-4}         (2 nodes - has 2/5 = 40% minority)
Why:  With 5 nodes, it IS possible to create two groups where one group
      has strict majority and the other does not. This is impossible with
      3 nodes (you cannot split 3 into two groups both having >50%).
Academic significance:
  - Tests asymmetric partition: one side maintains service, other loses it
  - Group A (3 nodes) should elect/retain a leader and continue operating
  - Group B (2 nodes) cannot reach quorum and must refuse writes/reads
  - This is the "correct" split-brain resolution: ZooKeeper correctly
    identifies which side has majority and only that side serves clients
  - Real-world relevance: datacenter partition where main DC has majority
""",
    "5node_threeway_partition": """\
TEST PURPOSE
------------
What: A 5-node ZooKeeper ensemble is split into THREE groups using two
      simultaneous NetworkChaos objects:
      Group A: {zk5-0, zk5-1}     (2 nodes - 2/5, minority)
      Group B: {zk5-2, zk5-3}     (2 nodes - 2/5, minority)
      Group C: {zk5-4}             (1 node  - 1/5, minority)
      NC1 partitions {zk5-0,1} from {zk5-2,3,4}
      NC2 partitions {zk5-2,3} from {zk5-0,1,4}
      Result: all three groups are mutually isolated with no group >= 3/5.
Why:  Three-way partition ensures NO group has 3/5 majority. ZooKeeper
      requires strictly more than half the total ensemble (not just visible
      peers) to form quorum. With total ensemble = 5, quorum threshold = 3.
      No group reaches 3, so the entire cluster loses write availability.
Academic significance:
  - Maximum fragmentation test: even the largest group (2 nodes) cannot
    form quorum without reaching 3/5
  - Contrasts with Experiment E where one group retained majority
  - Tests ZooKeeper's global quorum requirement (not just local majority
    among visible peers)
  - All 5 nodes expected to go to LOOKING/not_running within ~30 seconds
  - After healing: tests how 5 nodes re-form quorum when partitions lift
""",
    "leader_minority_partition": """\
TEST PURPOSE
------------
What: A 5-node ZooKeeper ensemble is partitioned into two groups where the
      CURRENT LEADER is deliberately placed on the MINORITY (2-node) side:
        Minority (2/5): [current_leader + one_follower] -- cannot form quorum
        Majority (3/5): [3 remaining followers]          -- can elect a new leader
      Groups are assigned dynamically at preflight based on who holds the lease.
Why:  This directly tests ZAB's response when the active leader loses quorum.
      The minority-side leader must detect it can no longer reach a majority
      of ensemble members and step down to LOOKING state.  The majority side
      must elect a new leader from its 3 nodes (3/5 = 60% > 50% threshold).
      Unlike Experiment E (which does not guarantee which side gets the leader),
      this experiment controls the partition topology so the outcome is always:
        old leader stranded on minority  →  new leader elected on majority.
Timeline markers captured:
  A = partition injected
  B = new leader detected on majority side (1-second zkServer.sh polling)
  C = write availability confirmed on majority (inferred: majority has leader)
  D = quorum loss confirmed on minority (all minority pods non-leader/follower)
  E = partition healed (chaos resource deleted)
  F = full recovery (all 5 pods Running + exactly 1 leader)
Academic significance:
  - Contrasts pod-kill (Experiment A) with network isolation: here the old
    leader is alive but unreachable, so both sides initially believe they may
    be leader -- the classic split-brain window is observable.
  - Measures B-A: time for majority to elect a new leader.
  - Measures D-A: time for minority to detect quorum loss and step down.
  - Validates ZAB quorum rule: 2-node minority with the old leader cannot
    continue serving even though the leader pod is healthy.
  - Expected: new majority leader within ~5-15s; minority quorum loss ~10-30s.
""",
}


# ---------------------------------------------------------------------------
# Timestamp helper
# ---------------------------------------------------------------------------
def ts(dt=None) -> str:
    if dt is None:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


# ---------------------------------------------------------------------------
# Dual Logger
# ---------------------------------------------------------------------------
class Logger:
    """Writes timestamped lines to run-specific log + master log + stdout."""

    def __init__(self, run_log_path: str):
        self._run_fh = open(run_log_path, "w", buffering=1)
        self._path   = run_log_path

    def _append_master(self, line: str):
        with open(MASTER_LOG, "a", buffering=1) as mf:
            mf.write(line + "\n")

    def log(self, msg: str):
        line = f"[{ts()}] {msg}"
        self._run_fh.write(line + "\n")
        self._append_master(line)
        print(line, flush=True)

    def raw(self, msg: str):
        """Write without timestamp (for purpose headers and structured blocks)."""
        self._run_fh.write(msg + "\n")
        self._append_master(msg)
        print(msg, flush=True)

    def close(self):
        self._run_fh.close()


# ---------------------------------------------------------------------------
# ZooKeeper status (parallel polling with ThreadPoolExecutor)
# ---------------------------------------------------------------------------
def get_zk_status(pod: str) -> str:
    """Query zkServer.sh status on one pod. Returns leader/follower/not_running/error/timeout."""
    try:
        r = subprocess.run(
            ["kubectl", "exec", pod, "--", ZK_BIN, "status"],
            capture_output=True, text=True, timeout=8,
        )
        out = (r.stdout + r.stderr).lower()
        if "leader"      in out: return "leader"
        if "follower"    in out: return "follower"
        if "not running" in out: return "not_running"
        if "error"       in out: return "error"
        return "unknown"
    except subprocess.TimeoutExpired:
        return "timeout"
    except Exception as e:
        return f"err:{e}"


def get_all_statuses_parallel(pods: list) -> dict:
    """Query all pods simultaneously using threads. Returns {pod: status}."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pods)) as ex:
        futures = {ex.submit(get_zk_status, pod): pod for pod in pods}
        results = {}
        for fut in concurrent.futures.as_completed(futures):
            pod = futures[fut]
            results[pod] = fut.result()
    # Return in original pod order
    return {pod: results[pod] for pod in pods}


def resolve_roles(statuses: dict):
    """Returns (leader_pod_or_None, [follower_pods])."""
    leader    = next((p for p, s in statuses.items() if s == "leader"), None)
    followers = [p for p, s in statuses.items() if s == "follower"]
    return leader, followers


def check_pods_running(label: str = "app=zookeeper") -> tuple:
    """
    Returns (all_running: bool, count_running: int, raw_output: str).
    label: kubectl label selector (app=zookeeper or app=zookeeper5)
    """
    try:
        r = subprocess.run(
            ["kubectl", "get", "pods", "-l", label, "--no-headers"],
            capture_output=True, text=True, timeout=20,
        )
        lines   = [l for l in r.stdout.strip().splitlines() if l.strip()]
        running = [l for l in lines if "Running" in l and "1/1" in l]
        return len(running) == len(lines) and len(running) > 0, len(running), r.stdout.strip()
    except Exception as e:
        return False, 0, str(e)


def poll_statuses(logger: Logger, pods: list, label: str = "") -> dict:
    """Parallel poll all pods, log the result, return statuses dict."""
    statuses   = get_all_statuses_parallel(pods)
    status_str = " | ".join(f"{p}:{statuses[p]}" for p in pods)
    prefix     = f"POLL{' ' + label if label else ''}"
    logger.log(f"{prefix}: {status_str}")
    return statuses


# ---------------------------------------------------------------------------
# Preflight check (3-node ensemble)
# ---------------------------------------------------------------------------
def preflight_3node(logger: Logger) -> tuple:
    """
    Checks:
      1. All 3 pods 1/1 Running
      2. Exactly 1 leader + 2 followers
    Retries up to 6 times with 30s gaps.
    Returns (leader, follower_1, follower_2) or sys.exit(1).
    """
    for attempt in range(1, 7):
        logger.log(f"PREFLIGHT attempt {attempt}/6 at {ts()}")

        pods_ok, n_running, pods_text = check_pods_running("app=zookeeper")
        if not pods_ok:
            logger.log(f"  Check 1 FAIL: only {n_running}/3 pods 1/1 Running")
            logger.log(f"  kubectl output:\n{pods_text}")
        else:
            logger.log("  Check 1 PASS: all 3 pods are 1/1 Running")
            statuses = get_all_statuses_parallel(ZK3_PODS)
            for pod, s in statuses.items():
                logger.log(f"    {pod}: {s}")
            leader, followers = resolve_roles(statuses)

            if leader and len(followers) == 2:
                logger.log(f"  Check 2 PASS: leader={leader}, followers={followers}")
                logger.log(f"PREFLIGHT PASSED [{ts()}]")
                return leader, followers[0], followers[1]
            else:
                logger.log(
                    f"  Check 2 FAIL: leader={leader}, followers={followers} "
                    f"(need exactly 1 leader + 2 followers)"
                )

        if attempt < 6:
            logger.log(f"  Waiting 30 s before retry {attempt + 1}/6 ...")
            time.sleep(30)

    logger.log(f"PREFLIGHT FAILED [{ts()}] after 6 attempts. Stopping.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Preflight check (5-node ensemble)
# ---------------------------------------------------------------------------
def preflight_5node(logger: Logger) -> tuple:
    """
    Checks:
      1. All 5 zookeeper5-* pods 1/1 Running
      2. Exactly 1 leader + 4 followers
    Retries up to 6 times with 30s gaps.
    Returns (leader, [4 followers]) or sys.exit(1).
    """
    for attempt in range(1, 7):
        logger.log(f"PREFLIGHT-5NODE attempt {attempt}/6 at {ts()}")

        pods_ok, n_running, pods_text = check_pods_running("app=zookeeper5")
        if not pods_ok:
            logger.log(f"  Check 1 FAIL: only {n_running}/5 pods 1/1 Running")
            logger.log(f"  kubectl output:\n{pods_text}")
        else:
            logger.log("  Check 1 PASS: all 5 zookeeper5 pods are 1/1 Running")
            statuses = get_all_statuses_parallel(ZK5_PODS)
            for pod, s in statuses.items():
                logger.log(f"    {pod}: {s}")
            leader, followers = resolve_roles(statuses)

            if leader and len(followers) == 4:
                logger.log(f"  Check 2 PASS: leader={leader}, followers={followers}")
                logger.log(f"PREFLIGHT-5NODE PASSED [{ts()}]")
                return leader, followers
            else:
                logger.log(
                    f"  Check 2 FAIL: leader={leader}, followers={followers} "
                    f"(need exactly 1 leader + 4 followers)"
                )

        if attempt < 6:
            logger.log(f"  Waiting 30 s before retry {attempt + 1}/6 ...")
            time.sleep(30)

    logger.log(f"PREFLIGHT-5NODE FAILED [{ts()}] after 6 attempts. Stopping.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# YAML builders
# ---------------------------------------------------------------------------
def build_pod_kill_yaml(resource_name: str, pod_name: str, ns: str = NAMESPACE) -> str:
    return f"""\
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: {resource_name}
  namespace: {ns}
spec:
  action: pod-kill
  mode: one
  gracePeriod: 0
  selector:
    namespaces:
      - {ns}
    expressionSelectors:
      - key: statefulset.kubernetes.io/pod-name
        operator: In
        values:
          - {pod_name}
"""


def build_partition_yaml(resource_name: str,
                         source_pods: list,
                         target_pods: list,
                         ns: str = NAMESPACE,
                         duration: str = "60s",
                         label_key: str = "statefulset.kubernetes.io/pod-name") -> str:
    """
    Creates a NetworkChaos partition object.
    source_pods: list of pod names in the 'source' selector
    target_pods: list of pod names in the 'target' selector
    direction: both (default) - traffic blocked in both directions
    """
    src_lines = "\n".join(f"          - {p}" for p in source_pods)
    tgt_lines = "\n".join(f"          - {p}" for p in target_pods)
    return f"""\
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: {resource_name}
  namespace: {ns}
spec:
  action: partition
  mode: all
  selector:
    namespaces:
      - {ns}
    expressionSelectors:
      - key: {label_key}
        operator: In
        values:
{src_lines}
  direction: both
  target:
    mode: all
    selector:
      namespaces:
        - {ns}
      expressionSelectors:
        - key: {label_key}
          operator: In
          values:
{tgt_lines}
  duration: "{duration}"
"""


def write_yaml(path: str, content: str) -> None:
    with open(path, "w") as f:
        f.write(content)


# ---------------------------------------------------------------------------
# Chaos Mesh apply / delete helpers
# ---------------------------------------------------------------------------
def apply_chaos(yaml_path: str) -> datetime:
    subprocess.run(["kubectl", "apply", "-f", yaml_path], check=True, timeout=30)
    return datetime.now()


def delete_chaos(yaml_path: str) -> datetime:
    subprocess.run(
        ["kubectl", "delete", "-f", yaml_path, "--ignore-not-found=true"],
        check=True, timeout=30,
    )
    return datetime.now()


def delete_chaos_by_name(kind: str, name: str) -> datetime:
    subprocess.run(
        ["kubectl", "delete", kind, name, "--ignore-not-found=true", "-n", NAMESPACE],
        check=True, timeout=30,
    )
    return datetime.now()


def chaos_object_exists(kind: str, name: str) -> bool:
    try:
        r = subprocess.run(
            ["kubectl", "get", kind, name, "--ignore-not-found", "-n", NAMESPACE],
            capture_output=True, text=True, timeout=15,
        )
        return name in r.stdout
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Recovery polling (parallel)
# ---------------------------------------------------------------------------
def wait_full_recovery_3node(logger: Logger,
                              inject_dt: datetime,
                              timeout_s: int = 180) -> tuple:
    """
    Polls every 1s until all 3 pods Running and exactly 1 leader.
    Returns (final_leader, recovery_s_from_inject, recovery_dt).
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout_s:
        pods_ok, _, _ = check_pods_running("app=zookeeper")
        statuses      = get_all_statuses_parallel(ZK3_PODS)
        status_str    = " | ".join(f"{p}:{statuses[p]}" for p in ZK3_PODS)
        leader, followers = resolve_roles(statuses)

        logger.log(f"RECOVERY POLL: pods_running={pods_ok} | {status_str}")

        if pods_ok and leader and len(followers) == 2:
            recovery_dt = datetime.now()
            recovery_s  = round((recovery_dt - inject_dt).total_seconds(), 1)
            logger.log(
                f"RECOVERY CONFIRMED at {ts(recovery_dt)}: "
                f"leader={leader}, recovery_seconds={recovery_s}s (from injection)"
            )
            return leader, recovery_s, recovery_dt

        time.sleep(1)

    elapsed = round(time.monotonic() - start, 1)
    logger.log(f"RECOVERY TIMEOUT after {elapsed}s - cluster did not fully recover")
    return None, elapsed, None


def wait_full_recovery_5node(logger: Logger,
                              inject_dt: datetime,
                              timeout_s: int = 240) -> tuple:
    """
    Polls every 1s until all 5 zookeeper5 pods Running and exactly 1 leader.
    Returns (final_leader, recovery_s_from_inject, recovery_dt).
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout_s:
        pods_ok, n, _ = check_pods_running("app=zookeeper5")
        statuses      = get_all_statuses_parallel(ZK5_PODS)
        status_str    = " | ".join(f"{p}:{statuses[p]}" for p in ZK5_PODS)
        leader, followers = resolve_roles(statuses)

        logger.log(f"RECOVERY POLL: pods_running={pods_ok}({n}/5) | {status_str}")

        if pods_ok and leader and len(followers) == 4:
            recovery_dt = datetime.now()
            recovery_s  = round((recovery_dt - inject_dt).total_seconds(), 1)
            logger.log(
                f"RECOVERY CONFIRMED at {ts(recovery_dt)}: "
                f"leader={leader}, recovery_seconds={recovery_s}s (from injection)"
            )
            return leader, recovery_s, recovery_dt

        time.sleep(1)

    elapsed = round(time.monotonic() - start, 1)
    logger.log(f"RECOVERY TIMEOUT after {elapsed}s - 5-node cluster did not fully recover")
    return None, elapsed, None


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------
_summary_rows = []


def init_results_csv():
    with open(RESULTS_LIVE, "w", newline="") as f:
        csv.DictWriter(f, fieldnames=RESULTS_LIVE_FIELDS).writeheader()


def append_result(row: dict):
    with open(RESULTS_LIVE, "a", newline="") as f:
        csv.DictWriter(f, fieldnames=RESULTS_LIVE_FIELDS).writerow(row)


# ---------------------------------------------------------------------------
# Experiment A: Kill Leader (3-node)
# ---------------------------------------------------------------------------
def run_kill_leader(run_num: int):
    log_path = os.path.join(LOG_DIR, f"log_v2_kill_leader_run{run_num}.txt")
    logger   = Logger(log_path)
    start_dt = datetime.now()
    try:
        logger.raw(TEST_PURPOSES["kill_leader"])
        logger.raw("EXPERIMENT: kill_leader  [v2 - 1-second parallel polling]")
        logger.raw(f"RUN: {run_num}")
        logger.raw(f"START TIME: {ts(start_dt)}")

        leader, follower_1, follower_2 = preflight_3node(logger)

        logger.raw(f"LEADER AT START: {leader}")
        logger.raw(f"FOLLOWER 1: {follower_1}")
        logger.raw(f"FOLLOWER 2: {follower_2}")
        logger.raw("")

        res_name  = f"v2-kill-leader-r{run_num}"
        yaml_path = os.path.join(YAML_DIR, f"chaos_v2_kill_leader_run{run_num}_dynamic.yaml")
        write_yaml(yaml_path, build_pod_kill_yaml(res_name, leader))
        logger.log(f"Generated YAML: {yaml_path} (targeting {leader})")

        inject_dt = apply_chaos(yaml_path)
        logger.log(f"FAULT INJECTED at {ts(inject_dt)}: kubectl apply -f {yaml_path}")

        new_leader_observed = None
        election_latency_s  = None
        logger.log("--- Polling every 1 s for 60 seconds (parallel) ---")
        poll_end = time.monotonic() + 60

        while time.monotonic() < poll_end:
            statuses = poll_statuses(logger, ZK3_PODS)
            ldr, _   = resolve_roles(statuses)
            if ldr and ldr != leader and new_leader_observed is None:
                new_leader_observed = ldr
                election_latency_s  = round((datetime.now() - inject_dt).total_seconds(), 3)
                logger.log(
                    f"NEW LEADER ELECTED: {ldr} at {ts()} "
                    f"[election_latency={election_latency_s}s from injection]"
                )
            time.sleep(1)

        removal_dt = delete_chaos(yaml_path)
        logger.log(f"FAULT REMOVED at {ts(removal_dt)}: kubectl delete -f {yaml_path}")

        logger.log("--- Polling for full recovery (all 3 pods Running, exactly 1 leader) ---")
        final_leader, recovery_s, recovery_dt = wait_full_recovery_3node(logger, inject_dt)

        end_dt = datetime.now()
        leadership_changed = "yes" if (final_leader and final_leader != leader) else "no"
        notes_parts = []
        if new_leader_observed:
            notes_parts.append(
                f"new leader elected during fault: {new_leader_observed} "
                f"(election_latency={election_latency_s}s)"
            )
        else:
            notes_parts.append("no leader change observed during 60s fault window")
        if final_leader is None:
            notes_parts.append("RECOVERY TIMEOUT")

        logger.raw("")
        logger.raw(f"EXPERIMENT END TIME: {ts(end_dt)}")
        logger.raw(f"LEADER AT END: {final_leader or 'unknown'}")
        logger.raw(f"LEADERSHIP CHANGED: {leadership_changed}")
        logger.raw(f"ELECTION LATENCY SECONDS: {election_latency_s or 'N/A'}")
        logger.raw(f"RECOVERY SECONDS: {recovery_s}")
        logger.raw(f"NOTES: {'; '.join(notes_parts)}")

        append_result({
            "experiment": "kill_leader", "run": run_num, "ensemble": "3-node",
            "leader_before": leader, "leader_after": final_leader or "unknown",
            "leadership_changed": leadership_changed,
            "injection_time": ts(inject_dt),
            "recovery_time": ts(recovery_dt) if recovery_dt else "timeout",
            "recovery_seconds": recovery_s, "quorum_lost": "no",
            "notes": "; ".join(notes_parts),
        })
        _summary_rows.append({
            "experiment": "A:kill_leader", "run": run_num,
            "leader_before": leader, "leader_after": final_leader or "unknown",
            "election_latency_s": election_latency_s, "recovery_s": recovery_s,
        })
        logger.log(f"Run complete - log saved to {log_path}")
    finally:
        logger.close()


# ---------------------------------------------------------------------------
# Experiment B: Network Partition (3-node, leader isolated)
# ---------------------------------------------------------------------------
def run_network_partition(run_num: int):
    log_path = os.path.join(LOG_DIR, f"log_v2_network_partition_run{run_num}.txt")
    logger   = Logger(log_path)
    start_dt = datetime.now()
    try:
        logger.raw(TEST_PURPOSES["network_partition"])
        logger.raw("EXPERIMENT: network_partition  [v2 - 1-second parallel polling]")
        logger.raw(f"RUN: {run_num}")
        logger.raw(f"START TIME: {ts(start_dt)}")

        leader, follower_1, follower_2 = preflight_3node(logger)

        logger.raw(f"LEADER AT START: {leader}")
        logger.raw(f"FOLLOWER 1: {follower_1}")
        logger.raw(f"FOLLOWER 2: {follower_2}")
        logger.raw("")

        res_name      = f"v2-partition-r{run_num}"
        yaml_path     = os.path.join(YAML_DIR, f"chaos_v2_partition_run{run_num}_dynamic.yaml")
        follower_pods = [follower_1, follower_2]
        write_yaml(yaml_path,
                   build_partition_yaml(res_name, [leader], follower_pods, duration="60s"))
        logger.log(f"Generated YAML: {yaml_path}")
        logger.log(
            f"  action=partition, direction=both, isolated={leader}, "
            f"target={follower_pods}, duration=60s"
        )

        inject_dt = apply_chaos(yaml_path)
        logger.log(f"FAULT INJECTED at {ts(inject_dt)}: kubectl apply -f {yaml_path}")

        new_leader_among_followers = None
        split_brain_detected       = False
        election_latency_s         = None
        logger.log("--- Polling every 1 s for 75 seconds (duration=60s + 15s buffer, parallel) ---")
        poll_end = time.monotonic() + 75

        while time.monotonic() < poll_end:
            statuses = poll_statuses(logger, ZK3_PODS)

            isolated_status      = statuses.get(leader, "unknown")
            followers_as_leader  = [p for p in follower_pods if statuses.get(p) == "leader"]

            if isolated_status == "leader" and followers_as_leader:
                split_brain_detected = True
                logger.log(
                    f"  SPLIT-BRAIN WINDOW: isolated leader {leader} still reports "
                    f"'leader' while follower side elected: {followers_as_leader}"
                )

            if followers_as_leader and new_leader_among_followers is None:
                new_leader_among_followers = followers_as_leader[0]
                election_latency_s = round(
                    (datetime.now() - inject_dt).total_seconds(), 3
                )
                logger.log(
                    f"  NEW LEADER ELECTED AMONG FOLLOWERS: {new_leader_among_followers} "
                    f"[election_latency={election_latency_s}s]"
                )

            time.sleep(1)

        # Check if chaos object persisted past its duration
        if chaos_object_exists("NetworkChaos", res_name):
            logger.log("Chaos object still present after 75s - deleting manually ...")
            removal_dt = delete_chaos(yaml_path)
            logger.log(f"FAULT REMOVED (manual) at {ts(removal_dt)}")
        else:
            removal_dt = datetime.now()
            logger.log(
                f"Chaos object auto-removed by Chaos Mesh before 75s mark. "
                f"FAULT REMOVED at {ts(removal_dt)}"
            )

        logger.log("--- Polling for full recovery (all 3 pods Running, exactly 1 leader) ---")
        final_leader, recovery_s, recovery_dt = wait_full_recovery_3node(logger, inject_dt)

        end_dt = datetime.now()
        leadership_changed = "yes" if (final_leader and final_leader != leader) else "no"
        notes_parts = []
        if split_brain_detected:
            notes_parts.append(
                "split-brain window observed: isolated leader + followers both reported leader"
            )
        if new_leader_among_followers:
            notes_parts.append(
                f"new leader among followers: {new_leader_among_followers} "
                f"(election_latency={election_latency_s}s)"
            )
        else:
            notes_parts.append("no new leader elected among followers during partition")
        if final_leader is None:
            notes_parts.append("RECOVERY TIMEOUT")

        logger.raw("")
        logger.raw(f"EXPERIMENT END TIME: {ts(end_dt)}")
        logger.raw(f"LEADER AT END: {final_leader or 'unknown'}")
        logger.raw(f"LEADERSHIP CHANGED: {leadership_changed}")
        logger.raw(f"ELECTION LATENCY SECONDS: {election_latency_s or 'N/A'}")
        logger.raw(f"RECOVERY SECONDS: {recovery_s}")
        logger.raw(f"NOTES: {'; '.join(notes_parts)}")

        append_result({
            "experiment": "network_partition", "run": run_num, "ensemble": "3-node",
            "leader_before": leader, "leader_after": final_leader or "unknown",
            "leadership_changed": leadership_changed,
            "injection_time": ts(inject_dt),
            "recovery_time": ts(recovery_dt) if recovery_dt else "timeout",
            "recovery_seconds": recovery_s,
            "quorum_lost": "partial (isolated leader loses quorum)",
            "notes": "; ".join(notes_parts),
        })
        _summary_rows.append({
            "experiment": "B:network_partition", "run": run_num,
            "leader_before": leader, "leader_after": final_leader or "unknown",
            "election_latency_s": election_latency_s, "recovery_s": recovery_s,
        })
        logger.log(f"Run complete - log saved to {log_path}")
    finally:
        logger.close()


# ---------------------------------------------------------------------------
# Experiment C: Cascading Failure (3-node)
# ---------------------------------------------------------------------------
def run_cascading_failure(run_num: int):
    log_path = os.path.join(LOG_DIR, f"log_v2_cascading_failure_run{run_num}.txt")
    logger   = Logger(log_path)
    start_dt = datetime.now()
    try:
        logger.raw(TEST_PURPOSES["cascading_failure"])
        logger.raw("EXPERIMENT: cascading_failure  [v2 - 1-second parallel polling]")
        logger.raw(f"RUN: {run_num}")
        logger.raw(f"START TIME: {ts(start_dt)}")

        leader, follower_1, follower_2 = preflight_3node(logger)

        logger.raw(f"LEADER AT START: {leader}")
        logger.raw(f"FOLLOWER 1: {follower_1}")
        logger.raw(f"FOLLOWER 2: {follower_2}")
        logger.raw("")
        logger.log(
            f"Plan: kill {follower_1} (follower), wait 10s, "
            f"then kill {leader} (leader). Surviving pod: {follower_2}"
        )

        follower_res  = f"v2-cascade-follower-r{run_num}"
        leader_res    = f"v2-cascade-leader-r{run_num}"
        follower_yaml = os.path.join(YAML_DIR, f"chaos_v2_cascade_follower_run{run_num}_dynamic.yaml")
        leader_yaml   = os.path.join(YAML_DIR, f"chaos_v2_cascade_leader_run{run_num}_dynamic.yaml")

        write_yaml(follower_yaml, build_pod_kill_yaml(follower_res, follower_1))
        write_yaml(leader_yaml,   build_pod_kill_yaml(leader_res,   leader))
        logger.log(f"Generated YAML: {follower_yaml} (targeting follower {follower_1})")
        logger.log(f"Generated YAML: {leader_yaml}   (targeting leader {leader})")

        # Phase 1 - Kill follower
        logger.raw("")
        logger.raw("--- PHASE 1: Kill follower ---")
        follower_kill_dt = apply_chaos(follower_yaml)
        logger.log(
            f"FAULT INJECTED (1/2) at {ts(follower_kill_dt)}: "
            f"killed {follower_1}"
        )
        logger.log("Waiting 10 seconds before killing leader ...")
        time.sleep(10)

        statuses_at_10s = poll_statuses(logger, ZK3_PODS, label="10s mark after follower kill")
        ldr_at_10s, _   = resolve_roles(statuses_at_10s)
        logger.log(f"  Leader at 10s: {ldr_at_10s or 'none'}")

        # Phase 2 - Kill leader (2/3 down = no quorum)
        logger.raw("")
        logger.raw("--- PHASE 2: Kill leader (quorum now impossible with 1/3 alive) ---")
        leader_kill_dt = apply_chaos(leader_yaml)
        logger.log(
            f"FAULT INJECTED (2/2) at {ts(leader_kill_dt)}: "
            f"killed {leader}"
        )
        time_between_kills = round(
            (leader_kill_dt - follower_kill_dt).total_seconds(), 1
        )
        logger.log(f"Time between follower kill and leader kill: {time_between_kills}s")
        logger.log(
            f"2/3 nodes are now down ({follower_1}, {leader}). "
            f"Surviving node: {follower_2}. NO QUORUM possible."
        )

        surviving_pod = follower_2
        quorum_lost   = False
        logger.log(
            f"--- Polling surviving pod ({surviving_pod}) every 1s for 60s ---"
        )
        poll_end = time.monotonic() + 60

        while time.monotonic() < poll_end:
            status = get_zk_status(surviving_pod)
            logger.log(f"POLL: {surviving_pod}={status}")
            if status in ("leader", "follower"):
                logger.log(
                    f"  WARNING: {surviving_pod} reports '{status}' unexpectedly "
                    f"(should not have quorum with only 1/3 nodes alive)"
                )
            else:
                if not quorum_lost:
                    quorum_lost = True
                    logger.log(
                        f"  QUORUM LOST CONFIRMED: {surviving_pod} cannot report "
                        f"leader/follower (status={status})"
                    )
            time.sleep(1)

        # Phase 3 - Remove faults and recover
        logger.raw("")
        logger.raw("--- PHASE 3: Remove both faults and recover ---")
        removal_dt_follower = delete_chaos(follower_yaml)
        removal_dt_leader   = delete_chaos(leader_yaml)
        logger.log(f"FAULT REMOVED at {ts(removal_dt_follower)}: deleted {follower_yaml}")
        logger.log(f"FAULT REMOVED at {ts(removal_dt_leader)}: deleted {leader_yaml}")

        logger.log("--- Polling for full recovery (all 3 pods Running, exactly 1 leader) ---")
        final_leader, recovery_s, recovery_dt = wait_full_recovery_3node(
            logger, follower_kill_dt, timeout_s=240
        )

        end_dt = datetime.now()
        leadership_changed = "yes" if (final_leader and final_leader != leader) else "no"
        notes_parts = [
            f"time_between_kills={time_between_kills}s (target=10s)",
            f"quorum_lost={'yes' if quorum_lost else 'no (pod restarted faster than ZK session timeout)'}",
        ]
        if final_leader:
            notes_parts.append(f"post-recovery leader: {final_leader}")
        else:
            notes_parts.append("RECOVERY TIMEOUT")

        logger.raw("")
        logger.raw(f"EXPERIMENT END TIME: {ts(end_dt)}")
        logger.raw(f"LEADER AT END: {final_leader or 'unknown'}")
        logger.raw(f"LEADERSHIP CHANGED: {leadership_changed}")
        logger.raw(f"RECOVERY SECONDS: {recovery_s} (from follower kill)")
        logger.raw(f"NOTES: {'; '.join(notes_parts)}")

        append_result({
            "experiment": "cascading_failure", "run": run_num, "ensemble": "3-node",
            "leader_before": leader, "leader_after": final_leader or "unknown",
            "leadership_changed": leadership_changed,
            "injection_time": ts(follower_kill_dt),
            "recovery_time": ts(recovery_dt) if recovery_dt else "timeout",
            "recovery_seconds": recovery_s,
            "quorum_lost": "yes" if quorum_lost else "no",
            "notes": "; ".join(notes_parts),
        })
        _summary_rows.append({
            "experiment": "C:cascading_failure", "run": run_num,
            "leader_before": leader, "leader_after": final_leader or "unknown",
            "election_latency_s": None, "recovery_s": recovery_s,
        })
        logger.log(f"Run complete - log saved to {log_path}")
    finally:
        logger.close()


# ---------------------------------------------------------------------------
# Experiment D: Three-Way Isolation (3-node, 1+1+1 partition)
# ---------------------------------------------------------------------------
def run_threeway_isolation(run_num: int):
    """
    Apply 3 separate NetworkChaos objects simultaneously, each isolating
    one pod from the other two. Result: 1+1+1, no quorum possible anywhere.
    """
    log_path = os.path.join(LOG_DIR, f"log_v2_threeway_isolation_run{run_num}.txt")
    logger   = Logger(log_path)
    start_dt = datetime.now()
    try:
        logger.raw(TEST_PURPOSES["threeway_isolation"])
        logger.raw("EXPERIMENT: threeway_isolation  [NEW - v2 only]")
        logger.raw("PARTITION LAYOUT: pod-0 isolated | pod-1 isolated | pod-2 isolated")
        logger.raw("                  (1+1+1) - no group has 2/3 majority")
        logger.raw(f"RUN: {run_num}")
        logger.raw(f"START TIME: {ts(start_dt)}")

        leader, follower_1, follower_2 = preflight_3node(logger)

        logger.raw(f"LEADER AT START: {leader}")
        logger.raw(f"FOLLOWER 1: {follower_1}")
        logger.raw(f"FOLLOWER 2: {follower_2}")
        logger.raw("")

        # Build 3 NetworkChaos objects:
        # NC-A: isolates pod-0 from {pod-1, pod-2}
        # NC-B: isolates pod-1 from {pod-0, pod-2}
        # NC-C: isolates pod-2 from {pod-0, pod-1}
        pods = ZK3_PODS
        chaos_objects = []
        yaml_paths    = []
        for i, pod in enumerate(pods):
            others   = [p for p in pods if p != pod]
            res_name = f"v2-3way-nc{i}-r{run_num}"
            ypath    = os.path.join(YAML_DIR, f"chaos_v2_3way_nc{i}_run{run_num}_dynamic.yaml")
            write_yaml(ypath, build_partition_yaml(res_name, [pod], others, duration="60s"))
            chaos_objects.append((res_name, ypath))
            yaml_paths.append(ypath)
            logger.log(
                f"Generated YAML: {ypath} | isolates {pod} from {others}"
            )

        # Apply all 3 simultaneously using threads
        logger.raw("")
        logger.raw("--- Applying all 3 NetworkChaos objects simultaneously ---")
        inject_dts = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            futs = [ex.submit(apply_chaos, yp) for _, yp in chaos_objects]
            for fut in concurrent.futures.as_completed(futs):
                inject_dts.append(fut.result())
        inject_dt = min(inject_dts)
        logger.log(
            f"ALL 3 FAULTS INJECTED at {ts(inject_dt)}: "
            f"3-way isolation in effect (1+1+1 partition)"
        )

        # Poll every 1s for 75s
        all_lost_quorum = False
        logger.log("--- Polling every 1 s for 75 seconds ---")
        poll_end = time.monotonic() + 75

        while time.monotonic() < poll_end:
            statuses = poll_statuses(logger, ZK3_PODS)
            leader_pods = [p for p, s in statuses.items() if s == "leader"]
            follower_pods = [p for p, s in statuses.items() if s == "follower"]

            if not leader_pods and not follower_pods:
                if not all_lost_quorum:
                    all_lost_quorum = True
                    elapsed = round((datetime.now() - inject_dt).total_seconds(), 3)
                    logger.log(
                        f"  ALL NODES LOST QUORUM at {ts()} "
                        f"[{elapsed}s after injection]: "
                        f"no leader or follower visible on any node"
                    )
            elif leader_pods:
                logger.log(
                    f"  NOTE: {leader_pods} still reports 'leader' - "
                    f"may not yet detect partition"
                )

            time.sleep(1)

        # Delete all 3 chaos objects simultaneously
        logger.raw("")
        logger.raw("--- Deleting all 3 NetworkChaos objects simultaneously ---")
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            futs = [ex.submit(delete_chaos, yp) for _, yp in chaos_objects]
            concurrent.futures.wait(futs)

        # Also check if any persist manually
        for res_name, ypath in chaos_objects:
            if chaos_object_exists("NetworkChaos", res_name):
                logger.log(f"  Manually deleting {res_name} ...")
                delete_chaos_by_name("NetworkChaos", res_name)

        removal_dt = datetime.now()
        logger.log(f"ALL FAULTS REMOVED at {ts(removal_dt)}")

        logger.log("--- Polling for full recovery (all 3 pods Running, exactly 1 leader) ---")
        final_leader, recovery_s, recovery_dt = wait_full_recovery_3node(
            logger, inject_dt, timeout_s=240
        )

        end_dt = datetime.now()
        notes_parts = [
            "3-way isolation (1+1+1): no node had majority",
            f"all_nodes_lost_quorum={'yes' if all_lost_quorum else 'no (pods may restart before ZK timeout)'}",
        ]
        if final_leader:
            notes_parts.append(f"post-recovery leader: {final_leader}")
        else:
            notes_parts.append("RECOVERY TIMEOUT")

        logger.raw("")
        logger.raw(f"EXPERIMENT END TIME: {ts(end_dt)}")
        logger.raw(f"LEADER AT END: {final_leader or 'unknown'}")
        logger.raw(f"ALL NODES LOST QUORUM: {'yes' if all_lost_quorum else 'no'}")
        logger.raw(f"RECOVERY SECONDS: {recovery_s} (from injection)")
        logger.raw(f"NOTES: {'; '.join(notes_parts)}")

        append_result({
            "experiment": "threeway_isolation", "run": run_num, "ensemble": "3-node",
            "leader_before": leader, "leader_after": final_leader or "unknown",
            "leadership_changed": "yes" if final_leader and final_leader != leader else "no",
            "injection_time": ts(inject_dt),
            "recovery_time": ts(recovery_dt) if recovery_dt else "timeout",
            "recovery_seconds": recovery_s,
            "quorum_lost": "yes" if all_lost_quorum else "partial",
            "notes": "; ".join(notes_parts),
        })
        _summary_rows.append({
            "experiment": "D:threeway_isolation", "run": run_num,
            "leader_before": leader, "leader_after": final_leader or "unknown",
            "election_latency_s": None, "recovery_s": recovery_s,
        })
        logger.log(f"Run complete - log saved to {log_path}")
    finally:
        logger.close()


# ---------------------------------------------------------------------------
# Experiment E: 5-node 3+2 Partition (one side has majority)
# ---------------------------------------------------------------------------
def run_5node_majority_partition(run_num: int):
    """
    Partition 5-node ensemble into:
      Group A: {zk5-0, zk5-1, zk5-2} -> 3/5 = majority, CAN elect leader
      Group B: {zk5-3, zk5-4}         -> 2/5 = minority, CANNOT elect leader
    One NetworkChaos object isolates group A from group B.
    """
    log_path = os.path.join(LOG_DIR, f"log_v2_5node_majority_partition_run{run_num}.txt")
    logger   = Logger(log_path)
    start_dt = datetime.now()
    try:
        logger.raw(TEST_PURPOSES["5node_majority_partition"])
        logger.raw("EXPERIMENT: 5node_majority_partition  [NEW - v2 only, 5-node ensemble]")
        logger.raw("PARTITION: {zk5-0, zk5-1, zk5-2} vs {zk5-3, zk5-4}")
        logger.raw("           Group A (3/5 = majority) | Group B (2/5 = minority)")
        logger.raw(f"RUN: {run_num}")
        logger.raw(f"START TIME: {ts(start_dt)}")

        leader5, followers5 = preflight_5node(logger)

        logger.raw(f"LEADER AT START: {leader5}")
        logger.raw(f"FOLLOWERS: {followers5}")
        logger.raw("")

        group_a = ["zookeeper5-0", "zookeeper5-1", "zookeeper5-2"]
        group_b = ["zookeeper5-3", "zookeeper5-4"]

        leader_group = "A" if leader5 in group_a else "B"
        logger.log(
            f"Leader {leader5} is in Group {leader_group}. "
            f"Group A has 3/5 majority so will retain/elect leader."
        )

        res_name  = f"v2-5node-majority-r{run_num}"
        yaml_path = os.path.join(YAML_DIR, f"chaos_v2_5node_majority_run{run_num}_dynamic.yaml")
        write_yaml(yaml_path,
                   build_partition_yaml(res_name, group_a, group_b,
                                        duration="60s"))
        logger.log(f"Generated YAML: {yaml_path}")
        logger.log(f"  Partitioning Group A={group_a} from Group B={group_b}")

        inject_dt = apply_chaos(yaml_path)
        logger.log(f"FAULT INJECTED at {ts(inject_dt)}: kubectl apply -f {yaml_path}")

        # Track leader in each group independently
        group_a_leader = None
        group_b_leader = None
        group_b_lost_quorum = False
        group_a_preserved_quorum = False

        logger.log("--- Polling every 1s for 75 seconds ---")
        poll_end = time.monotonic() + 75

        while time.monotonic() < poll_end:
            statuses = poll_statuses(logger, ZK5_PODS)

            a_leaders = [p for p in group_a if statuses.get(p) == "leader"]
            b_leaders = [p for p in group_b if statuses.get(p) == "leader"]
            b_nonrunning = [p for p in group_b
                            if statuses.get(p) in ("not_running", "error", "unknown", "timeout")]

            if a_leaders and not group_a_preserved_quorum:
                group_a_leader = a_leaders[0]
                group_a_preserved_quorum = True
                elapsed = round((datetime.now() - inject_dt).total_seconds(), 3)
                logger.log(
                    f"  GROUP A LEADER ACTIVE: {group_a_leader} "
                    f"[{elapsed}s after injection] - 3-node majority intact"
                )

            if len(b_nonrunning) == len(group_b) and not group_b_lost_quorum:
                group_b_lost_quorum = True
                elapsed = round((datetime.now() - inject_dt).total_seconds(), 3)
                logger.log(
                    f"  GROUP B LOST QUORUM at {ts()} [{elapsed}s]: "
                    f"both minority nodes are not_running/error"
                )

            if b_leaders:
                logger.log(
                    f"  NOTE: Group B pod {b_leaders} claims 'leader' "
                    f"but has only 2/5 - ZAB quorum not satisfied"
                )

            time.sleep(1)

        # Remove chaos
        if chaos_object_exists("NetworkChaos", res_name):
            logger.log("Chaos object still present after 75s - deleting manually ...")
            removal_dt = delete_chaos(yaml_path)
            logger.log(f"FAULT REMOVED (manual) at {ts(removal_dt)}")
        else:
            removal_dt = datetime.now()
            logger.log(f"Chaos object auto-removed. FAULT REMOVED at {ts(removal_dt)}")

        logger.log("--- Polling for full recovery (all 5 pods Running, exactly 1 leader) ---")
        final_leader, recovery_s, recovery_dt = wait_full_recovery_5node(
            logger, inject_dt, timeout_s=300
        )

        end_dt = datetime.now()
        notes_parts = [
            f"Group A (3/5 majority) preserved_quorum={group_a_preserved_quorum}",
            f"Group B (2/5 minority) lost_quorum={group_b_lost_quorum}",
        ]
        if group_a_leader:
            notes_parts.append(f"group_a_leader_during_partition={group_a_leader}")
        if final_leader:
            notes_parts.append(f"post-recovery leader: {final_leader}")
        else:
            notes_parts.append("RECOVERY TIMEOUT")

        logger.raw("")
        logger.raw(f"EXPERIMENT END TIME: {ts(end_dt)}")
        logger.raw(f"LEADER AT END: {final_leader or 'unknown'}")
        logger.raw(f"GROUP A (3/5 MAJORITY) PRESERVED QUORUM: {group_a_preserved_quorum}")
        logger.raw(f"GROUP B (2/5 MINORITY) LOST QUORUM: {group_b_lost_quorum}")
        logger.raw(f"RECOVERY SECONDS: {recovery_s}")
        logger.raw(f"NOTES: {'; '.join(notes_parts)}")

        append_result({
            "experiment": "5node_majority_partition", "run": run_num, "ensemble": "5-node",
            "leader_before": leader5, "leader_after": final_leader or "unknown",
            "leadership_changed": "partial (group-level change possible)",
            "injection_time": ts(inject_dt),
            "recovery_time": ts(recovery_dt) if recovery_dt else "timeout",
            "recovery_seconds": recovery_s,
            "quorum_lost": f"group_B_only (group_A={group_a_preserved_quorum})",
            "notes": "; ".join(notes_parts),
        })
        _summary_rows.append({
            "experiment": "E:5node_majority_partition", "run": run_num,
            "leader_before": leader5, "leader_after": final_leader or "unknown",
            "election_latency_s": None, "recovery_s": recovery_s,
        })
        logger.log(f"Run complete - log saved to {log_path}")
    finally:
        logger.close()


# ---------------------------------------------------------------------------
# Experiment F: 5-node 2+2+1 Three-Way Partition (no group has majority)
# ---------------------------------------------------------------------------
def run_5node_threeway_partition(run_num: int):
    """
    Two NetworkChaos objects create a three-way partition of the 5-node ensemble:
      NC1: isolates {zk5-0, zk5-1} from {zk5-2, zk5-3, zk5-4}
      NC2: isolates {zk5-2, zk5-3} from {zk5-0, zk5-1, zk5-4}

    Combined effect: three isolated groups
      Group A: {zk5-0, zk5-1}  - 2/5, minority
      Group B: {zk5-2, zk5-3}  - 2/5, minority
      Group C: {zk5-4}          - 1/5, minority
    None has 3/5 majority; entire cluster loses write availability.
    """
    log_path = os.path.join(LOG_DIR, f"log_v2_5node_threeway_partition_run{run_num}.txt")
    logger   = Logger(log_path)
    start_dt = datetime.now()
    try:
        logger.raw(TEST_PURPOSES["5node_threeway_partition"])
        logger.raw("EXPERIMENT: 5node_threeway_partition  [NEW - v2 only, 5-node ensemble]")
        logger.raw("NC1: {zk5-0, zk5-1} isolated from {zk5-2, zk5-3, zk5-4}")
        logger.raw("NC2: {zk5-2, zk5-3} isolated from {zk5-0, zk5-1, zk5-4}")
        logger.raw("Result: Group A={zk5-0,1}(2/5) | Group B={zk5-2,3}(2/5) | Group C={zk5-4}(1/5)")
        logger.raw("        No group reaches 3/5 quorum threshold")
        logger.raw(f"RUN: {run_num}")
        logger.raw(f"START TIME: {ts(start_dt)}")

        leader5, followers5 = preflight_5node(logger)

        logger.raw(f"LEADER AT START: {leader5}")
        logger.raw(f"FOLLOWERS: {followers5}")
        logger.raw("")

        group_a = ["zookeeper5-0", "zookeeper5-1"]
        group_b = ["zookeeper5-2", "zookeeper5-3"]
        group_c = ["zookeeper5-4"]

        # NC1: isolates group_a from (group_b + group_c)
        nc1_name  = f"v2-5node-3way-nc1-r{run_num}"
        nc1_path  = os.path.join(YAML_DIR, f"chaos_v2_5node_3way_nc1_run{run_num}_dynamic.yaml")
        nc1_tgts  = group_b + group_c
        write_yaml(nc1_path,
                   build_partition_yaml(nc1_name, group_a, nc1_tgts, duration="60s"))

        # NC2: isolates group_b from (group_a + group_c)
        nc2_name  = f"v2-5node-3way-nc2-r{run_num}"
        nc2_path  = os.path.join(YAML_DIR, f"chaos_v2_5node_3way_nc2_run{run_num}_dynamic.yaml")
        nc2_tgts  = group_a + group_c
        write_yaml(nc2_path,
                   build_partition_yaml(nc2_name, group_b, nc2_tgts, duration="60s"))

        logger.log(f"Generated YAML: {nc1_path}")
        logger.log(f"  NC1: isolates {group_a} from {nc1_tgts}")
        logger.log(f"Generated YAML: {nc2_path}")
        logger.log(f"  NC2: isolates {group_b} from {nc2_tgts}")
        logger.log(
            f"Combined effect: Group A={group_a} | Group B={group_b} | Group C={group_c}"
        )

        # Apply both simultaneously
        logger.raw("")
        logger.raw("--- Applying both NetworkChaos objects simultaneously ---")
        inject_dts = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            futs = [ex.submit(apply_chaos, nc1_path),
                    ex.submit(apply_chaos, nc2_path)]
            for fut in concurrent.futures.as_completed(futs):
                inject_dts.append(fut.result())
        inject_dt = min(inject_dts)
        logger.log(
            f"BOTH FAULTS INJECTED at {ts(inject_dt)}: "
            f"3-way partition active (2+2+1, no group has 3/5 majority)"
        )

        all_lost_quorum      = False
        any_spurious_leader  = False
        logger.log("--- Polling every 1s for 75 seconds ---")
        poll_end = time.monotonic() + 75

        while time.monotonic() < poll_end:
            statuses = poll_statuses(logger, ZK5_PODS)

            all_pods_no_quorum = all(
                statuses[p] in ("not_running", "error", "unknown", "timeout")
                for p in ZK5_PODS
            )
            leader_pods = [p for p in ZK5_PODS if statuses.get(p) == "leader"]

            if all_pods_no_quorum and not all_lost_quorum:
                all_lost_quorum = True
                elapsed = round((datetime.now() - inject_dt).total_seconds(), 3)
                logger.log(
                    f"  ALL 5 NODES LOST QUORUM at {ts()} [{elapsed}s after injection]: "
                    f"2+2+1 partition confirmed - entire cluster unavailable"
                )

            if leader_pods:
                any_spurious_leader = True
                logger.log(
                    f"  NOTE: {leader_pods} still claims 'leader' - "
                    f"may not have detected partition yet (ZK session timeout ~30s)"
                )

            time.sleep(1)

        # Delete both chaos objects
        logger.raw("")
        logger.raw("--- Deleting both NetworkChaos objects simultaneously ---")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            futs = [ex.submit(delete_chaos, nc1_path),
                    ex.submit(delete_chaos, nc2_path)]
            concurrent.futures.wait(futs)

        for kind_name, ypath in [(nc1_name, nc1_path), (nc2_name, nc2_path)]:
            if chaos_object_exists("NetworkChaos", kind_name):
                logger.log(f"  Manually deleting {kind_name} ...")
                delete_chaos_by_name("NetworkChaos", kind_name)

        removal_dt = datetime.now()
        logger.log(f"ALL FAULTS REMOVED at {ts(removal_dt)}")

        logger.log("--- Polling for full recovery (all 5 pods Running, exactly 1 leader) ---")
        final_leader, recovery_s, recovery_dt = wait_full_recovery_5node(
            logger, inject_dt, timeout_s=300
        )

        end_dt = datetime.now()
        notes_parts = [
            "2+2+1 three-way partition: no group had 3/5 majority",
            f"all_nodes_lost_quorum={all_lost_quorum}",
            f"spurious_leader_claims_observed={any_spurious_leader}",
        ]
        if final_leader:
            notes_parts.append(f"post-recovery leader: {final_leader}")
        else:
            notes_parts.append("RECOVERY TIMEOUT")

        logger.raw("")
        logger.raw(f"EXPERIMENT END TIME: {ts(end_dt)}")
        logger.raw(f"LEADER AT END: {final_leader or 'unknown'}")
        logger.raw(f"ALL NODES LOST QUORUM: {all_lost_quorum}")
        logger.raw(f"RECOVERY SECONDS: {recovery_s}")
        logger.raw(f"NOTES: {'; '.join(notes_parts)}")

        append_result({
            "experiment": "5node_threeway_partition", "run": run_num, "ensemble": "5-node",
            "leader_before": leader5, "leader_after": final_leader or "unknown",
            "leadership_changed": "yes" if final_leader and final_leader != leader5 else "no",
            "injection_time": ts(inject_dt),
            "recovery_time": ts(recovery_dt) if recovery_dt else "timeout",
            "recovery_seconds": recovery_s,
            "quorum_lost": "yes (all groups)" if all_lost_quorum else "partial",
            "notes": "; ".join(notes_parts),
        })
        _summary_rows.append({
            "experiment": "F:5node_threeway_partition", "run": run_num,
            "leader_before": leader5, "leader_after": final_leader or "unknown",
            "election_latency_s": None, "recovery_s": recovery_s,
        })
        logger.log(f"Run complete - log saved to {log_path}")
    finally:
        logger.close()


# ---------------------------------------------------------------------------
# Experiment G: 5-node — Leader on Minority Side (2+3 partition, leader in 2)
# ---------------------------------------------------------------------------
def run_5node_leader_minority_partition(run_num: int):
    """
    Partition the 5-node ensemble so the CURRENT LEADER ends up on the
    MINORITY (2-node) side, ensuring quorum and the new elected leader land
    on the MAJORITY (3-node) side.

      minority (2 nodes): [current_leader, followers[0]]  -- 2/5, loses quorum
      majority (3 nodes): [followers[1], followers[2], followers[3]] -- 3/5, elects new leader

    Milestone timestamps:
      A = inject_dt                 (partition applied)
      B = new_majority_leader_dt    (first poll: a majority pod reports 'leader')
      C = majority_write_avail_dt   (inferred = B; majority has an active leader)
      D = minority_quorum_lost_dt   (first poll: ALL minority pods non-leader/follower)
      E = removal_dt                (chaos deleted, partition healed)
      F = recovery_dt               (all 5 Running + exactly 1 leader)
    """
    log_path = os.path.join(LOG_DIR, f"log_v2_leader_minority_partition_run{run_num}.txt")
    logger   = Logger(log_path)
    start_dt = datetime.now()
    try:
        logger.raw(TEST_PURPOSES["leader_minority_partition"])
        logger.raw("EXPERIMENT: leader_minority_partition  [Experiment G - v2 only, 5-node ensemble]")
        logger.raw("PARTITION: [current_leader + 1 follower] (minority 2/5)")
        logger.raw("           vs [3 remaining followers]     (majority 3/5)")
        logger.raw(f"RUN: {run_num}")
        logger.raw(f"START TIME: {ts(start_dt)}")

        leader5, followers5 = preflight_5node(logger)

        # Assign groups: minority gets the current leader + first follower
        minority = [leader5, followers5[0]]
        majority = followers5[1:4]   # exactly 3 elements

        logger.raw(f"LEADER AT START: {leader5}")
        logger.raw(f"FOLLOWERS: {followers5}")
        logger.raw(f"MINORITY GROUP (2/5, has current leader): {minority}")
        logger.raw(f"MAJORITY GROUP (3/5, will elect new leader): {majority}")
        logger.raw("")
        logger.log(
            f"Partition design: minority={minority} (cannot form quorum) "
            f"vs majority={majority} (will retain/elect leader)"
        )

        # Build and apply chaos
        res_name  = f"v2-leader-minority-r{run_num}"
        yaml_path = os.path.join(YAML_DIR, f"chaos_v2_leader_minority_run{run_num}_dynamic.yaml")
        write_yaml(yaml_path,
                   build_partition_yaml(res_name, minority, majority, duration="60s"))
        logger.log(f"Generated YAML: {yaml_path}")
        logger.log(
            f"  NetworkChaos partition: {minority} <-> {majority}, "
            f"action=partition, direction=both, duration=60s"
        )

        inject_dt = apply_chaos(yaml_path)
        logger.log(f"[A] PARTITION INJECTED at {ts(inject_dt)}: kubectl apply -f {yaml_path}")

        # ---------------------------------------------------------------
        # Milestone trackers
        # ---------------------------------------------------------------
        A_inject_dt              = inject_dt
        B_new_majority_leader_dt = None   # first majority pod reports 'leader'
        C_majority_write_avail   = None   # inferred == B
        D_minority_quorum_lost   = None   # all minority pods non-leader/follower
        new_majority_leader      = None
        old_leader_stepped_down  = False
        split_brain_observed     = False

        logger.log("--- Polling every 1s for 75 seconds (parallel, duration=60s + 15s buffer) ---")
        logger.log(f"    Minority to watch: {minority}  |  Majority to watch: {majority}")
        poll_end = time.monotonic() + 75

        while time.monotonic() < poll_end:
            statuses = poll_statuses(logger, ZK5_PODS)
            elapsed  = round((datetime.now() - A_inject_dt).total_seconds(), 3)

            old_leader_status = statuses.get(leader5, "unknown")

            # Detect old leader stepping down
            if old_leader_status != "leader" and not old_leader_stepped_down:
                old_leader_stepped_down = True
                logger.log(
                    f"  [+{elapsed}s] OLD LEADER STEPPED DOWN: {leader5} now "
                    f"reports '{old_leader_status}'"
                )

            # Detect split-brain window: old leader still claims 'leader' while
            # majority has also elected a leader
            majority_leaders = [p for p in majority if statuses.get(p) == "leader"]
            if old_leader_status == "leader" and majority_leaders:
                if not split_brain_observed:
                    split_brain_observed = True
                    logger.log(
                        f"  [+{elapsed}s] SPLIT-BRAIN WINDOW: minority leader {leader5} "
                        f"still claims 'leader'; majority has also elected {majority_leaders}"
                    )

            # Milestone B: first majority leader detected
            if majority_leaders and B_new_majority_leader_dt is None:
                B_new_majority_leader_dt = datetime.now()
                C_majority_write_avail   = B_new_majority_leader_dt
                new_majority_leader      = majority_leaders[0]
                b_elapsed = round((B_new_majority_leader_dt - A_inject_dt).total_seconds(), 3)
                logger.log(
                    f"  [+{b_elapsed}s] [B] NEW LEADER ON MAJORITY SIDE: "
                    f"{new_majority_leader}  [B-A = {b_elapsed}s]"
                )
                logger.log(
                    f"  [+{b_elapsed}s] [C] MAJORITY WRITE AVAILABLE (inferred): "
                    f"{new_majority_leader} is active leader => majority side accepting writes"
                )

            # Milestone D: all minority pods have lost quorum
            minority_lost = [
                p for p in minority
                if statuses.get(p) not in ("leader", "follower")
            ]
            if len(minority_lost) == len(minority) and D_minority_quorum_lost is None:
                D_minority_quorum_lost = datetime.now()
                d_elapsed = round((D_minority_quorum_lost - A_inject_dt).total_seconds(), 3)
                logger.log(
                    f"  [+{d_elapsed}s] [D] MINORITY QUORUM LOST: all minority pods "
                    f"{minority} report non-quorum status "
                    f"({[statuses.get(p) for p in minority]})  [D-A = {d_elapsed}s]"
                )

            time.sleep(1)

        # ---------------------------------------------------------------
        # Milestone E: heal the partition
        # ---------------------------------------------------------------
        if chaos_object_exists("NetworkChaos", res_name):
            logger.log("Chaos object still present after 75s - deleting manually ...")
            removal_dt = delete_chaos(yaml_path)
            logger.log(f"FAULT REMOVED (manual) at {ts(removal_dt)}")
        else:
            removal_dt = datetime.now()
            logger.log(
                f"Chaos object auto-removed by Chaos Mesh before 75s mark. "
                f"FAULT REMOVED at {ts(removal_dt)}"
            )
        logger.log(f"[E] PARTITION HEALED at {ts(removal_dt)}")

        # ---------------------------------------------------------------
        # Milestone F: full recovery
        # ---------------------------------------------------------------
        logger.log("--- Polling for full recovery (all 5 pods Running, exactly 1 leader) ---")
        final_leader, recovery_s, recovery_dt = wait_full_recovery_5node(
            logger, A_inject_dt, timeout_s=300
        )
        if recovery_dt:
            logger.log(
                f"[F] FULL RECOVERY at {ts(recovery_dt)}: all 5 nodes Running + 1 leader"
            )

        # ---------------------------------------------------------------
        # Compute timing deltas
        # ---------------------------------------------------------------
        B_minus_A = (
            round((B_new_majority_leader_dt - A_inject_dt).total_seconds(), 3)
            if B_new_majority_leader_dt else "N/A"
        )
        D_minus_A = (
            round((D_minority_quorum_lost - A_inject_dt).total_seconds(), 3)
            if D_minority_quorum_lost else "N/A"
        )
        E_minus_A = round((removal_dt - A_inject_dt).total_seconds(), 1)
        F_minus_E = (
            round((recovery_dt - removal_dt).total_seconds(), 1)
            if recovery_dt else "N/A"
        )

        end_dt = datetime.now()

        # ---------------------------------------------------------------
        # Timeline summary footer
        # ---------------------------------------------------------------
        logger.raw("")
        logger.raw("=" * 62)
        logger.raw("TIMELINE SUMMARY")
        logger.raw("=" * 62)
        logger.raw(f"[A] PARTITION INJECTED      : {ts(A_inject_dt)}")
        logger.raw(
            f"[B] NEW MAJORITY LEADER     : "
            f"{ts(B_new_majority_leader_dt) if B_new_majority_leader_dt else 'N/A'}"
            f"  (B-A = {B_minus_A}s)"
        )
        logger.raw(
            f"[C] MAJORITY WRITE AVAIL    : same as B (inferred from leader election)"
        )
        logger.raw(
            f"[D] MINORITY QUORUM LOST    : "
            f"{ts(D_minority_quorum_lost) if D_minority_quorum_lost else 'N/A'}"
            f"  (D-A = {D_minus_A}s)"
        )
        logger.raw(f"[E] PARTITION HEALED        : {ts(removal_dt)}  (E-A = {E_minus_A}s)")
        logger.raw(
            f"[F] FULL RECOVERY           : "
            f"{ts(recovery_dt) if recovery_dt else 'N/A'}"
            f"  (F-E = {F_minus_E}s)"
        )
        logger.raw("")
        logger.raw(f"OLD LEADER (minority side)  : {leader5}")
        logger.raw(f"NEW LEADER (majority side)  : {new_majority_leader or 'not elected during 75s window'}")
        logger.raw(f"OLD LEADER STEPPED DOWN     : {old_leader_stepped_down}")
        logger.raw(f"SPLIT-BRAIN WINDOW OBSERVED : {split_brain_observed}")
        logger.raw(f"RECOVERY SECONDS (A to F)   : {recovery_s}")
        logger.raw(f"EXPERIMENT END TIME         : {ts(end_dt)}")
        logger.raw("=" * 62)

        # ---------------------------------------------------------------
        # CSV / summary
        # ---------------------------------------------------------------
        notes_parts = [
            f"minority={minority}, majority={majority}",
            f"B-A={B_minus_A}s (majority leader election latency from partition inject)",
            f"D-A={D_minus_A}s (minority quorum loss detection latency)",
            f"F-E={F_minus_E}s (recovery latency after partition healed)",
            f"old_leader_stepped_down={old_leader_stepped_down}",
            f"split_brain_window={split_brain_observed}",
        ]
        if new_majority_leader:
            notes_parts.append(f"new_leader={new_majority_leader}")
        if recovery_dt is None:
            notes_parts.append("RECOVERY TIMEOUT")

        append_result({
            "experiment":         "leader_minority_partition",
            "run":                run_num,
            "ensemble":           "5-node",
            "leader_before":      leader5,
            "leader_after":       final_leader or "unknown",
            "leadership_changed": "yes" if new_majority_leader else "no",
            "injection_time":     ts(A_inject_dt),
            "recovery_time":      ts(recovery_dt) if recovery_dt else "timeout",
            "recovery_seconds":   recovery_s,
            "quorum_lost":        f"minority_only (minority_pods={minority})",
            "notes":              "; ".join(notes_parts),
        })
        _summary_rows.append({
            "experiment":        "G:leader_minority_partition",
            "run":               run_num,
            "leader_before":     leader5,
            "leader_after":      final_leader or "unknown",
            "election_latency_s": B_minus_A if isinstance(B_minus_A, float) else None,
            "recovery_s":        recovery_s,
        })
        logger.log(f"Run complete - log saved to {log_path}")
    finally:
        logger.close()


# ---------------------------------------------------------------------------
# ZooKeeper5 StatefulSet lifecycle
# ---------------------------------------------------------------------------
def deploy_zookeeper5(logger=None):
    """Deploy the 5-node ZooKeeper StatefulSet from zookeeper5.yaml."""
    def _log(msg):
        if logger:
            logger.log(msg)
        else:
            print(f"[{ts()}] {msg}", flush=True)

    _log("Deploying 5-node ZooKeeper StatefulSet (zookeeper5.yaml) ...")
    r = subprocess.run(
        ["kubectl", "apply", "-f", "zookeeper5.yaml"],
        capture_output=True, text=True, timeout=60,
    )
    if r.returncode != 0:
        _log(f"ERROR deploying zookeeper5.yaml: {r.stderr}")
        sys.exit(1)
    _log(f"kubectl apply output: {r.stdout.strip()}")

    _log("Waiting for all 5 zookeeper5 pods to be 1/1 Running (up to 5 min) ...")
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        pods_ok, n, out = check_pods_running("app=zookeeper5")
        _log(f"  {n}/5 pods Running")
        if pods_ok:
            _log("All 5 zookeeper5 pods are Running.")
            # Give ZooKeeper time to elect a leader
            _log("Waiting 30s for leader election in new 5-node ensemble ...")
            time.sleep(30)
            return
        time.sleep(5)

    _log("ERROR: zookeeper5 pods did not start within 5 minutes. Aborting.")
    sys.exit(1)


def teardown_zookeeper5(logger=None):
    """Delete the 5-node ZooKeeper StatefulSet and its PVCs."""
    def _log(msg):
        if logger:
            logger.log(msg)
        else:
            print(f"[{ts()}] {msg}", flush=True)

    _log("Tearing down zookeeper5 (StatefulSet + Services + PVCs) ...")
    subprocess.run(
        ["kubectl", "delete", "-f", "zookeeper5.yaml", "--ignore-not-found=true"],
        capture_output=True, timeout=60,
    )
    # Delete PVCs created by the StatefulSet (not deleted by kubectl delete -f)
    r = subprocess.run(
        ["kubectl", "get", "pvc", "-l", "app=zookeeper5", "--no-headers",
         "-o", "custom-columns=:metadata.name"],
        capture_output=True, text=True, timeout=20,
    )
    pvcs = r.stdout.strip().splitlines()
    for pvc in pvcs:
        pvc = pvc.strip()
        if pvc:
            _log(f"  Deleting PVC: {pvc}")
            subprocess.run(
                ["kubectl", "delete", "pvc", pvc, "--ignore-not-found=true"],
                capture_output=True, timeout=20,
            )
    _log("zookeeper5 teardown complete.")


# ---------------------------------------------------------------------------
# Emergency cleanup
# ---------------------------------------------------------------------------
def cleanup_all():
    print(f"[{ts()}] [CLEANUP] Removing any lingering chaos resources ...")
    for n in range(1, 4):
        for kind, name in [
            ("PodChaos",     f"v2-kill-leader-r{n}"),
            ("NetworkChaos", f"v2-partition-r{n}"),
            ("PodChaos",     f"v2-cascade-follower-r{n}"),
            ("PodChaos",     f"v2-cascade-leader-r{n}"),
            ("NetworkChaos", f"v2-5node-majority-r{n}"),
            ("NetworkChaos", f"v2-5node-3way-nc1-r{n}"),
            ("NetworkChaos", f"v2-5node-3way-nc2-r{n}"),
            ("NetworkChaos", f"v2-leader-minority-r{n}"),
        ]:
            subprocess.run(
                ["kubectl", "delete", kind, name,
                 "--ignore-not-found=true", "-n", NAMESPACE],
                capture_output=True, timeout=15,
            )
        for i in range(3):
            subprocess.run(
                ["kubectl", "delete", "NetworkChaos", f"v2-3way-nc{i}-r{n}",
                 "--ignore-not-found=true", "-n", NAMESPACE],
                capture_output=True, timeout=15,
            )
    print(f"[{ts()}] [CLEANUP] Done.")


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------
def print_summary():
    print()
    print("ADVANCED EXPERIMENT SUMMARY (v2)")
    print("=" * 85)
    print(
        f"{'Experiment':<30} | {'Run':>3} | "
        f"{'Leader Before':<14} | {'Leader After':<14} | "
        f"{'Elect Lat(s)':<13} | Recovery(s)"
    )
    print(f"{'-'*30}+{'-'*5}+{'-'*16}+{'-'*16}+{'-'*15}+{'-'*12}")
    for r in _summary_rows:
        lb  = (r.get("leader_before") or "unknown")[:14]
        la  = (r.get("leader_after")  or "unknown")[:14]
        el  = str(r.get("election_latency_s") or "N/A")[:13]
        rs  = str(r.get("recovery_s") or "?")
        print(
            f"{r['experiment']:<30} | {r['run']:>3} | "
            f"{lb:<14} | {la:<14} | {el:<13} | {rs}s"
        )
    print("=" * 85)
    print()
    print("ALL LOG FILES (in logs/):")
    for exp, runs in [
        ("log_v2_kill_leader", 3),
        ("log_v2_network_partition", 3),
        ("log_v2_cascading_failure", 3),
        ("log_v2_threeway_isolation", 3),
        ("log_v2_5node_majority_partition", 3),
        ("log_v2_5node_threeway_partition", 3),
        ("log_v2_leader_minority_partition", 3),
    ]:
        for n in range(1, runs + 1):
            print(f"  {os.path.join(LOG_DIR, exp + '_run' + str(n) + '.txt')}")
    print(f"  {MASTER_LOG}  (master, all experiments combined)")
    print(f"  {RESULTS_LIVE}  (CSV summary)")
    print(f"  logs/workload/workload_*.txt  (workload disruption log - correlate by timestamp)")
    print(f"ALL CHAOS YAMLS (in chaos_yamls/): one file per run, named chaos_v2_<exp>_run<N>_dynamic.yaml")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # Ensure output directories exist before anything else
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(os.path.join(LOG_DIR, "workload"), exist_ok=True)
    os.makedirs(YAML_DIR, exist_ok=True)

    suite_start = ts()
    with open(MASTER_LOG, "w", buffering=1) as mf:
        mf.write("ZooKeeper Advanced Chaos Engineering Suite v2 - Master Log\n")
        mf.write(f"Suite started : {suite_start}\n")
        mf.write(f"Log directory : {LOG_DIR}/\n")
        mf.write(f"YAML directory: {YAML_DIR}/\n")
        mf.write(
            "Polling interval: 1 second (parallel ThreadPoolExecutor)\n"
            "Experiments: A=Kill Leader, B=Network Partition, C=Cascading Failure,\n"
            "             D=Three-Way Isolation, E=5-Node 3+2 Partition,\n"
            "             F=5-Node 2+2+1 Three-Way Partition\n"
            "\n"
            "WORKLOAD CORRELATION\n"
            "  Workload log: logs/workload/workload_*.txt\n"
            "  Match ERROR bursts in workload log to FAULT INJECTED events\n"
            "  in this master log using the shared YYYY-MM-DD HH:MM:SS.mmm timestamp.\n"
        )
        mf.write("=" * 70 + "\n\n")
    init_results_csv()

    print(f"[{ts()}] ====================================================")
    print(f"[{ts()}]  ZooKeeper Advanced Chaos Engineering Suite - v2")
    print(f"[{ts()}]  Polling: 1s parallel (ThreadPoolExecutor)")
    print(f"[{ts()}]  Master log : {MASTER_LOG}")
    print(f"[{ts()}]  Results    : {RESULTS_LIVE}")
    print(f"[{ts()}] ====================================================")
    print()

    try:
        # -- Experiment A: Kill Leader (3 runs) ---------------------------------
        print(f"[{ts()}] === EXPERIMENT A: KILL LEADER - 3 runs ===")
        for run in range(1, 4):
            print(f"\n[{ts()}] --- Kill Leader Run {run}/3 ---")
            run_kill_leader(run)
            if run < 3:
                print(f"[{ts()}] Waiting 90s for stabilisation ...")
                time.sleep(90)

        print(f"\n[{ts()}] === EXPERIMENT A COMPLETE ===")
        print(f"[{ts()}] Waiting 90s before Experiment B ...")
        time.sleep(90)

        # -- Experiment B: Network Partition (3 runs) ---------------------------
        print(f"\n[{ts()}] === EXPERIMENT B: NETWORK PARTITION - 3 runs ===")
        for run in range(1, 4):
            print(f"\n[{ts()}] --- Network Partition Run {run}/3 ---")
            run_network_partition(run)
            if run < 3:
                print(f"[{ts()}] Waiting 90s for stabilisation ...")
                time.sleep(90)

        print(f"\n[{ts()}] === EXPERIMENT B COMPLETE ===")
        print(f"[{ts()}] Waiting 90s before Experiment C ...")
        time.sleep(90)

        # -- Experiment C: Cascading Failure (3 runs) ---------------------------
        print(f"\n[{ts()}] === EXPERIMENT C: CASCADING FAILURE - 3 runs ===")
        for run in range(1, 4):
            print(f"\n[{ts()}] --- Cascading Failure Run {run}/3 ---")
            run_cascading_failure(run)
            if run < 3:
                print(f"[{ts()}] Waiting 90s for stabilisation ...")
                time.sleep(90)

        print(f"\n[{ts()}] === EXPERIMENT C COMPLETE ===")
        print(f"[{ts()}] Waiting 90s before Experiment D ...")
        time.sleep(90)

        # -- Experiment D: Three-Way Isolation (3 runs) -------------------------
        print(f"\n[{ts()}] === EXPERIMENT D: THREE-WAY ISOLATION (1+1+1) - 3 runs ===")
        for run in range(1, 4):
            print(f"\n[{ts()}] --- Three-Way Isolation Run {run}/3 ---")
            run_threeway_isolation(run)
            if run < 3:
                print(f"[{ts()}] Waiting 90s for stabilisation ...")
                time.sleep(90)

        print(f"\n[{ts()}] === EXPERIMENT D COMPLETE ===")
        print(f"[{ts()}] Waiting 60s then deploying 5-node ZooKeeper ensemble ...")
        time.sleep(60)

        # -- Deploy zookeeper5 -------------------------------------------------
        print(f"\n[{ts()}] === DEPLOYING 5-NODE ZOOKEEPER ENSEMBLE ===")
        deploy_zookeeper5()
        print(f"[{ts()}] 5-node ensemble ready. Waiting 30s before Experiment E ...")
        time.sleep(30)

        # -- Experiment E: 5-Node 3+2 Partition (3 runs) -----------------------
        print(f"\n[{ts()}] === EXPERIMENT E: 5-NODE 3+2 MAJORITY PARTITION - 3 runs ===")
        for run in range(1, 4):
            print(f"\n[{ts()}] --- 5-Node 3+2 Partition Run {run}/3 ---")
            run_5node_majority_partition(run)
            if run < 3:
                print(f"[{ts()}] Waiting 90s for stabilisation ...")
                time.sleep(90)

        print(f"\n[{ts()}] === EXPERIMENT E COMPLETE ===")
        print(f"[{ts()}] Waiting 90s before Experiment F ...")
        time.sleep(90)

        # -- Experiment F: 5-Node 2+2+1 Three-Way Partition (3 runs) -----------
        print(f"\n[{ts()}] === EXPERIMENT F: 5-NODE 2+2+1 THREE-WAY PARTITION - 3 runs ===")
        for run in range(1, 4):
            print(f"\n[{ts()}] --- 5-Node 2+2+1 Partition Run {run}/3 ---")
            run_5node_threeway_partition(run)
            if run < 3:
                print(f"[{ts()}] Waiting 90s for stabilisation ...")
                time.sleep(90)

        print(f"\n[{ts()}] === EXPERIMENT F COMPLETE ===")
        print(f"[{ts()}] Waiting 90s before Experiment G ...")
        time.sleep(90)

        # -- Experiment G: 5-Node Leader-on-Minority Partition (3 runs) --------
        print(f"\n[{ts()}] === EXPERIMENT G: 5-NODE LEADER-MINORITY PARTITION - 3 runs ===")
        print(f"[{ts()}]   Partition: [current_leader + 1 follower] (minority 2/5)")
        print(f"[{ts()}]   vs [3 remaining followers] (majority 3/5, elects new leader)")
        for run in range(1, 4):
            print(f"\n[{ts()}] --- Leader-Minority Partition Run {run}/3 ---")
            run_5node_leader_minority_partition(run)
            if run < 3:
                print(f"[{ts()}] Waiting 90s for cluster stabilisation before next run ...")
                time.sleep(90)

        print(f"\n[{ts()}] === EXPERIMENT G COMPLETE ===")

    except KeyboardInterrupt:
        print(f"\n[{ts()}] Interrupted. Running cleanup ...")
        cleanup_all()
        sys.exit(0)
    except Exception as exc:
        print(f"\n[{ts()}] UNHANDLED ERROR: {exc}")
        import traceback
        traceback.print_exc()
        cleanup_all()
        raise
    finally:
        # Always attempt to tear down 5-node cluster
        print(f"\n[{ts()}] Tearing down zookeeper5 ensemble ...")
        teardown_zookeeper5()

    with open(MASTER_LOG, "a", buffering=1) as mf:
        mf.write(f"\n{'=' * 70}\n")
        mf.write(f"ALL EXPERIMENTS COMPLETE: {ts()}\n")

    print_summary()


if __name__ == "__main__":
    main()
