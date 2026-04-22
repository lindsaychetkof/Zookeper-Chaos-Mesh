import csv
import socket
import subprocess
import time
from pathlib import Path

from kazoo.client import KazooClient


PARTITION_FILE = "zk-partition.yaml"
RESULTS_CSV = Path("partition_timeline_clean.csv")

# UPDATE THESE TO MATCH zk-partition.yaml
MAJORITY_PODS = ["zk-0", "zk-1", "zk-2"]
MINORITY_PODS = ["zk-3", "zk-4"]

LOCAL_PORT = 2181
TEST_ZNODE = "/partition_test"
POLL_INTERVAL = 0.5
PARTITION_TIMEOUT = 90
HEAL_TIMEOUT = 60


def run_cmd(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def get_mode(pod_name):
    code, out, _ = run_cmd(["kubectl", "exec", pod_name, "--", "zkServer.sh", "status"])
    if code != 0:
        return None
    out = out.lower()
    if "mode: leader" in out:
        return "leader"
    if "mode: follower" in out:
        return "follower"
    return None


def delete_partition_if_present():
    run_cmd(["kubectl", "delete", "-f", PARTITION_FILE, "--ignore-not-found=true"])


def apply_partition():
    code, _, err = run_cmd(["kubectl", "apply", "-f", PARTITION_FILE])
    if code != 0:
        raise RuntimeError(f"Failed to apply partition: {err}")


def delete_partition():
    code, _, err = run_cmd(["kubectl", "delete", "-f", PARTITION_FILE, "--ignore-not-found=true"])
    if code != 0:
        raise RuntimeError(f"Failed to delete partition: {err}")


def is_port_open(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        return sock.connect_ex((host, port)) == 0


def start_port_forward(pod_name, local_port=2181):
    proc = subprocess.Popen(
        ["kubectl", "port-forward", f"pod/{pod_name}", f"{local_port}:2181"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    for _ in range(30):
        if is_port_open("127.0.0.1", local_port):
            return proc
        time.sleep(0.2)

    proc.terminate()
    raise RuntimeError(f"Port-forward to {pod_name} failed.")


def stop_port_forward(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


def try_write_via_localhost():
    zk = KazooClient(hosts="127.0.0.1:2181", timeout=3.0)
    try:
        zk.start(timeout=5)

        if not zk.exists(TEST_ZNODE):
            zk.create(TEST_ZNODE, b"start")

        zk.set(TEST_ZNODE, str(time.time_ns()).encode())
        zk.stop()
        zk.close()
        return True
    except Exception:
        try:
            zk.stop()
            zk.close()
        except Exception:
            pass
        return False


def try_write_to_pod(pod_name):
    pf = None
    try:
        pf = start_port_forward(pod_name, LOCAL_PORT)
        ok = try_write_via_localhost()
        stop_port_forward(pf)
        return ok
    except Exception:
        stop_port_forward(pf)
        return False


def all_zk_pods_running():
    code, out, _ = run_cmd(
        [
            "kubectl",
            "get",
            "pods",
            "-o",
            "jsonpath={range .items[*]}{.metadata.name}:{.status.phase}:{.status.containerStatuses[0].ready}{'\\n'}{end}",
        ]
    )
    if code != 0:
        return False

    lines = [line for line in out.splitlines() if line.startswith("zk-")]
    return len(lines) == 5 and all(":Running:true" in line for line in lines)


def append_row(row):
    file_exists = RESULTS_CSV.exists()
    with RESULTS_CSV.open("a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "trial",
                "A_partition_injected_at",
                "B_new_leader",
                "B_new_leader_at",
                "B_minus_A_leader_failover_time_s",
                "C_majority_write_pod",
                "C_majority_write_success_at",
                "C_minus_A_majority_write_recovery_time_s",
                "D_minority_failed_pod",
                "D_minority_write_failure_at",
                "D_minus_A_minority_failure_time_s",
                "E_partition_healed_at",
                "F_post_heal_write_pod",
                "F_post_heal_write_success_at",
                "F_minus_E_post_heal_write_recovery_time_s",
            ],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def main():
    # Safety reset
    delete_partition_if_present()

    if not all_zk_pods_running():
        raise RuntimeError("Not all 5 ZooKeeper pods are healthy before starting.")

    trial = int(time.time())

    print("Applying partition...")
    apply_partition()
    tA = time.time()
    print(f"A = partition injected at {tA:.3f}")

    B_pod = None
    tB = None
    C_pod = None
    tC = None
    D_pod = None
    tD = None

    start = time.time()
    while time.time() - start < PARTITION_TIMEOUT:
        # D: first failed write on minority
        if tD is None:
            for pod in MINORITY_PODS:
                ok = try_write_to_pod(pod)
                if not ok:
                    D_pod = pod
                    tD = time.time()
                    print(f"D = first failed write on minority at {tD:.3f} via {pod}")
                    break

        # B: first leader detected on majority
        if tB is None:
            for pod in MAJORITY_PODS:
                mode = get_mode(pod)
                if mode == "leader":
                    B_pod = pod
                    tB = time.time()
                    print(f"B = new leader detected on majority at {tB:.3f} via {pod}")
                    break

        # C: first successful write on majority, only AFTER B
        if tB is not None and tC is None:
            for pod in MAJORITY_PODS:
                ok = try_write_to_pod(pod)
                if ok:
                    C_pod = pod
                    tC = time.time()
                    print(f"C = first successful write on majority at {tC:.3f} via {pod}")
                    break

        if tB is not None and tC is not None and tD is not None:
            break

        time.sleep(POLL_INTERVAL)

    print("\nHealing partition...")
    delete_partition()
    tE = time.time()
    print(f"E = partition healed at {tE:.3f}")

    tF = None
    F_pod = None

    start_heal = time.time()
    while time.time() - start_heal < HEAL_TIMEOUT:
        if all_zk_pods_running():
            for pod in MAJORITY_PODS + MINORITY_PODS:
                ok = try_write_to_pod(pod)
                if ok:
                    F_pod = pod
                    tF = time.time()
                    print(f"F = first successful write after heal at {tF:.3f} via {pod}")
                    break
        if tF is not None:
            break
        time.sleep(POLL_INTERVAL)

    row = {
        "trial": trial,
        "A_partition_injected_at": tA,
        "B_new_leader": B_pod or "",
        "B_new_leader_at": tB or "",
        "B_minus_A_leader_failover_time_s": (tB - tA) if tB is not None else "",
        "C_majority_write_pod": C_pod or "",
        "C_majority_write_success_at": tC or "",
        "C_minus_A_majority_write_recovery_time_s": (tC - tA) if tC is not None else "",
        "D_minority_failed_pod": D_pod or "",
        "D_minority_write_failure_at": tD or "",
        "D_minus_A_minority_failure_time_s": (tD - tA) if tD is not None else "",
        "E_partition_healed_at": tE,
        "F_post_heal_write_pod": F_pod or "",
        "F_post_heal_write_success_at": tF or "",
        "F_minus_E_post_heal_write_recovery_time_s": (tF - tE) if tF is not None else "",
    }

    append_row(row)

    print("\nDurations:")
    print(f"B - A leader failover time: {row['B_minus_A_leader_failover_time_s']}")
    print(f"C - A majority write recovery time: {row['C_minus_A_majority_write_recovery_time_s']}")
    print(f"D - A minority failure time: {row['D_minus_A_minority_failure_time_s']}")
    print(f"F - E post-heal write recovery time: {row['F_minus_E_post_heal_write_recovery_time_s']}")
    print(f"\nSaved to {RESULTS_CSV}")


if __name__ == "__main__":
    main()