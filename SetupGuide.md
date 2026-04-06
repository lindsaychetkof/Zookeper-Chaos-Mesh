# ZooKeeper Chaos Engineering — Setup Guide

A distributed systems project that deploys a 3-node ZooKeeper ensemble on Kubernetes and uses Chaos Mesh to inject failures, measuring availability and recovery behavior.

---

## Prerequisites

- Windows 10/11
- Docker Desktop ([docker.com](https://docker.com)) — open it and confirm "Engine running" before proceeding
- PowerShell run as Administrator for installation steps

> ⚠️ **Do NOT use the Bitnami ZooKeeper Helm chart.** It is now behind a paywall and will fail with `ImagePullBackOff`. Use the plain manifest in Step 2 instead.

---

## Step 1 — Install Tooling

**Install Chocolatey** (run PowerShell as Administrator):

```powershell
Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
```

Close and reopen PowerShell, then install minikube, kubectl, and helm:

```powershell
choco install minikube kubernetes-helm git
```

Close and reopen PowerShell again, then start minikube:

```powershell
minikube start --memory=4096 --cpus=4
```

Verify the cluster is up:

```powershell
kubectl get nodes
```

Expected output:
```
NAME       STATUS   ROLES           AGE   VERSION
minikube   Ready    control-plane   1m    v1.35.1
```

---

## Step 2 — Deploy ZooKeeper (3 nodes)

Save the following as `zookeeper.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: zookeeper-headless
spec:
  clusterIP: None
  selector:
    app: zookeeper
  ports:
    - port: 2181
      name: client
    - port: 2888
      name: peer
    - port: 3888
      name: leader-election
---
apiVersion: v1
kind: Service
metadata:
  name: zookeeper
spec:
  selector:
    app: zookeeper
  ports:
    - port: 2181
      name: client
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: zookeeper
spec:
  selector:
    matchLabels:
      app: zookeeper
  serviceName: zookeeper-headless
  replicas: 3
  template:
    metadata:
      labels:
        app: zookeeper
    spec:
      initContainers:
        - name: init-myid
          image: busybox
          command:
            - sh
            - -c
            - |
              ORDINAL=$(hostname | rev | cut -d- -f1 | rev)
              mkdir -p /data
              echo $((ORDINAL + 1)) > /data/myid
          volumeMounts:
            - name: data
              mountPath: /data
      containers:
        - name: zookeeper
          image: zookeeper:3.9.3
          ports:
            - containerPort: 2181
            - containerPort: 2888
            - containerPort: 3888
          env:
            - name: ZOO_MY_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: ZOO_SERVERS
              value: "server.1=zookeeper-0.zookeeper-headless:2888:3888;2181 server.2=zookeeper-1.zookeeper-headless:2888:3888;2181 server.3=zookeeper-2.zookeeper-headless:2888:3888;2181"
          volumeMounts:
            - name: data
              mountPath: /data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
```

Apply it (update the path to wherever you saved the file):

```powershell
kubectl apply -f C:\Users\YOUR_USERNAME\Desktop\zookeeper.yaml
```

Wait ~2 minutes, then verify all 3 pods are running:

```powershell
kubectl get pods
```

Expected output:
```
NAME          READY   STATUS    RESTARTS   AGE
zookeeper-0   1/1     Running   0          2m
zookeeper-1   1/1     Running   0          2m
zookeeper-2   1/1     Running   0          2m
```

Verify leader election — exactly one node should say `leader`, the others `follower`:

```powershell
kubectl exec zookeeper-0 -- zkServer.sh status
kubectl exec zookeeper-1 -- zkServer.sh status
kubectl exec zookeeper-2 -- zkServer.sh status
```

---

## Step 3 — Install Chaos Mesh

```powershell
helm repo add chaos-mesh https://charts.chaos-mesh.org
helm repo update
helm install chaos-mesh chaos-mesh/chaos-mesh --namespace=chaos-mesh --create-namespace
```

Verify:

```powershell
kubectl get pods -n chaos-mesh
```

Expected output (all pods `Running`):
```
chaos-controller-manager-...   1/1   Running   0   1m
chaos-daemon-...               1/1   Running   0   1m
chaos-dashboard-...            1/1   Running   0   1m
chaos-dns-server-...           1/1   Running   0   1m
```

---

## Step 4 — Python Workload Script

Install the ZooKeeper Python client:

```powershell
pip install kazoo
```

Save the following as `workload.py`:

```python
import time
from datetime import datetime
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException

ZK_HOST = "127.0.0.1:2181"
INTERVAL = 0.5  # seconds between operations

zk = KazooClient(hosts=ZK_HOST)
zk.start()
zk.ensure_path("/test")

counter = 0
while True:
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    try:
        zk.set("/test", str(counter).encode())
        data, _ = zk.get("/test")
        print(f"[{timestamp}] OK  - wrote {counter}, read back {data.decode()}")
        counter += 1
    except KazooException as e:
        print(f"[{timestamp}] ERROR - {e}")
    except Exception as e:
        print(f"[{timestamp}] ERROR - {e}")
    time.sleep(INTERVAL)
```

**Running the workload requires two PowerShell windows:**

Window 1 — port-forward (keep this running the entire time):
```powershell
kubectl port-forward svc/zookeeper 2181:2181
```

Window 2 — run the workload script:
```powershell
python C:\Users\YOUR_USERNAME\Desktop\workload.py
```

Baseline expected output (no faults injected):
```
[21:14:50.190] OK  - wrote 0, read back 0
[21:14:50.718] OK  - wrote 1, read back 1
[21:14:51.250] OK  - wrote 2, read back 2
```

---

## Running Experiments

Experiments are applied in a third PowerShell window while the port-forward and workload are running. Each experiment is a YAML file applied with `kubectl apply` and removed with `kubectl delete`.

**General pattern:**
```powershell
kubectl apply -f experiment.yaml   # inject fault — watch workload output
kubectl delete -f experiment.yaml  # remove fault — observe recovery
```

See the `experiments/` folder for the four scenario YAML files.

---

## Useful Diagnostic Commands

```powershell
kubectl get pods                     # check pod status
kubectl logs zookeeper-0             # see why a pod crashed
kubectl describe pod zookeeper-0     # detailed events including image pull errors
kubectl exec zookeeper-0 -- zkServer.sh status  # check leader/follower role
```

---

## Versions Confirmed Working

| Tool | Version |
|------|---------|
| Kubernetes (minikube) | v1.35.1 |
| ZooKeeper image | 3.9.3 (docker.io/zookeeper) |
| Chaos Mesh | latest via helm |
| Python kazoo | latest via pip |

---

## Troubleshooting

**`ImagePullBackOff`** — you used the Bitnami chart. Run `helm uninstall zookeeper` and use the `kubectl apply -f zookeeper.yaml` approach from Step 2.

**`CrashLoopBackOff`** — run `kubectl logs zookeeper-0` to see the error. If it says `No such file or directory`, the image is incompatible with the chart. Use the plain manifest.

**`helm` not recognized** — close and reopen PowerShell after installing with Chocolatey.

**Port-forward drops** — if the workload script starts showing errors and you didn't inject a fault, check that the Window 1 port-forward is still running.
