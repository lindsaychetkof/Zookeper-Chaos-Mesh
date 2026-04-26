# ZooKeeper Chaos Engineering with Chaos Mesh

Fault-injection study of Apache ZooKeeper under leader failure and network partitions using Kubernetes, Minikube, and Chaos Mesh.

## Key Results
| Experiment | Ensemble | Failure | Result |
|---|---:|---|---|
| Leader pod kill | 3 nodes | Kill active leader | Re-election avg ~3.6s |
| Leader minority partition | 5 nodes | 3–2 partition | Write blackout ~6.8s |
| Leader majority partition | 5 nodes | 3–2 partition | 0 write disruption on majority |

## Stack
Python, Kubernetes, Minikube, Docker, Chaos Mesh, Apache ZooKeeper

## Repo Structure
- `python_files/`: monitoring and experiment runners
- `chaos_yamls/`: Chaos Mesh fault specs
- `zookeeper_yamls/`: ZooKeeper deployment configs
- `graphs/`: experiment visualizations
- `logs/`: raw run outputs
- `SetupGuide.md`: reproduction instructions

## Main Artifacts
- Final report: `CS 390 FINAL REPORT CHAOS KEEPERS.pdf`
- Presentation: `CS390 Final Presentation.pdf`
