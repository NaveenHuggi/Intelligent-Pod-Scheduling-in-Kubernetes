# 🧠 Intelligent Pod Scheduling with Distributed Federated Learning

> A Kubernetes-native Intelligent Scheduler that uses **Distributed Federated Learning (FL)** driven by **Reinforcement Learning (Q-Learning)** to adaptively place pods across cluster nodes. Each worker node runs its own **FL Agent** that continuously learns from local resource pressure. The central scheduler aggregates these local models via **Federated Averaging (FedAvg)** to produce a globally optimized scheduling policy.

---

## 📋 Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Federated Learning Algorithm](#federated-learning-algorithm)
4. [Project Structure](#project-structure)
5. [Prerequisites](#prerequisites)
6. [Setup & Deployment](#setup--deployment)
7. [Running the Benchmark](#running-the-benchmark)
8. [Node Stress Testing](#node-stress-testing)
9. [Custom Dashboard](#custom-dashboard)
10. [Root Cause Analysis (RCA)](#root-cause-analysis-rca)
11. [Load Testing](#load-testing)
12. [Teardown](#teardown)

---

## Project Overview

Traditional Kubernetes scheduling uses a **static scoring algorithm** that evaluates nodes based on fixed internal priorities. It does not learn from previous decisions, adapt to real-time stress, or explain its placement rationale.

This project implements an **Intelligent Custom Scheduler** with a truly **distributed Federated Learning** architecture powered by local Reinforcement Learning:

- **FL Agent Pods** — deployed as a **DaemonSet**, one per worker node (4 workers total). Each agent monitors its own node's CPU, memory, health, uptime, and pod density in real time. It uses a **Tabular Q-Learning** model to adapt its weights based on the node's state.
- **Central Scheduler Pod** — discovers all FL Agent pods via the Kubernetes API, **queries their `/weights` endpoints** over HTTP, and runs **Federated Averaging (FedAvg)** to produce a **Global Model**.
- **Global Model Scoring** — the aggregated global weights are used to score and rank nodes for every new pod placement.
- **Deterministic RCA** — a Root Cause Analysis is generated for every scheduling decision, explaining exactly which FL weights and node metrics drove the placement.

### Key Differentiators vs Default Scheduler

| Feature | Default Scheduler | FL Intelligent Scheduler |
|---|---|---|
| **Scoring** | Static internal priorities | Adaptive FL global model |
| **Learning** | None | Distributed Federated Learning + Q-Learning |
| **Agent Pods** | None | 1 FL Agent per worker node (4 workers) |
| **CPU Awareness** | Basic fit check | Weighted CPU headroom with local RL adaptation |
| **Memory Awareness** | Basic fit check | Weighted memory headroom with local RL adaptation |
| **Pod Density** | Not considered | Actively balanced across nodes |
| **Recency Bias** | Not considered | Penalizes recently-loaded nodes |
| **Explainability** | None | Full FL-based RCA per decision |
| **Stress Response** | No adaptation | Local agents explore/exploit → FedAvg propagates globally |

---

## Architecture

### System Overview Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         HOST MACHINE (Ubuntu VM)                            │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                  kind Cluster — "scheduler-demo"                       │ │
│  │                                                                        │ │
│  │  ┌─────────────────┐  ┌──────────────────────┐ ┌────────────────────┐ │ │
│  │  │  Control Plane   │  │   Worker Node 1..4   │ │  Worker Node 2..3  │ │ │
│  │  │  (NoSchedule     │  │                      │ │                    │ │ │
│  │  │   taint)         │  │  ┌────────────────┐  │ │ ┌────────────────┐ │ │ │
│  │  │                  │  │  │  FL Agent Pod  │  │ │ │  FL Agent Pod  │ │ │ │
│  │  │  ┌────────────┐  │  │  │  (DaemonSet)   │  │ │ │  (DaemonSet)   │ │ │ │
│  │  │  │ API Server │  │  │  │                │  │ │ │                │ │ │ │
│  │  │  │  etcd      │  │  │  │ Monitors:      │  │ │ │ Monitors:      │ │ │ │
│  │  │  │ Scheduler  │  │  │  │ • CPU %        │  │ │ │ • CPU %        │ │ │ │
│  │  │  │ Controller │  │  │  │ • Memory %     │  │ │ │ • Memory %     │ │ │ │
│  │  │  │            │  │  │  │ • Health/Uptime│  │ │ │ • Health/Uptime│ │ │ │
│  │  │  │ metrics-   │◄─┼──┤  │ • Pod Count    │  │ │ │ • Pod Count    │ │ │ │
│  │  │  │ server     │  │  │  │                │  │ │ │                │ │ │ │
│  │  │  └────────────┘  │  │  │ Exposes:       │  │ │ │ Exposes:       │ │ │ │
│  │  │                  │  │  │  :5050/weights │  │ │ │  :5050/weights │ │ │ │
│  │  │  ┌────────────┐  │  │  └───────┬────────┘  │ │ └───────┬────────┘ │ │ │
│  │  │  │ Prometheus │  │  │          │            │ │         │          │ │ │
│  │  │  │ Grafana    │  │  │  ┌───────┴──────┐     │ │ ┌───────┴──────┐   │ │ │
│  │  │  │ (monitor)  │  │  │  │ Workload Pods│     │ │ │ Workload Pods│   │ │ │
│  │  │  └────────────┘  │  │  │ (default &   │     │ │ │ (default &   │   │ │ │
│  │  │                  │  │  │  intelligent) │     │ │ │  intelligent) │   │ │ │
│  │  │                  │  │  └──────────────┘     │ │ └──────────────┘   │ │ │
│  │  └─────────────────┘  └──────────────────────┘ └────────────────────┘ │ │
│  │                                                                        │ │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │ │
│  │  │              scheduler-system namespace                          │  │ │
│  │  │                                                                  │  │ │
│  │  │   ┌──────────────────────────────────────────────────────────┐   │  │ │
│  │  │   │            intelligent-scheduler pod                      │   │  │ │
│  │  │   │                                                          │   │  │ │
│  │  │   │   STEP 1: Discover FL Agent pods (K8s API)               │   │  │ │
│  │  │   │            ↓                                             │   │  │ │
│  │  │   │   STEP 2: HTTP GET /weights from each FL Agent           │   │  │ │
│  │  │   │            ↓                                             │   │  │ │
│  │  │   │   STEP 3: Run FedAvg → Global Model                     │   │  │ │
│  │  │   │            ↓                                             │   │  │ │
│  │  │   │   STEP 4: Score nodes using Global Model                 │   │  │ │
│  │  │   │            ↓                                             │   │  │ │
│  │  │   │   STEP 5: Bind pod to best-scoring node                  │   │  │ │
│  │  │   │            ↓                                             │   │  │ │
│  │  │   │   STEP 6: Generate deterministic FL-RCA                  │   │  │ │
│  │  │   │                                                          │   │  │ │
│  │  │   │   Exposes: :8080/metrics, /weights, /fl-agents, /rca     │   │  │ │
│  │  │   └──────────────────────────────────────────────────────────┘   │  │ │
│  │  └──────────────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │  Custom Flask Dashboard (:5000)                                        │ │
│  │  • Side-by-side comparison: Default vs Intelligent scheduler           │ │
│  │  • Average CPU/Node tracking for workloads                             │ │
│  │  • Live FL Agent cards showing RL state, reward, uptime, and health    │ │
│  │  • Real-time namespace resource metrics (CPU/Mem)                      │ │
│  │  • Pod density distribution charts                                     │ │
│  │  • FL Global Weights radar chart                                       │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
metrics-server ──► FL Agent (Worker 1) ──► /weights ──┐
                                                      ├──► Scheduler (FedAvg) ──► Global Model ──► Score ──► Bind
metrics-server ──► FL Agent (Worker 2) ──► /weights ──┘                                                       │
                                                                                                                                                                      ▼
                                                                                                         RCA Annotation
```

### FL Scheduling Metrics

The FL system considers the following metrics at multiple levels:

#### Metrics Collected by Each FL Agent (Per-Node, Every 10s)

| Metric | Source | How It's Used |
|---|---|---|
| **Initial Base Value** | Hardcoded Baseline | Anchors the starting values for RL state evaluation |
| **Health Status** | K8s `NodeReady` Condition | Triggers immediate massive penalty if node goes NotReady |
| **CPU Utilization %** | `metrics-server` | Defines the CPU axis of the RL State |
| **Memory Utilization %** | `metrics-server` | Defines the Memory axis of the RL State |
| **Pod Count** | K8s API | Reported to scheduler for pod density scoring |
| **Node Uptime (hours)** | Node `creationTimestamp` | Logged and tracked for long-term node health analysis |

#### Metrics Used by Central Scheduler (Per-Decision)

| Metric | Weight Key | Description | Scoring Formula |
|---|---|---|---|
| **CPU Utilization %** | `w_cpu` | How much CPU headroom the node has | `w_cpu × (1 - CPU%)` — lower CPU usage = higher score |
| **Memory Utilization %** | `w_mem` | How much memory headroom the node has | `w_mem × (1 - Mem%)` — lower memory usage = higher score |
| **Pod Density** | `w_pods` | How many pods already run on the node relative to the busiest node | `w_pods × (1 - PodDensity)` — fewer pods = higher score |
| **Recency Bias** | `w_hist` | How many pods were recently placed on this node (last 60s) | `w_hist × (1 - RecencyScore)` — less recent activity = higher score |

#### Final Node Score Formula

```
Score(node) = w_cpu  × (1 - CPU_utilization)
            + w_mem  × (1 - Memory_utilization)
            + w_pods × (1 - Pod_density)
            + w_hist × (1 - Recency_bias)
```

Where `w_cpu`, `w_mem`, `w_pods`, `w_hist` come from the **FedAvg Global Model** (average of all FL Agent local models).

---

## Federated Learning Algorithm

### FL Agent (per worker node)

Every 10 seconds, each FL Agent runs a **Tabular Q-Learning** iteration:
1. **Observe State**: Discretizes current CPU%, Memory%, and Node Health into a finite state (e.g., `LOW_MED_READY`).
2. **Select Action**: Uses an epsilon-greedy policy to either explore or exploit the Q-table to choose an action (increase `w_cpu`, increase `w_mem`, etc.).
3. **Calculate Reward**: Computes a reward based on the *new* state (high penalty if CPU/Mem > 70% or NotReady, high reward if < 40%).
4. **Update Q-Table**: Updates the state-action value using the Bellman equation.
5. **Expose Models**: Normalizes the new weights and exposes them via `GET /weights` along with extra metrics (Uptime, Health Status, Initial Base Value).

### Central Scheduler (per pod placement)

```
OBSERVE → COLLECT FL AGENTS → FedAvg → REASON → DECIDE → ACT → RCA
```

1. **OBSERVE**: Collect real-time node metrics from metrics-server
2. **COLLECT**: HTTP GET each FL Agent's `/weights` endpoint
3. **FedAvg**: `Global_w[k] = (1/N) × Σ Local_w[k][i]` for all N agents
4. **REASON**: Score each node using the Global Model
5. **DECIDE**: Select highest-scoring node
6. **ACT**: Bind pod to node
7. **RCA**: Generate deterministic analysis citing the FL weights and metrics

---

## Project Structure

```
Course Project/
├── custom-scheduler/
│   ├── scheduler.py           # Central FL Scheduler + FedAvg Coordinator
│   ├── fl_agent.py            # Distributed Q-Learning Agent (runs per node)
│   ├── Dockerfile             # Scheduler container image
│   ├── Dockerfile.agent       # FL Agent container image
│   └── requirements.txt       # Python dependencies
├── manifests/
│   ├── kind-cluster.yaml      # kind cluster (1 CP + 4 workers)
│   ├── namespace.yaml         # Namespace definitions
│   ├── scheduler-rbac.yaml    # Scheduler RBAC
│   ├── scheduler-deploy.yaml  # Scheduler Deployment + Service
│   └── fl-agent-daemonset.yaml # FL Agent DaemonSet + RBAC
├── benchmark/
│   └── benchmark.sh           # Automated benchmark
├── load-test/
│   ├── locust-test.py         # HTTP load test
│   └── stress-node.yaml       # Node stress container
├── templates/
│   └── index.html             # Dashboard frontend
├── RCA/                       # Exported RCA reports
├── metrics/                   # Exported CSV metric reports
├── dashboard.py               # Dashboard backend (Flask)
├── export_rcas.sh             # Export RCA annotations
├── collect_metrics.sh         # Export FL node metrics to CSV
├── stress_node.sh             # Node stress test script (dynamic/random support)
├── setup.sh                   # Full automated setup
└── README.md
```

---

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| `docker` | 20+ | Container runtime |
| `kind` | 0.20+ | Local Kubernetes cluster |
| `kubectl` | 1.27+ | Kubernetes CLI |
| `helm` | 3.x | Prometheus/Grafana |
| `python3` | 3.9+ | Dashboard + scheduler |
| `flask` | 2.x+ | Dashboard web server |

---

## Setup & Deployment

### 1. Run Full Setup
```bash
bash setup.sh
```

*(Note: If you need to completely reset the cluster or apply new node configurations, run: `kind delete cluster --name scheduler-demo && bash setup.sh` instead)*

This will:
- Create a kind cluster (1 control-plane + **4 workers**)
- Install metrics-server
- Install Prometheus & Grafana
- Build **two** Docker images: `intelligent-scheduler` and `fl-agent`
- Deploy the scheduler pod and the FL Agent DaemonSet (4 agents)

### 2. Verify All Pods
```bash
kubectl get pods -n scheduler-system -o wide
```

Expected output:
```
NAME                                     READY   STATUS    NODE
intelligent-scheduler-xxx                1/1     Running   scheduler-demo-worker2
fl-agent-aaa                             1/1     Running   scheduler-demo-worker
fl-agent-bbb                             1/1     Running   scheduler-demo-worker2
fl-agent-ccc                             1/1     Running   scheduler-demo-worker3
fl-agent-ddd                             1/1     Running   scheduler-demo-worker4
```

### 3. Verify FL Agents
```bash
# Check an agent's local Q-learning model
kubectl logs -n scheduler-system -l app=fl-agent --tail=10
```

---

## Running the Benchmark

You can specify a custom number of pods using the `--pods` argument. By default, it tests with 20 pods.

```bash
bash benchmark/benchmark.sh --pods 50
```

At the end of the benchmark, an ASCII table matching the format of the SDQN research paper will be output to directly compare Average CPU Utilization across nodes. This table is also automatically saved to `default_results.txt` and `intelligent_results.txt` in the main project directory.

Additionally, the benchmark automatically exports the complete live logs for all running FL agents into the `logs/` directory for post-analysis. These files are refreshed every time you run the benchmark.

After the benchmark, export the FL-generated RCAs:
```bash
bash export_rcas.sh
ls RCA/
ls logs/
```

---

## Node Stress Testing

The stress script supports **targeting specific nodes** and **selecting the stress type** (CPU, memory, or both).

### Apply Stress (Targeted)
```bash
# CPU-only stress on worker3
bash stress_node.sh apply scheduler-demo-worker3 cpu

# Memory-only stress on worker2
bash stress_node.sh apply scheduler-demo-worker2 mem

# Both CPU + Memory on worker
bash stress_node.sh apply scheduler-demo-worker both
```

### Apply Dynamic RL Training Stress (Random Mode)
To observe the RL model learning over time, use the `random` mode. This toggles heavy stress on and off periodically (e.g. 40s on, 20s off) to simulate bursty loads and force the RL agent to adapt its Q-values.
```bash
bash stress_node.sh random scheduler-demo-worker3 cpu
```

### Observe FL Adaptation
You can watch the FL agent's local model parameters (State, Action, Reward, Weights) updating in real-time as it learns via Q-Learning:

```bash
# Watch live RL logs for the agent on worker 2
POD=$(kubectl get pod -n scheduler-system -l app=fl-agent --field-selector spec.nodeName=scheduler-demo-worker2 -o jsonpath='{.items[0].metadata.name}')
kubectl logs -n scheduler-system -f $POD
```

Alternatively, query the weights manually:
```bash
kubectl exec -n scheduler-system <fl-agent-on-worker3> -- \
  python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:5050/weights').read().decode())"
```

The stressed agent's `w_cpu` (or `w_mem`) will increase as the threshold is breached. After FedAvg, the global model deprioritizes the stressed node.

### Remove Stress
```bash
bash stress_node.sh delete
```

---

## Custom Dashboard

```bash
python3 dashboard.py
# Open: http://localhost:5000
```

### Dashboard Layout

| Row | Content |
|---|---|
| **Stats Bar** | Default Pods, FL Pods, FL Decisions, FL RCAs, FL Agents count, Adaptation Rounds |
| **Row 1** | Side-by-side Total CPU/Mem and **Avg CPU/Node (%)** per namespace |
| **Row 2** | Side-by-side Pod Density distribution charts |
| **Row 3** | Default: "N/A" (static) vs Intelligent: Live FL Global Weights radar chart |
| **Row 4** | 🛰️ **Distributed FL Agents** — Live cards for each worker node showing: local weights, node metrics, RL State, RL Reward, Health Status, and Uptime |

---

## Root Cause Analysis (RCA)

Every scheduling decision includes a formatted, human-readable RCA based on the Federated Learning calculations:

### Example RCA
```text
[FL-RCA] Decision for Pod 'test-app-intelligent-7cfbc78599-4s7r9'
Selected Node: 'scheduler-demo-worker4' (Score: 0.8840)
Dominant Factor: 'CPU Headroom'

--- Global FL Model (FedAvg) ---
  • w_cpu:  0.25
  • w_mem:  0.15
  • w_pods: 0.10
  • w_hist: 0.49

--- Worker Node Candidates ---
  - scheduler-demo-worker4: Score=0.8840 | CPU=16.5% | Mem=7.3% | Pods=5
  - scheduler-demo-worker: Score=0.8619 | CPU=15.3% | Mem=7.1% | Pods=7
  - scheduler-demo-worker3: Score=0.8346 | CPU=25.9% | Mem=7.5% | Pods=7
  - scheduler-demo-worker2: Score=0.8149 | CPU=15.8% | Mem=4.8% | Pods=7

--- Local Agent Insight ---
The FL agent on 'scheduler-demo-worker4' contributed local weights (w_cpu=0.25, w_mem=0.15) which influenced this decision. This distributed approach adapts dynamically per-node.
```

### Export All RCAs
```bash
bash export_rcas.sh
cat RCA/test-app-intelligent-*.txt
```

---

## Load Testing

### HTTP Load Test (Locust)
```bash
kubectl port-forward svc/test-app-intelligent-svc 8081:80 -n intelligent-workloads &
locust -f load-test/locust-test.py --host http://localhost:8081 \
  --users 50 --spawn-rate 5 --run-time 60s --headless
```

### Node Stress + Benchmark
```bash
bash stress_node.sh random scheduler-demo-worker3 both &
bash benchmark/benchmark.sh --pods 20
bash stress_node.sh delete
```

---

## Teardown

```bash
kind delete cluster --name scheduler-demo
```
