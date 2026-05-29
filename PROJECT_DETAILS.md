# Project Details: Intelligent Pod Scheduler

This document details the architecture, algorithms, and technical implementations behind the Agentic AI Custom Kubernetes Scheduler.

---

## 1. Problem Statement
The default Kubernetes scheduler (`kube-scheduler`) uses a robust but rigid static scoring framework. It primarily relies on `NodeResourcesFit` (checking requested vs. allocatable resources). It does not dynamically adapt to real-time cluster behavior, such as preventing localized "hot-spotting" when many pods are deployed simultaneously, or utilizing machine-learning feedback loops to adjust its own scoring weights based on how nodes perform *after* placement.

Furthermore, when the default scheduler makes a decision, it provides no human-readable context. Operations teams are left wondering: *"Why did the scheduler put this pod on Node B instead of Node A?"*

This project solves both problems.

---

## 2. Intelligent Scheduler Architecture

The custom scheduler operates as an external, secondary scheduler running inside the cluster. It employs an **OBSERVE → REASON → DECIDE → ACT → FEEDBACK** loop.

### A. The Evaluation Metrics
Unlike the default scheduler, the intelligent scheduler assesses nodes across multiple dynamic dimensions:

| Metric | Intelligent Scheduler | Default Scheduler |
|---|---|---|
| **CPU Utilization %** | ✅ Uses real-time metrics-server data | ✅ (Based on static requests/limits) |
| **Memory Utilization %** | ✅ Uses real-time metrics-server data | ✅ (Based on static requests/limits) |
| **Pod Density** | ✅ Weighted factor to spread workloads evenly | ❌ Not heavily weighted by default |
| **Recency Bias** | ✅ Tracks placement history to prevent hot-spotting | ❌ Not considered |
| **Adaptive Weighting**| ✅ Online learning adjusts weights dynamically | ❌ Static scoring weights |

### B. The Scoring Formula
For each schedulable node, the AI computes a final score:
```
Score = (w_cpu × (1 - CPU%)) + (w_mem × (1 - Mem%)) + (w_pods × (1 - PodDensity)) + (w_hist × (1 - RecencyBias))
```
The node with the highest score wins the placement.

### C. Adaptive Feedback Loop (Online Learning)
After a pod is placed (ACT), the scheduler spawns a background thread (FEEDBACK) that waits 30 seconds. It then re-evaluates the node's metrics. If the node became overloaded (e.g., CPU > 80%), the scheduler dynamically penalizes its internal weights for future decisions, leaning heavier into spreading pods out (increasing `w_pods` weight).

---

## 3. Groq LLM Root Cause Analysis (RCA)

To solve the explainability problem, the scheduler integrates with Groq's ultra-fast LLM API (`llama-3.1-8b-instant` model).

1. After a pod is bound, the scheduler gathers all the raw metrics (CPU%, Mem%, Pod Counts) for all nodes, the current AI weights, and the final calculated scores.
2. It sends this context as a prompt to the LLM via an asynchronous background thread.
3. The LLM generates a 3-5 sentence RCA explaining *exactly why* the winning node was chosen over the others, highlighting the dominant factors (e.g., "Node B was chosen because despite Node A having slightly more CPU, Node A had received 4 placements in the last 60 seconds, triggering a recency penalty").
4. The scheduler patches the Pod object, injecting the LLM's response as the `intelligent-scheduler/rca` annotation.

---

## 4. Improvements Delivered

By comparing the `default` namespace against the `intelligent-workloads` namespace, the benchmark script demonstrates several tangible improvements:

1. **Load Balancing:** The standard deviation of pod distribution across worker nodes is consistently lower with the Intelligent Scheduler, proving a more equitable spread of workloads.
2. **Control-Plane Avoidance:** The custom scheduler programmatically identifies and ignores nodes with `NoSchedule` taints, preventing workloads from accidentally burdening the `control-plane`.
3. **True Explainability:** Every pod scheduled by the intelligent scheduler carries its own LLM-generated explanation, drastically reducing debugging and RCA time for cluster administrators.

---

## 5. Namespace Isolation

To maintain a clean separation of concerns and ensure accurate benchmarking:
- `scheduler-system`: Hosts the intelligent scheduler deployment, service account, and secrets.
- `default-workloads`: Target namespace for pods utilizing the standard Kubernetes scheduler.
- `intelligent-workloads`: Target namespace for pods utilizing the custom AI scheduler.

---

## 6. Metrics and Monitoring Pipeline

The scheduler is instrumented with the Prometheus Python client.
- **Node Score Gauges:** Live tracking of how the scheduler views each node.
- **Adaptive Weight Gauges:** Real-time visibility into the machine learning feedback loop.
- **LLM Counters & Latency:** Tracking the performance of the Groq API calls.

These metrics are scraped by a Helm-deployed Prometheus instance and visualized in a custom Grafana dashboard.
