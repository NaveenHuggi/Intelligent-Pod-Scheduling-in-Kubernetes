#!/usr/bin/env python3
"""
Intelligent Pod Scheduler — Federated Learning Coordinator
===========================================================
Algorithm: OBSERVE -> REASON -> DECIDE -> ACT -> RCA -> FEDERATED FEEDBACK

This scheduler queries distributed FL Agent pods (one per worker node) to
collect their local models, runs Federated Averaging (FedAvg) to produce
a Global Model, and uses it to score nodes for pod placement.
"""

import time
import threading
import logging
import json
import os
from datetime import datetime
from collections import defaultdict

from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from flask import Flask, jsonify, Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
import requests as http_requests

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("intelligent-scheduler")

# ─── Config ─────────────────────────────────────────────────────────────────
SCHEDULER_NAME      = "intelligent-scheduler"
METRICS_PORT        = 8080
FL_AGENT_PORT       = 5050
FL_AGENT_LABEL      = "app=fl-agent"
FL_AGENT_NAMESPACE  = "scheduler-system"
FEEDBACK_DELAY_SEC  = 10

# ─── Prometheus Metrics ──────────────────────────────────────────────────────
DECISIONS_TOTAL = Counter('intelligent_scheduler_decisions_total', 'Total pod placement decisions', ['node'])
LATENCY = Histogram('intelligent_scheduler_latency_seconds', 'Time to schedule a pod', buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0])
NODE_SCORE = Gauge('intelligent_scheduler_node_score', 'Last computed score for each node', ['node'])
WEIGHT_CPU = Gauge('intelligent_scheduler_weight_cpu', 'Global FL weight for CPU')
WEIGHT_MEM = Gauge('intelligent_scheduler_weight_mem', 'Global FL weight for Memory')
WEIGHT_PODS = Gauge('intelligent_scheduler_weight_pods', 'Global FL weight for pod density')
WEIGHT_HIST = Gauge('intelligent_scheduler_weight_hist', 'Global FL weight for recency')
REASONING_EVENTS = Counter('intelligent_scheduler_reasoning_events_total', 'Total reasoning steps')
SCHEDULING_ERRORS = Counter('intelligent_scheduler_errors_total', 'Total scheduling errors', ['reason'])
LLM_INVOCATIONS = Counter('intelligent_scheduler_llm_invocations_total', 'Total FL RCA generations', ['status'])

# ─── In-memory RCA Store ────────────────────────────────────────────────────
RCA_STORE = {}
RCA_STORE_LOCK = threading.Lock()
RCA_STORE_MAX = 200

def store_rca(pod_name, rca_text):
    with RCA_STORE_LOCK:
        RCA_STORE[pod_name] = {"rca": rca_text, "timestamp": datetime.utcnow().isoformat() + "Z"}
        if len(RCA_STORE) > RCA_STORE_MAX:
            oldest = min(RCA_STORE, key=lambda k: RCA_STORE[k]["timestamp"])
            del RCA_STORE[oldest]

# ─── Distributed Federated Learning ─────────────────────────────────────────
class DistributedFLCoordinator:
    """
    Queries real FL Agent pods running on each worker node,
    collects their local models, and runs FedAvg.
    """
    def __init__(self, core_api):
        self.core_api = core_api
        self.global_weights = {'w_cpu': 0.40, 'w_mem': 0.30, 'w_pods': 0.20, 'w_hist': 0.10}
        self.local_models = {}  # node_name -> {weights, metrics, rounds}
        self._lock = threading.Lock()
        self._sync_gauges()

    def _sync_gauges(self):
        WEIGHT_CPU.set(self.global_weights['w_cpu'])
        WEIGHT_MEM.set(self.global_weights['w_mem'])
        WEIGHT_PODS.set(self.global_weights['w_pods'])
        WEIGHT_HIST.set(self.global_weights['w_hist'])

    def discover_agents(self):
        """Find all FL Agent pod IPs in the cluster."""
        agents = []
        try:
            pods = self.core_api.list_namespaced_pod(
                namespace=FL_AGENT_NAMESPACE,
                label_selector=FL_AGENT_LABEL
            )
            for pod in pods.items:
                if pod.status.phase == "Running" and pod.status.pod_ip:
                    agents.append({
                        'name': pod.metadata.name,
                        'ip': pod.status.pod_ip,
                        'node': pod.spec.node_name
                    })
        except Exception as e:
            log.warning(f"[FL] Failed to discover agents: {e}")
        return agents

    def collect_and_aggregate(self):
        """Query all FL agents and run FedAvg."""
        agents = self.discover_agents()
        if not agents:
            log.warning("[FL] No FL agents found — using cached global weights")
            return

        collected = {}
        for agent in agents:
            try:
                url = f"http://{agent['ip']}:{FL_AGENT_PORT}/weights"
                resp = http_requests.get(url, timeout=3)
                if resp.status_code == 200:
                    data = resp.json()
                    node_name = data.get('node', agent['node'])
                    collected[node_name] = data
                    log.info(f"[FL] Collected local model from agent on '{node_name}': {data['weights']}")
            except Exception as e:
                log.warning(f"[FL] Failed to query agent '{agent['name']}' at {agent['ip']}: {e}")

        if not collected:
            return

        # Run FedAvg
        with self._lock:
            self.local_models = collected
            new_global = {'w_cpu': 0, 'w_mem': 0, 'w_pods': 0, 'w_hist': 0}
            n = len(collected)

            for data in collected.values():
                w = data.get('weights', {})
                for k in new_global:
                    new_global[k] += w.get(k, 0)

            for k in new_global:
                self.global_weights[k] = new_global[k] / n

            # Normalize
            total = sum(self.global_weights.values())
            if total > 0:
                for k in self.global_weights:
                    self.global_weights[k] = round(self.global_weights[k] / total, 4)

            self._sync_gauges()
            log.info(f"[FL] FedAvg complete ({n} agents). Global Model: {self.global_weights}")

    def get_global_dict(self):
        with self._lock:
            return self.global_weights.copy()

    def get_local_models(self):
        with self._lock:
            return {k: v.copy() for k, v in self.local_models.items()}

    def get_local_for_node(self, node_name):
        with self._lock:
            data = self.local_models.get(node_name, {})
            return data.get('weights', self.global_weights).copy()


# ─── Node Metrics ────────────────────────────────────────────────────────────
class NodeMetricsCollector:
    def __init__(self, api_client):
        self.custom_api = client.CustomObjectsApi(api_client)
        self.core_api = client.CoreV1Api(api_client)

    def get_node_metrics(self):
        result = {}
        nodes = self.core_api.list_node()
        allocatable = {}
        for n in nodes.items:
            name = n.metadata.name
            if n.spec.taints:
                if any(t.effect in ("NoSchedule", "NoExecute") for t in n.spec.taints):
                    continue
            alloc = n.status.allocatable
            allocatable[name] = {
                'cpu_m': _parse_cpu_to_m(alloc.get('cpu', '0')),
                'mem_b': _parse_mem_to_bytes(alloc.get('memory', '0Ki')),
            }

        try:
            metrics = self.custom_api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes")
            usage_map = {}
            for item in metrics.get('items', []):
                name = item['metadata']['name']
                if name not in allocatable:
                    continue
                usage = item.get('usage', {})
                usage_map[name] = {
                    'cpu_m': _parse_cpu_to_m(usage.get('cpu', '0n')),
                    'mem_b': _parse_mem_to_bytes(usage.get('memory', '0Ki'))
                }
        except Exception:
            usage_map = {}

        pods = self.core_api.list_pod_for_all_namespaces(field_selector="status.phase=Running")
        pod_count = defaultdict(int)
        for p in pods.items:
            if p.spec.node_name:
                pod_count[p.spec.node_name] += 1

        for name, alloc in allocatable.items():
            usage = usage_map.get(name, {})
            cpu_total = alloc['cpu_m'] or 1
            mem_total = alloc['mem_b'] or 1
            result[name] = {
                'cpu_pct': min(usage.get('cpu_m', 0) / cpu_total, 1.0),
                'mem_pct': min(usage.get('mem_b', 0) / mem_total, 1.0),
                'pod_count': pod_count.get(name, 0),
            }
        return result


def _parse_cpu_to_m(s):
    s = str(s)
    if s.endswith('n'):   return int(s[:-1]) / 1_000_000
    if s.endswith('u'):   return int(s[:-1]) / 1_000
    if s.endswith('m'):   return int(s[:-1])
    try:                  return float(s) * 1000
    except:               return 0

def _parse_mem_to_bytes(s):
    s = str(s)
    units = {'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3, 'Ti': 1024**4, 'K': 1000, 'M': 1000**2, 'G': 1000**3}
    for suffix, mult in units.items():
        if s.endswith(suffix):
            return int(s[:-len(suffix)]) * mult
    try:    return int(s)
    except: return 0


# ─── Deterministic FL RCA ────────────────────────────────────────────────────
def generate_fl_rca(pod_name, node_metrics, scores, chosen_node, global_weights, local_weights):
    m = node_metrics.get(chosen_node, {})
    cpu = m.get('cpu_pct', 0) * 100
    mem = m.get('mem_pct', 0) * 100
    pods = m.get('pod_count', 0)
    score = scores.get(chosen_node, 0)

    factors = {
        'CPU Headroom': global_weights['w_cpu'] * (1 - cpu / 100),
        'Memory Headroom': global_weights['w_mem'] * (1 - mem / 100),
        'Pod Density': global_weights['w_pods'] * (1 - min(pods / 20, 1.0))
    }
    dominant = max(factors, key=factors.get)

    all_nodes_info = []
    for node, s in sorted(scores.items(), key=lambda x: x[1], reverse=True):
        nm = node_metrics.get(node, {})
        all_nodes_info.append(
            f"  - {node}: Score={s:.4f} | CPU={nm.get('cpu_pct', 0)*100:.1f}% | "
            f"Mem={nm.get('mem_pct', 0)*100:.1f}% | Pods={nm.get('pod_count', 0)}"
        )
    all_nodes_str = "\n".join(all_nodes_info)

    rca = (
        f"[FL-RCA] Decision for Pod '{pod_name}'\n"
        f"Selected Node: '{chosen_node}' (Score: {score:.4f})\n"
        f"Dominant Factor: '{dominant}'\n\n"
        f"--- Global FL Model (FedAvg) ---\n"
        f"  • w_cpu:  {global_weights['w_cpu']:.2f}\n"
        f"  • w_mem:  {global_weights['w_mem']:.2f}\n"
        f"  • w_pods: {global_weights['w_pods']:.2f}\n"
        f"  • w_hist: {global_weights['w_hist']:.2f}\n\n"
        f"--- Worker Node Candidates ---\n"
        f"{all_nodes_str}\n\n"
        f"--- Local Agent Insight ---\n"
        f"The FL agent on '{chosen_node}' contributed local weights (w_cpu={local_weights.get('w_cpu', 0):.2f}, w_mem={local_weights.get('w_mem', 0):.2f}) "
        f"which influenced this decision. This distributed approach adapts dynamically per-node."
    )
    LLM_INVOCATIONS.labels(status="success").inc()
    return rca


# ─── Agentic Scheduler ───────────────────────────────────────────────────────
class AgenticScheduler:
    def __init__(self):
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        self.api_client = client.ApiClient()
        self.core_api = client.CoreV1Api(self.api_client)
        self.metrics_collector = NodeMetricsCollector(self.api_client)
        self.placement_history = defaultdict(list)
        self.fl = DistributedFLCoordinator(self.core_api)

    def observe(self):
        try:
            return self.metrics_collector.get_node_metrics()
        except Exception as e:
            log.error(f"[OBSERVE] Failed: {e}")
            SCHEDULING_ERRORS.labels(reason="observe_failed").inc()
            return {}

    def reason(self, node_metrics, pod_name):
        # Collect from distributed agents before scoring
        self.fl.collect_and_aggregate()
        w = self.fl.get_global_dict()
        now = time.time()
        scores = {}
        reasoning_lines = []

        max_pods = max((m['pod_count'] for m in node_metrics.values()), default=1) or 1

        for node, m in node_metrics.items():
            cpu_pct = m['cpu_pct']
            mem_pct = m['mem_pct']
            pod_count = m['pod_count']
            pod_density = pod_count / (max_pods + 1)
            recent = [t for t in self.placement_history[node] if now - t < 60]
            recency_score = len(recent) / 10.0

            score = (
                w['w_cpu']  * (1 - cpu_pct) +
                w['w_mem']  * (1 - mem_pct) +
                w['w_pods'] * (1 - pod_density) +
                w['w_hist'] * (1 - min(recency_score, 1.0))
            )
            scores[node] = round(score, 4)
            NODE_SCORE.labels(node=node).set(score)
            REASONING_EVENTS.inc()

            line = f"  [{node}] cpu={cpu_pct*100:.1f}% mem={mem_pct*100:.1f}% pods={pod_count} recent={len(recent)} → score={score:.4f}"
            reasoning_lines.append((score, line))

        reasoning_lines.sort(key=lambda x: -x[0])
        best_node = max(scores, key=scores.get) if scores else None

        log.info(f"[REASON] Pod '{pod_name}' — Global FL Model: {w}")
        for _, line in reasoning_lines:
            marker = " ← BEST" if line.split(']')[0].split('[')[1] == best_node else ""
            log.info(line + marker)

        return scores, best_node

    def act(self, pod, node_name):
        binding = client.V1Binding(
            api_version="v1", kind="Binding",
            metadata=client.V1ObjectMeta(name=pod.metadata.name, namespace=pod.metadata.namespace),
            target=client.V1ObjectReference(api_version="v1", kind="Node", name=node_name)
        )
        try:
            self.core_api.create_namespaced_pod_binding(
                name=pod.metadata.name, namespace=pod.metadata.namespace,
                body=binding, _preload_content=False
            )
            DECISIONS_TOTAL.labels(node=node_name).inc()
            self.placement_history[node_name].append(time.time())
            cutoff = time.time() - 300
            self.placement_history[node_name] = [t for t in self.placement_history[node_name] if t > cutoff]
            log.info(f"[ACT] Bound '{pod.metadata.name}' → '{node_name}'")
            return True
        except ApiException as e:
            if e.status == 409:
                log.warning(f"[ACT] '{pod.metadata.name}' already bound")
            else:
                log.error(f"[ACT] Binding failed: {e.status} {e.reason}")
                SCHEDULING_ERRORS.labels(reason="bind_failed").inc()
            return False
        except ValueError:
            DECISIONS_TOTAL.labels(node=node_name).inc()
            self.placement_history[node_name].append(time.time())
            return True

    def annotate_rca(self, pod, rca_text):
        if not rca_text:
            return
        try:
            body = {"metadata": {"annotations": {"intelligent-scheduler/rca": rca_text[:1024]}}}
            self.core_api.patch_namespaced_pod(name=pod.metadata.name, namespace=pod.metadata.namespace, body=body)
        except Exception:
            pass

    def run(self):
        log.info(f"[SCHEDULER] Distributed FL Scheduler starting (schedulerName={SCHEDULER_NAME})")
        w = watch.Watch()

        while True:
            try:
                for event in w.stream(self.core_api.list_pod_for_all_namespaces, field_selector="status.phase=Pending"):
                    pod = event['object']
                    if pod.spec.node_name or pod.spec.scheduler_name != SCHEDULER_NAME or event['type'] not in ('ADDED', 'MODIFIED'):
                        continue

                    pod_name = pod.metadata.name
                    start_time = time.time()
                    log.info(f"[SCHEDULER] New pod: '{pod_name}'")

                    node_metrics = self.observe()
                    if not node_metrics:
                        continue

                    scores, best_node = self.reason(node_metrics, pod_name)
                    if not best_node:
                        continue

                    success = self.act(pod, best_node)
                    elapsed = time.time() - start_time
                    LATENCY.observe(elapsed)

                    if success:
                        log.info(f"[SCHEDULER] ✅ '{pod_name}' → '{best_node}' in {elapsed:.3f}s")
                        global_w = self.fl.get_global_dict()
                        local_w = self.fl.get_local_for_node(best_node)
                        rca = generate_fl_rca(pod_name, node_metrics, scores, best_node, global_w, local_w)
                        self.annotate_rca(pod, rca)
                        store_rca(pod_name, rca)

            except Exception as e:
                log.error(f"[SCHEDULER] Watch error: {e}")
                SCHEDULING_ERRORS.labels(reason="watch_error").inc()
                time.sleep(5)


# ─── Flask Metrics Server ────────────────────────────────────────────────────
app = Flask(__name__)
scheduler_instance = None

@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

@app.route('/healthz')
def healthz():
    fl_data = scheduler_instance.fl if scheduler_instance else None
    return jsonify(status="ok", scheduler=SCHEDULER_NAME,
                   weights=fl_data.get_global_dict() if fl_data else {})

@app.route('/weights')
def weights():
    if scheduler_instance:
        return jsonify(scheduler_instance.fl.get_global_dict())
    return jsonify({})

@app.route('/fl-agents')
def fl_agents():
    """Query all FL agents live and return their local model data."""
    if scheduler_instance:
        scheduler_instance.fl.collect_and_aggregate()
        return jsonify(scheduler_instance.fl.get_local_models())
    return jsonify({})

@app.route('/history')
def history():
    if scheduler_instance:
        return jsonify({k: len(v) for k, v in scheduler_instance.placement_history.items()})
    return jsonify({})

@app.route('/rca/<pod_name>')
def get_rca(pod_name):
    with RCA_STORE_LOCK:
        entry = RCA_STORE.get(pod_name)
    if entry:
        return jsonify(pod=pod_name, **entry)
    return jsonify(error=f"No RCA found for pod '{pod_name}'"), 404

@app.route('/rca')
def list_rcas():
    with RCA_STORE_LOCK:
        return jsonify({k: v for k, v in RCA_STORE.items()})


if __name__ == '__main__':
    scheduler_instance = AgenticScheduler()
    threading.Thread(
        target=lambda: app.run(host='0.0.0.0', port=METRICS_PORT, use_reloader=False),
        daemon=True
    ).start()
    log.info(f"[METRICS] Metrics server running on :{METRICS_PORT}")
    scheduler_instance.run()
