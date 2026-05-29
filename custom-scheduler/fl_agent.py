#!/usr/bin/env python3
import os
import time
import threading
import logging
import random
from collections import defaultdict
from flask import Flask, jsonify
from kubernetes import client, config
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger("fl-agent")

NODE_NAME       = os.getenv("NODE_NAME", "unknown")
AGENT_PORT      = int(os.getenv("AGENT_PORT", "5050"))
ADAPT_INTERVAL  = 10

class QLearningModel:
    def __init__(self):
        self.weights = {'w_cpu': 0.40, 'w_mem': 0.30, 'w_pods': 0.20, 'w_hist': 0.10}
        self.initial_base_value = 100.0
        self.node_metrics = {
            'cpu_pct': 0.0, 'mem_pct': 0.0, 'pod_count': 0, 
            'health_status': 'Unknown', 'uptime_hours': 0.0,
            'initial_base_value': 100.0
        }
        self.q_table = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])
        self.alpha = 0.1
        self.gamma = 0.9
        self.epsilon = 0.2
        self.rounds = 0
        self.last_state = None
        self.last_action = None
        self.last_reward = 0.0
        self._lock = threading.Lock()

    def _discretize(self, cpu_pct, mem_pct, health):
        c = 0 if cpu_pct < 0.4 else (1 if cpu_pct < 0.7 else 2)
        m = 0 if mem_pct < 0.4 else (1 if mem_pct < 0.7 else 2)
        h = 0 if health == 'Ready' else 1
        return f"{c}_{m}_{h}"

    def get_reward(self, cpu_pct, mem_pct, health):
        if health != 'Ready': return -20.0
        if cpu_pct > 0.7 or mem_pct > 0.7: return -10.0
        if cpu_pct < 0.4 and mem_pct < 0.4: return 10.0
        return 5.0

    def adapt(self, cpu_pct, mem_pct, pod_count, health_status, uptime_hours):
        with self._lock:
            self.node_metrics = {
                'cpu_pct': round(cpu_pct, 4),
                'mem_pct': round(mem_pct, 4),
                'pod_count': pod_count,
                'health_status': health_status,
                'uptime_hours': round(uptime_hours, 2),
                'initial_base_value': self.initial_base_value
            }
            current_state = self._discretize(cpu_pct, mem_pct, health_status)
            current_reward = self.get_reward(cpu_pct, mem_pct, health_status)

            if self.last_state is not None:
                max_next = max(self.q_table[current_state])
                old_q = self.q_table[self.last_state][self.last_action]
                self.q_table[self.last_state][self.last_action] = old_q + self.alpha * (current_reward + self.gamma * max_next - old_q)
            
            if random.random() < self.epsilon:
                action = random.randint(0, 3)
            else:
                action = self.q_table[current_state].index(max(self.q_table[current_state]))
            
            lr = 0.08
            if action == 1: self.weights['w_cpu'] = min(0.80, self.weights['w_cpu'] + lr)
            elif action == 2: self.weights['w_mem'] = min(0.80, self.weights['w_mem'] + lr)
            elif action == 3: self.weights['w_pods'] = min(0.60, self.weights['w_pods'] + lr)
            else:
                self.weights['w_cpu'] = max(0.25, self.weights['w_cpu'] - lr * 0.3)
                self.weights['w_mem'] = max(0.15, self.weights['w_mem'] - lr * 0.3)
                self.weights['w_pods']= max(0.10, self.weights['w_pods']- lr * 0.2)

            self._normalize()
            self.rounds += 1
            self.last_state = current_state
            self.last_action = action
            self.last_reward = current_reward
            log.info(f"[RL-ADAPT] State:{current_state} Action:{action} Reward:{current_reward} Weights:{self.weights}")

    def _normalize(self):
        total = sum(self.weights.values())
        if total > 0:
            for k in self.weights:
                self.weights[k] = round(self.weights[k] / total, 4)

    def get_state(self):
        with self._lock:
            return {
                'node': NODE_NAME,
                'weights': self.weights.copy(),
                'metrics': self.node_metrics.copy(),
                'rounds': self.rounds,
                'rl_state': self.last_state,
                'rl_action': self.last_action,
                'rl_reward': self.last_reward
            }

LOCAL_MODEL = QLearningModel()

def _parse_cpu_to_m(s):
    s = str(s)
    if s.endswith('n'): return int(s[:-1]) / 1_000_000
    if s.endswith('u'): return int(s[:-1]) / 1_000
    if s.endswith('m'): return int(s[:-1])
    try: return float(s) * 1000
    except: return 0

def _parse_mem_to_bytes(s):
    s = str(s)
    units = {'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3, 'Ti': 1024**4, 'K': 1000, 'M': 1000**2, 'G': 1000**3}
    for suffix, mult in units.items():
        if s.endswith(suffix): return int(s[:-len(suffix)]) * mult
    try: return int(s)
    except: return 0

def collect_and_adapt():
    try: config.load_incluster_config()
    except: config.load_kube_config()
    api = client.CustomObjectsApi()
    core = client.CoreV1Api()
    while True:
        try:
            node = core.read_node(NODE_NAME)
            alloc = node.status.allocatable
            cpu_alloc = _parse_cpu_to_m(alloc.get('cpu', '1'))
            mem_alloc = _parse_mem_to_bytes(alloc.get('memory', '1Gi'))
            
            health_status = 'NotReady'
            for cond in node.status.conditions:
                if cond.type == 'Ready':
                    health_status = 'Ready' if cond.status == 'True' else 'NotReady'
                    break
            
            creation_time = node.metadata.creation_timestamp
            uptime_hours = 0.0
            if creation_time:
                now = datetime.now(timezone.utc)
                uptime_hours = (now - creation_time).total_seconds() / 3600.0

            metrics = api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes")
            cpu_used = 0
            mem_used = 0
            for item in metrics.get('items', []):
                if item['metadata']['name'] == NODE_NAME:
                    usage = item.get('usage', {})
                    cpu_used = _parse_cpu_to_m(usage.get('cpu', '0n'))
                    mem_used = _parse_mem_to_bytes(usage.get('memory', '0Ki'))
                    break
            cpu_pct = min(cpu_used / (cpu_alloc or 1), 1.0)
            mem_pct = min(mem_used / (mem_alloc or 1), 1.0)
            
            pods = core.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={NODE_NAME},status.phase=Running")
            pod_count = len(pods.items)

            LOCAL_MODEL.adapt(cpu_pct, mem_pct, pod_count, health_status, uptime_hours)
        except Exception as e:
            log.warning(f"[COLLECT] Failed: {e}")
        time.sleep(ADAPT_INTERVAL)

app = Flask(__name__)

@app.route('/weights')
def weights():
    return jsonify(LOCAL_MODEL.get_state())

@app.route('/healthz')
def healthz():
    return jsonify(status="ok", node=NODE_NAME, rounds=LOCAL_MODEL.rounds)

if __name__ == '__main__':
    threading.Thread(target=collect_and_adapt, daemon=True).start()
    app.run(host='0.0.0.0', port=AGENT_PORT, use_reloader=False)
