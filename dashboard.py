import json
import subprocess
import threading
import time
from flask import Flask, render_template, jsonify

app = Flask(__name__)

# Cache the data to avoid spamming kubectl
CACHE = {
    "data": None,
    "last_updated": 0
}
CACHE_TTL = 3  # seconds

def _run_cmd(cmd):
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return ""

def fetch_data():
    now = time.time()
    if CACHE["data"] and now - CACHE["last_updated"] < CACHE_TTL:
        return CACHE["data"]

    data = {
        "nodes": [],
        "namespaces": {
            "default-workloads": 0,
            "intelligent-workloads": 0
        },
        "pod_distribution": {
            "default": {},
            "intelligent": {}
        },
        "ai_weights": {},
        "scheduler_metrics": {
            "decisions": 0,
            "llm_calls": 0,
            "errors": 0
        }
    }

    # 1. Fetch Node Metrics
    out_nodes = _run_cmd("kubectl top nodes --no-headers")
    for line in out_nodes.strip().split('\n'):
        parts = line.split()
        if len(parts) >= 5:
            node_name = parts[0]
            cpu_pct = parts[2].rstrip('%')
            mem_pct = parts[4].rstrip('%')
            data["nodes"].append({
                "name": node_name,
                "cpu": int(cpu_pct),
                "memory": int(mem_pct)
            })

    # 2. Fetch Pods & Namespaces
    out_pods = _run_cmd("kubectl get pods -A -o json")
    if out_pods:
        try:
            pods = json.loads(out_pods).get("items", [])
            for p in pods:
                ns = p["metadata"]["namespace"]
                if ns in data["namespaces"]:
                    data["namespaces"][ns] += 1
                
                # Distribution
                node = p.get("spec", {}).get("nodeName", "unassigned")
                if ns == "intelligent-workloads":
                    data["pod_distribution"]["intelligent"][node] = data["pod_distribution"]["intelligent"].get(node, 0) + 1
                elif ns == "default-workloads":
                    data["pod_distribution"]["default"][node] = data["pod_distribution"]["default"].get(node, 0) + 1
        except Exception:
            pass
            
    # 2.5 Fetch Namespace Resource Metrics
    data["ns_metrics"] = {
        "default-workloads": {"cpu": 0, "mem": 0},
        "intelligent-workloads": {"cpu": 0, "mem": 0}
    }
    out_top = _run_cmd("kubectl top pod -A --no-headers")
    for line in out_top.strip().split('\n'):
        parts = line.split()
        if len(parts) >= 4:
            ns = parts[0]
            if ns in data["ns_metrics"]:
                cpu = int(parts[2].replace('m', ''))
                mem = int(parts[3].replace('Mi', ''))
                data["ns_metrics"][ns]["cpu"] += cpu
                data["ns_metrics"][ns]["mem"] += mem

    # Calculate Average CPU per Node for both namespaces
    def_active_nodes = len(data["pod_distribution"]["default"])
    if def_active_nodes > 0:
        avg_m = data["ns_metrics"]["default-workloads"]["cpu"] / def_active_nodes
        data["ns_metrics"]["default-workloads"]["avg_cpu_per_node"] = round((avg_m / 4000.0) * 100, 2)
    else:
        data["ns_metrics"]["default-workloads"]["avg_cpu_per_node"] = 0

    int_active_nodes = len(data["pod_distribution"]["intelligent"])
    if int_active_nodes > 0:
        avg_m = data["ns_metrics"]["intelligent-workloads"]["cpu"] / int_active_nodes
        data["ns_metrics"]["intelligent-workloads"]["avg_cpu_per_node"] = round((avg_m / 4000.0) * 100, 2)
    else:
        data["ns_metrics"]["intelligent-workloads"]["avg_cpu_per_node"] = 0

    # 3. Fetch AI Scheduler Weights & Metrics via kubectl exec
    out_sched_pod = _run_cmd("kubectl get pod -n scheduler-system -l app=intelligent-scheduler -o jsonpath='{.items[0].metadata.name}'").strip()
    if out_sched_pod:
        try:
            # Fetch weights
            weights_json = _run_cmd(f'kubectl exec -n scheduler-system {out_sched_pod} -- python -c "import urllib.request; print(urllib.request.urlopen(\'http://localhost:8080/weights\').read().decode())"')
            if weights_json:
                data["ai_weights"] = json.loads(weights_json)
            
            # Fetch metrics
            metrics_txt = _run_cmd(f'kubectl exec -n scheduler-system {out_sched_pod} -- python -c "import urllib.request; print(urllib.request.urlopen(\'http://localhost:8080/metrics\').read().decode())"')
            if metrics_txt:
                decisions = 0
                errors = 0
                llm = 0
                for line in metrics_txt.split('\n'):
                    if line.startswith("intelligent_scheduler_decisions_total"):
                        decisions += float(line.split()[1])
                    if line.startswith("intelligent_scheduler_errors_total"):
                        errors += float(line.split()[1])
                    if line.startswith("intelligent_scheduler_llm_invocations_total"):
                        llm += float(line.split()[1])
                
                data["scheduler_metrics"]["decisions"] = int(decisions)
                data["scheduler_metrics"]["errors"] = int(errors)
                data["scheduler_metrics"]["llm_calls"] = int(llm)
            # Fetch FL Agent local models
            fl_json = _run_cmd(f'kubectl exec -n scheduler-system {out_sched_pod} -- python -c "import urllib.request; print(urllib.request.urlopen(\'http://localhost:8080/fl-agents\').read().decode())"')
            if fl_json:
                try:
                    data["fl_agents"] = json.loads(fl_json)
                except Exception:
                    data["fl_agents"] = {}
        except Exception:
            pass

    CACHE["data"] = data
    CACHE["last_updated"] = now
    return data

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def api_data():
    return jsonify(fetch_data())

if __name__ == '__main__':
    # Ensure templates directory exists
    subprocess.run("mkdir -p templates", shell=True)
    app.run(host='0.0.0.0', port=5000, debug=False)
