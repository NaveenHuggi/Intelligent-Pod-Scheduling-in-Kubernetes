#!/usr/bin/env bash
# =============================================================================
# collect_metrics.sh — Collect FL scheduling metrics to CSV
# =============================================================================
# Produces a CSV file with per-node metrics + FL weights + suitability scores
# so you can determine which node is most suitable for deployment at any point.
#
# Usage:  bash collect_metrics.sh [output-file.csv]
# =============================================================================
set -euo pipefail

OUT_FILE="${1:-metrics/scheduling_metrics_$(date +%Y%m%d_%H%M%S).csv}"
mkdir -p "$(dirname "$OUT_FILE")"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; NC='\033[0m'
log() { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*"; }
ok()  { echo -e "${GREEN}✅ $*${NC}"; }

log "Collecting FL scheduling metrics..."

# ── Get scheduler pod ────────────────────────────────────────────────────────
SCHED_POD=$(kubectl get pod -n scheduler-system -l app=intelligent-scheduler \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [[ -z "$SCHED_POD" ]]; then
  echo "ERROR: No intelligent-scheduler pod found."
  exit 1
fi

# ── Collect FL Agent data via scheduler ──────────────────────────────────────
FL_JSON=$(kubectl exec -n scheduler-system "$SCHED_POD" -- \
  python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8080/fl-agents').read().decode())" 2>/dev/null)

GLOBAL_JSON=$(kubectl exec -n scheduler-system "$SCHED_POD" -- \
  python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8080/weights').read().decode())" 2>/dev/null)

# ── Collect node metrics ─────────────────────────────────────────────────────
NODE_TOP=$(kubectl top nodes --no-headers 2>/dev/null)

# ── Collect pod distribution ─────────────────────────────────────────────────
POD_DIST_DEFAULT=$(kubectl get pods -n default-workloads -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | sort | uniq -c | awk '{print $2":"$1}')
POD_DIST_INTELLIGENT=$(kubectl get pods -n intelligent-workloads -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null | sort | uniq -c | awk '{print $2":"$1}')

# ── Generate CSV ─────────────────────────────────────────────────────────────
python3 - "$FL_JSON" "$GLOBAL_JSON" "$NODE_TOP" "$POD_DIST_DEFAULT" "$POD_DIST_INTELLIGENT" "$OUT_FILE" <<'PYEOF'
import sys, json, csv
from datetime import datetime

fl_json_str = sys.argv[1]
global_json_str = sys.argv[2]
node_top_str = sys.argv[3]
pod_dist_def_str = sys.argv[4]
pod_dist_int_str = sys.argv[5]
out_file = sys.argv[6]

timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Parse FL agent data
fl_agents = {}
try:
    fl_agents = json.loads(fl_json_str)
except:
    pass

# Parse global weights
global_w = {}
try:
    global_w = json.loads(global_json_str)
except:
    pass

# Parse node top
node_top = {}
for line in node_top_str.strip().split('\n'):
    parts = line.split()
    if len(parts) >= 5:
        node_top[parts[0]] = {
            'cpu_cores': parts[1],
            'cpu_pct': int(parts[2].rstrip('%')),
            'mem_bytes': parts[3],
            'mem_pct': int(parts[4].rstrip('%'))
        }

# Parse pod distribution
def parse_dist(s):
    d = {}
    for item in s.strip().split('\n'):
        if ':' in item:
            node, count = item.strip().split(':')
            d[node] = int(count)
    return d

pod_dist_default = parse_dist(pod_dist_def_str)
pod_dist_intelligent = parse_dist(pod_dist_int_str)

# Build rows
rows = []
all_nodes = sorted(set(list(node_top.keys()) + list(fl_agents.keys())))

for node in all_nodes:
    if 'control-plane' in node:
        continue

    nt = node_top.get(node, {})
    agent = fl_agents.get(node, {})
    w = agent.get('weights', {})
    m = agent.get('metrics', {})
    rounds = agent.get('rounds', 0)

    cpu_pct = m.get('cpu_pct', nt.get('cpu_pct', 0) / 100.0 if nt else 0)
    mem_pct = m.get('mem_pct', nt.get('mem_pct', 0) / 100.0 if nt else 0)
    pod_count = m.get('pod_count', 0)

    # Compute suitability score using global weights
    gw = global_w if global_w else {'w_cpu': 0.4, 'w_mem': 0.3, 'w_pods': 0.2, 'w_hist': 0.1}
    max_pods = max((a.get('metrics', {}).get('pod_count', 1) for a in fl_agents.values()), default=1) or 1
    pod_density = pod_count / (max_pods + 1)

    score = (
        gw.get('w_cpu', 0.4) * (1 - cpu_pct) +
        gw.get('w_mem', 0.3) * (1 - mem_pct) +
        gw.get('w_pods', 0.2) * (1 - pod_density) +
        gw.get('w_hist', 0.1) * 1.0  # no recency data from CLI
    )

    rows.append({
        'timestamp': timestamp,
        'node': node,
        'cpu_pct': round(cpu_pct * 100, 2),
        'mem_pct': round(mem_pct * 100, 2),
        'cpu_cores_used': nt.get('cpu_cores', 'N/A'),
        'mem_used': nt.get('mem_bytes', 'N/A'),
        'pod_count': pod_count,
        'pods_default_ns': pod_dist_default.get(node, 0),
        'pods_intelligent_ns': pod_dist_intelligent.get(node, 0),
        'fl_w_cpu': round(w.get('w_cpu', 0), 4),
        'fl_w_mem': round(w.get('w_mem', 0), 4),
        'fl_w_pods': round(w.get('w_pods', 0), 4),
        'fl_w_hist': round(w.get('w_hist', 0), 4),
        'fl_rounds': rounds,
        'global_w_cpu': round(gw.get('w_cpu', 0), 4),
        'global_w_mem': round(gw.get('w_mem', 0), 4),
        'global_w_pods': round(gw.get('w_pods', 0), 4),
        'global_w_hist': round(gw.get('w_hist', 0), 4),
        'suitability_score': round(score, 4),
        'recommendation': ''
    })

# Determine best node
if rows:
    best = max(rows, key=lambda r: r['suitability_score'])
    for r in rows:
        if r['node'] == best['node']:
            r['recommendation'] = 'BEST'
        elif r['suitability_score'] >= best['suitability_score'] * 0.95:
            r['recommendation'] = 'GOOD'
        elif r['suitability_score'] < best['suitability_score'] * 0.70:
            r['recommendation'] = 'AVOID'
        else:
            r['recommendation'] = 'OK'

# Write CSV
fieldnames = [
    'timestamp', 'node',
    'cpu_pct', 'mem_pct', 'cpu_cores_used', 'mem_used', 'pod_count',
    'pods_default_ns', 'pods_intelligent_ns',
    'fl_w_cpu', 'fl_w_mem', 'fl_w_pods', 'fl_w_hist', 'fl_rounds',
    'global_w_cpu', 'global_w_mem', 'global_w_pods', 'global_w_hist',
    'suitability_score', 'recommendation'
]

with open(out_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

# Print summary table
print()
print(f"{'Node':<28} {'CPU%':>6} {'Mem%':>6} {'Pods':>5} {'FL w_cpu':>9} {'FL w_mem':>9} {'Score':>8} {'Rec':>6}")
print("-" * 90)
for r in sorted(rows, key=lambda x: -x['suitability_score']):
    print(f"{r['node']:<28} {r['cpu_pct']:>5.1f}% {r['mem_pct']:>5.1f}% {r['pod_count']:>5} {r['fl_w_cpu']:>9.4f} {r['fl_w_mem']:>9.4f} {r['suitability_score']:>8.4f} {r['recommendation']:>6}")
print()
PYEOF

ok "Metrics saved to $OUT_FILE"
echo ""
log "CSV columns:"
echo "  timestamp, node, cpu_pct, mem_pct, cpu_cores_used, mem_used, pod_count,"
echo "  pods_default_ns, pods_intelligent_ns,"
echo "  fl_w_cpu, fl_w_mem, fl_w_pods, fl_w_hist, fl_rounds,"
echo "  global_w_cpu, global_w_mem, global_w_pods, global_w_hist,"
echo "  suitability_score, recommendation"
echo ""
echo "The node with the highest suitability_score is BEST for deployment."
echo "Recommendation key: BEST=deploy here, GOOD=acceptable, OK=fair, AVOID=stressed"
