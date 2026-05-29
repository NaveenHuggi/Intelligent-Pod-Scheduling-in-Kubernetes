#!/usr/bin/env bash
# =============================================================================
# benchmark.sh — Compare Default vs Intelligent Scheduler
# =============================================================================
# Usage: bash benchmark/benchmark.sh [--pods N]
# =============================================================================
set -euo pipefail

# ── Config ───────────────────────────────────────────────────────────────────
PODS=20

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --pods) PODS="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

NS_DEFAULT="default-workloads"
NS_INTELLIGENT="intelligent-workloads"
WAIT_TIMEOUT=${WAIT_TIMEOUT:-120s}
RESULTS_FILE="benchmark/results-$(date +%Y%m%d-%H%M%S).txt"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*"; }
ok()   { echo -e "${GREEN}✅ $*${NC}"; }
warn() { echo -e "${YELLOW}⚠️  $*${NC}"; }
err()  { echo -e "${RED}❌ $*${NC}"; exit 1; }

# ── Helper: pod distribution across nodes ────────────────────────────────────
pod_distribution() {
  local label=$1
  local ns=$2
  kubectl get pods -n "$ns" -l "app=$label" \
    -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null \
    | sort | uniq -c | sort -rn
}

# ── Helper: scheduling latency (creation → scheduled) ────────────────────────
scheduling_latency() {
  local label=$1
  local ns=$2
  local total=0
  local count=0
  while IFS= read -r pod; do
    local created scheduled
    created=$(kubectl get pod "$pod" -n "$ns" \
      -o jsonpath='{.metadata.creationTimestamp}' 2>/dev/null)
    scheduled=$(kubectl get pod "$pod" -n "$ns" \
      -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].lastTransitionTime}' 2>/dev/null)
    if [[ -n "$created" && -n "$scheduled" ]]; then
      # calculate diff in seconds using Python
      diff=$(python3 -c "
from datetime import datetime
fmt = '%Y-%m-%dT%H:%M:%SZ'
c = datetime.strptime('$created', fmt)
s = datetime.strptime('$scheduled', fmt)
print(max(0, (s - c).total_seconds()))
" 2>/dev/null || echo 0)
      total=$(python3 -c "print($total + $diff)" 2>/dev/null || echo "$total")
      ((count++)) || true
    fi
  done < <(kubectl get pods -n "$ns" -l "app=$label" \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null)

  if [[ $count -gt 0 ]]; then
    python3 -c "print(f'{$total/$count:.3f}')" 2>/dev/null || echo "N/A"
  else
    echo "N/A"
  fi
}

# ── Helper: std deviation of pod counts across nodes ─────────────────────────
load_balance_score() {
  local label=$1
  local ns=$2
  local counts
  counts=$(kubectl get pods -n "$ns" -l "app=$label" \
    -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' 2>/dev/null \
    | sort | uniq -c | awk '{print $1}')

  if [[ -z "$counts" ]]; then echo "N/A"; return; fi

  python3 -c "
import math, sys
vals = [int(x) for x in '''$counts'''.split()]
if not vals:
    print('N/A')
    sys.exit()
mean = sum(vals)/len(vals)
variance = sum((x-mean)**2 for x in vals)/len(vals)
print(f'{math.sqrt(variance):.4f}  (mean={mean:.1f}, nodes={len(vals)})')
" 2>/dev/null || echo "N/A"
}

# ── Phase 1: Cleanup ─────────────────────────────────────────────────────────
phase1_cleanup() {
  log "Phase 1: Cleaning up previous test deployments..."
  kubectl delete deployment test-app-default -n "$NS_DEFAULT" --ignore-not-found=true
  kubectl delete deployment test-app-intelligent -n "$NS_INTELLIGENT" --ignore-not-found=true
  kubectl wait --for=delete pod -l "app=test-app-default" -n "$NS_DEFAULT" --timeout=60s 2>/dev/null || true
  kubectl wait --for=delete pod -l "app=test-app-intelligent" -n "$NS_INTELLIGENT" --timeout=60s 2>/dev/null || true
  ok "Cleanup complete"
}

# ── Phase 2: Default Scheduler Test ──────────────────────────────────────────
phase2_default() {
  log "Phase 2: Deploying $PODS pods with DEFAULT scheduler..."
  kubectl apply -f manifests/test-app-default.yaml

  # Scale to requested pod count
  kubectl scale deployment test-app-default --replicas="$PODS" -n "$NS_DEFAULT"

  log "Waiting for pods to be scheduled (timeout: $WAIT_TIMEOUT)..."
  local start_epoch=$SECONDS

  kubectl wait --for=condition=PodScheduled pod \
    -l app=test-app-default -n "$NS_DEFAULT" \
    --timeout="$WAIT_TIMEOUT" 2>/dev/null || warn "Some pods not scheduled in time"

  DEFAULT_SCHEDULE_TIME=$((SECONDS - start_epoch))
  ok "Default scheduler: all pods scheduled in ~${DEFAULT_SCHEDULE_TIME}s"

  log "Gathering CPU metrics and pod distribution..."
  DEF_AVG_CPU=$(kubectl top nodes | grep worker | awk '{sum+=$3+0} END {printf "%.2f%%", sum/NR}' || echo "N/A")
  DEF_P1=$(kubectl get pods -n "$NS_DEFAULT" --field-selector spec.nodeName=scheduler-demo-worker -o name | wc -l || echo 0)
  DEF_P2=$(kubectl get pods -n "$NS_DEFAULT" --field-selector spec.nodeName=scheduler-demo-worker2 -o name | wc -l || echo 0)
  DEF_P3=$(kubectl get pods -n "$NS_DEFAULT" --field-selector spec.nodeName=scheduler-demo-worker3 -o name | wc -l || echo 0)
  DEF_P4=$(kubectl get pods -n "$NS_DEFAULT" --field-selector spec.nodeName=scheduler-demo-worker4 -o name | wc -l || echo 0)
}

# ── Phase 3: Intelligent Scheduler Test ──────────────────────────────────────
phase3_intelligent() {
  log "Phase 3: Deploying $PODS pods with INTELLIGENT scheduler..."

  # Check scheduler is running
  local sched_ready
  sched_ready=$(kubectl get pods -n scheduler-system -l app=intelligent-scheduler \
    -o jsonpath='{.items[0].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
  if [[ "$sched_ready" != "True" ]]; then
    warn "Intelligent scheduler pod not ready! Check: kubectl get pods -n scheduler-system"
  fi

  kubectl apply -f manifests/test-app-custom.yaml
  kubectl scale deployment test-app-intelligent --replicas="$PODS" -n "$NS_INTELLIGENT"

  log "Waiting for pods to be scheduled (timeout: $WAIT_TIMEOUT)..."
  local start_epoch=$SECONDS

  kubectl wait --for=condition=PodScheduled pod \
    -l app=test-app-intelligent -n "$NS_INTELLIGENT" \
    --timeout="$WAIT_TIMEOUT" 2>/dev/null || warn "Some pods not scheduled in time"

  INTELLIGENT_SCHEDULE_TIME=$((SECONDS - start_epoch))
  ok "Intelligent scheduler: all pods scheduled in ~${INTELLIGENT_SCHEDULE_TIME}s"

  log "Gathering CPU metrics and pod distribution..."
  INT_AVG_CPU=$(kubectl top nodes | grep worker | awk '{sum+=$3+0} END {printf "%.2f%%", sum/NR}' || echo "N/A")
  INT_P1=$(kubectl get pods -n "$NS_INTELLIGENT" --field-selector spec.nodeName=scheduler-demo-worker -o name | wc -l || echo 0)
  INT_P2=$(kubectl get pods -n "$NS_INTELLIGENT" --field-selector spec.nodeName=scheduler-demo-worker2 -o name | wc -l || echo 0)
  INT_P3=$(kubectl get pods -n "$NS_INTELLIGENT" --field-selector spec.nodeName=scheduler-demo-worker3 -o name | wc -l || echo 0)
  INT_P4=$(kubectl get pods -n "$NS_INTELLIGENT" --field-selector spec.nodeName=scheduler-demo-worker4 -o name | wc -l || echo 0)
}

# ── Phase 4: Collect & Print Results ─────────────────────────────────────────
phase4_results() {
  log "Phase 4: Collecting metrics and generating report..."

  local def_latency intel_latency
  local def_balance intel_balance

  def_latency=$(scheduling_latency "test-app-default" "$NS_DEFAULT")
  intel_latency=$(scheduling_latency "test-app-intelligent" "$NS_INTELLIGENT")
  def_balance=$(load_balance_score "test-app-default" "$NS_DEFAULT")
  intel_balance=$(load_balance_score "test-app-intelligent" "$NS_INTELLIGENT")

  # Find control plane pods
  local def_cp intel_cp
  def_cp=$(kubectl get pods -n "$NS_DEFAULT" -l app=test-app-default -o wide | grep control-plane | wc -l || echo 0)
  intel_cp=$(kubectl get pods -n "$NS_INTELLIGENT" -l app=test-app-intelligent -o wide | grep control-plane | wc -l || echo 0)

  # Fetch an RCA sample
  local rca_sample="None (RCA disabled or not yet generated)"
  local sample_pod=$(kubectl get pods -n "$NS_INTELLIGENT" -l app=test-app-intelligent -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -n "$sample_pod" ]]; then
    local rca_ann=$(kubectl get pod "$sample_pod" -n "$NS_INTELLIGENT" -o jsonpath='{.metadata.annotations.intelligent-scheduler/rca}' 2>/dev/null || true)
    if [[ -n "$rca_ann" ]]; then
      rca_sample="$rca_ann"
    fi
  fi

  local report
  report=$(cat <<EOF

╔══════════════════════════════════════════════════════════════════════════════════╗
║        SCHEDULER BENCHMARK RESULTS — $(date '+%Y-%m-%d %H:%M:%S')                        ║
╠══════════════════════════════════════════════════════════════════════════════════╣
║  Test pods per scheduler : $PODS
║  Namespaces              : $NS_DEFAULT / $NS_INTELLIGENT
╠══════════════════════════════════════════════════════════════════════════════════╣
║  METRIC                  DEFAULT         INTELLIGENT
╠══════════════════════════════════════════════════════════════════════════════════╣
║  Total schedule time (s) : ${DEFAULT_SCHEDULE_TIME:-N/A}              ${INTELLIGENT_SCHEDULE_TIME:-N/A}
║  Avg latency per pod (s) : ${def_latency}              ${intel_latency}
║  Load balance (std dev)  : ${def_balance}
║                          : vs ${intel_balance}
║  Control-Plane Placements: ${def_cp}               ${intel_cp}  <-- Should be 0 for Intelligent!
╠══════════════════════════════════════════════════════════════════════════════════╣
╠══════════════════════════════════════════════════════════════════════════════════╣
║  POD DISTRIBUTION — DEFAULT SCHEDULER
$(pod_distribution "test-app-default" "$NS_DEFAULT" | awk '{printf "║    %s pods → %s\n", $1, $2}')
╠══════════════════════════════════════════════════════════════════════════════════╣
║  POD DISTRIBUTION — INTELLIGENT SCHEDULER
$(pod_distribution "test-app-intelligent" "$NS_INTELLIGENT" | awk '{printf "║    %s pods → %s\n", $1, $2}')
╠══════════════════════════════════════════════════════════════════════════════════╣
║  PHASE 5: IMPROVEMENT ANALYSIS
║
║  Sample Groq LLM RCA for pod '${sample_pod}':
║  "${rca_sample}"
╚══════════════════════════════════════════════════════════════════════════════════╝

EOF
)

  echo -e "$report"
  echo -e "$report" >> "$RESULTS_FILE"
  ok "Results saved to $RESULTS_FILE"

  # Save paper metrics table to separate files in the root directory
  echo -e "Scheduler Model | Trial | Slave 1 | Slave 2 | Slave 3 | Slave 4 | Avg CPU Util" > default_results.txt
  echo -e "Default         | 1     | $DEF_P1      | $DEF_P2      | $DEF_P3      | $DEF_P4      | $DEF_AVG_CPU" >> default_results.txt
  
  echo -e "Scheduler Model | Trial | Slave 1 | Slave 2 | Slave 3 | Slave 4 | Avg CPU Util" > intelligent_results.txt
  echo -e "Intelligent     | 1     | $INT_P1      | $INT_P2      | $INT_P3      | $INT_P4      | $INT_AVG_CPU" >> intelligent_results.txt
  
  ok "Paper metrics saved to default_results.txt and intelligent_results.txt"

  # Print node current load
  echo ""
  log "Current node resource usage:"
  kubectl top nodes 2>/dev/null || warn "metrics-server not available for kubectl top"
  
  echo ""
  bash export_rcas.sh
}

# ── Phase 5: Export Agent Logs ───────────────────────────────────────────────
phase5_export_logs() {
  log "Phase 5: Exporting FL agent logs..."
  mkdir -p logs
  rm -f logs/*-agent.log
  
  local agents
  agents=$(kubectl get pods -n scheduler-system -l app=fl-agent -o jsonpath='{range .items[*]}{.metadata.name}{","}{.spec.nodeName}{"\n"}{end}' 2>/dev/null || true)
  
  for agent in $agents; do
    local pod_name
    local node_name
    pod_name=$(echo "$agent" | cut -d',' -f1)
    node_name=$(echo "$agent" | cut -d',' -f2)
    if [[ -n "$pod_name" && -n "$node_name" ]]; then
      kubectl logs -n scheduler-system "$pod_name" > "logs/${node_name}-agent.log" 2>/dev/null || true
    fi
  done
  ok "Agent logs successfully exported to the 'logs/' directory"
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  echo ""
  echo -e "${CYAN}════════════════════════════════════════════════════${NC}"
  echo -e "${CYAN}  Intelligent Pod Scheduler — Benchmark Suite       ${NC}"
  echo -e "${CYAN}════════════════════════════════════════════════════${NC}"
  echo ""

  mkdir -p benchmark
  echo "" > "$RESULTS_FILE"

  phase1_cleanup
  echo ""
  phase2_default
  echo ""
  phase3_intelligent
  echo ""
  phase4_results
  echo ""
  phase5_export_logs
}

main "$@"
