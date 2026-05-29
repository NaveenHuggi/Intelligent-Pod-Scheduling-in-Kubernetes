#!/usr/bin/env bash
# =============================================================================
# setup.sh — Full project setup script
# Run this once to create the cluster, install tools, build + deploy everything
# =============================================================================
set -euo pipefail

CLUSTER_NAME="scheduler-demo"
GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
log()  { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*"; }
ok()   { echo -e "${GREEN}✅ $*${NC}"; }
warn() { echo -e "${YELLOW}⚠️  $*${NC}"; }
err()  { echo -e "${RED}❌ $*${NC}"; exit 1; }

# ── Step 1: Install Helm ─────────────────────────────────────────────────────
step1_install_helm() {
  if command -v helm &>/dev/null; then
    ok "Helm already installed: $(helm version --short)"
    return
  fi
  log "Installing Helm..."
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  ok "Helm installed: $(helm version --short)"
}

# ── Step 2: Create kind cluster ──────────────────────────────────────────────
step2_create_cluster() {
  if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    warn "Cluster '${CLUSTER_NAME}' already exists — skipping creation"
    kubectl config use-context "kind-${CLUSTER_NAME}"
    return
  fi
  log "Creating kind cluster '${CLUSTER_NAME}' (1 control-plane + 4 workers)..."
  kind create cluster --config manifests/kind-cluster.yaml --wait 90s
  ok "Cluster '${CLUSTER_NAME}' created"
  kubectl cluster-info --context "kind-${CLUSTER_NAME}"
}

# ── Step 3: Install metrics-server ──────────────────────────────────────────
step3_metrics_server() {
  log "Installing metrics-server..."
  kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml

  # Patch metrics-server to disable TLS verification (required for kind)
  kubectl patch deployment metrics-server -n kube-system \
    --type='json' \
    -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'

  log "Waiting for metrics-server to be ready..."
  kubectl rollout status deployment/metrics-server -n kube-system --timeout=120s
  ok "metrics-server ready"
}

# ── Step 4: Install Prometheus + Grafana via Helm ────────────────────────────
step4_monitoring() {
  log "Adding Prometheus Helm repo..."
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
  helm repo add grafana https://grafana.github.io/helm-charts 2>/dev/null || true
  helm repo update

  # Create monitoring namespace
  kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

  # Install Prometheus
  log "Installing Prometheus..."
  helm upgrade --install prometheus prometheus-community/prometheus \
    --namespace monitoring \
    --values monitoring/prometheus-values.yaml \
    --wait --timeout 180s
  ok "Prometheus installed"

  # Install Grafana
  log "Installing Grafana..."
  helm upgrade --install grafana grafana/grafana \
    --namespace monitoring \
    --set adminPassword=admin123 \
    --set service.type=NodePort \
    --set service.nodePort=32000 \
    --set persistence.enabled=false \
    --set resources.requests.cpu=100m \
    --set resources.requests.memory=128Mi \
    --wait --timeout 180s
  ok "Grafana installed (NodePort 32000, password: admin123)"
}

# ── Step 5: Import Grafana Dashboard ────────────────────────────────────────
step5_grafana_dashboard() {
  log "Importing Grafana dashboard..."
  # Wait for Grafana pod to be fully ready
  kubectl rollout status deployment/grafana -n monitoring --timeout=60s

  # Get the Grafana pod
  local grafana_pod
  grafana_pod=$(kubectl get pods -n monitoring -l app.kubernetes.io/name=grafana \
    -o jsonpath='{.items[0].metadata.name}')

  # Add Prometheus datasource
  kubectl exec -n monitoring "$grafana_pod" -- \
    curl -s -X POST http://admin:admin123@localhost:3000/api/datasources \
    -H 'Content-Type: application/json' \
    -d '{
      "name":"Prometheus",
      "type":"prometheus",
      "url":"http://prometheus-server.monitoring.svc:9090",
      "access":"proxy",
      "isDefault":true
    }' 2>/dev/null | grep -o '"message":"[^"]*"' || true

  # Import dashboard
  local dash_json
  dash_json=$(cat monitoring/grafana-dashboard.json)
  kubectl exec -n monitoring "$grafana_pod" -- \
    curl -s -X POST http://admin:admin123@localhost:3000/api/dashboards/import \
    -H 'Content-Type: application/json' \
    -d "{\"dashboard\": $dash_json, \"overwrite\": true, \"folderId\": 0}" 2>/dev/null \
    | grep -o '"status":"[^"]*"' || true

  ok "Dashboard imported into Grafana"
}

# ── Step 6: Build & Load Docker images ────────────────────────────────────────
step6_build_image() {
  log "Building intelligent-scheduler Docker image..."
  docker build -t intelligent-scheduler:latest custom-scheduler/
  ok "Image built: intelligent-scheduler:latest"

  log "Building fl-agent Docker image..."
  docker build -t fl-agent:latest -f custom-scheduler/Dockerfile.agent custom-scheduler/
  ok "Image built: fl-agent:latest"

  log "Loading images into kind cluster '${CLUSTER_NAME}'..."
  kind load docker-image intelligent-scheduler:latest --name "${CLUSTER_NAME}"
  kind load docker-image fl-agent:latest --name "${CLUSTER_NAME}"
  ok "Both images loaded into kind"
}

# ── Step 7: Deploy RBAC + Scheduler + FL Agents ─────────────────────────────
step7_deploy_scheduler() {
  log "Applying namespaces + RBAC..."
  kubectl apply -f manifests/namespace.yaml
  kubectl apply -f manifests/scheduler-rbac.yaml

  log "Deploying intelligent-scheduler..."
  kubectl apply -f manifests/scheduler-deploy.yaml
  kubectl rollout restart deployment intelligent-scheduler -n scheduler-system
  kubectl rollout status deployment/intelligent-scheduler \
    -n scheduler-system --timeout=120s
  ok "Intelligent scheduler is RUNNING ✅"

  log "Deploying FL Agent DaemonSet on worker nodes..."
  kubectl apply -f manifests/fl-agent-daemonset.yaml
  sleep 5
  kubectl rollout status daemonset/fl-agent -n scheduler-system --timeout=120s
  ok "FL Agents deployed ✅"

  # Show all scheduler-system pods
  kubectl get pods -n scheduler-system -o wide
}

# ── Step 8: Verify + Print Access Info ──────────────────────────────────────
step8_verify() {
  echo ""
  echo -e "${CYAN}════════════════════════════════════════════════════════${NC}"
  echo -e "${CYAN}  🎉 Setup Complete!                                    ${NC}"
  echo -e "${CYAN}════════════════════════════════════════════════════════${NC}"
  echo ""

  local node_ip
  node_ip=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || echo "localhost")

  echo -e "  ${GREEN}Cluster:${NC}            kind-${CLUSTER_NAME}"
  echo -e "  ${GREEN}Nodes:${NC}"
  kubectl get nodes -o wide
  echo ""
  echo -e "  ${GREEN}Grafana UI:${NC}         1. Run: kubectl port-forward --address 0.0.0.0 svc/grafana 32000:80 -n monitoring\n                      2. Open: http://localhost:32000"
  echo -e "  ${GREEN}Grafana login:${NC}      admin / admin123"
  echo ""
  echo -e "  ${GREEN}Scheduler metrics:${NC}  kubectl port-forward svc/intelligent-scheduler 8080:8080 -n scheduler-system"
  echo -e "                      then open: http://localhost:8080/metrics"
  echo ""
  echo -e "  ${GREEN}Run benchmark:${NC}      bash benchmark/benchmark.sh"
  echo ""
  echo -e "  ${GREEN}Scheduler logs:${NC}     kubectl logs -f -l app=intelligent-scheduler -n scheduler-system"
  echo ""

  # Show all running components
  echo -e "  ${GREEN}All components:${NC}"
  kubectl get pods -A | grep -v "kube-system" | grep -v "^NAMESPACE"
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  echo ""
  echo -e "${CYAN}════════════════════════════════════════════════════════${NC}"
  echo -e "${CYAN}  Intelligent Pod Scheduling — Full Setup               ${NC}"
  echo -e "${CYAN}════════════════════════════════════════════════════════${NC}"
  echo ""

  step1_install_helm
  step2_create_cluster
  step3_metrics_server
  step4_monitoring
  step5_grafana_dashboard
  step6_build_image
  step7_deploy_scheduler
  step8_verify
}

main "$@"
