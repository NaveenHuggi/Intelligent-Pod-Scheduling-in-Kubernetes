#!/usr/bin/env bash
# =============================================================================
# stress_node.sh — Apply artificial load to a specific cluster node for testing
# Usage:
#   bash stress_node.sh apply <node-name> <cpu|mem|both>
#   bash stress_node.sh delete
#   bash stress_node.sh random <node-name> <cpu|mem|both>
# =============================================================================

ACTION=${1:-help}
NODE=${2:-scheduler-demo-worker}
TYPE=${3:-both}

if [ "$ACTION" == "apply" ] || [ "$ACTION" == "start" ]; then
    echo "🔥 Starting node stress test on $NODE (Type: $TYPE)..."

    # Generate dynamic stress arguments based on type
    STRESS_ARGS=""
    if [ "$TYPE" == "cpu" ]; then
        STRESS_ARGS='["--cpu", "4"]'
    elif [ "$TYPE" == "mem" ]; then
        STRESS_ARGS='["--vm", "2", "--vm-bytes", "1024M"]'
    elif [ "$TYPE" == "both" ]; then
        STRESS_ARGS='["--cpu", "4", "--vm", "2", "--vm-bytes", "1024M"]'
    else
        echo "Unknown stress type: $TYPE. Use 'cpu', 'mem', or 'both'."
        exit 1
    fi

    # Create dynamic yaml
    cat <<EOF > load-test/stress-node-dynamic.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: node-stress-test
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: node-stress
  template:
    metadata:
      labels:
        app: node-stress
    spec:
      nodeSelector:
        kubernetes.io/hostname: $NODE
      containers:
      - name: stress
        image: polinux/stress
        command: ["stress"]
        args: $STRESS_ARGS
        resources:
          requests:
            cpu: "2000m"
            memory: "1024Mi"
          limits:
            cpu: "4000m"
            memory: "2048Mi"
EOF

    kubectl apply -f load-test/stress-node-dynamic.yaml
    echo "Wait a few moments for metrics to spike."
    echo "Run 'kubectl top nodes' to verify the load."

elif [ "$ACTION" == "delete" ] || [ "$ACTION" == "stop" ]; then
    echo "🧊 Stopping node stress test..."
    kubectl delete deployment node-stress-test -n default --ignore-not-found=true
    rm -f load-test/stress-node-dynamic.yaml
    echo "Stress test removed. Metrics will normalize shortly."
elif [ "$ACTION" == "random" ]; then
    echo "🎲 Starting dynamic/random node stress test on $NODE (Type: $TYPE)..."
    echo "Press Ctrl+C to stop."
    # Trap Ctrl+C to clean up before exiting
    trap "bash $0 delete; exit 0" SIGINT
    while true; do
        bash $0 apply $NODE $TYPE
        echo "Stress applied, sleeping for 40 seconds..."
        sleep 40
        bash $0 delete
        echo "Stress removed, sleeping for 20 seconds..."
        sleep 20
    done
else
    echo "Usage: bash stress_node.sh apply <node-name> <cpu|mem|both>"
    echo "       bash stress_node.sh delete"
    echo ""
    echo "Examples:"
    echo "  bash stress_node.sh apply scheduler-demo-worker3 cpu"
    echo "  bash stress_node.sh apply scheduler-demo-worker mem"
    echo "  bash stress_node.sh apply scheduler-demo-worker2 both"
    echo "  bash stress_node.sh random scheduler-demo-worker3 cpu"
    echo "  bash stress_node.sh delete"
    exit 1
fi
