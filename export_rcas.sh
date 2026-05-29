#!/usr/bin/env bash
# =============================================================================
# export_rcas.sh — Export Federated Learning RCA for all intelligent pods
#
# This script:
#   1. Checks each intelligent pod for the FL-generated RCA annotation.
#   2. Saves each RCA as a separate text file in the RCA/ directory.
# =============================================================================
set -euo pipefail

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

NS_INTELLIGENT="intelligent-workloads"
RCA_DIR="RCA"

echo -e "${CYAN}═══════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Exporting FL Root Cause Analyses to ${RCA_DIR}/ directory  ${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════════${NC}"

mkdir -p "$RCA_DIR"
rm -rf "$RCA_DIR"/*

python3 - "$NS_INTELLIGENT" "$RCA_DIR" <<'PYEOF'
import sys, os, json, subprocess

NS = sys.argv[1]
RCA_DIR = sys.argv[2]

def get_pods():
    """Get all intelligent scheduler pods with their annotations."""
    try:
        out = subprocess.check_output([
            "kubectl", "get", "pods", "-n", NS,
            "-l", "app=test-app-intelligent",
            "-o", "json"
        ])
        return json.loads(out).get("items", [])
    except Exception:
        return []

pods = get_pods()
if not pods:
    print("No intelligent scheduler pods found.")
    sys.exit(0)

exported = 0

for pod in pods:
    pod_name = pod["metadata"]["name"]
    node_name = pod.get("spec", {}).get("nodeName", "unassigned")
    annotations = pod.get("metadata", {}).get("annotations", {})
    rca_text = annotations.get("intelligent-scheduler/rca")

    if rca_text:
        source = "federated-learning-annotation"
    else:
        rca_text = f"FL RCA unavailable — pod '{pod_name}' was placed on node '{node_name}'. Annotation missing."
        source = "fallback"

    # Write the RCA file
    filepath = os.path.join(RCA_DIR, f"{pod_name}.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"Pod:    {pod_name}\n")
        f.write(f"Node:   {node_name}\n")
        f.write(f"Source: {source}\n")
        f.write(f"{'='*60}\n\n")
        f.write(rca_text + "\n")
    exported += 1

print(f"\n\033[0;32m✅ Exported {exported} Federated Learning RCA reports to {RCA_DIR}/\033[0m")
PYEOF
