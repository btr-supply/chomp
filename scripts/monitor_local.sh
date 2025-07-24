#!/bin/bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
source "scripts/utils.sh"

echo "📊 Monitoring local Chomp services..."

# Get PIDs from runtime script
pids_json=$(uv run python -c "from src.utils.runtime import runtime; import json; print(json.dumps(runtime.get_pids()))")
api_pid=$(echo "$pids_json" | jq -r .api_pid)
ingester_pids=$(echo "$pids_json" | jq -r '.pids | join(" ")')

# Monitor API Server
if [ -n "$api_pid" ] && [ "$api_pid" != "null" ]; then
    echo "--- API Server (PID: $api_pid) ---"
    ps -p "$api_pid" -o pid,ppid,%cpu,%mem,start,etime,command || echo "API server not running"
else
    echo "--- API Server (Not Running) ---"
fi

echo ""

# Monitor Ingester Processes
if [ -n "$ingester_pids" ]; then
    echo "--- Ingester Processes (PIDs: $ingester_pids) ---"
    ps -p "$ingester_pids" -o pid,ppid,%cpu,%mem,start,etime,command || echo "Ingester processes not running"
else
    echo "--- Ingester Processes (Not Running) ---"
fi

echo ""
echo "--- Logs (tail) ---"
tail -n 50 chomp.log || echo "No log file found."
