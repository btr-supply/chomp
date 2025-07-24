#!/bin/bash
set -euo pipefail

# Change to the script's directory to ensure paths are correct
cd "$(dirname "$0")/.."

# Source utils for consistent configuration handling
source "scripts/utils.sh"

# Parse arguments
KEEP_DB=false
for arg in "$@"; do
  [[ "$arg" == "--keep-db" ]] && KEEP_DB=true
done

# Get saved configuration or use defaults
CONFIG=$(uv run python -c "
from src.utils.runtime import runtime
try:
    config = runtime.get_config()
    instance_info = runtime.get_instance_info()
    print(f\"{config['MODE']} {config['DEPLOYMENT']} {config['API']}\")
    print(f\"{instance_info['name']} ({instance_info['uid'][:8]}...)\")
except:
    print('default')
" 2>/dev/null)

if echo "$CONFIG" | grep -q "default"; then
    find_env "$@"
    parse_common_args "$@"
    RUNTIME_CONFIG="$MODE $DEPLOYMENT $API"
    INSTANCE_INFO="using environment defaults"
else
    RUNTIME_CONFIG=$(echo "$CONFIG" | head -n 1)
    INSTANCE_INFO=$(echo "$CONFIG" | tail -n 1)
fi

echo "🔄 Restarting Chomp: $RUNTIME_CONFIG $([ "$KEEP_DB" = true ] && echo "(keeping database)")"
echo "   Instance: $INSTANCE_INFO"

# Stop and restart
make stop $([ "$KEEP_DB" = true ] && echo "keep-db")
sleep 2
make run $RUNTIME_CONFIG $([ "$KEEP_DB" = true ] && echo "keep-db")

echo "✅ Restart complete"
