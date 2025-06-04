#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

ENV_FILE=${ENV_FILE:-".env"}
HOST=${HOST:-"127.0.0.1"}
PORT=${PORT:-"40004"}

echo "Starting API server: http://$HOST:$PORT"
echo "WebSocket: ws://$HOST:$PORT/ws"

# Activate venv if exists
[ -f ".venv/bin/activate" ] && source .venv/bin/activate

# Start server
if [ -f "$ENV_FILE" ]; then
  uv run python main.py -e "$ENV_FILE" --server --host "$HOST" --port "$PORT" -v
else
  echo "⚠️ Environment file not found: $ENV_FILE"
  uv run python main.py --server --host "$HOST" --port "$PORT" -v
fi
