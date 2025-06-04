#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

ENV_FILE=${ENV_FILE:-".env"}
CONFIG_FILE=${CONFIG_FILE:-"examples/diverse.yml"}
MAX_JOBS=${MAX_JOBS:-"16"}

echo "Starting Chomp ingester..."
echo "Environment: $ENV_FILE"
echo "Config: $CONFIG_FILE"
echo "Max jobs: $MAX_JOBS"
echo ""

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
  echo "❌ Config file not found: $CONFIG_FILE"
  echo "Available configs in examples/:"
  ls -la examples/*.yml 2>/dev/null || echo "No example configs found"
  exit 1
fi

# Check if environment file exists
if [ ! -f "$ENV_FILE" ]; then
  echo "⚠️  Environment file not found: $ENV_FILE"
  echo "Using default environment variables"
fi

# Count jobs in config
TOTAL_JOBS=$(count_yaml_jobs "$CONFIG_FILE")
echo "Total jobs in config: $TOTAL_JOBS"

# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
  echo "Activating virtual environment..."
  source .venv/bin/activate
fi

# Start ingester
echo "Starting ingester with $MAX_JOBS concurrent jobs..."
if [ -f "$ENV_FILE" ]; then
  uv run python main.py \
    -e "$ENV_FILE" \
    -c "$CONFIG_FILE" \
    -j "$MAX_JOBS" \
    -v
else
  uv run python main.py \
    -c "$CONFIG_FILE" \
    -j "$MAX_JOBS" \
    -v
fi
