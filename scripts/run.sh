#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

# Parse command line arguments
KEEP_DB=false
ARGS=()
for arg in "$@"; do
  case $arg in
    --keep-db)
      KEEP_DB=true
      ;;
    *)
      ARGS+=("$arg")
      ;;
  esac
done

find_env "${ARGS[@]+"${ARGS[@]}"}"
parse_common_args "${ARGS[@]+"${ARGS[@]}"}"

# Save runtime configuration and instance name
uv run python -c "
from src.utils.runtime import runtime
from src.utils.uid import get_or_generate_instance_name
import os

# Set configuration
runtime.set_config('$MODE', '$DEPLOYMENT', '$API')

# Set instance name if provided via environment variable, otherwise generate one
instance_name = os.environ.get('INSTANCE_NAME')
if instance_name:
    runtime.set_instance_name(instance_name)
else:
    # Use sophisticated name generation from uid.py
    get_or_generate_instance_name()

print(f'Runtime configuration saved: {runtime.get_config()}')
print(f'Instance: {runtime.get_instance_info()}')
"

echo "🚀 Starting Chomp: MODE=$MODE DEPLOYMENT=$DEPLOYMENT API=$API"

# Export environment variables for downstream scripts
export MAX_JOBS=${MAX_JOBS:-6}
export VERBOSE=${VERBOSE:-false}

# Check Docker daemon status early (needed for database in all deployments)
check_docker

# Main startup sequence
echo "📦 Setting up dependencies..."
if [ "$DEPLOYMENT" = "docker" ]; then
  bash scripts/setup.sh images
else
  bash scripts/setup.sh deps
fi

if [ "$KEEP_DB" = true ]; then
  echo "🗄️ Skipping database startup (--keep-db flag)"
else
  echo "🗄️ Starting database..."
  MODE=$MODE DEPLOYMENT=$DEPLOYMENT bash scripts/database.sh $MODE $DEPLOYMENT

  # Wait for database initialization
  sleep 3
fi

echo "⚙️ Starting ingesters..."
MODE=$MODE DEPLOYMENT=$DEPLOYMENT VERBOSE=$VERBOSE bash scripts/services.sh ingester $MODE $DEPLOYMENT

if [ "$API" = "api" ]; then
  echo "🌐 Starting API server..."
  MODE=$MODE DEPLOYMENT=$DEPLOYMENT VERBOSE=$VERBOSE bash scripts/services.sh api $MODE $DEPLOYMENT
fi

echo ""
echo "✅ Chomp started successfully!"
echo "   Mode: $MODE"
echo "   Deployment: $DEPLOYMENT"
echo "   API: $API"
echo ""

if [ "$API" = "api" ]; then
  echo "🔗 API available at: http://localhost:40004"
fi

echo "📊 Monitor with: make monitor"
echo "📋 View logs with: make logs"
echo "🛑 Stop with: make stop"

# Tail the log file to show output from all services
LOG_FILE_PATH="${PARENT}/${OUT_LOG:-out.log}"
if [ -f "$LOG_FILE_PATH" ]; then
    echo "Tailing chomp cluster logs at: $LOG_FILE_PATH ..."
    tail -f "$LOG_FILE_PATH"
else
    echo "⚠️ Log file not found at: $LOG_FILE_PATH"
fi
