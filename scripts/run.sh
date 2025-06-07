#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

# Parse arguments
parse_common_args "$@"

echo "🚀 Starting Chomp: MODE=$MODE DEPLOYMENT=$DEPLOYMENT API=$API"

# Setup environment
MAX_JOBS=${MAX_JOBS:-15}
VERBOSE=${VERBOSE:-false}

# Export for downstream scripts
export MAX_JOBS
export VERBOSE

start_services() {
  local mode=$1 deployment=$2 api=$3

  echo "📦 Setting up dependencies..."
  if [ "$deployment" = "docker" ]; then
    bash scripts/setup.sh images
  else
    bash scripts/setup.sh deps
  fi

  echo "🗄️ Starting database..."
  MODE=$mode DEPLOYMENT=$deployment bash scripts/database.sh

  # Wait for database to be ready
  sleep 3

  echo "⚙️ Starting ingesters..."
  MODE=$mode DEPLOYMENT=$deployment VERBOSE=$VERBOSE bash scripts/services.sh ingester

  if [ "$api" = "api" ]; then
    echo "🌐 Starting API server..."
    MODE=$mode DEPLOYMENT=$deployment VERBOSE=$VERBOSE bash scripts/services.sh api
  fi

  echo ""
  echo "✅ Chomp started successfully!"
  echo "   Mode: $mode"
  echo "   Deployment: $deployment"
  echo "   API: $api"
  echo ""
  echo ""

  if [ "$api" = "api" ]; then
    echo "🔗 API available at: http://localhost:40004"
  fi
  echo "📊 Monitor with: make monitor"
  echo "📋 View logs with: make logs"
  echo "🛑 Stop with: make stop"
}

# Execute with error handling
if start_services "$MODE" "$DEPLOYMENT" "$API"; then
  exit 0
else
  echo "❌ Failed to start services"
  exit 1
fi
