#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

echo "Stopping Chomp ingester cluster..."

# Stop and remove all ingester containers
echo "Stopping ingester containers..."
cleanup_containers "chomp-ingester.*"

# Check if any containers are still running
REMAINING_CONTAINERS=$(docker ps --filter "name=chomp-ingester.*" --format "{{.Names}}" | wc -l)

if [ "$REMAINING_CONTAINERS" -eq 0 ]; then
  echo "✅ All ingester containers stopped successfully!"
else
  echo "⚠️  Some containers may still be running:"
  docker ps --filter "name=chomp-ingester.*" --format "table {{.Names}}\t{{.Status}}"
fi

echo ""
echo "Cluster stopped. Database and other services remain running."
echo "To stop all services: make stop-all"
