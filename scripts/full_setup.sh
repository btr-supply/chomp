#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

echo "=== Chomp Full Setup ==="
echo "This will set up a complete Chomp deployment with:"
echo "- TDengine + Redis database backend"
echo "- Core application container"
echo "- Health checks and verification"
echo ""

check_sudo
check_docker
ensure_network $DOCKER_NET

# Step 1: Database setup
echo "Step 1/3: Setting up database backend..."
bash "$(dirname "${BASH_SOURCE[0]}")/db_setup.sh"

# Step 2: Core application setup
echo "Step 2/3: Setting up core application..."
bash "$(dirname "${BASH_SOURCE[0]}")/core_setup.sh"

# Step 3: Final verification
echo "Step 3/3: Final verification..."
sleep 5

# Check all services are running
echo "Checking all services..."
if check_container_running "$DB_CONTAINER" && check_container_running "$CORE_CONTAINER"; then
  echo "✓ All containers are running"
else
  echo "✗ Some containers are not running"
  exit 1
fi

# Final health check
echo "Performing final health check..."
if db_client_health_check $DOCKER_NET "healthcheck-final" $CORE_IMAGE $ENV; then
  echo ""
  echo "🎉 Full setup completed successfully!"
  echo ""
  echo "Services available:"
  echo "  - Database (TDengine): localhost:$TAOS_PORT"
  echo "  - Database (Redis): localhost:$REDIS_PORT"
  echo "  - API Server: http://localhost:$PORT"
  echo "  - WebSocket: ws://localhost:$PORT/ws"
  echo ""
  echo "Try these commands:"
  echo "  make ping          # Health check"
  echo "  make logs          # View logs"
  echo "  make monitor       # Monitor services"
  echo "  make stop-all      # Stop all services"
else
  echo "✗ Health check failed"
  exit 1
fi
