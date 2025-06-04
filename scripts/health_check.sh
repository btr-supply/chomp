#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

ENV_FILE=${ENV_FILE:-".env"}
[ -f "$ENV_FILE" ] && source "$ENV_FILE"

echo "🏥 Chomp Health Check"
echo "===================="

# Docker & Containers
docker info &>/dev/null || { echo "❌ Docker not responding"; exit 1; }
echo "✅ Docker running"

containers_found=false
if docker ps --format '{{.Names}}' | grep -q "^chomp-"; then
  containers_found=true
  echo "✅ Containers running"
  docker ps --filter "name=chomp-*" --format "table {{.Names}}\t{{.Status}}"
else
  echo "⚠️ No containers"
fi

# Database health
echo ""
echo "Database:"
if [ "$containers_found" = true ]; then
  db_client_health_check "$DOCKER_NET" "healthcheck" "$CORE_IMAGE" "$ENV_FILE" && echo "✅ DB connectivity" || { echo "❌ DB check failed"; exit 1; }
else
  db_health_check "${REDIS_PORT:-40001}" "${TAOS_PORT:-40002}" "${DB_RW_USER:-rw}" "${DB_RW_PASS:-pass}" && echo "✅ Direct DB connection" || { echo "❌ DB connection failed"; exit 1; }
fi

# Network & Images
echo ""
docker network inspect "$DOCKER_NET" &>/dev/null && echo "✅ Network exists" || echo "⚠️ Network missing"

images_exist=true
docker image inspect "$DB_IMAGE" &>/dev/null || { echo "❌ DB image missing"; images_exist=false; }
docker image inspect "$CORE_IMAGE" &>/dev/null || { echo "❌ Core image missing"; images_exist=false; }
[ "$images_exist" = true ] && echo "✅ Images available"

# Summary
echo ""
if [ "$containers_found" = true ] && [ "$images_exist" = true ]; then
  echo "🎉 All systems healthy!"
  echo "Services: Redis:${REDIS_PORT:-40001} | TDengine:${TAOS_PORT:-40002} | API:${PORT:-40004}"
else
  echo "⚠️ Issues detected:"
  [ "$images_exist" = false ] && echo "  Run: make build-images"
  [ "$containers_found" = false ] && echo "  Run: make full-setup"
  exit 1
fi
