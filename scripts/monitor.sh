#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

echo "📊 Chomp Monitor"
echo "==============="

# Container status
containers=$(docker ps --filter "name=chomp-*" --format "{{.Names}}")
if [ -n "$containers" ]; then
  docker ps --filter "name=chomp-*" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
  echo ""
  docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" $(echo $containers | tr '\n' ' ')
else
  echo "⚠️ No containers. Use: make full-setup"
  exit 0
fi

# Health & Network
echo ""
echo "🏥 Health:"
command -v curl &>/dev/null && curl -s "http://localhost:${PORT:-40004}/health" &>/dev/null && echo "✅ API responding" || echo "⚠️ API not responding"

echo ""
echo "🌐 Network:"
docker network inspect "$DOCKER_NET" &>/dev/null && echo "✅ Network active" || echo "❌ Network missing"

# Quick commands
echo ""
echo "Commands: make logs | make health-check | make stop-all"

# Auto-refresh option
echo ""
read -p "🔄 Auto-refresh every 10s? (y/N): " -n 1 -r
[[ $REPLY =~ ^[Yy]$ ]] && {
  echo -e "\nPress Ctrl+C to stop..."
  while true; do sleep 10; clear; exec "$0"; done
}
