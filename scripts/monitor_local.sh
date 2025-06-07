#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

echo "📊 Chomp Local Monitor"
echo "====================="

# Check if PID files exist and processes are running
check_service() {
  local service=$1
  local pid_file="chomp/.${service}.pid"

  if [ -f "$pid_file" ]; then
    local pid=$(cat "$pid_file")
    if ps -p "$pid" > /dev/null 2>&1; then
      echo "✅ $service is running (PID: $pid)"
      return 0
    else
      echo "❌ $service PID file exists but process is not running (PID: $pid)"
      return 1
    fi
  else
    echo "❌ $service is not running (no PID file)"
    return 1
  fi
}

# Change to parent directory
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

echo ""
echo "Service Status:"
echo "---------------"
api_running=false
ingester_running=false

if check_service "api"; then
  api_running=true
fi

if check_service "ingester"; then
  ingester_running=true
fi

echo ""
echo "Database Status:"
echo "----------------"
if docker_container_running "$DB_CONTAINER"; then
  echo "✅ Database container is running"
else
  echo "❌ Database container is not running"
fi

echo ""
echo "Recent Logs (last 20 lines):"
echo "-----------------------------"
LOG_FILE="${OUT_LOG:-out.log}"

if [ -f "$LOG_FILE" ]; then
  tail -20 "$LOG_FILE"
else
  echo "❌ Log file not found: $LOG_FILE"
fi

echo ""
echo "Commands:"
echo "---------"
echo "📋 View full logs: tail -f $(realpath "$LOG_FILE" 2>/dev/null || echo "$LOG_FILE")"
echo "🛑 Stop services: make stop"
echo "🚀 Restart: make run dev local"

# Exit with error only if both services are down
if [ "$api_running" = false ] && [ "$ingester_running" = false ]; then
  echo ""
  echo "⚠️ All services are down"
  exit 1
fi
