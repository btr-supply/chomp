#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

echo "🛑 Stopping Chomp services..."

# Stop local processes if they exist
stop_local_processes() {
  echo "🔧 Stopping local processes..."

    # Stop using unified .pid file
  if [ -f ".pid" ]; then
    local pid=$(cat .pid)
    if ps -p "$pid" > /dev/null 2>&1; then
      echo "Stopping chomp service (PID: $pid)..."
      kill "$pid" 2>/dev/null || true
      sleep 2
      # Force kill if still running
      if ps -p "$pid" > /dev/null 2>&1; then
        echo "Force stopping chomp service..."
        kill -9 "$pid" 2>/dev/null || true
      fi
    fi
    rm -f .pid
    echo "✅ Chomp service stopped"
  fi

  # Kill any remaining python processes related to chomp
  local chomp_pids=$(ps aux | grep -E "(main\.py|python.*chomp)" | grep -v grep | awk '{print $2}' || true)
  if [ -n "$chomp_pids" ]; then
    echo "Stopping remaining chomp processes..."
    echo "$chomp_pids" | xargs -r kill 2>/dev/null || true
    sleep 1
    echo "$chomp_pids" | xargs -r kill -9 2>/dev/null || true
  fi
}

# Stop Docker containers
stop_docker_containers() {
  echo "🐳 Stopping Docker containers..."
  local containers=$(docker_cmd ps --filter "name=chomp-*" --format "{{.Names}}" || true)
  if [ -n "$containers" ]; then
    for container in $containers; do
      echo "Stopping $container..."
      docker_cmd stop "$container" || true
    done
    echo "✅ All Chomp containers stopped"
  else
    echo "No Chomp containers running"
  fi
}

# Stop all services
stop_local_processes
stop_docker_containers

echo "✅ All Chomp services stopped"

