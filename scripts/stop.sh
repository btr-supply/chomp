#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

# Parse arguments
KEEP_DB=false
DB_ONLY=false
for arg in "$@"; do
  case $arg in
    --keep-db) KEEP_DB=true ;;
    --db-only) DB_ONLY=true ;;
    *) continue ;;
  esac
done

find_env "$@"

# Stop local processes (tracked and untracked)
if [ "$DB_ONLY" = false ]; then
  echo "🔍 Stopping chomp processes..."

  # Stop tracked processes first
  tracked_count=0
  if command -v jq >/dev/null 2>&1; then
    pids_json=$(uv run python -c "from src.utils.runtime import runtime; import json; print(json.dumps(runtime.get_pids()))" 2>/dev/null || echo '{"api_pid":"","pids":[]}')
    api_pid=$(echo "$pids_json" | jq -r .api_pid)
    tracked_pids=$(echo "$pids_json" | jq -r '.pids[]' | grep -v "null" || true)

    if [ -n "$api_pid" ] && [ "$api_pid" != "null" ]; then
      echo "  📋 Found tracked API process: $api_pid"
      kill "$api_pid" 2>/dev/null || true
      ((tracked_count++))
    fi

    if [ -n "$tracked_pids" ]; then
      echo "  📋 Found tracked ingester PIDs: $(echo "$tracked_pids" | tr '\n' ' ')"
      echo "$tracked_pids" | while read -r pid; do
        [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
        ((tracked_count++))
      done
    fi

    sleep 1

    # Force kill tracked processes
    [ -n "$api_pid" ] && [ "$api_pid" != "null" ] && kill -9 "$api_pid" 2>/dev/null || true
    echo "$tracked_pids" | while read -r pid; do
      [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
    done

    uv run python -c "from src.utils.runtime import runtime; runtime.clear_pids()" 2>/dev/null || true
    [ $tracked_count -gt 0 ] && echo "  ✅ Stopped $tracked_count tracked processes"
  fi

    # Find and kill any remaining chomp processes by command line patterns
  echo "  🔍 Searching for orphaned processes with patterns:"
  echo "    - python.*chomp.*main\.py"
  echo "    - uv run python.*main\.py"
  echo "    - chomp.*ingester"

  orphaned_count=0
  for pattern in "python.*chomp.*main\.py" "uv run python.*main\.py" "chomp.*ingester"; do
    orphaned_pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "$orphaned_pids" ]; then
      echo "  🚨 Found orphaned processes matching '$pattern': $(echo "$orphaned_pids" | tr '\n' ' ')"
      echo "$orphaned_pids" | while read -r pid; do
        kill "$pid" 2>/dev/null || true
        ((orphaned_count++))
      done
    fi
  done

  if [ $orphaned_count -eq 0 ]; then
    echo "  ℹ️  No orphaned processes found"
  fi

  sleep 1

  # Force kill any remaining
  stubborn_found=false
  for pattern in "python.*chomp.*main\.py" "uv run python.*main\.py" "chomp.*ingester"; do
    orphaned_pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "$orphaned_pids" ]; then
      echo "  💀 Force killing stubborn processes: $(echo "$orphaned_pids" | tr '\n' ' ')"
      echo "$orphaned_pids" | while read -r pid; do
        kill -9 "$pid" 2>/dev/null || true
      done
      stubborn_found=true
    fi
  done

  if [ $orphaned_count -gt 0 ]; then
    echo "  ✅ Cleaned up $orphaned_count orphaned processes"
  fi
fi

# Stop Docker containers
echo "🐳 Checking Docker containers with pattern: chomp-*"
containers=$(docker_cmd ps --filter "name=chomp-*" --format "{{.Names}}" 2>/dev/null || true)
if [ -n "$containers" ]; then
  echo "  📦 Found containers: $(echo "$containers" | tr '\n' ' ')"
  stopped_count=0
  for container in $containers; do
    if [ "$DB_ONLY" = true ]; then
      if [[ "$container" =~ chomp-(db|redis) ]]; then
        echo "  🛑 Stopping database container: $container"
        docker_cmd stop "$container" 2>/dev/null || true
        ((stopped_count++))
      fi
    elif [ "$KEEP_DB" = false ] || [[ ! "$container" =~ chomp-(db|redis) ]]; then
      echo "  🛑 Stopping container: $container"
      docker_cmd stop "$container" 2>/dev/null || true
      ((stopped_count++))
    fi
  done
  [ $stopped_count -gt 0 ] && echo "  ✅ Stopped $stopped_count containers"
else
  echo "  ℹ️  No chomp containers found"
fi

# Kill remaining processes on ports (more comprehensive)
echo "🔌 Checking processes on ports..."
if [ "$DB_ONLY" = true ]; then
  for port in 40001 40002; do
    port_pids=$(lsof -Pi :$port -sTCP:LISTEN -t 2>/dev/null || true)
    if [ -n "$port_pids" ]; then
      echo "  🚨 Found processes on port $port: $(echo "$port_pids" | tr '\n' ' ')"
      echo "$port_pids" | xargs -r kill -9 2>/dev/null || true
    fi
  done
  echo "✅ Database stopped"
else
  # Kill API processes on port 40004
  api_port_pids=$(lsof -Pi :40004 -sTCP:LISTEN -t 2>/dev/null || true)
  if [ -n "$api_port_pids" ]; then
    echo "  🚨 Found API processes on port 40004: $(echo "$api_port_pids" | tr '\n' ' ')"
    echo "$api_port_pids" | xargs -r kill -9 2>/dev/null || true
  fi

  # Kill database processes if not keeping DB
  if [ "$KEEP_DB" = false ]; then
    for port in 40001 40002; do
      port_pids=$(lsof -Pi :$port -sTCP:LISTEN -t 2>/dev/null || true)
      if [ -n "$port_pids" ]; then
        echo "  🚨 Found database processes on port $port: $(echo "$port_pids" | tr '\n' ' ')"
        echo "$port_pids" | xargs -r kill -9 2>/dev/null || true
      fi
    done
  fi

  # Final cleanup: find any python processes still writing to out.log
  if [ -f "out.log" ]; then
    echo "  🔍 Checking for processes still writing to out.log..."
    log_pids=$(lsof "out.log" 2>/dev/null | awk 'NR>1 {print $2}' | sort -u || true)
    if [ -n "$log_pids" ]; then
      echo "  🚨 Found processes still writing to out.log: $(echo "$log_pids" | tr '\n' ' ')"
      echo "$log_pids" | while read -r pid; do
        if [ -n "$pid" ]; then
          echo "    💀 Force killing process $pid"
          kill -9 "$pid" 2>/dev/null || true
        fi
      done
    else
      echo "  ✅ No processes writing to out.log"
    fi
  fi

  echo "✅ Services stopped"
fi
