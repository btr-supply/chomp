#!/bin/bash
set -e

# Project directory detection - fix to correctly point to /back (two levels up from scripts)
PARENT="${PARENT:-$(cd "$(dirname "$(dirname "${BASH_SOURCE[0]}")")/.." && pwd)}"
export PARENT
CHOMP_DIR="${CHOMP_DIR:-$PARENT/chomp}"
export CHOMP_DIR

# Environment setup
find_env() {
  local mode_arg=""
  local deployment_arg=""
  for arg in "$@"; do
    case "$arg" in
      dev|prod) mode_arg="$arg" ;;
      local|docker) deployment_arg="$arg" ;;
    esac
  done

  local mode=${MODE:-${mode_arg:-"dev"}}
  local deployment=${DEPLOYMENT:-${deployment_arg:-"docker"}}
  local env_file="${ENV_FILE:-}"

  if [ -z "$env_file" ]; then
    env_file="$PARENT/.env${mode:+.$mode}"
    [ "$mode" = "prod" ] && env_file="$PARENT/.env"
    [ ! -f "$env_file" ] && env_file="$PARENT/.env.dev"
  fi

  if [ -f "$env_file" ]; then
    set -a # auto-export all variables
    source "$env_file" 2>/dev/null || true
    set +a
    # Re-parse args after sourcing, to allow .env to override
    for arg in "$@"; do
      case "$arg" in
        dev|prod) mode_arg="$arg" ;;
        local|docker) deployment_arg="$arg" ;;
      esac
    done
    deployment=${deployment_arg:-${DEPLOYMENT:-"$deployment"}}
    export ENV="$env_file" MODE=${mode_arg:-${MODE:-"dev"}} DEPLOYMENT="$deployment"

    if [ "$deployment" = "local" ]; then
      export DB_HOST="localhost" REDIS_HOST="localhost"
    else
      export DB_HOST="${DB_CONTAINER:-chomp-db}" REDIS_HOST="${DB_CONTAINER:-chomp-db}"
    fi
    return 0
  fi

  echo "❌ Environment file not found: $env_file" >&2
  return 1
}

# Configuration
CORE_IMAGE="${CORE_IMAGE:-chomp-core:${MODE:-dev}}"
DB_IMAGE="${DB_IMAGE:-chomp-db:${MODE:-dev}}"
DOCKER_NET="${DOCKER_NET:-chomp-net}"
DB_CONTAINER="${DB_CONTAINER:-chomp-db}"
API_CONTAINER="${API_CONTAINER:-chomp-api}"

# Argument parsing
parse_common_args() {
  for arg in "$@"; do
    case "$arg" in
      dev|prod) export MODE="$arg" ;;
      local|docker) export DEPLOYMENT="$arg" ;;
      api|noapi) export API="$arg" ;;
    esac
  done
  export MODE=${MODE:-"dev"} DEPLOYMENT=${DEPLOYMENT:-"docker"} API=${API:-"api"}
}

# Docker utilities
check_docker() {
  command -v docker &>/dev/null || { echo "❌ Docker not found"; exit 1; }
  [[ "$OSTYPE" == "darwin"* ]] && ! pgrep -f "Docker Desktop" >/dev/null 2>&1 && { echo "❌ Docker Desktop not running"; exit 1; }

  echo "🔍 Checking Docker daemon..."
  if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker not running: daemon not responding"
    exit 1
  fi
}

docker_cmd() {
  if [ "$EUID" -eq 0 ] && [ -n "$SUDO_USER" ]; then
    sudo -u "$SUDO_USER" docker "$@"
  else
    docker "$@"
  fi
}

ensure_network() {
  docker_cmd network inspect "$1" &>/dev/null || docker_cmd network create "$1";
}

build_docker_images() {
  local dockerfile=$1 image=$2; shift 2
  docker_cmd build -f "$dockerfile" -t "$image" .
  for tag in "$@"; do docker_cmd tag "$image" "$tag"; done
}

docker_image_exists() {
  docker_cmd images --format "{{.Repository}}:{{.Tag}}" | grep -q "^${1}:latest$";
}
docker_container_running() {
  docker_cmd ps --format "{{.Names}}" | grep -q "^${1}$";
}

docker_stop_remove() {
  local container=$1
  docker_container_running "$container" && docker_cmd stop "$container"
  docker_cmd ps -a --format "{{.Names}}" | grep -q "^${container}$" && docker_cmd rm "$container"
}

docker_cleanup_pattern() {
  local pattern="$1"
  local running_containers=$(docker_cmd ps --format '{{.Names}}' --filter "name=$pattern" || true)
  if [ -n "$running_containers" ]; then
    docker_cmd stop $running_containers
  fi

  local all_containers=$(docker_cmd ps -a --format '{{.Names}}' --filter "name=$pattern" || true)
  if [ -n "$all_containers" ]; then
    docker_cmd rm $all_containers
  fi
}

docker_stop_all() {
  local containers=$(docker_cmd ps --filter "name=chomp-*" --format "{{.Names}}" || true)
  [ -n "$containers" ] && docker_cmd stop $containers
}

docker_cleanup_all() {
  docker_stop_all
  local containers=$(docker_cmd ps -a --filter "name=chomp-*" --format "{{.Names}}" || true)
  [ -n "$containers" ] && docker_cmd rm $containers
  local networks=$(docker_cmd network ls --filter "name=chomp*" --format "{{.Name}}" | grep -v "^bridge$" || true)
  [ -n "$networks" ] && docker_cmd network rm $networks
}

docker_monitor() {
  local containers=$(docker_cmd ps --filter "name=chomp-*" --format "{{.Names}}")
  if [ -n "$containers" ]; then
    docker_cmd ps --filter "name=chomp-*" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    docker_cmd stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" $containers
  else
    echo "⚠️ No containers running"
    return 1
  fi
}

docker_show_logs() {
  local container=${1:-""}
  if [ -n "$container" ]; then
    docker_cmd logs "$container" --tail 50
  else
    for container in $(docker_cmd ps --filter "name=chomp-*" --format "{{.Names}}"); do
      echo "=== $container ===" && docker_cmd logs "$container" --tail 20 2>/dev/null || echo "No logs"
    done
  fi
}

# Path resolution utilities
resolve_config_path() {
  local config_path="$1"

  # If absolute path, use as-is
  if [[ "$config_path" == /* ]]; then
    echo "$config_path"
  # Otherwise, treat as relative to chomp directory
  else
    echo "$CHOMP_DIR/$config_path"
  fi
}

# Configuration utilities
count_config_resources() {
  [ ! -f "$1" ] && { echo "Config $1 not found" >&2; return 1; }

  # Count top-level resources (jobs) - entries that start with "- name:" at the beginning of lines
  # This counts actual ingester resources, not individual fields within them
  # Exclude commented lines by ensuring the line doesn't start with whitespace followed by #
  local resource_count=$(grep "^[[:space:]]*- name:" "$1" | grep -v "^\s*#" | wc -l)
  [ "$resource_count" -gt 0 ] && { echo "$resource_count"; return; }

  # BTR nested configs (if no direct resources found) - also exclude comments
  grep -E "^\s*-\s*\"?\./" "$1" | grep -v "^\s*#" | wc -l
}

count_config_fields() {
  [ ! -f "$1" ] && { echo "Config $1 not found" >&2; return 1; }

  # Count individual fields within resources (for informational purposes)
  # This counts all field definitions regardless of their format (inline or under fields: sections)
  # Exclude commented lines by ensuring the line doesn't start with whitespace followed by #
  local field_count=$(grep -E "^\s*-\s*\{.*name:" "$1" | grep -v "^\s*#" | wc -l)
  echo "$field_count"
}

# Backward compatibility alias
count_config_jobs() {
  count_config_resources "$@"
}

extract_nested_configs() {
  [ ! -f "$1" ] && { echo "Config $1 not found" >&2; return 1; }
  grep -E '^\s*-\s*"\.\/.*\.yml"' "$1" | sed 's/.*"\(.*\)".*/\1/' | sed 's|^\./||'
}

expand_config() {
  local config_file="$1"
  [ ! -f "$config_file" ] && { echo "Config file not found: $config_file" >&2; return 1; }

  local parent_namespace=$(basename "$config_file" .yml)
  local nested_configs=$(extract_nested_configs "$config_file" 2>/dev/null)

  if [ -n "$nested_configs" ]; then
    while IFS= read -r nested_config; do
      if [ -n "$nested_config" ]; then
        local nested_path="$(dirname "$config_file")/$nested_config"
        if [ -f "$nested_path" ]; then
          local child_namespace=$(basename "$nested_config" .yml)
          local resource_count=$(count_config_resources "$nested_path")
          local field_count=$(count_config_fields "$nested_path")
          echo "${parent_namespace}.${child_namespace}:$nested_path:$resource_count:$field_count"
        fi
      fi
    done <<< "$nested_configs"
  else
    local resource_count=$(count_config_resources "$config_file")
    local field_count=$(count_config_fields "$config_file")
    echo "$parent_namespace:$config_file:$resource_count:$field_count"
  fi
}

expand_all_configs() {
  local temp_file=$(mktemp)
  echo "$1" | tr ',' '\n' > "$temp_file"

  while read -r config; do
    config=$(echo "$config" | xargs)
    if [ -n "$config" ]; then
      # Resolve the config path using unified resolution
      local resolved_config="$(resolve_config_path "$config")"
      if [ -f "$resolved_config" ]; then
        expand_config "$resolved_config"
      else
        echo "⚠️ Config file not found: $config (resolved: $resolved_config)" >&2
      fi
    fi
  done < "$temp_file"

  rm -f "$temp_file"
  return 0
}

# Health checks
test_api_health() { command -v curl &>/dev/null && curl -s "http://${1:-localhost}:${2:-40004}/health" &>/dev/null; }

wait_for_api_health() {
  local attempt=1
  while [ $attempt -le 30 ]; do
    sleep 2 && test_api_health "$@" && return 0
    ((attempt++))
  done
  return 1
}

wait_for_db_health() {
  local attempt=1
  while [ $attempt -le 30 ]; do
    sleep 2
    docker_cmd exec "$DB_CONTAINER" redis-cli -p "$REDIS_PORT" --user "$DB_RW_USER" --pass "$DB_RW_PASS" ping &>/dev/null && \
    docker_cmd exec "$DB_CONTAINER" taos -h localhost -P "$DB_PORT" -u "$DB_RW_USER" -p"$DB_RW_PASS" -k &>/dev/null && return 0
    ((attempt++))
  done
  return 1
}

# Docker run with environment
docker_run_with_env() {
  local env_abs=$(realpath "$ENV" 2>/dev/null || echo "$ENV")
  docker_cmd run --env-file "$env_abs" \
    -e "DB_HOST=$DB_HOST" -e "DB_PORT=$DB_PORT" -e "DB_RW_USER=$DB_RW_USER" -e "DB_RW_PASS=$DB_RW_PASS" \
    -e "REDIS_HOST=$REDIS_HOST" -e "REDIS_PORT=$REDIS_PORT" "$@"
}

# find_env
