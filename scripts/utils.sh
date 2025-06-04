#!/bin/bash
set -e

# Environment setup
find_env() {
  # Try multiple approaches to find .env file
  local env_file=""

  # First, try relative to script location if BASH_SOURCE is available
  if [ -n "${BASH_SOURCE[0]}" ]; then
    local script_dir="$(dirname "${BASH_SOURCE[0]}")"
    local depth=0
    local dir="$script_dir"
    while [ "$dir" != "/" ] && [ $depth -lt 10 ]; do
      if [ -f "$dir/.env" ]; then
        env_file="$dir/.env"
        break
      fi
      dir="$(dirname "$dir")"
      ((depth++))
    done
  fi

  # If not found, try common locations
  if [ -z "$env_file" ]; then
    for candidate in "./.env" "../.env" "../../.env"; do
      if [ -f "$candidate" ]; then
        env_file="$candidate"
        break
      fi
    done
  fi

  # Source the found .env file
  if [ -n "$env_file" ]; then
    echo "Using $env_file"
    source "$env_file" 2>/dev/null || true
    return 0
  fi

  return 1
}

find_env

# Core utilities
check_sudo() { [ "$EUID" -eq 0 ] || { echo "Requires sudo"; exit 1; }; }

check_docker() {
  # Check if docker command exists
  if ! command -v docker &>/dev/null; then
    echo "❌ Error: Docker is not installed or not in PATH"
    echo "Please install Docker Desktop: https://www.docker.com/products/docker-desktop"
    exit 1
  fi

  # Check if Docker Desktop is running as a process (simple and fast)
  if pgrep -f "Docker Desktop" >/dev/null 2>&1; then
    echo "✅ Docker Desktop is running"
  else
    echo "❌ Error: Docker Desktop is not running"
    echo ""
    echo "Please start Docker Desktop:"
    echo "  1. Run: open -a Docker"
    echo "  2. Wait for Docker to fully initialize"
    echo "  3. Look for Docker whale icon in your menu bar"
    echo "  4. Then retry this command"
    exit 1
  fi
}

# Function to run Docker commands with proper permissions
docker_cmd() {
  if [ "$EUID" -eq 0 ] && [ -n "$SUDO_USER" ]; then
    # Running as sudo, execute docker as the original user
    sudo -u "$SUDO_USER" docker "$@"
  else
    # Running normally, execute docker directly
    docker "$@"
  fi
}

ensure_network() {
  docker_cmd network inspect "$1" &>/dev/null || docker_cmd network create "$1"
}

build_images() {
  local dockerfile=$1 image=$2; shift 2
  docker_cmd build -f "$dockerfile" -t "$image" .
  for tag in "$@"; do docker_cmd tag "$image" "$tag"; done
}

check_container_running() {
  docker_cmd ps --format '{{.Names}}' | grep -q "^${1}$" || { echo "Error: ${2:-Container $1 not running}"; return 1; }
}

cleanup_containers() {
  local containers=$(docker_cmd ps -a --format '{{.Names}}' | grep -E "$1" || true)
  [ -n "$containers" ] && echo "$containers" | xargs -r -n1 sh -c 'docker_cmd stop "$1" && docker_cmd rm "$1"' _
}

count_yaml_jobs() {
  [ -f "$1" ] && grep "^[[:space:]]*- name:" "$1" | wc -l || { echo "Config $1 not found" >&2; return 1; }
}

db_client_health_check() {
  local uid=$(uuidgen)
  docker_cmd run --rm --env-file "$4" --network "$1" --name "$2-$uid" "$3" -v -e "$4" --ping
}

run_ingester_cluster() {
  local network=$1 project=$2 container_base=$3 image=$4 env=$5 config=$6 jobs_per_instance=$7
  local total_jobs=${8:-$(count_yaml_jobs "$config")}
  local required_instances=$(( ($total_jobs + $jobs_per_instance - 1) / $jobs_per_instance ))
  local config_stem=$(basename "$config" | cut -d. -f1)

  for ((i = 1; i <= required_instances; i++)); do
    docker_cmd run -d --env-file "$env" --network "$network" --label project="$project" \
      --name "${container_base}.${config_stem}-${i}" "$image" \
      -e "$env" -c "./ingesters/$config" -j "$jobs_per_instance" -sh "0.0.0.0"
    sleep 1
  done
  return $required_instances
}

wait_for_healthy() {
  local max_attempts=30 attempt=1
  while [ $attempt -le $max_attempts ]; do
    local all_healthy=true
    for container in "$@"; do
      local status=$(docker_cmd inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null)
      [ "$status" != "healthy" ] && [ "$status" != "running" ] && { all_healthy=false; break; }
    done
    $all_healthy && return 0
    sleep 2; ((attempt++))
  done
  return 1
}

db_health_check() {
  local success=true
  command -v redis-cli &>/dev/null && {
    redis-cli -h localhost -p "$1" --user "$3" --pass "$4" ping &>/dev/null || success=false
  }
  command -v taos &>/dev/null && {
    taos -h localhost -P "$2" -u "$3" -p"$4" -k 2>/dev/null | grep -q "service ok" || success=false
  }
  $success
}

wait_for_db() {
  local attempt=1
  while [ $attempt -le 30 ]; do
    sleep 2
    db_health_check "$REDIS_PORT" "$TAOS_PORT" "$DB_RW_USER" "$DB_RW_PASS" && return 0
    ((attempt++))
  done
  return 1
}
