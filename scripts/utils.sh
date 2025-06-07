#!/bin/bash
set -e

# Set project directories
PARENT="${PARENT:-$(dirname "$(dirname "$(dirname "$(realpath "${BASH_SOURCE[0]}")")")")}"
export PARENT

# Environment setup
find_env() {
  local mode=${MODE:-"dev"}
  local deployment=${DEPLOYMENT:-"docker"}

  # Use ENV_FILE if explicitly set, otherwise determine based on mode
  local env_file=""
  if [ -n "${ENV_FILE:-}" ] && [ -f "${ENV_FILE:-}" ]; then
    env_file="$ENV_FILE"
    echo "📁 Using explicitly set ENV_FILE: $env_file"
  else
    case "$mode" in
      "dev") env_file="$PARENT/.env.dev" ;;
      "prod") env_file="$PARENT/.env" ;;
      *) echo "⚠️ Unknown mode '$mode'. Using dev defaults."; env_file="$PARENT/.env.dev" ;;
    esac

    # Fallback search if not found in parent
    if [ ! -f "$env_file" ]; then
      for candidate in ".env.dev" ".env" "../.env.dev" "../.env"; do
        if [ -f "$candidate" ]; then
          env_file="$candidate"
          break
        fi
      done
    fi
  fi

  # Source the found env file
  if [ -n "$env_file" ] && [ -f "$env_file" ]; then
    echo "📁 Loading environment from: $env_file (mode=$mode, deployment=$deployment)"
    source "$env_file" 2>/dev/null || true

    # Export key variables
    export ENV="$env_file"
    export MODE="$mode"
    export DEPLOYMENT="$deployment"

    # Set host/port overrides based on deployment type
    if [ "$deployment" = "local" ]; then
      export DB_HOST="localhost"
      export REDIS_HOST="localhost"
      echo "🔧 Local deployment: Using localhost for connections"
    elif [ "$deployment" = "docker" ]; then
      export DB_HOST="${DB_CONTAINER:-chomp-db}"
      export REDIS_HOST="${DB_CONTAINER:-chomp-db}"
      echo "🐳 Docker deployment: Using container network (DB: $DB_HOST, Redis: $REDIS_HOST)"
    fi

    return 0
  fi

  echo "❌ Could not find environment file"
  echo "📁 Searched: $env_file"
  return 1
}

# Initialize environment on source
find_env

# Docker configuration - from environment with sensible defaults
CORE_IMAGE="${CORE_IMAGE:-chomp-core:${MODE:-dev}}"
API_IMAGE="${API_IMAGE:-$CORE_IMAGE}"
INGESTER_IMAGE="${INGESTER_IMAGE:-$CORE_IMAGE}"
DB_IMAGE="${DB_IMAGE:-chomp-db:${MODE:-dev}}"

DOCKER_NET="${DOCKER_NET:-chomp-net}"
DB_CONTAINER="${DB_CONTAINER:-chomp-db}"
API_CONTAINER="${API_CONTAINER:-chomp-api}"

# Helper function to parse common arguments
parse_common_args() {
  for arg in "$@"; do
    case "$arg" in
      dev|prod) export MODE="$arg" ;;
      local|docker) export DEPLOYMENT="$arg" ;;
      api|noapi) export API="$arg" ;;
    esac
  done

  # Set defaults
  export MODE=${MODE:-"dev"}
  export DEPLOYMENT=${DEPLOYMENT:-"docker"}
  export API=${API:-"api"}
}

# Core utilities
check_sudo() { [ "$EUID" -eq 0 ] || { echo "Requires sudo"; exit 1; }; }

check_docker() {
  # Check if docker command exists
  if ! command -v docker &>/dev/null; then
    echo "❌ Error: Docker is not installed or not in PATH"
    echo "Please install Docker Desktop: https://www.docker.com/products/docker-desktop"
    exit 1
  fi

  # Check if Docker Desktop is running
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

build_docker_images() {
  local dockerfile=$1 image=$2; shift 2
  docker_cmd build -f "$dockerfile" -t "$image" .
  for tag in "$@"; do docker_cmd tag "$image" "$tag"; done
}

# Docker utility functions
docker_image_exists() {
  docker_cmd images --format "{{.Repository}}:{{.Tag}}" | grep -q "^${1}:latest$"
}

docker_container_running() {
  docker_cmd ps --format "{{.Names}}" | grep -q "^${1}$"
}

docker_stop_remove() {
  local container=$1
  if docker_container_running "$container"; then
    echo "Stopping $container..."
    docker_cmd stop "$container"
  fi
  if docker_cmd ps -a --format "{{.Names}}" | grep -q "^${container}$"; then
    echo "Removing $container..."
    docker_cmd rm "$container"
  fi
}

docker_cleanup_pattern() {
  local pattern=$1
  local containers=$(docker_cmd ps -a --format '{{.Names}}' | grep -E "$pattern" || true)
  if [ -n "$containers" ]; then
    for container in $containers; do
      docker_cmd stop "$container" && docker_cmd rm "$container"
    done
  fi
}

# Monitoring functions
docker_monitor() {
  echo "📊 Chomp Monitor"
  echo "==============="

  local containers=$(docker_cmd ps --filter "name=chomp-*" --format "{{.Names}}")
  if [ -n "$containers" ]; then
    docker_cmd ps --filter "name=chomp-*" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    echo ""
    docker_cmd stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" $(echo $containers | tr '\n' ' ')
  else
    echo "⚠️ No containers running"
    return 1
  fi
}

docker_stop_all() {
  local containers=$(docker_cmd ps --filter "name=chomp-*" --format "{{.Names}}" || true)
  if [ -n "$containers" ]; then
    echo "$containers" | xargs -r docker_cmd stop
    echo "✅ All Chomp containers stopped"
  else
    echo "No Chomp containers running"
  fi
}

docker_cleanup_all() {
  docker_stop_all
  local containers=$(docker_cmd ps -a --filter "name=chomp-*" --format "{{.Names}}" || true)
  if [ -n "$containers" ]; then
    echo "$containers" | xargs -r docker_cmd rm
    echo "✅ All Chomp containers removed"
  fi

  # Clean up networks
  local networks=$(docker_cmd network ls --filter "name=chomp*" --format "{{.Name}}" | grep -v "^bridge$" || true)
  if [ -n "$networks" ]; then
    echo "$networks" | xargs -r docker_cmd network rm
    echo "✅ Chomp networks removed"
  fi
}

docker_show_logs() {
  local container=${1:-""}
  if [ -n "$container" ]; then
    docker_cmd logs "$container" --tail 50
  else
    # Show logs for all chomp containers
    for container in $(docker_cmd ps --filter "name=chomp-*" --format "{{.Names}}"); do
      echo "=== $container ==="
      docker_cmd logs "$container" --tail 20 2>/dev/null || echo "No logs available"
      echo ""
    done
  fi
}

# Configuration utilities
count_config_jobs() {
  if [ ! -f "$1" ]; then
    echo "Config $1 not found" >&2
    return 1
  fi

  # Try chomp format first (- name:)
  local chomp_count=$(grep "^[[:space:]]*- name:" "$1" | wc -l)
  if [ "$chomp_count" -gt 0 ]; then
    echo "$chomp_count"
    return 0
  fi

  # Try BTR format with quotes (- "./file.yml")
  local btr_quoted_count=$(grep -E "^\s*-\s*\"\./" "$1" | wc -l)
  if [ "$btr_quoted_count" -gt 0 ]; then
    echo "$btr_quoted_count"
    return 0
  fi

  # Try BTR format without quotes (- ./file.yml)
  local btr_unquoted_count=$(grep -E "^\s*-\s*\./" "$1" | wc -l)
  echo "$btr_unquoted_count"
}

# Health check functions
db_health_check() {
  local redis_port="$1" DB_port="$2" db_user="$3" db_pass="$4"
  local success=true

  # Test Redis connection via Docker exec with user authentication
  docker_cmd exec "$DB_CONTAINER" redis-cli -p "$redis_port" --user "$db_user" --pass "$db_pass" ping &>/dev/null || success=false

  # Test TDengine connection via Docker exec
  docker_cmd exec "$DB_CONTAINER" taos -h localhost -P "$DB_port" -u "$db_user" -p"$db_pass" -k &>/dev/null || success=false

  $success
}

wait_for_db_health() {
  local attempt=1
  while [ $attempt -le 30 ]; do
    sleep 2
    db_health_check "$REDIS_PORT" "$DB_PORT" "$DB_RW_USER" "$DB_RW_PASS" && return 0
    ((attempt++))
  done
  return 1
}

test_api_health() {
  local host=${1:-"localhost"}
  local port=${2:-40004}
  command -v curl &>/dev/null && curl -s "http://$host:$port/health" &>/dev/null
}

wait_for_api_health() {
  local host=${1:-"localhost"}
  local port=${2:-40004}
  local attempt=1
  while [ $attempt -le 30 ]; do
    sleep 2
    test_api_health "$host" "$port" && return 0
    ((attempt++))
  done
  return 1
}

# Docker run with environment variables
docker_run_with_env() {
  local env_abs=$(realpath "$ENV" 2>/dev/null || echo "$ENV")

  # Separate Docker flags from image and app arguments
  local docker_args=()
  local app_args=()
  local found_image=false

  for arg in "$@"; do
    if ! $found_image && [[ "$arg" != -* ]] && [[ "$arg" != --* ]] && [[ "$arg" =~ ^[a-zA-Z0-9_-]+[:/]?[a-zA-Z0-9_.-]*$ ]]; then
      # This looks like an image name (not starting with - and matches image name pattern)
      found_image=true
      docker_args+=("$arg")
    elif $found_image; then
      # Everything after image name goes to app
      app_args+=("$arg")
    else
      # Docker flags go before image name
      docker_args+=("$arg")
    fi
  done

  docker_cmd run \
    --env-file "$env_abs" \
    -e "DB_HOST=$DB_HOST" \
    -e "DB_PORT=$DB_PORT" \
    -e "DB_RW_USER=$DB_RW_USER" \
    -e "DB_RW_PASS=$DB_RW_PASS" \
    -e "REDIS_HOST=$REDIS_HOST" \
    -e "REDIS_PORT=$REDIS_PORT" \
    "${docker_args[@]}" \
    "${app_args[@]}"
}
