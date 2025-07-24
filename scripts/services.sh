#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

find_env "$@"
parse_common_args "$@"

SERVICE=${1:-"ingester"}
max_jobs=${MAX_JOBS:-6}
host=${SERVER_HOST:-"0.0.0.0"}
port=${SERVER_PORT:-40004}

echo "🚀 Starting $SERVICE: MODE=$MODE DEPLOYMENT=$DEPLOYMENT"

# Deduplicate configs
deduplicate_configs() {
  local result=$(echo "$1" | tr ',' '\n' | sort -u | tr '\n' ',' | sed 's/,$//')
  echo "$result"
  return 0
}

# Spawn instances for configs
spawn_instances() {
  local expanded_configs="$1" deployment="$2" spawned_count=0
  local spawned_pids=()

  while IFS=':' read -r namespace config_path resource_count field_count; do
    [ -z "$namespace" ] && continue
    [ "$resource_count" -eq 0 ] && { echo "⚠️ No resources in $namespace, skipping..."; continue; }

    local instances_needed=$(( (resource_count + max_jobs - 1) / max_jobs ))
    echo "📦 Config: $namespace ($resource_count resources, $field_count fields) → $instances_needed instances" >&2

    for ((i=1; i<=instances_needed; i++)); do
      if [ "$deployment" = "docker" ]; then
        # For Docker, ../ingesters is mounted as /app/ingesters.
        # The config_path is like ../ingesters/..., so we remove ../
        local docker_config_path=$(echo "$config_path" | sed 's|^\.\./||')
        docker_run_with_env -d --network "$DOCKER_NET" --label project=Chomp \
          --name "chomp-ingester-${namespace}-${i}" \
          -v "$(realpath ../ingesters):/app/ingesters:ro" \
          "$CORE_IMAGE" \
          --ingester_configs "$docker_config_path" --max_jobs "$max_jobs" ${VERBOSE_FLAG:-}
      else
        # For local deployment, config_path is relative to project root, which is the CWD
        nohup uv run python chomp/main.py ${ENV:+-e "$ENV"} --ingester_configs "$config_path" --max_jobs "$max_jobs" ${VERBOSE_FLAG:-} >> "$LOG_FILE" 2>&1 &
        spawned_pids+=($!)
        echo "  Instance $i: PID $!" >&2
      fi
      spawned_count=$((spawned_count + 1))
      sleep 0.5
    done
  done <<< "$expanded_configs"

  # Write PIDs for local deployment
  [ "$deployment" = "local" ] && [ ${#spawned_pids[@]} -gt 0 ] && printf '%s\n' "${spawned_pids[@]}" > "./chomp/.pid"
  echo "$spawned_count"
}

# Common setup for both deployments
setup_common() {
  # Validate required environment variables
  if [ -z "${INGESTER_CONFIGS:-}" ]; then
    echo "❌ INGESTER_CONFIGS environment variable is not set"
    echo "   Please set INGESTER_CONFIGS in your environment file"
    exit 1
  fi

  # Set ingester_configs after environment is loaded
  ingester_configs=$(deduplicate_configs "$INGESTER_CONFIGS")

  # Validate server config for API deployment
  if [ "$API" = "api" ] && [ -z "${SERVER_CONFIG:-}" ]; then
    echo "❌ SERVER_CONFIG environment variable is not set"
    echo "   Please set SERVER_CONFIG in your environment file"
    exit 1
  fi

  VERBOSE_FLAG=""
  [ "${VERBOSE:-false}" = "true" ] && VERBOSE_FLAG="-v"
}

# API service startup
start_api() {
  local deployment=$1
  if [ "$deployment" = "local" ]; then
    # Start the API server in the background and record its PID
    (
      cd chomp &&
      uv run python -m src.server --port 40004 >> "$LOG_FILE" 2>&1
    ) &
    local api_pid=$!
    (cd chomp && uv run python -c "from src.utils.runtime import runtime; runtime.add_pid($api_pid, True)")
    echo "✅ API started (PID: $api_pid)"

  elif [ "$deployment" = "docker" ]; then
    docker_stop_remove "$API_CONTAINER"
    echo "Starting API server..."

    # Mount server config file at runtime
    local server_config_path="${SERVER_CONFIG}"
    local host_config_path="$(resolve_config_path "$server_config_path")"

    docker_run_with_env -d --network "$DOCKER_NET" --label project=Chomp \
      --name "$API_CONTAINER" -p "$port:$port" \
      -v "$host_config_path:/app/server-config.yml:ro" \
      "$CORE_IMAGE" --server --server_config "./server-config.yml" $VERBOSE_FLAG
    wait_for_api_health "$host" "$port" && echo "✅ API ready"
  fi
}

# Ingester service startup
start_ingesters() {
  # Use the existing spawn_instances function which handles both docker and local deployments
  local expanded_configs=$(expand_all_configs "$ingester_configs")
  local spawned_count=$(spawn_instances "$expanded_configs" "$DEPLOYMENT")

  echo "✅ Started $spawned_count ingester instances"
}

# Main execution
setup_common

if [ "$DEPLOYMENT" = "docker" ]; then
  check_docker && ensure_network "$DOCKER_NET"
  ! docker_image_exists "$CORE_IMAGE" && { echo "Building $CORE_IMAGE..."; build_docker_images ./Dockerfile.core "$CORE_IMAGE"; }
  docker_container_running "$DB_CONTAINER" || { echo "❌ Database not running"; exit 1; }
else
  cd "$PARENT"
  [ ! -f ".venv/bin/activate" ] && { echo "❌ Run setup first"; exit 1; }
  source .venv/bin/activate
  LOG_FILE="${OUT_LOG:-out.log}"
  [[ "$LOG_FILE" != /* ]] && LOG_FILE="$PARENT/$LOG_FILE"
fi

case "$SERVICE" in
  api)
    start_api "$DEPLOYMENT"
    ;;
  ingester)
    start_ingesters
    echo "✅ Started ingester processes"
    ;;
  *) echo "❌ Unknown service: $SERVICE"; exit 1 ;;
esac

exit 0
