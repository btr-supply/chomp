#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

parse_common_args "$@"

SERVICE=${1:-"ingester"}
max_jobs=${MAX_JOBS:-15}
host=${SERVER_HOST:-"0.0.0.0"}
port=${SERVER_PORT:-40004}
ingester_configs=${INGESTER_CONFIGS:-"ingesters/cexs.yml,ingesters/evm_dexs.yml,ingesters/other_dexs.yml,ingesters/processors.yml"}

echo "🚀 Starting $SERVICE: MODE=$MODE DEPLOYMENT=$DEPLOYMENT"

if [ "$DEPLOYMENT" = "docker" ]; then
  check_docker
  ensure_network "$DOCKER_NET"

  # Build image if needed
  if ! docker_image_exists "$CORE_IMAGE"; then
    echo "Building $CORE_IMAGE..."
    build_docker_images ./Dockerfile.core "$CORE_IMAGE"
  fi

  # Check database
  docker_container_running "$DB_CONTAINER" || { echo "❌ Database not running"; exit 1; }

  # Prepare verbose flag
  VERBOSE_FLAG=""
  if [ "${VERBOSE:-false}" = "true" ]; then
    VERBOSE_FLAG="-v"
  fi

  if [ "$SERVICE" = "api" ]; then
    # API server
    docker_stop_remove "$API_CONTAINER"
    echo "Starting API on http://$host:$port"
         docker_run_with_env -d --network "$DOCKER_NET" --label project=Chomp \
       --name "$API_CONTAINER" -p "$port:$port" \
       -v "$(realpath ../ingesters):/app/ingesters:ro" \
       "$CORE_IMAGE" \
       --ingester_configs "$ingester_configs" --server --host "$host" --port "$port" $VERBOSE_FLAG
    wait_for_api_health "$host" "$port" && echo "✅ API ready"
  else
    # Ingesters
    docker_cleanup_pattern "chomp-ingester.*"

         # Calculate containers needed
     total_jobs=0
     IFS=',' read -ra configs <<< "$ingester_configs"
     for config in "${configs[@]}"; do
       config=$(echo "$config" | xargs)
       config_path="../$config"  # Look in parent directory where BTR ingesters are
       [ -f "$config_path" ] && total_jobs=$((total_jobs + $(count_config_jobs "$config_path")))
     done

    containers=$((( total_jobs + max_jobs - 1 ) / max_jobs))
    echo "Starting $containers containers for $total_jobs jobs (max $max_jobs each)"

         for ((i=1; i<=containers; i++)); do
       docker_run_with_env -d --network "$DOCKER_NET" --label project=Chomp \
         --name "chomp-ingester-$i" \
         -v "$(realpath ../ingesters):/app/ingesters:ro" \
         "$CORE_IMAGE" \
         --ingester_configs "$ingester_configs" --max_jobs "$max_jobs" $VERBOSE_FLAG --host "0.0.0.0"
       sleep 0.5
     done
    echo "✅ Started $containers ingester containers"
  fi
else
  # Local deployment
  [ ! -f ".venv/bin/activate" ] && { echo "❌ Run setup first"; exit 1; }
  source .venv/bin/activate

  # Determine log file path (parent directory)
  LOG_FILE="${OUT_LOG:-out.log}"
  if [[ "$LOG_FILE" != /* ]]; then
    # Relative path, make it relative to parent directory
    LOG_FILE="../$LOG_FILE"
  fi

  # Change to parent directory to run main.py so config paths are correct
  cd "$(dirname "${BASH_SOURCE[0]}")/../.."

  # Prepare verbose flag
  VERBOSE_FLAG=""
  if [ "${VERBOSE:-false}" = "true" ]; then
    VERBOSE_FLAG="-v"
  fi

  # Unified PID file: .pid
  PID_FILE="chomp/.pid"

  if [ "$SERVICE" = "api" ]; then
    # API server uses configs for startup
    nohup uv run python chomp/main.py ${ENV:+-e "$ENV"} --server --host "$host" --port "$port" --ingester_configs "$ingester_configs" $VERBOSE_FLAG >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "✅ API started (PID: $!, file: $PID_FILE)"
  else
    # Validate configs exist in parent directory
    IFS=',' read -ra configs <<< "$ingester_configs"
    for config in "${configs[@]}"; do
      config=$(echo "$config" | xargs)
      [ ! -f "$config" ] && { echo "❌ Config not found: $config"; exit 1; }
    done

    nohup uv run python chomp/main.py ${ENV:+-e "$ENV"} --ingester_configs "$ingester_configs" --max_jobs "$max_jobs" $VERBOSE_FLAG >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "✅ Ingesters started (PID: $!, file: $PID_FILE)"
  fi
fi
