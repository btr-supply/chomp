#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

ENV_FILE=${ENV_FILE:-".env"}
CONFIG_FILE=${CONFIG_FILE:-"examples/diverse.yml"}
MAX_JOBS=${MAX_JOBS:-"8"}

check_sudo
check_docker
ensure_network $DOCKER_NET

# Validation
[ -f "$CONFIG_FILE" ] || { echo "❌ Config not found: $CONFIG_FILE"; exit 1; }
[ -f "$ENV_FILE" ] || { echo "❌ Environment not found: $ENV_FILE"; exit 1; }
check_container_running "$DB_CONTAINER" "Database not running. Run 'make db-setup' first." || exit 1
docker image inspect $CORE_IMAGE &>/dev/null || { echo "❌ Core image not found. Run 'make build-images'"; exit 1; }

# Start cluster
total_jobs=$(count_yaml_jobs "$CONFIG_FILE")
echo "Starting cluster: $total_jobs jobs, $MAX_JOBS per instance"
cleanup_containers "chomp-ingester.*"

instances=$(run_ingester_cluster "$DOCKER_NET" "Chomp" "chomp-ingester" "$CORE_IMAGE" "$ENV_FILE" "$CONFIG_FILE" "$MAX_JOBS" "$total_jobs")

echo "✅ Cluster started: $instances instances, $((instances * MAX_JOBS)) capacity"
echo "Monitor: make monitor | make logs"
