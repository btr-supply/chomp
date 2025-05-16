#!/bin/bash
set -e
source "$(dirname "${BASH_SOURCE[0]}")/utils.bash"

check_sudo
ensure_network $DOCKER_NET
cd $PARENT
build_images Dockerfile.core $CORE_IMAGE $API_IMAGE $INGESTER_IMAGE
check_container_running $DB_CONTAINER "Database not running. Run db-setup.bash first." || exit 1
cleanup_containers $API_CONTAINER chomp-ingester.core-*
db_client_health_check $DOCKER_NET $HEALTHCHECK_CONTAINER $INGESTER_IMAGE $ENV || exit 1

# Start ingestion instances
echo "Starting Chomp ingestion cluster ($CLUSTER_INSTANCES instances)..."
run_ingester_cluster $DOCKER_NET Chomp chomp-ingester $INGESTER_IMAGE $ENV $CONFIG_PATH 10

# Start server node
echo "Starting Chomp server node..."
docker run -d --env-file $ENV --network $DOCKER_NET --name $API_CONTAINER $API_IMAGE -v -s -e $ENV -c $CONFIG_PATH

# wait_for_healthy $API_CONTAINER "chomp-ingester.core-"*

echo "Core setup completed successfully!"
