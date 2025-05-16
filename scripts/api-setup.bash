#!/bin/bash
set -e
source "$(dirname "${BASH_SOURCE[0]}")/utils.bash"

check_sudo
ensure_network $DOCKER_NET
cd $PARENT
build_images Dockerfile.core $CORE_IMAGE $API_IMAGE
check_container_running $DB_CONTAINER "Database not running. Run db-setup.bash first." || exit 1
cleanup_containers $API_CONTAINER
# db_client_health_check $DOCKER_NET $HEALTHCHECK_CONTAINER $CORE_IMAGE $ENV || exit 1

# Start server node
echo "Starting Chomp server node..."
docker run -d \
  --env-file $ENV \
  --network $DOCKER_NET \
  --name $API_CONTAINER \
  -p $SERVER_PORT:$SERVER_PORT \
  $API_IMAGE \
  -v -s -e $ENV -sh $SERVER_HOST -sp $SERVER_PORT

wait_for_healthy $API_CONTAINER || exit 1

echo "Chomp server node running!"
