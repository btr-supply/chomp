#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

check_sudo
check_docker
ensure_network $DOCKER_NET

# Build core image
echo "Building $CORE_IMAGE image..."
cd $PARENT
build_images ./Dockerfile.core $CORE_IMAGE

cleanup_containers $CORE_CONTAINER

# Run the new container instance
echo "Starting $CORE_CONTAINER container..."
docker run -d \
    --env-file "$ENV" \
    --label project=Chomp \
    --network "$DOCKER_NET" \
    --name "$CORE_CONTAINER" \
    -p "$PORT:$PORT" \
    "$CORE_IMAGE" \
    -e "$ENV" \
    -c "./examples/diverse.yml" \
    -j "$MAX_JOBS" \
    -sh "0.0.0.0" \
    --server

# Wait for service to be ready
echo "Waiting for core service to be ready..."
sleep 10

# Health check
echo "Performing health check..."
if db_client_health_check $DOCKER_NET $CORE_CONTAINER $CORE_IMAGE $ENV; then
  echo "Core setup completed successfully!"
else
  echo "Core setup failed - check container logs with: docker logs $CORE_CONTAINER"
  exit 1
fi
