#!/bin/bash
set -e
source "$(dirname "${BASH_SOURCE[0]}")/utils.bash"

check_sudo
ensure_network $DOCKER_NET

# Build DB image
echo "Building $DB_IMAGE image..."
cd $PARENT
build_images ./Dockerfile.db $DB_IMAGE

cleanup_containers $DB_CONTAINER

# Run the new container instance
echo "Starting $DB_CONTAINER container..."
docker run -d \
    --env-file "$ENV" \
    --label project=Chomp \
    --network "$DOCKER_NET" \
    --network-alias "$REDIS_HOST" \
    --network-alias "$TAOS_HOST" \
    --name "$DB_CONTAINER" \
    -p "$REDIS_PORT:$REDIS_PORT" \
    -p "$TAOS_PORT:$TAOS_PORT" \
    -p "$TAOS_HTTP_PORT:$TAOS_HTTP_PORT" \
    "$DB_IMAGE"

# Wait for database to be ready and test connections (db_health_check)
wait_for_db || exit 1

echo "Database setup completed successfully!"
