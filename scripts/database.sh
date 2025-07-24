#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

find_env "$@"
parse_common_args "$@"

echo "🚀 Database setup: MODE=$MODE DEPLOYMENT=$DEPLOYMENT"

setup_docker_db() {
  echo "🐳 Setting up Docker database..."

  check_docker
  ensure_network "$DOCKER_NET"

  # Build DB image if needed
  if ! docker_image_exists "$DB_IMAGE"; then
    echo "Building $DB_IMAGE image..."
    cd "$PARENT" 2>/dev/null || cd "$(dirname "$PARENT")"
    build_docker_images ./Dockerfile.db "$DB_IMAGE"
  fi

  # Clean up existing container
  docker_stop_remove "$DB_CONTAINER"

  # Start new container with appropriate port configuration
  echo "Starting $DB_CONTAINER container..."

  # For dev mode or local access, expose ports on localhost
  if [ "$MODE" = "dev" ] || [ "$DEPLOYMENT" = "local" ]; then
    echo "Starting database services..."
    docker_run_with_env -d \
      -p "$REDIS_PORT:$REDIS_PORT" \
      -p "$DB_PORT:$DB_PORT" \
      --name "$DB_CONTAINER" \
      --network "$DOCKER_NET" \
      --label project=Chomp \
      "$DB_IMAGE"
  else
    # For prod mode, don't expose ports externally
    echo "Starting database services (prod mode)..."
    docker_run_with_env -d \
      --name "$DB_CONTAINER" \
      --network "$DOCKER_NET" \
      --label project=Chomp \
      "$DB_IMAGE"
  fi

  # Wait for database to be ready
  echo "Waiting for database to be ready..."
  if wait_for_db_health; then
    echo "✅ Docker database setup completed successfully!"
    echo "   Mode: $MODE, Deployment: $DEPLOYMENT"
    echo "   Redis: $REDIS_HOST:$REDIS_PORT"
    echo "   TDengine: $DB_HOST:$DB_PORT"
  else
    echo "⚠️ Database health check failed, but container is running"
    echo "Check status with: make monitor"
    echo "Check logs with: make logs $DB_CONTAINER"
  fi
}

setup_local_db() {
  echo "🔧 Local mode: Using Docker databases with exposed ports"

  check_docker
  ensure_network "$DOCKER_NET"

  # Build DB image if needed
  if ! docker_image_exists "$DB_IMAGE"; then
    echo "Building $DB_IMAGE image..."
    cd "$PARENT" 2>/dev/null || cd "$(dirname "$PARENT")"
    build_docker_images ./Dockerfile.db "$DB_IMAGE"
  fi

  # Clean up existing container
  docker_stop_remove "$DB_CONTAINER"

  # Start container with ports exposed for local access
  echo "Starting database with exposed ports..."
  docker_run_with_env -d \
    -p "$REDIS_PORT:$REDIS_PORT" \
    -p "$DB_PORT:$DB_PORT" \
    -p "$DB_HTTP_PORT:$DB_HTTP_PORT" \
    --name "$DB_CONTAINER" \
    --network "$DOCKER_NET" \
    --label project=Chomp \
    "$DB_IMAGE"

  # Wait for database to be ready
  echo "Waiting for database to be ready..."
  if wait_for_db_health; then
    echo "✅ Database ready!"
    echo "   Redis: localhost:$REDIS_PORT"
    echo "   TDengine: localhost:$DB_PORT"
  else
    echo "⚠️ Database health check failed"
    echo "Check logs with: make logs $DB_CONTAINER"
  fi
}

case "$DEPLOYMENT" in
  "docker")
    setup_docker_db
    ;;
  "local")
    setup_local_db
    ;;
  *)
    echo "Usage: $0 [MODE=dev|prod] [DEPLOYMENT=docker|local]"
    echo "  MODE=dev    - Development configuration"
    echo "  MODE=prod   - Production configuration"
    echo "  DEPLOYMENT=docker - Start database in Docker containers"
    echo "  DEPLOYMENT=local  - Start local database services"
    exit 1
    ;;
esac
