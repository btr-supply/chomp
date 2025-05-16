#!/bin/bash
set -e
DIR="$(readlink -f "$(dirname "${BASH_SOURCE[1]}")")"
UTILS_DIR="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")"
PARENT="$(dirname $DIR)"
ENV=""
ENV_DIR="$UTILS_DIR"

while [ "$ENV_DIR" != "/" ]; do
  if [ -f "$ENV_DIR/.env" ]; then
    ENV="$ENV_DIR/.env"
    echo "Using $ENV as .env file"
    break
  fi
  ENV_DIR="$(dirname "$ENV_DIR")"
done

[ -n "$ENV" ] && source "$ENV"

# Check if script is run with sudo
check_sudo() {
  if [ "$EUID" -ne 0 ]; then
    echo "This script must be run with sudo."
    exit 1
  fi
}

# Create Docker network if it doesn't exist
ensure_network() {
  local network=$1
  if ! docker network inspect $network &> /dev/null; then
    echo "Creating docker network $network..."
    docker network create $network
  else
    echo "Using existing docker network $network..."
  fi
}

# Build Docker images with tags
build_images() {
  local dockerfile=$1
  local image=$2
  shift 2
  local tags=("$@")

  echo "Building Docker image ($image)..."
  docker build -f $dockerfile -t $image . # --no-cache
  
  for tag in "${tags[@]}"; do
    echo "Tagging $image as $tag..."
    docker tag $image $tag
  done
}

# Check if container exists and is running
check_container_running() {
  local container=$1
  local error_msg=${2:-"Container $container is not running"}
  
  if ! docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
    echo "Error: $error_msg"
    return 1
  fi
  return 0
}

# Delete existing containers
cleanup_containers() {
  local pattern="$1"
  # Get all matching container names
  local containers=$(docker ps -a --format '{{.Names}}' | grep -E "$pattern")

  if [[ -z "$containers" ]]; then
    echo "No containers matching pattern '$pattern' found."
    return 0
  fi

  for container in $containers; do
    echo "Stopping and removing container: $container"
    docker stop "$container" && docker rm "$container"
  done
}

# Perform API health check
db_client_health_check() {
  local network=$1
  local container=$2
  local image=$3
  local env=$4

  echo "Health checking DB client..."

  # Add a random id after container name
  local uid=$(uuidgen)
  # Run container and ping
  docker run --env-file $env --network $network --name $container-$uid $image -v -e $env --ping
  local exit_code=$?

  if [ $exit_code -ne 0 ]; then
    echo "Health check $container failed with code: $exit_code, db or cache connection issue"
    return $exit_code
  else
    echo "Health check $container successful: db and cache connection established"
    return 0
  fi
  # Remove container after shutdown
  docker rm $container
}

# Count jobs in YAML config file
count_yaml_jobs() {
  local yaml_file=$1
  local count=0

  # Count number of top-level feed groups (name entries under evm_caller)
  if [ -f "$yaml_file" ]; then
    count=$(grep "^[[:space:]]*- name:" "$yaml_file" | wc -l)
  else
    echo "Error: Config file $yaml_file not found"
    return 1
  fi
  
  echo $count
}

run_ingester_cluster() {
  local network=$1
  local project=$2
  local container_base=$3
  local image=$4
  local env=$5
  local config=$6
  local jobs_per_instance=$7
  local total_jobs=${8:-$(count_yaml_jobs "$config")}

  local required_instances=$(( ($total_jobs + $jobs_per_instance - 1) / $jobs_per_instance ))
  
  # Extract filenames and config stem from absolute paths
  local env_filename=$(basename "$env")
  local config_stem=$(basename "$config" | cut -d. -f1)  # Get the filename without extension

  echo "Starting $required_instances instances for $config (${total_jobs} jobs, ${jobs_per_instance} per instance)..."
  # echo "Binding: $env -> /app/$env_filename - $config -> /app/$config_filename"
  echo "Network: $network"

  for ((i = 1; i <= required_instances; i++)); do
    # Update container name to include config stem and index
    local container_name="${container_base}.${config_stem}-${i}"
    echo "Starting container $container_name..."
    docker run -d \
        --env-file "$env" \
        --network "$network" \
        --label project=$project \
        --name "$container_name" \
        "$image" \
        -e "$env" \
        -c "./ingesters/$config" \
        -j "$jobs_per_instance" \
        -sh "0.0.0.0"
    sleep 1.5 # Wait to avoid race conditions at resource claim
  done

  return $required_instances
}

# Wait for containers to be healthy
wait_for_healthy() {
  local containers=("$@")
  local max_attempts=30
  local attempt=1
  
  echo "Waiting for containers to be healthy..."
  while [ $attempt -le $max_attempts ]; do
    local all_healthy=true
    for container in "${containers[@]}"; do
      # Check the container health status or fall back to 'running' if not present
      local status=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null)
      
      # Containers without health checks default to 'running'
      if [ "$status" != "healthy" ] && [ "$status" != "running" ]; then
        all_healthy=false
        break
      fi
    done
    
    if $all_healthy; then
      echo "All containers are healthy!"
      return 0
    fi
    
    echo "Attempt $attempt/$max_attempts: Waiting for containers to be healthy..."
    sleep 2
    ((attempt++))
  done
  
  echo "Timeout waiting for containers to be healthy or running"
  return 1
}

# Test databases connections (Redis + TDengine)
db_health_check() {
  local redis_port=$1
  local taos_port=$2
  local user=$3
  local pass=$4
  local success=true

  # Test redis connection
  if command -v redis-cli &> /dev/null; then
    echo "Testing local connection to Redis..."
    output=$(redis-cli -h localhost -p $redis_port --user $user --pass $pass ping 2>/dev/null)
    if [ "$output" == "PONG" ]; then
      echo ">> Connection to redis successful"
    else
      echo ">> Connection to redis failed"
      success=false
    fi
  else
    echo "Warning: redis-cli not found, skipping Redis connection test"
  fi

  # Test tdengine connection
  if command -v taos &> /dev/null; then
    echo "Testing local connection to TDengine..."
    output=$(taos -h localhost -P $taos_port -u $user -p$pass -k)
    if [[ "$output" == *"2: service ok"* ]]; then
      echo ">> Connection to TDengine successful"
    else
      echo ">> Connection to TDengine failed"
      success=false
    fi
  else
    echo "Warning: taos cli not found, skipping TDengine connection test"
  fi

  $success && return 0 || return 1
}

# Wait for database to be ready
wait_for_db() {
  local max_attempts=30
  local attempt=1
  
  echo "Waiting for database services to be ready..."
  while [ $attempt -le $max_attempts ]; do
    sleep 2
    if db_health_check $REDIS_PORT $TAOS_PORT $DB_RW_USER $DB_RW_PASS; then
      echo "Database services are ready!"
      return 0
    fi
    
    echo "Attempt $attempt/$max_attempts: Waiting for database services..."
    ((attempt++))
  done
  
  echo "Timeout waiting for database services"
  return 1
}
