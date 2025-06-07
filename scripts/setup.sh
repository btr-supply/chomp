#!/bin/bash
set -euo pipefail

# Set project directories
PARENT="${PARENT:-$(dirname "$(dirname "$(realpath "$0")")")}"
export PARENT

source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

# Parse arguments and set defaults
parse_common_args "$@"

# Handle setup-specific arguments
ACTION="all"
for arg in "$@"; do
  case "$arg" in
    deps|images|all) ACTION="$arg" ;;
  esac
done

echo "🚀 Setup: MODE=$MODE DEPLOYMENT=$DEPLOYMENT ACTION=$ACTION"

install_deps() {
  local extra=${EXTRA:-"default"}
  echo "📦 Installing Chomp dependencies with EXTRA='$extra'..."

  # Check if uv is installed
  if ! command -v uv &> /dev/null; then
    echo "Installing UV package manager..."
    pip install uv
  fi

  # Create virtual environment if it doesn't exist
  if [ ! -d ".venv" ]; then
    echo "Creating virtual environment with Python 3.11..."
    uv venv --python 3.11
  fi

  # Install dependencies based on EXTRA parameter
  case "$extra" in
    "all")
      echo "Installing all dependencies (core + all extras)..."
      uv pip install -e ".[all]"
      ;;
    "default")
      echo "Installing default dependencies (core + web2 + web3 + tdengine)..."
      uv pip install -e ".[default]"
      ;;
    "core")
      echo "Installing core dependencies only..."
      uv pip install -e .
      ;;
    *)
      echo "Installing core + custom extras: $extra"
      uv pip install -e ".[$extra]"
      ;;
  esac

  # Verify installation
  echo "Verifying installation..."
  if [ -f ".venv/bin/python" ]; then
    .venv/bin/python -c "import src; print('✓ Chomp core modules imported successfully')"
  fi

  echo "✅ Dependencies installed successfully!"
}

build_images() {
  echo "🐳 Building Docker images..."
  check_docker
  ensure_network "$DOCKER_NET"

  # Detect if we're in chomp submodule or parent directory
  if [ -f "./Dockerfile.db" ]; then
    # Running from chomp directory
    echo "🔍 Building from chomp directory"
    build_docker_images ./Dockerfile.db "$DB_IMAGE"
    build_docker_images ./Dockerfile.core "$CORE_IMAGE" "$API_IMAGE" "$INGESTER_IMAGE"
  elif [ -f "./chomp/Dockerfile.db" ]; then
    # Running from parent directory
    echo "🔍 Building from parent directory"
    build_docker_images ./chomp/Dockerfile.db "$DB_IMAGE"
    build_docker_images ./chomp/Dockerfile.core "$CORE_IMAGE" "$API_IMAGE" "$INGESTER_IMAGE"
  else
    # Try parent directory context
    echo "🔍 Trying parent directory context"
    cd "$PARENT"
    if [ -f "./chomp/Dockerfile.db" ]; then
      build_docker_images ./chomp/Dockerfile.db "$DB_IMAGE"
              build_docker_images ./chomp/Dockerfile.core "$CORE_IMAGE" "$API_IMAGE" "$INGESTER_IMAGE"
    else
      echo "❌ Error: Cannot find Dockerfile.db in current directory or ./chomp/"
      echo "   Current directory: $(pwd)"
      echo "   Parent directory: $PARENT"
      exit 1
    fi
  fi

  echo "✅ Docker images built successfully!"
}

case "$ACTION" in
  "deps")
    install_deps
    ;;
  "images")
    build_images
    ;;
  "all")
    install_deps
    [ "$DEPLOYMENT" = "docker" ] && build_images
    ;;
  *)
    echo "Usage: $0 [MODE=dev|prod] [DEPLOYMENT=local|docker] [deps|images|all]"
    echo "  MODE=dev    - Development configuration"
    echo "  MODE=prod   - Production configuration"
    echo "  DEPLOYMENT=local  - Local development setup"
    echo "  DEPLOYMENT=docker - Docker-based setup"
    echo "  ACTION=deps   - Install dependencies only"
    echo "  ACTION=images - Build Docker images only"
    echo "  ACTION=all    - Install deps + build images (if docker)"
    exit 1
    ;;
esac

echo ""
echo "✅ Setup completed for MODE=$MODE DEPLOYMENT=$DEPLOYMENT ACTION=$ACTION"
