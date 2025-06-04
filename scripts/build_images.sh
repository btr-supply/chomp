#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

check_docker
echo "Building Chomp Docker images..."

cd $PARENT

# Build database image
echo "Building database image ($DB_IMAGE)..."
build_images ./Dockerfile.db $DB_IMAGE

# Build core application image
echo "Building core application image ($CORE_IMAGE)..."
build_images ./Dockerfile.core $CORE_IMAGE

# Verify images were built
echo "Verifying built images..."
if docker image inspect $DB_IMAGE >/dev/null 2>&1; then
  echo "✓ Database image built successfully"
else
  echo "✗ Database image build failed"
  exit 1
fi

if docker image inspect $CORE_IMAGE >/dev/null 2>&1; then
  echo "✓ Core image built successfully"
else
  echo "✗ Core image build failed"
  exit 1
fi

echo ""
echo "🎉 All images built successfully!"
echo "Images:"
echo "  - $DB_IMAGE"
echo "  - $CORE_IMAGE"
echo ""
echo "Image sizes:"
docker images --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}" | grep -E "(chomp-|REPOSITORY)"
