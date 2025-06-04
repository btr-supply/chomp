#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

[ -f "out.log" ] && tail -50 out.log || {
  echo "No log file found. Checking Docker container logs..."
  docker logs chomp-api 2>/dev/null | tail -20 || echo "No API container logs."
}
