#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

containers=$(docker ps --filter "name=chomp-*" --format "{{.Names}}" || true)
[ -n "$containers" ] && echo "$containers" | xargs -r docker stop || echo "No Chomp containers running"
remaining=$(docker ps --filter "name=chomp-*" --format "{{.Names}}" | wc -l)
[ "$remaining" -eq 0 ] && echo "✅ All stopped" || echo "⚠️ Some may still be running"

echo ""
echo "To remove containers completely: make cleanup"
echo "To restart services: make full-setup"
