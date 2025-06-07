#!/bin/bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

echo "📋 Chomp Local Logs Monitor"
echo "============================"

# Change to parent directory
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

LOG_FILE="${OUT_LOG:-out.log}"

if [ -f "$LOG_FILE" ]; then
  echo "Monitoring: $LOG_FILE"
  echo ""
  tail -f "$LOG_FILE"
else
  echo "❌ Log file not found: $LOG_FILE"
  exit 1
fi
