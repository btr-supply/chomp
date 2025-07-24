#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHOMP_DIR="$(dirname "$SCRIPT_DIR")"
SCOPE="${1:-chomp}"

echo "Formatting Python files with yapf..."

format_python_files() {
  local base_dir="$1"
  local use_git="${2:-true}"

  cd "$base_dir"

  if [ "$use_git" = true ] && git rev-parse --git-dir >/dev/null 2>&1; then
    # Format git modified files
    { git diff --name-only --diff-filter=ACMR; git diff --name-only --cached --diff-filter=ACMR; } | \
    sort -u | grep '\.py$' | while read -r f; do
      [ -f "$f" ] && uv run yapf -i "$f" >/dev/null 2>&1 && echo "✅ $f" || echo "❌ $f"
    done
  else
    # Format all Python files (excluding common dirs)
    find . -name '*.py' -type f \
      -not -path './.venv/*' \
      -not -path './__pycache__/*' \
      -not -path './.pytest_cache/*' \
      -not -path './.mypy_cache/*' \
      -not -path './.ruff_cache/*' \
      -not -path './.*' | while read -r f; do
      uv run yapf -i "$f" >/dev/null 2>&1 && echo "✅ $f" || echo "❌ $f"
    done
  fi
}

case "$SCOPE" in
  "all")
    echo "Formatting root files..."
    format_python_files "$CHOMP_DIR/.."
    echo "Formatting chomp files..."
    format_python_files "$CHOMP_DIR"
    ;;
  "chomp")
    echo "Formatting chomp files..."
    format_python_files "$CHOMP_DIR"
    ;;
  *)
    echo "Usage: $0 [all|chomp]"
    exit 1
    ;;
esac

echo "✅ Formatting completed"
