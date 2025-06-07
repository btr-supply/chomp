#!/bin/bash
set -euo pipefail

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHOMP_DIR="$(dirname "$SCRIPT_DIR")"
ROOT_DIR="$(dirname "$CHOMP_DIR")"

# Parse arguments
SCOPE="${1:-chomp}"  # "chomp" or "all" (root+chomp)

echo "Linting Python files with ruff and mypy..."

case "$SCOPE" in
  "all")
    echo "Linting root files..."
    cd "$ROOT_DIR"
    echo "Running ruff on root files..."
    uv run ruff check . --exclude chomp --fix --exit-zero
    echo "Running mypy on root files..."
    # Check root src directory specifically
    if [ -d "src" ]; then
      echo "Running mypy on root src directory..."
      PYTHONPATH="$ROOT_DIR" uv run mypy src --explicit-package-bases --ignore-missing-imports || true
    fi
    # Also check any other root Python files
    uv run mypy . --exclude chomp --exclude src --explicit-package-bases --ignore-missing-imports || true

    echo "Linting chomp files..."
    cd "$CHOMP_DIR"
    echo "Running ruff on chomp files..."
    uv run ruff check . --fix --exit-zero
    echo "Running mypy on chomp files..."
    PYTHONPATH="$CHOMP_DIR" uv run mypy -p src --explicit-package-bases --ignore-missing-imports || true
    ;;

  "chomp")
    echo "Linting chomp files..."
    cd "$CHOMP_DIR"
    echo "Running ruff on chomp files..."
    uv run ruff check . --fix --exit-zero
    echo "Running mypy on chomp files..."
    PYTHONPATH="$CHOMP_DIR" uv run mypy -p src --explicit-package-bases --ignore-missing-imports || true
    ;;

  *)
    echo "Usage: $0 [all|chomp]"
    echo "  all   - Lint root + chomp files"
    echo "  chomp - Lint chomp files only"
    exit 1
    ;;
esac

echo "✅ Linting completed (warnings may exist)"
