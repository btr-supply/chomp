#!/bin/bash
set -euo pipefail

# Parameters
PROJECT_ROOT="${1:-$(pwd)}"
PROJECT_NAME="${2:-$(basename "$PROJECT_ROOT")}"

echo "$PROJECT_NAME - Comprehensive Linting"
echo "$(printf '=%.0s' $(seq 1 ${#PROJECT_NAME}))===============================)"

# Track exit codes
RUFF_EXIT_CODE=0
MYPY_EXIT_CODE=0

# Change to project root directory
cd "$PROJECT_ROOT"

# Function to check if command exists
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi

# Determine lint targets based on project structure
LINT_TARGETS=()

# Always check for src/ directory
if [ -d "src/" ]; then
  LINT_TARGETS+=("src/")
fi

# Check for main.py (chomp specific)
if [ -f "main.py" ]; then
  LINT_TARGETS+=("main.py")
fi

# Check for test_adapters.py (BTR specific)
if [ -f "test_adapters.py" ]; then
  LINT_TARGETS+=("test_adapters.py")
fi

# Check for tests directory
if [ -d "tests/" ]; then
  LINT_TARGETS+=("tests/")
fi

# If no targets found, default to current directory
if [ ${#LINT_TARGETS[@]} -eq 0 ]; then
  LINT_TARGETS+=(".")
fi

echo ""
echo "📋 Linting targets: ${LINT_TARGETS[*]}"

# Install linters if not available (try uv first, then pip)
if ! command_exists ruff; then
  echo "Installing ruff linter..."
  if command_exists uv; then
    uv pip install ruff
  else
    pip install ruff
  fi
fi

if ! command_exists mypy; then
  echo "Installing mypy type checker..."
  if command_exists uv; then
    uv pip install mypy
  else
    pip install mypy
  fi
fi

# Run ruff
echo ""
echo "Running ruff linter..."
if command_exists uv && [ -f "pyproject.toml" ]; then
  uv run ruff check "${LINT_TARGETS[@]}" 2>&1 || RUFF_EXIT_CODE=$?
else
  ruff check "${LINT_TARGETS[@]}" 2>&1 || RUFF_EXIT_CODE=$?
fi

# Run mypy
echo ""
echo "Running mypy type checker..."
if command_exists uv && [ -f "pyproject.toml" ]; then
  uv run mypy "${LINT_TARGETS[@]}" --ignore-missing-imports --explicit-package-bases 2>&1 || MYPY_EXIT_CODE=$?
else
  mypy "${LINT_TARGETS[@]}" --ignore-missing-imports --explicit-package-bases 2>&1 || MYPY_EXIT_CODE=$?
fi

# Summary
echo ""
echo "=========================================="
echo "📊 Final Linting Summary for $PROJECT_NAME"
echo "=========================================="

if [ $RUFF_EXIT_CODE -eq 0 ] && [ $MYPY_EXIT_CODE -eq 0 ]; then
  echo "✅ All linting checks passed!"
  echo "  ✓ Ruff style/quality check"
  echo "  ✓ MyPy type check"
  exit 0
else
  echo "❌ Linting issues found:"
  [ $RUFF_EXIT_CODE -ne 0 ] && echo "  - Ruff found style/quality issues"
  [ $MYPY_EXIT_CODE -ne 0 ] && echo "  - MyPy found type issues"
  echo ""
  echo "Fix issues and run linting again."
  exit 1
fi
