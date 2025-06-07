#!/bin/bash
set -euo pipefail

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHOMP_DIR="$(dirname "$SCRIPT_DIR")"

# Parse arguments
SCOPE="${1:-chomp}"  # "chomp" or "all" (root+chomp)

echo "Formatting Python files with yapf..."

format_files() {
  local search_path="$1"
  local exclude_patterns="$2"
  local label="$3"

  echo "Formatting $label Python files..."

    local find_cmd="find $search_path -name '*.py' -type f"

  # Add exclude patterns
  if [ -n "$exclude_patterns" ]; then
    IFS=',' read -ra EXCLUDES <<< "$exclude_patterns"
    for exclude in "${EXCLUDES[@]}"; do
      find_cmd="$find_cmd -not -path '$exclude'"
    done
  fi

  local total=0
  eval "$find_cmd" | while read -r f; do
    # Skip files with corrupted extensions or weird names
    if [[ "$f" =~ \.py[a-z]+\.py$ ]] || [[ "$f" =~ \.py/.+\.py$ ]]; then
      continue
    fi

    if [ -f "$f" ] && [[ "$f" =~ \.py$ ]]; then
      printf "Processing %-30s" "$f"
      if uv run yapf -i "$f" >/dev/null 2>&1; then
        printf "\r✔️ %s\n" "$f"
        total=$((total+1))
      else
        printf "\r❌ %s\n" "$f"
      fi
    fi
  done
}

case "$SCOPE" in
  "all")
    # Format root files (excluding chomp and common cache dirs)
    cd "$CHOMP_DIR/.."
    format_files "." "./chomp/*,./.venv/*,./__pycache__/*,./.pytest_cache/*,./.mypy_cache/*,./.ruff_cache/*,./.*" "root"

    # Format chomp files
    cd "$CHOMP_DIR"
    format_files "." "./.venv/*,./__pycache__/*,./.pytest_cache/*,./.mypy_cache/*,./.ruff_cache/*,./.*" "chomp"
    ;;

  "chomp")
    # Format only chomp files (staged for git if in git context)
    cd "$CHOMP_DIR"

    # Check if we're in a git repository and have staged files
    if git rev-parse --git-dir >/dev/null 2>&1; then
      echo "Formatting staged Python files in chomp..."
      git diff --name-only --cached --diff-filter=ACMR | while read -r f; do
        if [[ $f == *.py && -f "$f" ]]; then
          printf "Processing %-30s" "$f"
          if uv run yapf -i "$f" >/dev/null 2>&1; then
            printf "\r✔️ %s\n" "$f"
            git add "$f"
          else
            printf "\r❌ %s\n" "$f"
          fi
        fi
      done
    else
      # No git or no staged files, format all chomp files
      format_files "." "./.venv/*,./__pycache__/*,./.pytest_cache/*,./.mypy_cache/*,./.ruff_cache/*,./.*" "chomp"
    fi
    ;;

  *)
    echo "Usage: $0 [all|chomp]"
    echo "  all   - Format root + chomp files"
    echo "  chomp - Format chomp files only (staged if in git)"
    exit 1
    ;;
esac

echo "✅ Formatting completed"
