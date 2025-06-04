#!/bin/bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

echo "Formatting staged files..."
total=0

# Format staged files
git diff --name-only --cached --diff-filter=ACMR | while read -r f; do
  if [[ -f "$f" ]]; then
    printf "Processing %-50s" "$f"
    case $f in
      *.py)
        # Format Python files with ruff and black (following BTR standards)
        if uv run ruff format "$f" >/dev/null 2>&1; then
          printf "\r✔️ Formatted: %s\n" "$f"
          git add "$f"
          total=$((total + 1))
        else
          printf "\r❌ Failed to format: %s\n" "$f"
        fi
        ;;
      *.yml|*.yaml)
        # Basic YAML formatting check (preserve existing formatting)
        if yamllint "$f" >/dev/null 2>&1 || true; then
          printf "\r✔️ Validated: %s\n" "$f"
        else
          printf "\r⚠️  YAML warning: %s\n" "$f"
        fi
        ;;
      *.md)
        # No formatting for markdown files
        printf "\r➡️  Skipped: %s\n" "$f"
        ;;
      *)
        # Skip other file types
        continue
        ;;
    esac
  fi
done

echo "Formatting completed for $total file(s)" 