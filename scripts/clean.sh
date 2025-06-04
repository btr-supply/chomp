#!/bin/bash
set -euo pipefail

echo "Cleaning temporary files and build artifacts..."

# Python cache and build artifacts
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true
find . -type f -name "*.pyo" -delete 2>/dev/null || true
find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
rm -rf build/ dist/ .eggs/ .pytest_cache/ htmlcov/ .coverage coverage.xml 2>/dev/null || true

# IDE and temporary files
find . -name ".DS_Store" -delete 2>/dev/null || true
rm -rf .vscode/settings.json .idea/ 2>/dev/null || true
find . -name "*.tmp" -delete 2>/dev/null || true
find . -name "*.log" -delete 2>/dev/null || true
rm -rf tmp/ temp/ 2>/dev/null || true

# Docker build cache (optional)
command -v docker &>/dev/null && docker builder prune -f 2>/dev/null || true

echo "✅ Cleanup completed!"
