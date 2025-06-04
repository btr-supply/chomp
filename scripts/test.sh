#!/bin/bash
set -euo pipefail

echo "Running Chomp test suite..."

# Activate virtual environment if it exists
if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi

# Install pytest if not available
if ! command -v pytest &> /dev/null; then
  echo "Installing pytest and coverage tools..."
  uv pip install pytest pytest-cov pytest-asyncio
fi

# Run tests with coverage if tests directory exists
if [ -d "tests/" ]; then
  echo "Running tests with coverage..."
  pytest tests/ \
    --cov=src \
    --cov-report=html:htmlcov \
    --cov-report=term-missing \
    --cov-report=xml \
    -v \
    --tb=short

  if [ $? -eq 0 ]; then
    echo ""
    echo "✅ All tests passed!"
    echo "Coverage report: htmlcov/index.html"
  else
    echo "❌ Some tests failed!"
    exit 1
  fi
else
  echo "⚠️  No tests/ directory found - creating basic test structure..."
  mkdir -p tests

  cat > tests/__init__.py << 'EOF'
"""Test package for Chomp."""
EOF

  cat > tests/test_basic.py << 'EOF'
"""Basic tests for Chomp functionality."""
import pytest


def test_import_main():
    """Test that main module can be imported."""
    try:
        import main
        assert hasattr(main, 'main')
    except ImportError:
        pytest.skip("main.py not found")


def test_import_src():
    """Test that src module can be imported."""
    import src
    assert src is not None
EOF

  echo "✅ Basic test structure created. Run 'make test' again to execute tests."
fi
