#!/bin/bash
set -euo pipefail

EXTRA=${1:-"default"}
echo "Installing Chomp dependencies with EXTRA='$EXTRA'..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
  echo "Installing UV package manager..."
  pip install uv
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment with Python 3.11..."
  uv venv --python 3.11
fi

# Install dependencies based on EXTRA parameter
if [ "$EXTRA" = "all" ]; then
  echo "Installing all dependencies (core + all extras)..."
  uv pip install -e ".[all]"
elif [ "$EXTRA" = "default" ]; then
  echo "Installing default dependencies (core + web2 + web3 + tdengine)..."
  uv pip install -e ".[default]"
elif [ "$EXTRA" = "core" ]; then
  echo "Installing core dependencies only..."
  uv pip install -e .
else
  echo "Installing core + custom extras: $EXTRA"
  uv pip install -e ".[$EXTRA]"
fi

echo "Dependencies installed successfully!"
echo ""
echo "To activate the virtual environment, run:"
echo "  source .venv/bin/activate"
echo ""
echo "Available extras:"
echo "  default     - TDengine, web2 scraping, EVM/Solana blockchain tools"
echo "  all         - All available adapters and ingesters"
echo "  web2        - Web scraping tools (playwright, beautifulsoup4)"
echo "  evm         - Ethereum blockchain tools"
echo "  solana      - Solana blockchain tools"
echo "  tdengine    - TDengine database adapter"
echo "  all-adapters - All database adapters"
echo "  all-ingesters - All ingester tools"

# Verify installation
echo "Verifying installation..."
if [ -f ".venv/bin/python" ]; then
  .venv/bin/python -c "import src; print('✓ Chomp core modules imported successfully')"
else
  echo "Warning: Virtual environment not found in expected location"
fi

echo "Dependency installation completed."
