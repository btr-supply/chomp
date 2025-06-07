# Testing Guide

## Overview

Chomp aims for **100% test coverage** using **real dependencies** instead of mocks. Tests automatically skip when dependencies are unavailable, ensuring they run reliably across environments.

## Coverage Target: 100%

- **Minimal Mocking**: Use actual databases, APIs, and services when available
- **Real Integration**: Test actual component interactions, not mock behavior
- **Graceful Degradation**: Skip tests when dependencies missing, don't fail
- **Run Anywhere**: Works in dev, CI, and production environments

## Dependency Detection Pattern

```python
from src.deps import safe_import

# Check dependency availability
dependency = safe_import("package_name")
AVAILABLE = dependency is not None

# Import real implementation only if available
if AVAILABLE:
    from src.module import ActualClass

# Skip entire test class if dependencies unavailable
@pytest.mark.skipif(not AVAILABLE, reason="Dependencies not available (package-name)")
class TestClass:
    def test_with_real_dependency(self):
        # Uses actual dependency, not mocks
        adapter = ActualClass()
        result = adapter.method()
        assert result is not None
```

## Running Tests

### Primary Method
```bash
# Run all tests via Makefile
make test
```

### Direct pytest
```bash
# All tests
pytest

# Specific file
pytest tests/test_adapters_tdengine.py

# Show skipped tests
pytest -rs
```

## Current Test Coverage

### Database Adapters (6 files)
- `test_adapters_tdengine.py` - `taospy` dependency
- `test_adapters_influxdb.py` - `influxdb-client`
- `test_adapters_clickhouse.py` - `asynch`
- `test_adapters_mongodb.py` - `motor`
- `test_adapters_duckdb.py` - `duckdb`
- `test_adapters_evm_rpc.py` - `eth_utils`, `hexbytes`, `eth_account`

### Data Ingesters (5 files)
- `test_ingesters_dynamic_scrapper.py` - `playwright`
- `test_ingesters_evm_caller.py` - `web3`, `multicall`
- `test_ingesters_evm_logger.py` - `web3`
- `test_ingesters_svm_caller.py` - no external dependencies
- `test_ingesters_svm_logger.py` - handles TODO implementation

### Expected Output
```
tests/test_adapters_tdengine.py .......... PASSED
tests/test_adapters_mongodb.py .......... SKIPPED (Dependencies not available)
tests/test_ingesters_dynamic_scrapper.py. PASSED
```

## Installing Dependencies

```bash
# Database adapters
pip install taospy influxdb-client asynch motor duckdb

# Blockchain/Web3
pip install web3 eth-utils hexbytes eth-account multicall

# Browser automation
pip install playwright && playwright install
```

## Writing New Tests

### Pattern for New Adapter
```python
from src.deps import safe_import

new_dep = safe_import("new_package")
AVAILABLE = new_dep is not None

if AVAILABLE:
    from src.adapters.new_adapter import NewAdapter

@pytest.mark.skipif(not AVAILABLE, reason="Dependencies not available (new-package)")
class TestNewAdapter:
    def test_real_functionality(self):
        adapter = NewAdapter()
        data = adapter.fetch_data()
        assert len(data) > 0
```

### Multiple Dependencies
```python
dep1 = safe_import("package1")
dep2 = safe_import("package2")
AVAILABLE = all([dep1 is not None, dep2 is not None])

@pytest.mark.skipif(not AVAILABLE, reason="Dependencies not available (package1, package2)")
```

## Key Benefits

1. **Real Testing**: Catches actual integration issues
2. **Easy Maintenance**: No complex mock setup
3. **Better Debugging**: Real failures are easier to diagnose
4. **Environment Flexibility**: Adapts to available infrastructure
5. **100% Coverage Goal**: All components tested with real dependencies when available
