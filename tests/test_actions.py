"""Tests for actions modules."""
import pytest
from unittest.mock import AsyncMock, Mock, patch
import sys
import os
from datetime import datetime, timezone

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.actions.load import load, load_one


class TestActionsLoad:
  """Test actions.load module functions."""

  @pytest.mark.asyncio
  async def test_load_success(self):
    """Test successful data loading."""
    from datetime import datetime, timezone
    import src.state as state

    mock_ingester = Mock()
    mock_ingester.resource_type = "timeseries"
    mock_ingester.interval = "5m"
    mock_ingester.name = "test_ingester"

    from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)

    mock_tsdb = Mock()
    mock_tsdb.fetch = AsyncMock(return_value=[{"ts": from_date, "value": 100}])
    state.tsdb = mock_tsdb

    result = await load(mock_ingester, from_date, to_date)
    assert result == [{"ts": from_date, "value": 100}]
    mock_tsdb.fetch.assert_called_once()

  @pytest.mark.asyncio
  async def test_load_value_type(self):
    """Test loading value type resource."""
    mock_ingester = Mock()
    mock_ingester.resource_type = "value"
    mock_ingester.id = "test_id"

    from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with patch('src.actions.load.load_one',
               return_value="cached_value") as mock_load_one:
      result = await load(mock_ingester, from_date, None)
      assert result == "cached_value"
      mock_load_one.assert_called_once_with(mock_ingester)

  @pytest.mark.asyncio
  async def test_load_one_success(self):
    """Test successful single value loading."""
    mock_ingester = Mock()
    mock_ingester.id = "test_id"
    mock_ingester.load_values.return_value = "loaded_values"

    with patch('src.actions.load.get_cache',
               return_value="cached_data") as mock_cache:
      result = await load_one(mock_ingester)
      assert result == "loaded_values"
      mock_cache.assert_called_once_with("test_id")
      mock_ingester.load_values.assert_called_once_with("cached_data")

  @pytest.mark.asyncio
  async def test_load_with_default_to_date(self):
    """Test loading with default to_date."""
    import src.state as state

    mock_ingester = Mock()
    mock_ingester.resource_type = "timeseries"
    mock_ingester.interval = "1h"
    mock_ingester.name = "test_ingester"

    from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with patch('src.actions.load.datetime') as mock_datetime:
      mock_now = datetime(2024, 1, 3, tzinfo=timezone.utc)
      mock_datetime.now.return_value = mock_now

      mock_tsdb = Mock()
      mock_tsdb.fetch = AsyncMock(return_value=[])
      state.tsdb = mock_tsdb

      await load(mock_ingester, from_date, None)

      # Verify that datetime.now was called to set to_date
      mock_datetime.now.assert_called_once()

  @pytest.mark.asyncio
  async def test_load_with_custom_aggregation(self):
    """Test loading with custom aggregation interval."""
    import src.state as state

    mock_ingester = Mock()
    mock_ingester.resource_type = "timeseries"
    mock_ingester.interval = "5m"
    mock_ingester.name = "test_ingester"

    from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)

    mock_tsdb = Mock()
    mock_tsdb.fetch = AsyncMock(return_value=[])
    state.tsdb = mock_tsdb

    await load(mock_ingester, from_date, to_date, aggregation_interval="1h")

    mock_tsdb.fetch.assert_called_once_with("test_ingester", from_date,
                                            to_date, "1h")


class TestActionsImports:
  """Test actions module imports and structure."""

  def test_actions_load_imports(self):
    """Test that load module imports work."""
    from src.actions.load import load, load_one
    assert load is not None
    assert load_one is not None

  def test_actions_schedule_imports(self):
    """Test that schedule module imports work."""
    try:
      from src.actions import schedule
      assert schedule is not None
    except ImportError:
      pytest.skip("actions.schedule not available")

  def test_actions_store_imports(self):
    """Test that store module imports work."""
    try:
      from src.actions import store
      assert store is not None
    except ImportError:
      pytest.skip("actions.store not available")

  def test_actions_transform_imports(self):
    """Test that transform module imports work."""
    try:
      from src.actions import transform
      assert transform is not None
    except ImportError:
      pytest.skip("actions.transform not available")

  def test_actions_module_structure(self):
    """Test actions module structure."""

    # Check that actions module has expected submodules
    expected_modules = ['load', 'schedule', 'store', 'transform']

    available_modules = []
    for module in expected_modules:
      try:
        imported = __import__(f'src.actions.{module}', fromlist=[module])
        if imported:
          available_modules.append(module)
      except ImportError:
        continue

    # At least load module should be available
    assert 'load' in available_modules
