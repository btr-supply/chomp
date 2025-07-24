"""Tests for actions modules."""
import pytest
from unittest.mock import AsyncMock, patch
import sys
import os
from datetime import datetime, timezone

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.actions.load import load_resource


class TestActionsLoad:
  """Test actions.load module functions."""

  @pytest.mark.asyncio
  async def test_load_resource_single_uid(self):
    """Test loading a single resource by UID."""
    with patch('src.actions.load.state') as mock_state:
      mock_state.tsdb.fetch_by_id = AsyncMock(return_value={
          "uid": "test123",
          "value": 100
      })

      result = await load_resource("test_table", uid="test123")

      assert result == {"uid": "test123", "value": 100}
      mock_state.tsdb.fetch_by_id.assert_called_once_with(
          "test_table", "test123")

  @pytest.mark.asyncio
  async def test_load_resource_multiple_uids(self):
    """Test loading multiple resources by UIDs."""
    with patch('src.actions.load.state') as mock_state:
      mock_state.tsdb.fetch_batch_by_ids = AsyncMock(return_value=[{
          "uid": "test1",
          "value": 100
      }, {
          "uid": "test2",
          "value": 200
      }])

      result = await load_resource("test_table", uids=["test1", "test2"])

      assert len(result) == 2
      assert result[0]["uid"] == "test1"
      mock_state.tsdb.fetch_batch_by_ids.assert_called_once_with(
          "test_table", ["test1", "test2"])

  @pytest.mark.asyncio
  async def test_load_resource_by_time_range(self):
    """Test time series data loading by time range."""
    with patch('src.actions.load.state') as mock_state:
      from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
      to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)

      mock_state.tsdb.fetch = AsyncMock(return_value=(["ts", "value"],
                                                      [[from_date, 100]]))

      result = await load_resource("test_table",
                                   from_date=from_date,
                                   to_date=to_date,
                                   aggregation_interval="1h")

      assert len(result) == 1
      assert result[0]["ts"] == from_date
      assert result[0]["value"] == 100
      mock_state.tsdb.fetch.assert_called_once()
      call_args = mock_state.tsdb.fetch.call_args[0]
      assert call_args[0] == "test_table"
      assert call_args[1] == from_date
      assert call_args[2] == to_date
      assert call_args[3] == "1h"

  @pytest.mark.asyncio
  async def test_load_resource_bulk_with_pagination(self):
    """Test bulk loading with pagination."""
    with patch('src.actions.load.state') as mock_state:
      mock_state.tsdb.fetch_by_id = AsyncMock(return_value=None)
      mock_state.tsdb.fetch = AsyncMock(
          return_value=(["id", "value"], [[1, 100], [2, 200], [3, 300]]))

      result = await load_resource("test_table", limit=2, offset=1)

      assert len(result) == 2
      assert result[0]["id"] == 2
      assert result[1]["id"] == 3
      mock_state.tsdb.fetch.assert_called_once()

  @pytest.mark.asyncio
  async def test_load_resource_not_found(self):
    """Test that None is returned when a single resource is not found."""
    with patch('src.actions.load.state') as mock_state:
      mock_state.tsdb.fetch_by_id = AsyncMock(return_value=None)
      result = await load_resource("test_table", uid="not_found")
      assert result is None

  @pytest.mark.asyncio
  async def test_load_resource_empty_list(self):
    """Test that an empty list is returned for bulk queries with no results."""
    with patch('src.actions.load.state') as mock_state:
      mock_state.tsdb.fetch_batch_by_ids = AsyncMock(return_value=[])
      result = await load_resource("test_table",
                                   uids=["not_found_1", "not_found_2"])
      assert result == []


class TestActionsImports:
  """Test actions module imports and structure."""

  def test_actions_load_imports(self):
    """Test that load module imports work."""
    from src.actions.load import load_resource
    assert load_resource is not None

  def test_data_centric_patterns(self):
    """Test that new data-centric patterns are available."""
    from src.actions.load import load_resource
    import inspect

    assert callable(load_resource)
    assert inspect.iscoroutinefunction(load_resource)
