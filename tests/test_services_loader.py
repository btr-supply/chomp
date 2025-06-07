"""Tests for loader module."""
import pytest
from unittest.mock import AsyncMock, Mock, patch
import sys
import os
from datetime import datetime, timezone

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
  import polars as pl
  from src.services.loader import (trim_resource, get_resources,
                                   parse_resources, parse_fields,
                                   parse_resources_fields, get_schema,
                                   format_table, get_last_values, get_history)
  from src.model import Scope
  POLARS_AVAILABLE = True
except ImportError:
  POLARS_AVAILABLE = False


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestTrimResource:
  """Test trim_resource function."""

  def test_trim_resource_empty(self):
    """Test trimming empty resource."""
    result = trim_resource({})
    assert result == {}

  def test_trim_resource_basic(self):
    """Test basic resource trimming."""
    resource = {
        "name": "test_resource",
        "type": "api",
        "fields": {
            "field1": {
                "type": "float",
                "tags": ["tag1"],
                "transient": False
            }
        }
    }

    result = trim_resource(resource, Scope.DEFAULT)

    assert result["name"] == "test_resource"
    assert result["type"] == "api"
    assert "field1" in result["fields"]
    assert result["fields"]["field1"]["type"] == "float"

  def test_trim_resource_transient_filtered(self):
    """Test that transient fields are filtered correctly."""
    resource = {
        "name": "test_resource",
        "type": "api",
        "fields": {
            "field1": {
                "type": "float",
                "transient": False
            },
            "field2": {
                "type": "string",
                "transient": True
            }
        }
    }

    result = trim_resource(resource, Scope.DEFAULT)

    assert "field1" in result["fields"]
    assert "field2" not in result["fields"]

  def test_trim_resource_transient_included(self):
    """Test that transient fields are included with TRANSIENT scope."""
    resource = {
        "name": "test_resource",
        "type": "api",
        "fields": {
            "field1": {
                "type": "float",
                "transient": False
            },
            "field2": {
                "type": "string",
                "transient": True
            }
        }
    }

    result = trim_resource(resource, Scope.TRANSIENT | Scope.DEFAULT)

    assert "field1" in result["fields"]
    assert "field2" in result["fields"]


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestGetResources:
  """Test get_resources function."""

  def setup_method(self):
    """Set up test fixtures."""
    # Clear cache before each test
    from src.services.loader import _resources_by_scope
    _resources_by_scope[Scope.ALL] = None
    _resources_by_scope[Scope.DEFAULT] = None
    _resources_by_scope[Scope.DETAILED] = None

  @pytest.mark.asyncio
  async def test_get_resources_cache_miss(self):
    """Test resource retrieval with cache miss."""
    mock_resources = {
        "resource1": {
            "name": "resource1",
            "type": "api",
            "fields": {
                "field1": {
                    "type": "float"
                }
            }
        }
    }

    with patch('src.services.loader.get_resource_status', new_callable=AsyncMock, return_value=mock_resources), \
         patch('src.services.loader.state') as mock_state, \
         patch('src.services.loader.get_running_loop') as mock_loop:

      mock_state.thread_pool = Mock()
      mock_loop.return_value.run_in_executor = AsyncMock(return_value={
          "name": "resource1",
          "fields": {}
      })

      err, result = await get_resources(Scope.DEFAULT)

      assert err == ""
      assert isinstance(result, dict)

  @pytest.mark.asyncio
  async def test_get_resources_unsupported_scope(self):
    """Test error handling for unsupported scope."""
    with patch('src.services.loader._resources_by_scope',
               {Scope.DEFAULT: None}):
      err, result = await get_resources(999)  # Invalid scope

      assert "Unsupported resource scope" in err
      assert result == {}


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestParseResources:
  """Test parse_resources function."""

  @pytest.mark.asyncio
  async def test_parse_resources_all(self):
    """Test parsing resources with 'all' keyword."""
    mock_resources = {"resource1": {}, "resource2": {}}

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await parse_resources("all")

      assert err == ""
      assert result == ["resource1", "resource2"]

  @pytest.mark.asyncio
  async def test_parse_resources_wildcard(self):
    """Test parsing resources with wildcard."""
    mock_resources = {"resource1": {}, "resource2": {}}

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await parse_resources("*")

      assert err == ""
      assert result == ["resource1", "resource2"]

  @pytest.mark.asyncio
  async def test_parse_resources_specific(self):
    """Test parsing specific resources."""
    with patch('src.services.loader._resources_by_scope',
               {Scope.DEFAULT: {
                   "resource1": {},
                   "resource2": {}
               }}):
      err, result = await parse_resources("resource1,resource2")

      assert err == ""
      assert result == ["resource1", "resource2"]

  @pytest.mark.asyncio
  async def test_parse_resources_not_found(self):
    """Test parsing non-existent resources."""
    with patch('src.services.loader._resources_by_scope', {Scope.DEFAULT: {}}):
      err, result = await parse_resources("nonexistent")

      assert err == "No resources found"
      assert result == []

  @pytest.mark.asyncio
  async def test_parse_resources_exception(self):
    """Test exception handling."""
    with patch('src.services.loader.split',
               side_effect=Exception("Test error")):
      err, result = await parse_resources("resource1")

      assert "Error parsing resources" in err
      assert result == []


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestParseFields:
  """Test parse_fields function."""

  @pytest.mark.asyncio
  async def test_parse_fields_all(self):
    """Test parsing fields with 'all' keyword."""
    mock_resources = {"resource1": {"fields": {"field1": {}, "field2": {}}}}

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await parse_fields("resource1", "all")

      assert err == ""
      assert result == ["field1", "field2"]

  @pytest.mark.asyncio
  async def test_parse_fields_specific(self):
    """Test parsing specific fields."""
    mock_resources = {
        "resource1": {
            "fields": {
                "field1": {},
                "field2": {},
                "field3": {}
            }
        }
    }

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await parse_fields("resource1", "field1,field2")

      assert err == ""
      assert result == ["field1", "field2"]

  @pytest.mark.asyncio
  async def test_parse_fields_resource_not_found(self):
    """Test parsing fields for non-existent resource."""
    mock_resources = {}

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await parse_fields("nonexistent", "field1")

      assert "Resource not found" in err
      assert result == []

  @pytest.mark.asyncio
  async def test_parse_fields_no_fields(self):
    """Test parsing fields when resource has no fields."""
    mock_resources = {"resource1": {}}

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await parse_fields("resource1", "field1")

      assert "Fields not found" in err
      assert result == []

  @pytest.mark.asyncio
  async def test_parse_fields_exception(self):
    """Test exception handling."""
    with patch('src.services.loader.get_resources',
               side_effect=Exception("Test error")):
      err, result = await parse_fields("resource1", "field1")

      assert "Error parsing fields" in err
      assert result == []


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestParseResourcesFields:
  """Test parse_resources_fields function."""

  @pytest.mark.asyncio
  async def test_parse_resources_fields_success(self):
    """Test successful parsing of resources and fields."""
    with patch('src.services.loader.parse_resources', new_callable=AsyncMock, return_value=("", ["resource1"])), \
         patch('src.services.loader.parse_fields', new_callable=AsyncMock, return_value=("", ["field1"])):

      err, result = await parse_resources_fields("resource1", "field1")

      assert err == ""
      assert result == (["resource1"], ["field1"])

  @pytest.mark.asyncio
  async def test_parse_resources_fields_resource_error(self):
    """Test error handling when parse_resources fails."""
    with patch('src.services.loader.parse_resources',
               new_callable=AsyncMock,
               return_value=("Resource error", [])):

      err, result = await parse_resources_fields("resource1", "field1")

      assert err == "Resource error"
      assert result == ([], [])

  @pytest.mark.asyncio
  async def test_parse_resources_fields_field_error(self):
    """Test error handling when parse_fields fails."""
    with patch('src.services.loader.parse_resources', new_callable=AsyncMock, return_value=("", ["resource1"])), \
         patch('src.services.loader.parse_fields', new_callable=AsyncMock, return_value=("Field error", [])):

      err, result = await parse_resources_fields("resource1", "field1")

      assert err == "Field error"
      assert result == ([], [])


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestGetSchema:
  """Test get_schema function."""

  @pytest.mark.asyncio
  async def test_get_schema_all(self):
    """Test getting schema for all resources."""
    mock_resources = {"resource1": {"field1": {}}, "resource2": {"field2": {}}}

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await get_schema()

      assert err == ""
      assert result == mock_resources

  @pytest.mark.asyncio
  async def test_get_schema_specific_resources(self):
    """Test getting schema for specific resources."""
    mock_resources = {"resource1": {"field1": {}}, "resource2": {"field2": {}}}

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await get_schema(resources=["resource1", "resource2"])

      assert err == ""
      assert "resource1" in result
      assert "resource2" in result

  @pytest.mark.asyncio
  async def test_get_schema_single_resource(self):
    """Test getting schema for single resource."""
    mock_resources = {"resource1": {"field1": {}}, "resource2": {"field2": {}}}

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await get_schema(resources=["resource1"])

      assert err == ""
      assert result == {"field1": {}}

  @pytest.mark.asyncio
  async def test_get_schema_error(self):
    """Test error handling in get_schema."""
    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("Resources error", {})):
      err, result = await get_schema()

      assert err == "Resources error"
      assert result == {}


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestFormatTable:
  """Test format_table function."""

  def test_format_table_empty_data(self):
    """Test formatting empty data."""
    err, result = format_table(pl.DataFrame())

    assert err == "No dataset to format"
    assert result is None

  def test_format_table_same_format(self):
    """Test formatting with same from/to format."""
    data = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    err, result = format_table(data, from_format="polars", to_format="polars")

    assert err == ""
    assert result.equals(data)

  def test_format_table_py_row_to_polars(self):
    """Test converting from Python rows to Polars."""
    data = [[1, 2], [3, 4]]
    err, result = format_table(data, from_format="py:row", to_format="polars")

    assert err == ""
    assert isinstance(result, pl.DataFrame)
    assert result.height == 2

  def test_format_table_polars_to_json_row(self):
    """Test converting from Polars to JSON row format."""
    data = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    err, result = format_table(data,
                               from_format="polars",
                               to_format="json:row")

    assert err == ""
    assert isinstance(result, bytes)

  def test_format_table_with_columns(self):
    """Test formatting with custom column names."""
    data = pl.DataFrame({"old1": [1, 2], "old2": [3, 4]})
    err, result = format_table(data,
                               from_format="polars",
                               to_format="polars",
                               columns=["new1", "new2"])

    assert err == ""
    assert result.columns == ["new1", "new2"]

  def test_format_table_unsupported_from_format(self):
    """Test error handling for unsupported from format."""
    data = [[1, 2], [3, 4]]
    err, result = format_table(data,
                               from_format="unsupported",
                               to_format="polars")

    assert "Unsupported from_format" in err
    assert result is None

  def test_format_table_unsupported_to_format(self):
    """Test error handling for unsupported to format."""
    data = pl.DataFrame({"col1": [1, 2]})
    err, result = format_table(data,
                               from_format="polars",
                               to_format="unsupported")

    assert "Unsupported to_format" in err
    assert result is None

  def test_format_table_json_input(self):
    """Test formatting JSON input."""
    data = '{"col1": [1, 2], "col2": [3, 4]}'
    err, result = format_table(data,
                               from_format="json:column",
                               to_format="polars")

    assert err == ""
    assert isinstance(result, pl.DataFrame)

  def test_format_table_csv_output(self):
    """Test CSV output format."""
    data = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    err, result = format_table(data, from_format="polars", to_format="csv")

    assert err == ""
    assert isinstance(result, str)
    assert "col1,col2" in result

  def test_format_table_exception_handling(self):
    """Test exception handling in format_table."""
    # Test with invalid data that causes polars to fail
    with patch('polars.DataFrame', side_effect=Exception("Test error")):
      err, result = format_table([[1, 2]],
                                 from_format="py:row",
                                 to_format="polars")

      assert "Error formatting table" in err
      assert result is None


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestGetLastValues:
  """Test get_last_values function."""

  @pytest.mark.asyncio
  async def test_get_last_values_single_resource(self):
    """Test getting last values for single resource."""
    mock_data = {"field1": 100.0, "field2": 200.0}

    with patch('src.services.loader.get_cache',
               new_callable=AsyncMock,
               return_value=mock_data):
      err, result = await get_last_values(["resource1"])

      assert err == ""
      assert result["resource1"]["field1"] == 100.0
      assert result["resource1"]["quote"] == "USDC.idx"
      assert result["resource1"]["precision"] == 6

  @pytest.mark.asyncio
  async def test_get_last_values_multiple_resources(self):
    """Test getting last values for multiple resources."""
    mock_batch_data = {
        "resource1": {
            "field1": 100.0
        },
        "resource2": {
            "field1": 200.0
        }
    }

    with patch('src.services.loader.get_cache_batch',
               new_callable=AsyncMock,
               return_value=mock_batch_data):
      err, result = await get_last_values(["resource1", "resource2"])

      assert err == ""
      assert "resource1" in result
      assert "resource2" in result

  @pytest.mark.asyncio
  async def test_get_last_values_missing_resource(self):
    """Test error handling for missing resource."""
    with patch('src.services.loader.get_cache',
               new_callable=AsyncMock,
               return_value=None):
      err, result = await get_last_values(["nonexistent"])

      assert "Resources not found" in err
      assert result == {}

  @pytest.mark.asyncio
  async def test_get_last_values_with_quote(self):
    """Test getting last values with quote conversion."""
    mock_resource_data = {"field1": 100.0}
    mock_quote_data = {"idx": 1.5}

    with patch('src.services.loader.get_cache',
               new_callable=AsyncMock) as mock_cache:
      mock_cache.side_effect = [mock_resource_data, mock_quote_data]

      err, result = await get_last_values(["resource1"], quote="OTHER.idx")

      assert err == ""
      assert result["resource1"]["field1"] == 150.0  # 100 * 1.5

  @pytest.mark.asyncio
  async def test_get_last_values_quote_not_found(self):
    """Test error handling when quote resource not found."""
    mock_resource_data = {"field1": 100.0}

    with patch('src.services.loader.get_cache',
               new_callable=AsyncMock) as mock_cache:
      mock_cache.side_effect = [mock_resource_data, None]

      err, result = await get_last_values(["resource1"],
                                          quote="NONEXISTENT.idx")

      assert "Quote resource not found" in err
      assert result == {}

  @pytest.mark.asyncio
  async def test_get_last_values_invalid_quote_format(self):
    """Test error handling for invalid quote format."""
    mock_data = {"field1": 100.0}

    with patch('src.services.loader.get_cache',
               new_callable=AsyncMock,
               return_value=mock_data):
      err, result = await get_last_values(["resource1"],
                                          quote="invalid_format")

      assert "Quote must be in format resource.field" in err
      assert result == {}


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestGetHistory:
  """Test get_history function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)

  @pytest.mark.asyncio
  async def test_get_history_success(self):
    """Test successful history retrieval."""
    mock_columns = ["ts", "field1"]
    mock_data = [[[self.from_date, 100.0], [self.to_date, 110.0]]]

    with patch('src.services.loader.state') as mock_state, \
         patch('src.services.loader.format_table', return_value=("", {"data": "formatted"})):

      mock_state.tsdb.fetch_batch = AsyncMock(return_value=(mock_columns,
                                                            mock_data))

      err, result = await get_history(["resource1"], ["field1"],
                                      self.from_date, self.to_date, "m5")

      assert err == ""
      assert result == {"data": "formatted"}

  @pytest.mark.asyncio
  async def test_get_history_no_data(self):
    """Test handling when no data found."""
    with patch('src.services.loader.state') as mock_state:
      mock_state.tsdb.fetch_batch = AsyncMock(return_value=([], []))

      err, result = await get_history(["resource1"], ["field1"],
                                      self.from_date, self.to_date, "m5")

      assert err == "No data found"
      assert result is None

  @pytest.mark.asyncio
  async def test_get_history_with_quote(self):
    """Test history retrieval with quote conversion."""
    mock_columns = ["ts", "field1"]
    mock_data = [[[self.from_date, 100.0]]]
    mock_quote_columns = ["ts", "USDC.idx"]
    mock_quote_data = [[self.from_date, 1.5]]

    with patch('src.services.loader.state') as mock_state, \
         patch('src.services.loader.format_table', return_value=("", {"data": "formatted"})):

      mock_state.tsdb.fetch_batch = AsyncMock(return_value=(mock_columns,
                                                            mock_data))
      mock_state.tsdb.fetch = AsyncMock(return_value=(mock_quote_columns,
                                                      mock_quote_data))

      err, result = await get_history(["resource1"], ["field1"],
                                      self.from_date,
                                      self.to_date,
                                      "m5",
                                      quote="USDC.idx")

      assert err == ""
      assert result == {"data": "formatted"}

  @pytest.mark.asyncio
  async def test_get_history_no_quote_data(self):
    """Test error handling when quote data not found."""
    mock_columns = ["ts", "field1"]
    mock_data = [[[self.from_date, 100.0]]]

    with patch('src.services.loader.state') as mock_state:
      mock_state.tsdb.fetch_batch = AsyncMock(return_value=(mock_columns,
                                                            mock_data))
      mock_state.tsdb.fetch = AsyncMock(return_value=([], []))

      err, result = await get_history(["resource1"], ["field1"],
                                      self.from_date,
                                      self.to_date,
                                      "m5",
                                      quote="OTHER.idx")

      assert err == "No quote data found"
      assert result is None


# Integration tests
class TestLoaderIntegration:
  """Integration tests for loader module."""

  def test_imports_available(self):
    """Test that loader functions can be imported."""
    if POLARS_AVAILABLE:
      assert trim_resource is not None
      assert get_resources is not None
      assert parse_resources is not None
      assert parse_fields is not None
      assert parse_resources_fields is not None
      assert get_schema is not None
      assert format_table is not None
      assert get_last_values is not None
      assert get_history is not None

  def test_functions_callable(self):
    """Test that all functions are callable."""
    if POLARS_AVAILABLE:
      assert callable(trim_resource)
      assert callable(get_resources)
      assert callable(parse_resources)
      assert callable(parse_fields)
      assert callable(parse_resources_fields)
      assert callable(get_schema)
      assert callable(format_table)
      assert callable(get_last_values)
      assert callable(get_history)


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestParseResourcesEdgeCases:
  """Test edge cases in parse_resources."""

  @pytest.mark.asyncio
  async def test_parse_resources_none_cached_resources(self):
    """Test parse_resources when cached resources is None."""
    with patch('src.services.loader._resources_by_scope', {Scope.DEFAULT: None}), \
         patch('src.services.loader.get_resources', new_callable=AsyncMock, return_value=("", {})):

      err, result = await parse_resources("resource1")

      assert err == "No resources found"
      assert result == []

  @pytest.mark.asyncio
  async def test_parse_resources_problem_initializing(self):
    """Test parse_resources when resources initialization fails."""
    with patch('src.services.loader._resources_by_scope', {Scope.DEFAULT: None}), \
         patch('src.services.loader.get_resources', new_callable=AsyncMock, side_effect=[None]):

      err, result = await parse_resources("resource1")

      assert err == "Problem initializing resources"
      assert result == []


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestParseFieldsEdgeCases:
  """Test edge cases in parse_fields."""

  @pytest.mark.asyncio
  async def test_parse_fields_dict_filtered_case(self):
    """Test parse_fields when filtered result is a dict."""
    mock_resources = {
        "resource1": {
            "fields": {
                "field1": {
                    "type": "float"
                },
                "field2": {
                    "type": "string"
                }
            }
        }
    }

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await parse_fields("resource1", "field1,field2")

      assert err == ""
      assert result == ["field1", "field2"]


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestFormatTableAdvanced:
  """Test advanced format_table functionality."""

  def test_format_table_timestamp_conversion(self):
    """Test timestamp conversion for JS compatibility."""
    data = pl.DataFrame({
        "ts": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
        "value": [100.0]
    })
    err, result = format_table(data,
                               from_format="polars",
                               to_format="json:row")

    assert err == ""
    assert isinstance(result, bytes)

  def test_format_table_json_string_input(self):
    """Test JSON string input parsing."""
    data = '{"col1": [1, 2], "col2": [3, 4]}'
    err, result = format_table(data,
                               from_format="json:row",
                               to_format="polars")

    assert err == ""
    assert isinstance(result, pl.DataFrame)

  def test_format_table_all_formats(self):
    """Test various output formats."""
    data = pl.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    # Test TSV
    err, result = format_table(data, from_format="polars", to_format="tsv")
    assert err == ""
    assert isinstance(result, str)
    assert "\t" in result

    # Test PSV
    err, result = format_table(data, from_format="polars", to_format="psv")
    assert err == ""
    assert isinstance(result, str)
    assert "|" in result

    # Test py:column
    err, result = format_table(data,
                               from_format="polars",
                               to_format="py:column")
    assert err == ""
    assert isinstance(result, list)

    # Test np:row
    err, result = format_table(data, from_format="polars", to_format="np:row")
    assert err == ""
    import numpy as np
    assert isinstance(result, np.ndarray)

  def test_format_table_series_to_df(self):
    """Test polars Series conversion to DataFrame."""
    data = pl.Series("values", [1, 2, 3])
    err, result = format_table(data, from_format="polars", to_format="polars")

    assert err == ""
    assert isinstance(result, pl.DataFrame)


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestGetLastValuesAdvanced:
  """Test advanced get_last_values functionality."""

  @pytest.mark.asyncio
  async def test_get_last_values_quote_field_nan(self):
    """Test handling of NaN quote values."""
    mock_resource_data = {"field1": 100.0}
    mock_quote_data = {"idx": float('nan')}

    with patch('src.services.loader.get_cache',
               new_callable=AsyncMock) as mock_cache:
      mock_cache.side_effect = [mock_resource_data, mock_quote_data]

      err, result = await get_last_values(["resource1"], quote="OTHER.idx")

      assert "Quote field not found or NaN" in err
      assert result == {}

  @pytest.mark.asyncio
  async def test_get_last_values_quote_field_none(self):
    """Test handling of None quote values."""
    mock_resource_data = {"field1": 100.0}
    mock_quote_data = {"other": 1.5}  # Missing idx field

    with patch('src.services.loader.get_cache',
               new_callable=AsyncMock) as mock_cache:
      mock_cache.side_effect = [mock_resource_data, mock_quote_data]

      err, result = await get_last_values(["resource1"], quote="OTHER.idx")

      assert "Quote field not found or NaN" in err
      assert result == {}

  @pytest.mark.asyncio
  async def test_get_last_values_non_float_conversion(self):
    """Test quote conversion with non-float values."""
    mock_resource_data = {"field1": 100.0, "field2": "string_value"}
    mock_quote_data = {"idx": 1.5}

    with patch('src.services.loader.get_cache',
               new_callable=AsyncMock) as mock_cache:
      mock_cache.side_effect = [mock_resource_data, mock_quote_data]

      err, result = await get_last_values(["resource1"], quote="OTHER.idx")

      assert err == ""
      assert result["resource1"]["field1"] == 150.0  # 100 * 1.5
      assert result["resource1"]["field2"] == "string_value"  # unchanged


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestGetHistoryAdvanced:
  """Test advanced get_history functionality."""

  def setup_method(self):
    """Set up test fixtures."""
    self.from_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    self.to_date = datetime(2024, 1, 2, tzinfo=timezone.utc)

  @pytest.mark.asyncio
  async def test_get_history_fill_mode_none(self):
    """Test history with fill_mode none."""
    mock_columns = ["ts", "field1"]
    mock_data = [[[self.from_date, 100.0], [self.to_date, None]]]

    with patch('src.services.loader.state') as mock_state, \
         patch('src.services.loader.format_table', return_value=("", {"data": "formatted"})), \
         patch('src.services.loader.numeric_columns', return_value=["field1"]):

      mock_state.tsdb.fetch_batch = AsyncMock(return_value=(mock_columns,
                                                            mock_data))

      err, result = await get_history(["resource1"], ["field1"],
                                      self.from_date,
                                      self.to_date,
                                      "m5",
                                      fill_mode="none")

      assert err == ""
      assert result == {"data": "formatted"}

  @pytest.mark.asyncio
  async def test_get_history_no_numeric_columns(self):
    """Test history with no numeric columns."""
    mock_columns = ["ts", "text_field"]
    mock_data = [[[self.from_date, "value1"], [self.to_date, "value2"]]]

    with patch('src.services.loader.state') as mock_state, \
         patch('src.services.loader.format_table', return_value=("", {"data": "formatted"})), \
         patch('src.services.loader.numeric_columns', return_value=[]):

      mock_state.tsdb.fetch_batch = AsyncMock(return_value=(mock_columns,
                                                            mock_data))

      err, result = await get_history(["resource1"], ["text_field"],
                                      self.from_date, self.to_date, "m5")

      assert err == ""
      assert result == {"data": "formatted"}

  @pytest.mark.asyncio
  async def test_get_history_truncate_disabled(self):
    """Test history with truncate_leading_zeros disabled."""
    mock_columns = ["ts", "field1"]
    mock_data = [[[self.from_date, 0.0], [self.to_date, 100.0]]]

    with patch('src.services.loader.state') as mock_state, \
         patch('src.services.loader.format_table', return_value=("", {"data": "formatted"})), \
         patch('src.services.loader.numeric_columns', return_value=["field1"]):

      mock_state.tsdb.fetch_batch = AsyncMock(return_value=(mock_columns,
                                                            mock_data))

      err, result = await get_history(["resource1"], ["field1"],
                                      self.from_date,
                                      self.to_date,
                                      "m5",
                                      truncate_leading_zeros=False)

      assert err == ""
      assert result == {"data": "formatted"}

  @pytest.mark.asyncio
  async def test_get_history_empty_data_after_filter(self):
    """Test history when all rows are filtered out."""
    mock_columns = ["ts", "field1"]
    mock_data = [[[None, 100.0]]]  # ts is null

    with patch('src.services.loader.state') as mock_state:

      mock_state.tsdb.fetch_batch = AsyncMock(return_value=(mock_columns,
                                                            mock_data))

      err, result = await get_history(["resource1"], ["field1"],
                                      self.from_date, self.to_date, "m5")

      # Should handle empty DataFrame gracefully
      assert err == "No dataset to format"
      assert result is None


@pytest.mark.skipif(not POLARS_AVAILABLE, reason="Dependencies not available")
class TestGetSchemaAdvanced:
  """Test advanced get_schema functionality."""

  @pytest.mark.asyncio
  async def test_get_schema_multiple_resources_filtered(self):
    """Test schema with multiple resources and field filtering."""
    mock_resources = {
        "resource1": {
            "field1": {
                "type": "float"
            },
            "field2": {
                "type": "string"
            }
        },
        "resource2": {
            "field1": {
                "type": "int"
            },
            "field3": {
                "type": "bool"
            }
        }
    }

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await get_schema(["resource1", "resource2"], ["field1"])

      assert err == ""
      assert "resource1" in result
      assert "resource2" in result
      assert "field1" in result["resource1"]
      assert "field1" in result["resource2"]
      assert "field2" not in result["resource1"]  # Filtered out
      assert "field3" not in result["resource2"]  # Filtered out

  @pytest.mark.asyncio
  async def test_get_schema_single_resource_dict_extraction(self):
    """Test schema extraction for single resource (dict case)."""
    mock_resources = {
        "resource1": {
            "field1": {
                "type": "float"
            },
            "field2": {
                "type": "string"
            }
        }
    }

    with patch('src.services.loader.get_resources',
               new_callable=AsyncMock,
               return_value=("", mock_resources)):
      err, result = await get_schema(["resource1"])

      assert err == ""
      # Should return the resource data directly for single resource
      assert "field1" in result
      assert "field2" in result
