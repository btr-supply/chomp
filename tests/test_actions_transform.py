"""Tests for transform module."""
import pytest
from unittest.mock import AsyncMock, Mock, patch
import sys
import os
import numpy as np

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
  from src.actions.transform import (BASE_TRANSFORMERS, SERIES_TRANSFORMERS,
                                     get_cached_field_value,
                                     parse_cached_reference,
                                     process_cached_references,
                                     apply_transformer, transform,
                                     transform_all, _cached_data_cache)
  from src.model import Ingester, ResourceField
  DEPENDENCIES_AVAILABLE = True
except ImportError:
  DEPENDENCIES_AVAILABLE = False


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE,
                    reason="Dependencies not available")
class TestBaseTransformers:
  """Test BASE_TRANSFORMERS functionality."""

  def test_string_transformers(self):
    """Test string transformation functions."""
    test_value = "Hello World"

    # Test basic string transformers
    assert BASE_TRANSFORMERS["lower"](None, test_value) == "hello world"
    assert BASE_TRANSFORMERS["upper"](None, test_value) == "HELLO WORLD"
    assert BASE_TRANSFORMERS["capitalize"](None, test_value) == "Hello world"
    assert BASE_TRANSFORMERS["title"](None, test_value) == "Hello World"
    assert BASE_TRANSFORMERS["strip"](None, " test ") == "test"
    assert BASE_TRANSFORMERS["reverse"](None, "abc") == "cba"

  def test_case_transformers(self):
    """Test case transformation functions."""
    test_value = "hello world"

    assert BASE_TRANSFORMERS["to_snake"](None, test_value) == "hello_world"
    assert BASE_TRANSFORMERS["to_kebab"](None, test_value) == "hello-world"
    assert BASE_TRANSFORMERS["slugify"](None, test_value) == "hello-world"
    assert BASE_TRANSFORMERS["to_camel"](None, test_value) == "HelloWorld"
    assert BASE_TRANSFORMERS["to_pascal"](None, test_value) == "HelloWorld"

  def test_type_transformers(self):
    """Test type conversion transformers."""
    assert BASE_TRANSFORMERS["int"](None, "123") == 123
    assert BASE_TRANSFORMERS["float"](None, "123.45") == 123.45
    assert BASE_TRANSFORMERS["str"](None, 123) == "123"
    assert BASE_TRANSFORMERS["bool"](None, 1) is True

  def test_encoding_transformers(self):
    """Test encoding and hashing transformers."""
    test_value = "test"

    # Test binary and hex
    assert BASE_TRANSFORMERS["bin"](None, 5) == "101"
    assert BASE_TRANSFORMERS["hex"](None, 15) == "f"

    # Test hashing
    sha_result = BASE_TRANSFORMERS["sha256digest"](None, test_value)
    assert len(sha_result) == 64  # SHA256 produces 64 character hex string

    md5_result = BASE_TRANSFORMERS["md5digest"](None, test_value)
    assert len(md5_result) == 32  # MD5 produces 32 character hex string

  def test_address_transformer(self):
    """Test address shortening transformer."""
    address = "0x1234567890abcdef1234567890abcdef12345678"
    result = BASE_TRANSFORMERS["shorten_address"](None, address)
    assert result == "0x1234...5678"

  def test_rounding_transformers(self):
    """Test rounding transformers."""
    test_value = 123.456789

    assert BASE_TRANSFORMERS["round"](None, test_value) == 123
    assert BASE_TRANSFORMERS["round2"](None, test_value) == 123.46
    assert BASE_TRANSFORMERS["round4"](None, test_value) == 123.4568
    assert BASE_TRANSFORMERS["round6"](None, test_value) == 123.456789
    assert BASE_TRANSFORMERS["round8"](None, test_value) == 123.456789
    assert BASE_TRANSFORMERS["round10"](None, test_value) == 123.456789

  def test_json_transformer(self):
    """Test JSON transformation."""
    test_data = {"key": "value", "number": 42}
    result = BASE_TRANSFORMERS["to_json"](None, test_data)
    assert '"key":"value"' in result
    assert '"number":42' in result

  def test_punctuation_removal(self):
    """Test punctuation removal transformer."""
    test_value = "Hello, World! How are you?"
    result = BASE_TRANSFORMERS["remove_punctuation"](None, test_value)
    assert result == "Hello World How are you"


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE,
                    reason="Dependencies not available")
class TestSeriesTransformers:
  """Test SERIES_TRANSFORMERS functionality."""

  def test_statistical_transformers(self):
    """Test statistical series transformers."""
    test_series = [1, 2, 3, 4, 5]

    assert SERIES_TRANSFORMERS["median"](None, test_series) == 3.0
    assert SERIES_TRANSFORMERS["mean"](None, test_series) == 3.0
    assert SERIES_TRANSFORMERS["min"](None, test_series) == 1
    assert SERIES_TRANSFORMERS["max"](None, test_series) == 5
    assert SERIES_TRANSFORMERS["sum"](None, test_series) == 15
    assert SERIES_TRANSFORMERS["prod"](None, test_series) == 120

  def test_variance_transformers(self):
    """Test variance and standard deviation transformers."""
    test_series = [1, 2, 3, 4, 5]

    std_result = SERIES_TRANSFORMERS["std"](None, test_series)
    var_result = SERIES_TRANSFORMERS["var"](None, test_series)

    assert isinstance(std_result, (int, float))
    assert isinstance(var_result, (int, float))
    assert var_result > 0  # Variance should be positive
    assert std_result > 0  # Standard deviation should be positive

  def test_cumsum_transformer(self):
    """Test cumulative sum transformer."""
    test_series = [1, 2, 3]
    result = SERIES_TRANSFORMERS["cumsum"](None, test_series)

    expected = np.cumsum(test_series)
    assert np.array_equal(result, expected)


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE,
                    reason="Dependencies not available")
class TestGetCachedFieldValue:
  """Test get_cached_field_value function."""

  def setup_method(self):
    """Clear cache before each test."""
    global _cached_data_cache
    _cached_data_cache.clear()

  @pytest.mark.asyncio
  async def test_get_cached_field_value_idx_field(self):
    """Test getting idx field value."""
    mock_cache_data = {"idx": {"price": 100.0, "volume": 1000}}

    with patch('src.actions.transform.get_cache',
               new_callable=AsyncMock,
               return_value=mock_cache_data):
      result = await get_cached_field_value("USDC", "idx")

      assert result == {"price": 100.0, "volume": 1000}

  @pytest.mark.asyncio
  async def test_get_cached_field_value_regular_field(self):
    """Test getting regular field value."""
    mock_cache_data = {"price": 100.0, "volume": 1000}

    with patch('src.actions.transform.get_cache',
               new_callable=AsyncMock,
               return_value=mock_cache_data):
      result = await get_cached_field_value("USDC", "price")

      assert result == 100.0

  @pytest.mark.asyncio
  async def test_get_cached_field_value_local_cache_hit(self):
    """Test local cache hit scenario."""
    # Populate local cache
    _cached_data_cache["USDC"] = {"price": 100.0}

    with patch('src.actions.transform.get_cache',
               new_callable=AsyncMock) as mock_get_cache:
      result = await get_cached_field_value("USDC", "price")

      assert result == 100.0
      mock_get_cache.assert_not_called()  # Should not call Redis

  @pytest.mark.asyncio
  async def test_get_cached_field_value_no_data(self):
    """Test when no cached data found."""
    with patch('src.actions.transform.get_cache', new_callable=AsyncMock, return_value=None), \
         patch('src.actions.transform.log_error') as mock_log_error:

      result = await get_cached_field_value("NONEXISTENT", "price")

      assert result is None
      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_get_cached_field_value_field_not_found(self):
    """Test when field not found in cached data."""
    mock_cache_data = {"other_field": 100.0}

    with patch('src.actions.transform.get_cache', new_callable=AsyncMock, return_value=mock_cache_data), \
         patch('src.actions.transform.log_error') as mock_log_error:

      result = await get_cached_field_value("USDC", "nonexistent_field")

      assert result is None
      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_get_cached_field_value_idx_not_found(self):
    """Test when idx field not found."""
    mock_cache_data = {"price": 100.0}  # No idx field

    with patch('src.actions.transform.get_cache', new_callable=AsyncMock, return_value=mock_cache_data), \
         patch('src.actions.transform.log_debug') as mock_log_debug:

      result = await get_cached_field_value("USDC", "idx")

      assert result is None
      mock_log_debug.assert_called_once()


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE,
                    reason="Dependencies not available")
class TestParseCachedReference:
  """Test parse_cached_reference function."""

  def test_parse_with_dot(self):
    """Test parsing reference with dot."""
    ingester, field = parse_cached_reference("USDC.idx")
    assert ingester == "USDC"
    assert field == "idx"

  def test_parse_without_dot(self):
    """Test parsing reference without dot."""
    ingester, field = parse_cached_reference("price")
    assert ingester is None
    assert field == "price"

  def test_parse_complex_field(self):
    """Test parsing complex field reference."""
    ingester, field = parse_cached_reference("ETHEREUM.price.24h")
    assert ingester == "ETHEREUM"
    assert field == "price.24h"


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE,
                    reason="Dependencies not available")
class TestProcessCachedReferences:
  """Test process_cached_references function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_ingester = Mock(spec=Ingester)
    self.mock_ingester.name = "TEST_INGESTER"
    self.mock_ingester.data_by_field = {"existing_field": 123}

  @pytest.mark.asyncio
  async def test_process_simple_reference(self):
    """Test processing simple cached reference."""
    transformer = "price * {USDC.idx}"

    with patch('src.actions.transform.get_cached_field_value',
               new_callable=AsyncMock,
               return_value=1.5):
      result = await process_cached_references(self.mock_ingester, transformer)

      assert result == "price * 1.5"

  @pytest.mark.asyncio
  async def test_process_self_reference_skip(self):
    """Test that self references are skipped."""
    transformer = "price * {self}"

    result = await process_cached_references(self.mock_ingester, transformer)

    assert result == "price * {self}"  # Should remain unchanged

  @pytest.mark.asyncio
  async def test_process_series_reference_skip(self):
    """Test that series references are skipped."""
    transformer = "price * {price::mean(24h)}"

    result = await process_cached_references(self.mock_ingester, transformer)

    assert result == "price * {price::mean(24h)}"  # Should remain unchanged

  @pytest.mark.asyncio
  async def test_process_existing_field_skip(self):
    """Test that existing field references are skipped."""
    transformer = "price * {existing_field}"

    result = await process_cached_references(self.mock_ingester, transformer)

    assert result == "price * {existing_field}"  # Should remain unchanged

  @pytest.mark.asyncio
  async def test_process_cross_ingester_reference(self):
    """Test processing cross-ingester reference."""
    transformer = "price * {OTHER.value}"

    with patch('src.actions.transform.get_cached_field_value',
               new_callable=AsyncMock,
               return_value=2.0):
      result = await process_cached_references(self.mock_ingester, transformer)

      assert result == "price * 2.0"

  @pytest.mark.asyncio
  async def test_process_unresolved_reference(self):
    """Test handling of unresolved reference."""
    transformer = "price * {NONEXISTENT.value}"

    with patch('src.actions.transform.get_cached_field_value', new_callable=AsyncMock, return_value=None), \
         patch('src.actions.transform.log_error') as mock_log_error:

      result = await process_cached_references(self.mock_ingester, transformer)

      assert result == "price * {NONEXISTENT.value}"  # Should remain unchanged
      mock_log_error.assert_called_once()


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE,
                    reason="Dependencies not available")
class TestApplyTransformer:
  """Test apply_transformer function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_ingester = Mock(spec=Ingester)
    self.mock_ingester.name = "TEST_INGESTER"
    self.mock_ingester.data_by_field = {"other_field": 50}
    self.mock_ingester.fields = []

    self.mock_field = Mock(spec=ResourceField)
    self.mock_field.name = "test_field"
    self.mock_field.value = 100
    self.mock_field.transformers = []

  @pytest.mark.asyncio
  async def test_apply_transformer_empty(self):
    """Test applying empty transformer."""
    result = await apply_transformer(self.mock_ingester, self.mock_field, "")
    assert result == 100  # Should return original value

  @pytest.mark.asyncio
  async def test_apply_transformer_numeric(self):
    """Test applying numeric transformer."""
    result = await apply_transformer(self.mock_ingester, self.mock_field,
                                     "123.45")
    assert result == 123.45

  @pytest.mark.asyncio
  async def test_apply_transformer_base_transformer(self):
    """Test applying base transformer."""
    self.mock_field.value = "hello world"
    result = await apply_transformer(self.mock_ingester, self.mock_field,
                                     "upper")
    assert result == "HELLO WORLD"

  @pytest.mark.asyncio
  async def test_apply_transformer_expression(self):
    """Test applying mathematical expression."""
    with patch('src.actions.transform.safe_eval', return_value=150):
      result = await apply_transformer(self.mock_ingester, self.mock_field,
                                       "{self} + 50")
      assert result == 150

  @pytest.mark.asyncio
  async def test_apply_transformer_field_reference(self):
    """Test applying transformer with field reference."""
    with patch('src.actions.transform.safe_eval', return_value=150):
      result = await apply_transformer(self.mock_ingester, self.mock_field,
                                       "{self} + {other_field}")
      assert result == 150

  @pytest.mark.asyncio
  async def test_apply_transformer_series_operation(self):
    """Test applying transformer with series operation."""
    self.mock_field.value = 100

    # Mock the series transformer components
    with patch('src.actions.transform.interval_to_delta') as mock_interval, \
         patch('src.actions.transform.load', new_callable=AsyncMock) as mock_load, \
         patch('src.actions.transform.safe_eval', return_value=110):

      from datetime import timedelta
      mock_interval.return_value = timedelta(hours=-24)  # Mock timedelta
      mock_load.return_value = [90, 95, 100]  # Mock series data

      # Create a mock field for the series target
      mock_target_field = Mock(spec=ResourceField)
      mock_target_field.name = "price"
      self.mock_ingester.fields = [mock_target_field]

      transformer = "{price::mean(h24)} + 10"
      result = await apply_transformer(self.mock_ingester, self.mock_field,
                                       transformer)

      assert result == 110

  @pytest.mark.asyncio
  async def test_apply_transformer_invalid_series_target(self):
    """Test error handling for invalid series target."""
    transformer = "{nonexistent::mean(h24)}"

    with pytest.raises(ValueError, match="Invalid transformer target"):
      await apply_transformer(self.mock_ingester, self.mock_field, transformer)

  @pytest.mark.asyncio
  async def test_apply_transformer_invalid_series_syntax(self):
    """Test error handling for invalid series syntax."""
    transformer = "{price::}"  # Missing function and lookback

    with pytest.raises(ValueError, match="Invalid transformer"):
      await apply_transformer(self.mock_ingester, self.mock_field, transformer)

  @pytest.mark.asyncio
  async def test_apply_transformer_cache_cleared(self):
    """Test that cache is cleared at start of transformation."""
    global _cached_data_cache
    _cached_data_cache["test"] = {"data": "value"}

    await apply_transformer(self.mock_ingester, self.mock_field, "123")

    assert len(_cached_data_cache) == 0


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE,
                    reason="Dependencies not available")
class TestTransform:
  """Test transform function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_ingester = Mock(spec=Ingester)
    self.mock_ingester.name = "TEST_INGESTER"
    self.mock_ingester.data_by_field = {}

    self.mock_field = Mock(spec=ResourceField)
    self.mock_field.name = "test_field"
    self.mock_field.value = 100
    self.mock_field.transformers = ["upper", "strip"]

  @pytest.mark.asyncio
  async def test_transform_success(self):
    """Test successful transformation."""
    with patch('src.actions.transform.apply_transformer',
               new_callable=AsyncMock) as mock_apply:
      mock_apply.side_effect = ["UPPER", "STRIP"]

      result = await transform(self.mock_ingester, self.mock_field)

      assert result == "STRIP"
      assert self.mock_ingester.data_by_field["test_field"] == "STRIP"
      assert mock_apply.call_count == 2

  @pytest.mark.asyncio
  async def test_transform_no_transformers(self):
    """Test transformation with no transformers."""
    self.mock_field.transformers = None

    result = await transform(self.mock_ingester, self.mock_field)

    assert result == 100
    assert self.mock_ingester.data_by_field["test_field"] == 100

  @pytest.mark.asyncio
  async def test_transform_empty_transformers(self):
    """Test transformation with empty transformers list."""
    self.mock_field.transformers = []

    result = await transform(self.mock_ingester, self.mock_field)

    assert result == 100
    assert self.mock_ingester.data_by_field["test_field"] == 100


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE,
                    reason="Dependencies not available")
class TestTransformAll:
  """Test transform_all function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.mock_ingester = Mock(spec=Ingester)
    self.mock_ingester.name = "TEST_INGESTER"
    self.mock_ingester.data_by_field = {}

    self.mock_field1 = Mock(spec=ResourceField)
    self.mock_field1.name = "field1"
    self.mock_field1.value = 100

    self.mock_field2 = Mock(spec=ResourceField)
    self.mock_field2.name = "field2"
    self.mock_field2.value = 200

    self.mock_ingester.fields = [self.mock_field1, self.mock_field2]

  @pytest.mark.asyncio
  async def test_transform_all_success(self):
    """Test successful transformation of all fields."""
    import src.state as state

    with patch('src.actions.transform.transform',
               new_callable=AsyncMock) as mock_transform:
      mock_transform.return_value = Mock()

      # Mock state.args
      mock_args = Mock()
      mock_args.verbose = False
      state.args = mock_args

      result = await transform_all(self.mock_ingester)

      assert result == 2
      assert mock_transform.call_count == 2

  @pytest.mark.asyncio
  async def test_transform_all_with_error(self):
    """Test transformation with some fields failing."""
    import src.state as state

    with patch('src.actions.transform.transform', new_callable=AsyncMock) as mock_transform, \
         patch('src.actions.transform.log_error') as mock_log_error:

      mock_transform.side_effect = [Mock(), Exception("Transform error")]

      # Mock state.args
      mock_args = Mock()
      mock_args.verbose = False
      state.args = mock_args

      result = await transform_all(self.mock_ingester)

      assert result == 1  # Only one successful transformation
      mock_log_error.assert_called_once()

  @pytest.mark.asyncio
  async def test_transform_all_verbose_logging(self):
    """Test verbose logging in transform_all."""
    with patch('src.actions.transform.transform', new_callable=AsyncMock), \
         patch('src.actions.transform.state') as mock_state, \
         patch('src.actions.transform.log_debug') as mock_log_debug:

      mock_state.args.verbose = True

      result = await transform_all(self.mock_ingester)

      assert result == 2
      mock_log_debug.assert_called_once()

  @pytest.mark.asyncio
  async def test_transform_all_no_fields(self):
    """Test transformation with no fields."""
    import src.state as state

    self.mock_ingester.fields = []

    # Mock state.args
    mock_args = Mock()
    mock_args.verbose = False
    state.args = mock_args

    result = await transform_all(self.mock_ingester)

    assert result == 0


# Integration tests
class TestTransformIntegration:
  """Integration tests for transform module."""

  def test_imports_available(self):
    """Test that transform functions can be imported."""
    if DEPENDENCIES_AVAILABLE:
      assert BASE_TRANSFORMERS is not None
      assert SERIES_TRANSFORMERS is not None
      assert get_cached_field_value is not None
      assert parse_cached_reference is not None
      assert process_cached_references is not None
      assert apply_transformer is not None
      assert transform is not None
      assert transform_all is not None

  def test_functions_callable(self):
    """Test that all functions are callable."""
    if DEPENDENCIES_AVAILABLE:
      assert callable(get_cached_field_value)
      assert callable(parse_cached_reference)
      assert callable(process_cached_references)
      assert callable(apply_transformer)
      assert callable(transform)
      assert callable(transform_all)

  def test_transformers_dictionaries(self):
    """Test that transformer dictionaries are properly defined."""
    if DEPENDENCIES_AVAILABLE:
      assert isinstance(BASE_TRANSFORMERS, dict)
      assert isinstance(SERIES_TRANSFORMERS, dict)
      assert len(BASE_TRANSFORMERS) > 0
      assert len(SERIES_TRANSFORMERS) > 0
