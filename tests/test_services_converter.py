"""Tests for converter service module."""
import pytest
from unittest.mock import patch
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.services.converter import convert, pegcheck


class TestConverterService:
  """Test the converter service functionality."""

  @pytest.mark.asyncio
  async def test_convert_valid_pair_base_amount(self):
    """Test convert with valid pair and base amount."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      # Setup mock data
      mock_loader.return_value = ("", {
        'btc': {'price': 50000.0},
        'usd': {'value': 1.0}
      })

      err, result = await convert('btc.price-usd.value', base_amount=2.0)

      assert err == ""
      assert result['base'] == 'btc.price'
      assert result['quote'] == 'usd.value'
      assert result['base_amount'] == 2.0
      assert result['quote_amount'] == 100000.0  # 2 * 50000
      assert result['rate'] == 50000.0
      assert result['result'] == 100000.0
      assert result['precision'] == 6

  @pytest.mark.asyncio
  async def test_convert_valid_pair_quote_amount(self):
    """Test convert with valid pair and quote amount."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      # Setup mock data
      mock_loader.return_value = ("", {
        'btc': {'price': 50000.0},
        'usd': {'value': 1.0}
      })

      err, result = await convert('btc.price-usd.value', quote_amount=100000.0)

      assert err == ""
      assert result['base'] == 'btc.price'
      assert result['quote'] == 'usd.value'
      assert result['base_amount'] == 2.0  # 100000 / 50000
      assert result['quote_amount'] == 100000.0
      assert result['rate'] == 50000.0
      assert result['result'] == 2.0

  @pytest.mark.asyncio
  async def test_convert_default_base_amount(self):
    """Test convert with default base amount (1.0)."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      # Setup mock data
      mock_loader.return_value = ("", {
        'eth': {'price': 3000.0},
        'usd': {'value': 1.0}
      })

      err, result = await convert('eth.price-usd.value')

      assert err == ""
      assert result['base_amount'] == 1.0
      assert result['quote_amount'] == 3000.0
      assert result['rate'] == 3000.0
      assert result['result'] == 3000.0

  @pytest.mark.asyncio
  async def test_convert_custom_precision(self):
    """Test convert with custom precision."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      # Setup mock data with high precision values
      mock_loader.return_value = ("", {
        'btc': {'price': 50000.123456789},
        'usd': {'value': 1.0}
      })

      err, result = await convert('btc.price-usd.value', base_amount=1.0, precision=3)

      assert err == ""
      assert result['precision'] == 3
      # Values should be rounded to 3 significant figures

  @pytest.mark.asyncio
  async def test_convert_invalid_pair_format_no_dash(self):
    """Test convert with invalid pair format (no dash)."""
    err, result = await convert('btc.price')

    assert err == "Invalid pair format. Must be 'resource.field-resource.field'"
    assert result == {}

  @pytest.mark.asyncio
  async def test_convert_invalid_pair_format_no_dot(self):
    """Test convert with invalid pair format (no dot)."""
    err, result = await convert('btc-usd')

    assert err == "Invalid pair format. Must be 'resource.field-resource.field'"
    assert result == {}

  @pytest.mark.asyncio
  async def test_convert_both_amounts_specified(self):
    """Test convert with both base and quote amounts specified."""
    err, result = await convert('btc.price-usd.value', base_amount=1.0, quote_amount=50000.0)

    assert err == "Cannot specify both base and quote amounts"
    assert result == {}

  @pytest.mark.asyncio
  async def test_convert_invalid_base_format(self):
    """Test convert with invalid base format (no field)."""
    err, result = await convert('btc-usd.value')

    assert err == "Both base and quote must contain a field specified with dot notation"
    assert result == {}

  @pytest.mark.asyncio
  async def test_convert_invalid_quote_format(self):
    """Test convert with invalid quote format (no field)."""
    err, result = await convert('btc.price-usd')

    assert err == "Both base and quote must contain a field specified with dot notation"
    assert result == {}

  @pytest.mark.asyncio
  async def test_convert_loader_error(self):
    """Test convert when loader returns an error."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("Resource not found", {})

      err, result = await convert('btc.price-usd.value')

      assert err == "Resource not found"
      assert result == {}

  @pytest.mark.asyncio
  async def test_convert_missing_base_field(self):
    """Test convert when base field is missing from resource."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'btc': {'other_field': 50000.0},  # missing 'price' field
        'usd': {'value': 1.0}
      })

      err, result = await convert('btc.price-usd.value')

      assert err == "Required fields not found in resources"
      assert result == {}

  @pytest.mark.asyncio
  async def test_convert_missing_quote_field(self):
    """Test convert when quote field is missing from resource."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'btc': {'price': 50000.0},
        'usd': {'other_field': 1.0}  # missing 'value' field
      })

      err, result = await convert('btc.price-usd.value')

      assert err == "Required fields not found in resources"
      assert result == {}

  @pytest.mark.asyncio
  async def test_convert_missing_resource(self):
    """Test convert when a resource is missing entirely."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'btc': {'price': 50000.0}
        # missing 'usd' resource
      })

      err, result = await convert('btc.price-usd.value')

      assert err == "Required fields not found in resources"
      assert result == {}

  @pytest.mark.asyncio
  async def test_convert_non_numeric_values(self):
    """Test convert when field values are not numeric."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'btc': {'price': "not_a_number"},
        'usd': {'value': 1.0}
      })

      err, result = await convert('btc.price-usd.value')

      assert err == "Field values must be numeric"
      assert result == {}

  @pytest.mark.asyncio
  async def test_pegcheck_valid_in_range(self):
    """Test pegcheck with values within acceptable range."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'usdc': {'price': 1.001},
        'usdt': {'price': 1.000}
      })

      err, result = await pegcheck('usdc.price-usdt.price')

      assert err == ""
      assert result['base'] == 'usdc.price'
      assert result['quote'] == 'usdt.price'
      assert result['base_price'] == 1.001
      assert result['quote_price'] == 1.000
      assert result['adjusted_base'] == 1.001  # factor = 1.0
      assert result['factor'] == 1.0
      assert result['max_deviation'] == 0.002
      assert abs(result['deviation']) <= 0.002
      assert result['in_range'] is True

  @pytest.mark.asyncio
  async def test_pegcheck_valid_out_of_range(self):
    """Test pegcheck with values outside acceptable range."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'usdc': {'price': 1.005},  # 0.5% deviation
        'usdt': {'price': 1.000}
      })

      err, result = await pegcheck('usdc.price-usdt.price')

      assert err == ""
      assert result['deviation'] == 0.005  # 0.5%
      assert result['in_range'] is False  # Outside 0.2% default range

  @pytest.mark.asyncio
  async def test_pegcheck_with_factor(self):
    """Test pegcheck with custom factor."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'btc': {'price': 50000.0},
        'eth': {'price': 3000.0}
      })

      err, result = await pegcheck('btc.price-eth.price', factor=0.06)  # 1 BTC = 16.67 ETH

      assert err == ""
      assert result['factor'] == 0.06
      assert result['adjusted_base'] == 3000.0  # 50000 * 0.06
      # Should be exactly in range since adjusted_base equals quote_price

  @pytest.mark.asyncio
  async def test_pegcheck_custom_deviation(self):
    """Test pegcheck with custom max deviation."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'usdc': {'price': 1.008},  # 0.8% deviation
        'usdt': {'price': 1.000}
      })

      err, result = await pegcheck('usdc.price-usdt.price', max_deviation=0.01)  # 1% tolerance

      assert err == ""
      assert result['max_deviation'] == 0.01
      assert result['in_range'] is True  # Within 1% range

  @pytest.mark.asyncio
  async def test_pegcheck_custom_precision(self):
    """Test pegcheck with custom precision."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'usdc': {'price': 1.0012345},
        'usdt': {'price': 1.0009876}
      })

      err, result = await pegcheck('usdc.price-usdt.price', precision=4)

      assert err == ""
      assert result['precision'] == 4
      # Values should be processed with 4 decimal places precision

  @pytest.mark.asyncio
  async def test_pegcheck_invalid_format_no_field(self):
    """Test pegcheck with invalid format (no field)."""
    err, result = await pegcheck('usdc-usdt')

    assert err == "Both base and quote must contain a field specified with dot notation"
    assert result == {}

  @pytest.mark.asyncio
  async def test_pegcheck_invalid_format_no_dash(self):
    """Test pegcheck with invalid format (no dash)."""
    err, result = await pegcheck('usdc.price')

    assert err == "Both base and quote must contain a field specified with dot notation"
    assert result == {}

  @pytest.mark.asyncio
  async def test_pegcheck_loader_error(self):
    """Test pegcheck when loader returns an error."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("Database connection failed", {})

      err, result = await pegcheck('usdc.price-usdt.price')

      assert err == "Database connection failed"
      assert result == {}

  @pytest.mark.asyncio
  async def test_pegcheck_missing_fields(self):
    """Test pegcheck when required fields are missing."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'usdc': {'other_field': 1.001},  # missing 'price' field
        'usdt': {'price': 1.000}
      })

      err, result = await pegcheck('usdc.price-usdt.price')

      assert err == "Required fields not found in resources"
      assert result == {}

  @pytest.mark.asyncio
  async def test_pegcheck_missing_resource(self):
    """Test pegcheck when a resource is missing."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'usdc': {'price': 1.001}
        # missing 'usdt' resource
      })

      err, result = await pegcheck('usdc.price-usdt.price')

      assert err == "Required fields not found in resources"
      assert result == {}

  @pytest.mark.asyncio
  async def test_pegcheck_non_numeric_values(self):
    """Test pegcheck with non-numeric field values."""
    with patch('src.services.converter.loader.get_last_values') as mock_loader:
      mock_loader.return_value = ("", {
        'usdc': {'price': "invalid"},
        'usdt': {'price': 1.000}
      })

      err, result = await pegcheck('usdc.price-usdt.price')

      assert err == "Price values must be numeric"
      assert result == {}

  def test_converter_imports(self):
    """Test that converter service can be imported correctly."""
    import src.services.converter
    assert hasattr(src.services.converter, 'convert')
    assert hasattr(src.services.converter, 'pegcheck')

  def test_converter_module_structure(self):
    """Test the converter module has the expected structure."""
    from src.services import converter

    # Check functions exist and are callable
    assert callable(converter.convert)
    assert callable(converter.pegcheck)

    # Check they are async functions
    import asyncio
    assert asyncio.iscoroutinefunction(converter.convert)
    assert asyncio.iscoroutinefunction(converter.pegcheck)
