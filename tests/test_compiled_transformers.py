"""Tests for pre-compiled transformer system."""
import pytest
import time
from unittest.mock import Mock
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
  from src.actions.transform import (
    compile_transformer, get_compiled_transformer, CompiledTransformer,
    apply_transformer, _compiled_transformers_cache, BASE_TRANSFORMERS
  )
  from src.models.base import ResourceField
  from src.models.ingesters import Ingester
  DEPENDENCIES_AVAILABLE = True
except ImportError:
  DEPENDENCIES_AVAILABLE = False


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE, reason="Dependencies not available")
class TestCompiledTransformers:
  """Test pre-compiled transformer functionality."""

  def setup_method(self):
    """Clear transformer cache before each test."""
    global _compiled_transformers_cache
    _compiled_transformers_cache.clear()

  def test_compile_numeric_transformer(self):
    """Test compilation of numeric transformers."""
    transformer = "123.45"
    compiled = compile_transformer(transformer)

    assert isinstance(compiled, CompiledTransformer)
    assert compiled.transformer_string == transformer
    assert compiled.field_references == set()
    assert compiled.dotted_references == set()
    assert compiled.series_steps == []
    assert not compiled.has_self_reference

    # Test execution
    result = compiled({}, None)
    assert result == 123.45

  def test_compile_base_transformer(self):
    """Test compilation of base transformers."""
    transformer = "upper"
    compiled = compile_transformer(transformer)

    assert isinstance(compiled, CompiledTransformer)
    assert compiled.transformer_string == transformer
    assert compiled.field_references == set()
    assert compiled.series_steps == []
    assert compiled.has_self_reference

    # Test execution
    result = compiled({}, "hello world")
    assert result == "HELLO WORLD"

  def test_compile_mathematical_expression(self):
    """Test compilation of mathematical expressions."""
    transformer = "{price} * {volume}"
    compiled = compile_transformer(transformer)

    assert isinstance(compiled, CompiledTransformer)
    assert compiled.transformer_string == transformer
    assert compiled.field_references == {"price", "volume"}
    assert compiled.dotted_references == set()
    assert compiled.series_steps == []
    assert not compiled.has_self_reference

    # Test execution
    data = {"price": 100.0, "volume": 1000}
    result = compiled(data, None)
    assert result == 100000.0

  def test_compile_self_reference_expression(self):
    """Test compilation of expressions with self reference."""
    transformer = "{self} * 1.1"
    compiled = compile_transformer(transformer)

    assert isinstance(compiled, CompiledTransformer)
    assert compiled.has_self_reference

    # Test execution
    result = compiled({}, 100.0)
    assert result == 110.0

  def test_compile_mixed_expression(self):
    """Test compilation of complex mixed expressions."""
    transformer = "{self} + {price} * {volume} / 100"
    compiled = compile_transformer(transformer)

    assert isinstance(compiled, CompiledTransformer)
    assert compiled.field_references == {"price", "volume"}
    assert compiled.has_self_reference

    # Test execution
    data = {"price": 50.0, "volume": 200}
    result = compiled(data, 25.0)
    assert result == 125.0  # 25 + 50 * 200 / 100 = 25 + 100 = 125

  def test_compile_dotted_references(self):
    """Test compilation of dotted references."""
    transformer = "{AVAX.idx} * {price}"
    compiled = compile_transformer(transformer)

    assert isinstance(compiled, CompiledTransformer)
    assert compiled.field_references == {"price"}
    assert compiled.dotted_references == {"AVAX.idx"}

  def test_compile_series_steps(self):
    """Test compilation detects series operations."""
    transformer = "{self} + {price::mean(h24)}"
    compiled = compile_transformer(transformer)

    assert isinstance(compiled, CompiledTransformer)
    assert compiled.has_self_reference
    assert compiled.series_steps == [("price", "mean", "h24")]

  def test_transformer_caching(self):
    """Test that compiled transformers are cached correctly."""
    transformer = "{price} * 2"

    # First compilation
    compiled1 = compile_transformer(transformer)

    # Second compilation should return cached version
    compiled2 = compile_transformer(transformer)

    assert compiled1 is compiled2
    assert transformer in [ct.raw for ct in _compiled_transformers_cache.values()]

  def test_get_compiled_transformer(self):
    """Test the get_compiled_transformer helper function."""
    transformer = "{self} / 10"

    compiled = get_compiled_transformer(transformer)
    assert isinstance(compiled, CompiledTransformer)

    # Should return same instance on subsequent calls
    compiled2 = get_compiled_transformer(transformer)
    assert compiled is compiled2

  def test_none_value_handling(self):
    """Test proper handling of None values."""
    transformer = "{self} * {price}"
    compiled = compile_transformer(transformer)

    # Test with None field value
    data = {"price": None}
    result = compiled(data, 100.0)
    assert result is None

    # Test with None self value
    data = {"price": 50.0}
    result = compiled(data, None)
    assert result is None

  def test_error_handling(self):
    """Test error handling in compiled transformers."""
    transformer = "{self} / {divisor}"
    compiled = compile_transformer(transformer)

    # Test division by zero
    data = {"divisor": 0}
    result = compiled(data, 100.0)
    assert result is None

  def test_complex_transformer_chain(self):
    """Test multiple chained transformations."""
    transformers = [
      "{self} * 2",
      "{self} + 10",
      "round2"
    ]

    compiled_transformers = [compile_transformer(t) for t in transformers]

    # Simulate transformation chain
    value = 45.123
    for compiled in compiled_transformers:
      if compiled.has_self_reference or not compiled.field_references:
        value = compiled({}, value)
      else:
        value = compiled({}, value)

    assert value == 100.25  # (45.123 * 2 + 10) rounded to 2 decimals


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE, reason="Dependencies not available")
class TestPerformanceComparison:
  """Test performance improvements from pre-compilation."""

  def setup_method(self):
    """Set up mock ingester and field for performance tests."""
    self.mock_ingester = Mock()
    self.mock_ingester.name = "TEST_INGESTER"
    self.mock_ingester.get_field_values.return_value = {
      "price": 100.0,
      "volume": 1000,
      "base_price": 50.0
    }
    self.mock_ingester.data_by_field = {
      "price": Mock(value=100.0),
      "volume": Mock(value=1000),
      "base_price": Mock(value=50.0)
    }
    self.mock_ingester.fields = []

    self.mock_field = Mock()
    self.mock_field.name = "test_field"
    self.mock_field.value = 25.0

  @pytest.mark.asyncio
  async def test_performance_simple_expression(self):
    """Test performance improvement for simple mathematical expressions."""
    transformer = "{price} * {volume}"

    # Warm up compilation cache
    compile_transformer(transformer)

    # Time the compiled version (multiple runs for accuracy)
    start_time = time.time()
    for _ in range(1000):
      compiled = get_compiled_transformer(transformer)
      result = compiled(self.mock_ingester.get_field_values(), self.mock_field.value)
    compiled_time = time.time() - start_time

    # Verify correctness
    assert result == 100000.0

    print(f"Compiled transformer execution time for 1000 runs: {compiled_time:.4f}s")
    print(f"Average per execution: {compiled_time/1000*1000:.4f}ms")

  @pytest.mark.asyncio
  async def test_performance_complex_expression(self):
    """Test performance for complex expressions."""
    transformer = "({self} + {price}) * {volume} / {base_price}"

    # Warm up compilation cache
    compile_transformer(transformer)

    # Time the compiled version
    start_time = time.time()
    for _ in range(1000):
      compiled = get_compiled_transformer(transformer)
      result = compiled(self.mock_ingester.get_field_values(), self.mock_field.value)
    compiled_time = time.time() - start_time

    # Verify correctness: (25 + 100) * 1000 / 50 = 2500
    assert result == 2500.0

    print(f"Complex compiled transformer execution time for 1000 runs: {compiled_time:.4f}s")
    print(f"Average per execution: {compiled_time/1000*1000:.4f}ms")

  def test_cache_effectiveness(self):
    """Test that the transformer cache is working effectively."""
    transformers = [
      "{price} * 2",
      "{volume} / 10",
      "{self} + {price}",
      "{price} * 2",  # Duplicate - should hit cache
      "{volume} / 10"  # Duplicate - should hit cache
    ]

    # Clear cache
    global _compiled_transformers_cache
    _compiled_transformers_cache.clear()

    # Compile all transformers
    for transformer in transformers:
      compile_transformer(transformer)

    # Should only have 3 unique transformers cached
    assert len(_compiled_transformers_cache) == 3


@pytest.mark.skipif(not DEPENDENCIES_AVAILABLE, reason="Dependencies not available")
class TestIngesterIntegration:
  """Test integration with Ingester model."""

  def test_ingester_compile_transformers(self):
    """Test that ingesters pre-compile their transformers."""
    from src.models.ingesters import Ingester
    from src.models.base import ResourceField

    # Create mock fields with transformers
    field1 = ResourceField(
      name="price",
      type="float64",
      transformers=["{self} * 1.1", "round2"]
    )
    field2 = ResourceField(
      name="volume",
      type="int64",
      transformers=["{self} / 1000"]
    )

    # Create ingester
    ingester = Ingester(
      name="test_ingester",
      fields=[field1, field2]
    )

    # Test compilation method
    initial_cache_size = len(_compiled_transformers_cache)
    ingester.compile_transformers()

    # Should have compiled all transformers
    final_cache_size = len(_compiled_transformers_cache)
    assert final_cache_size >= initial_cache_size + 3  # 3 unique transformers

  def test_from_config_auto_compiles(self):
    """Test that from_config automatically compiles transformers."""
    from src.models.ingesters import Ingester
    from src.models.base import ResourceField

    # Clear cache
    global _compiled_transformers_cache
    initial_cache_size = len(_compiled_transformers_cache)

    config = {
      "name": "test_ingester",
      "fields": [
        ResourceField(
          name="test_field",
          type="float64",
          transformers=["{self} * 2", "round4"]
        )
      ]
    }

    # Create ingester via from_config
    ingester = Ingester.from_config(config)

    # Should have auto-compiled transformers
    final_cache_size = len(_compiled_transformers_cache)
    assert final_cache_size >= initial_cache_size + 2
