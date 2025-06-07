"""Tests for utils.types module."""
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.types import is_bool, to_bool, is_float, is_primitive, is_epoch, is_iterable, flatten


class TestIsBool:
  """Test is_bool function."""

  def test_true_values(self):
    """Test values that should return True."""
    assert is_bool("true")
    assert is_bool("True")
    assert is_bool("TRUE")
    assert is_bool("yes")
    assert is_bool("Yes")
    assert is_bool("1")

  def test_false_values(self):
    """Test values that should return True for is_bool."""
    assert is_bool("false")
    assert is_bool("False")
    assert is_bool("FALSE")
    assert is_bool("no")
    assert is_bool("No")
    assert is_bool("0")

  def test_invalid_values(self):
    """Test values that should return False."""
    assert not is_bool("maybe")
    assert not is_bool("2")
    assert not is_bool("")
    assert not is_bool("none")


class TestToBool:
  """Test to_bool function."""

  def test_true_conversions(self):
    """Test values that convert to True."""
    assert to_bool("true") is True
    assert to_bool("True") is True
    assert to_bool("yes") is True
    assert to_bool("1") is True

  def test_false_conversions(self):
    """Test values that convert to False."""
    assert to_bool("false") is False
    assert to_bool("False") is False
    assert to_bool("no") is False
    assert to_bool("0") is False


class TestIsFloat:
  """Test is_float function."""

  def test_valid_floats(self):
    """Test valid float strings."""
    assert is_float("1.5")
    assert is_float("0.0")
    assert is_float("-1.5")
    assert is_float("1")
    assert is_float("123")

  def test_invalid_floats(self):
    """Test invalid float strings."""
    assert not is_float("abc")
    assert not is_float("")
    assert not is_float("1.2.3")
    assert not is_float(" 1.5 ")  # Has whitespace
    assert not is_float("1.5 ")   # Trailing whitespace
    assert not is_float(" 1.5")   # Leading whitespace


class TestIsPrimitive:
  """Test is_primitive function."""

  def test_primitive_types(self):
    """Test primitive types."""
    assert is_primitive(1)
    assert is_primitive(1.5)
    assert is_primitive("string")
    assert is_primitive(True)
    assert is_primitive(None)

  def test_non_primitive_types(self):
    """Test non-primitive types."""
    assert not is_primitive([1, 2, 3])
    assert not is_primitive({"key": "value"})
    assert not is_primitive((1, 2))
    assert not is_primitive({1, 2, 3})
    assert not is_primitive(frozenset([1, 2]))


class TestIsEpoch:
  """Test is_epoch function."""

  def test_valid_epochs(self):
    """Test valid epoch timestamps."""
    assert is_epoch(1609459200)  # Jan 1, 2021
    assert is_epoch(0)           # Unix epoch start
    assert is_epoch(1234567890)  # Feb 13, 2009

  def test_invalid_epochs(self):
    """Test invalid epoch values."""
    assert not is_epoch(-1)         # Negative timestamp
    assert not is_epoch(99999999999999)  # Too large
    assert not is_epoch("abc")      # String
    assert not is_epoch([1, 2, 3])  # List
    assert not is_epoch({"key": "value"})  # Dict

  def test_edge_cases(self):
    """Test edge cases for epoch validation."""
    assert is_epoch("1609459200")  # String number
    assert not is_epoch(None)      # None value


class TestIsIterable:
  """Test is_iterable function."""

  def test_iterable_types(self):
    """Test iterable types."""
    assert is_iterable([1, 2, 3])
    assert is_iterable((1, 2, 3))
    assert is_iterable({1, 2, 3})
    assert is_iterable(frozenset([1, 2]))

  def test_non_iterable_types(self):
    """Test non-iterable types."""
    assert not is_iterable(1)
    assert not is_iterable("string")  # String is iterable but not in our list
    assert not is_iterable({"key": "value"})  # Dict is iterable but not in our list
    assert not is_iterable(None)


class TestFlatten:
  """Test flatten function."""

  def test_simple_flatten(self):
    """Test basic flattening."""
    result = list(flatten([1, [2, 3], 4]))
    assert result == [1, 2, 3, 4]

  def test_nested_flatten(self):
    """Test nested flattening with depth."""
    nested = [1, [2, [3, 4]], 5]
    result = list(flatten(nested, depth=2))
    assert result == [1, 2, 3, 4, 5]

  def test_depth_limit(self):
    """Test depth limitation."""
    nested = [1, [2, [3, 4]], 5]
    result = list(flatten(nested, depth=1))
    assert result == [1, 2, [3, 4], 5]

  def test_non_iterable_input(self):
    """Test non-iterable input."""
    result = list(flatten(42))
    assert result == [42]

  def test_empty_list(self):
    """Test empty list."""
    result = list(flatten([]))
    assert result == []

  def test_flatten_with_maps(self):
    """Test flattening with dictionaries."""
    # Test that flatten_maps=True allows dict iteration when depth permits
    data = [1, {"a": 1, "b": 2}, 3]
    result_with_maps = list(flatten(data, flatten_maps=True))
    result_without_maps = list(flatten(data, flatten_maps=False))

    # Both should be the same for this case since dict is at depth 0
    assert result_with_maps == [1, {"a": 1, "b": 2}, 3]
    assert result_without_maps == [1, {"a": 1, "b": 2}, 3]
