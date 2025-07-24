"""Tests for src.models module."""
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import ResourceField, Scope, ing


class TestResourceFieldModel:
  """Test ResourceField model functionality."""

  def test_field_creation(self):
    """Test ResourceField creation."""
    field = ResourceField(name="price", selector="data.price", type="float64")

    assert field.name == "price"
    assert field.selector == "data.price"
    assert field.type == "float64"
    assert field.transient is False

  def test_field_with_value(self):
    """Test ResourceField with value."""
    field = ResourceField(name="volume", selector="data.volume", value=1000000)

    assert field.name == "volume"
    assert field.value == 1000000

  def test_field_serialization(self):
    """Test ResourceField serialization."""
    field = ResourceField(name="price",
                          selector="data.price",
                          type="float64",
                          transformers=["normalize"])

    # Default scope doesn't include transformers
    field_dict = field.to_dict()
    assert field_dict["type"] == "float64"
    assert field_dict["transformers"] is None  # Not included in default scope

    # Test with TRANSFORMERS scope
    field_dict_detailed = field.to_dict(Scope.TRANSFORMERS)
    assert field_dict_detailed["transformers"] == ["normalize"]


class TestScopeModel:
  """Test Scope enumeration functionality."""

  def test_scope_flags(self):
    """Test Scope flag values."""
    assert Scope.TRANSIENT.value == 1
    assert Scope.TARGET.value == 2
    assert Scope.SELECTOR.value == 4
    assert Scope.TRANSFORMERS.value == 8

  def test_scope_combinations(self):
    """Test Scope flag combinations."""
    combined = Scope.TARGET | Scope.SELECTOR
    assert combined & Scope.TARGET
    assert combined & Scope.SELECTOR
    assert not (combined & Scope.TRANSIENT)


class TestIngesterModel:
  """Test Ingester model functionality."""

  def test_ingester_creation(self):
    """Test Ingester model creation."""
    field = ResourceField(name="price", selector="data.price")

    ingester = ing(name="crypto_ingester",
                   ingester_type="http_api",
                   interval="5m",
                   fields=[field])

    assert ingester.name == "crypto_ingester"
    assert ingester.ingester_type == "http_api"
    assert ingester.interval == "5m"
    assert len(ingester.fields) == 1
    assert ingester.fields[0].name == "price"

  def test_ingester_types(self):
    """Test different ingester types."""
    valid_types = ["http_api", "ws_api", "evm_caller", "processor"]

    for ingester_type in valid_types:
      ingester = ing(name=f"test_{ingester_type}",
                     ingester_type=ingester_type,
                     interval="1m",
                     fields=[])
      assert ingester.ingester_type == ingester_type

  def test_ingester_properties(self):
    """Test Ingester computed properties."""
    ingester = ing(name="test",
                   ingester_type="http_api",
                   interval="m5",
                   fields=[])

    # Test interval_sec property
    assert ingester.interval_sec == 300  # 5 minutes = 300 seconds

    # Test id property
    assert isinstance(ingester.id, str)
    assert len(ingester.id) > 0

  def test_ingester_serialization(self):
    """Test Ingester serialization."""
    field = ResourceField(name="price", selector="data.price")
    ingester = ing(name="test_ingester",
                   ingester_type="http_api",
                   interval="1h",
                   fields=[field])

    ingester_dict = ingester.to_dict()
    assert ingester_dict["name"] == "test_ingester"
    assert ingester_dict["interval"] == "1h"
    assert ingester_dict["ingester_type"] == "http_api"


class TestModelValidation:
  """Test model validation functionality."""

  def test_field_name_validation(self):
    """Test field name validation rules."""
    # Valid names
    valid_names = ["price", "volume_24h", "market_cap", "price_usd"]
    for name in valid_names:
      field = ResourceField(name=name, selector="data.test")
      assert field.name == name

  def test_selector_validation(self):
    """Test selector validation."""
    # Valid selectors
    valid_selectors = [
        "data.price", "result[0].value", "response.data.market.price"
    ]
    for selector in valid_selectors:
      field = ResourceField(name="test", selector=selector)
      assert field.selector == selector

  def test_url_validation(self):
    """Test URL validation in fields."""
    valid_urls = [
        "https://api.example.com/data", "http://localhost:8080/api",
        "wss://stream.example.com/ws"
    ]

    for url in valid_urls:
      field = ResourceField(name="test", selector="data.value", target=url)
      assert field.target == url


class TestModelMethods:
  """Test additional model methods."""

  def test_field_signature(self):
    """Test ResourceField signature generation."""
    field = ResourceField(name="price",
                          type="float64",
                          target="https://api.example.com",
                          selector="data.price")

    signature = field.signature()
    assert isinstance(signature, str)
    assert "price" in signature
    assert "float64" in signature

  def test_ingester_dependencies(self):
    """Test Ingester dependency extraction."""
    field1 = ResourceField(name="field1", selector="SourceA.value")
    field2 = ResourceField(name="field2", selector="SourceB.price")
    field3 = ResourceField(name="field3",
                           selector="data.volume")  # This is also a dependency

    ingester = ing(name="test",
                   ingester_type="processor",
                   fields=[field1, field2, field3])

    deps = ingester.dependencies()
    assert "SourceA" in deps
    assert "SourceB" in deps
    assert "data" in deps  # This is also extracted as a dependency
    assert len(deps) == 3
