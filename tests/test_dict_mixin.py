import unittest
from dataclasses import dataclass
from datetime import datetime
from typing import Union


class DictMixin:
  """Lightweight mixin for dict conversion using __iter__ - standalone for testing"""

  def __iter__(self):
    """Iterate over all attributes (class + instance), excluding private ones"""
    # Get class attributes (excluding private/magic methods)
    items = dict((k, v) for k, v in self.__class__.__dict__.items() if not k.startswith('_'))
    # Update with instance attributes (excluding private ones)
    items.update({k: v for k, v in self.__dict__.items() if not k.startswith('_')})
    # Yield items, handling datetime serialization
    for key, value in items.items():
      if isinstance(value, datetime):
        yield key, value.isoformat()
      else:
        yield key, value

  def to_dict(self) -> dict:
    """Convert to dictionary using __iter__"""
    return dict(self)

  @classmethod
  def from_dict(cls, data: dict):
    """Create instance from dictionary, handling datetime strings"""
    kwargs = {}
    for key, value in data.items():
      # Handle datetime fields for dataclasses
      if hasattr(cls, '__dataclass_fields__') and key in cls.__dataclass_fields__:
        field_type = cls.__dataclass_fields__[key].type
        if field_type == datetime or (hasattr(field_type, '__origin__') and
                                    field_type.__origin__ is Union and
                                    datetime in field_type.___args_):
          if isinstance(value, str):
            try:
              value = datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
              pass  # Keep original value if parsing fails
      kwargs[key] = value
    return cls(**kwargs)


@dataclass
class TestClass(DictMixin):
    class_attr: str = "class_value"
    name: str = ""
    timestamp: datetime = None

    def __init__(self, name: str):
        self.name = name
        self.timestamp = datetime.now()
        self.instance_attr = "instance_value"


@dataclass
class SimpleClass(DictMixin):
    value: str = "test"
    number: int = 42


class TestDictMixin(unittest.TestCase):

    def test_to_dict_basic(self):
        """Test basic to_dict functionality"""
        simple = SimpleClass()
        result = simple.to_dict()

        self.assertIn('value', result)
        self.assertIn('number', result)
        self.assertEqual(result['value'], 'test')
        self.assertEqual(result['number'], 42)

    def test_to_dict_with_datetime(self):
        """Test to_dict with datetime serialization"""
        test_obj = TestClass("test_name")
        result = test_obj.to_dict()

        self.assertIn('name', result)
        self.assertIn('timestamp', result)
        self.assertIn('instance_attr', result)
        self.assertIn('class_attr', result)

        self.assertEqual(result['name'], 'test_name')
        self.assertEqual(result['instance_attr'], 'instance_value')
        self.assertEqual(result['class_attr'], 'class_value')

        # Datetime should be serialized as ISO string
        self.assertIsInstance(result['timestamp'], str)
        self.assertTrue(result['timestamp'].count(':') >= 2)  # ISO format check

    def test_dict_conversion_directly(self):
        """Test dict() conversion using __iter__"""
        test_obj = TestClass("direct_test")
        result = dict(test_obj)

        self.assertIn('name', result)
        self.assertIn('class_attr', result)
        self.assertEqual(result['name'], 'direct_test')
        self.assertEqual(result['class_attr'], 'class_value')

    def test_from_dict_basic(self):
        """Test from_dict class method"""
        data = {
            'value': 'new_test',
            'number': 100
        }

        obj = SimpleClass.from_dict(data)
        self.assertEqual(obj.value, 'new_test')
        self.assertEqual(obj.number, 100)

    def test_private_attributes_excluded(self):
        """Test that private attributes are excluded"""
        simple = SimpleClass()
        simple._private = "should_not_appear"
        simple.__very_private = "definitely_not_appear"

        result = simple.to_dict()
        self.assertNotIn('_private', result)
        self.assertNotIn('__very_private', result)
        self.assertNotIn('_SimpleClass__very_private', result)

    def test_class_and_instance_attributes(self):
        """Test that both class and instance attributes are included"""
        test_obj = TestClass("mixed_test")
        result = test_obj.to_dict()

        # Class attribute should be present
        self.assertEqual(result['class_attr'], 'class_value')
        # Instance attributes should be present
        self.assertEqual(result['name'], 'mixed_test')
        self.assertEqual(result['instance_attr'], 'instance_value')


if __name__ == '__main__':
    unittest.main()
