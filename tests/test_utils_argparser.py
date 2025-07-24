"""
Test suite for argparser utility functions.

Purpose: Test the ArgParser class which extends ArgumentParser with additional
functionality for environment variable loading and type detection.
"""
from os import environ as env
from unittest.mock import patch

from src.utils.argparser import ArgParser


class TestArgParser:
  """Test ArgParser class functionality."""

  def test_init(self):
    """Test ArgParser initialization."""
    parser = ArgParser(description="Test parser")
    assert parser.info == {}
    assert parser.origin == {}
    assert parser.parsed is None
    assert parser.description == "Test parser"

  def test_add_argument_basic(self):
    """Test basic argument addition."""
    parser = ArgParser()
    action = parser.add_argument("--test", default="default_value")

    assert action.dest == "test"
    assert action.default == "default_value"
    assert "test" in parser.info
    assert parser.info["test"] == (str, "default_value")

  def test_add_argument_with_bool_default(self):
    """Test argument addition with boolean default."""
    parser = ArgParser()
    action = parser.add_argument("--flag", default=True)

    assert action.dest == "flag"
    assert action.type is bool
    assert parser.info["flag"] == (bool, True)

  def test_add_argument_with_group(self):
    """Test argument addition with group."""
    parser = ArgParser()
    group = parser.add_argument_group("test_group")
    action = parser.add_argument("--grouped", default="value", group=group)

    assert action.dest == "grouped"
    assert "grouped" in parser.info

  def test_add_argument_type_detection(self):
    """Test automatic type detection."""
    parser = ArgParser()

    # Test string type
    parser.add_argument("--string", default="test")
    assert parser.info["string"] == (str, "test")

    # Test int type
    parser.add_argument("--number", default=42)
    assert parser.info["number"] == (int, 42)

    # Test float type
    parser.add_argument("--decimal", default=3.14)
    assert parser.info["decimal"] == (float, 3.14)

    # Test bool type
    parser.add_argument("--boolean", default=False)
    assert parser.info["boolean"] == (bool, False)

  def test_get_info(self):
    """Test get_info method."""
    parser = ArgParser()
    parser.add_argument("--test", default="value")

    info = parser.get_info("test")
    assert info == (str, "value")

    # Test non-existent argument
    assert parser.get_info("nonexistent") is None

  def test_parse_args(self):
    """Test parse_args method."""
    parser = ArgParser()
    parser.add_argument("--test", default="default")

    args = parser.parse_args(["--test", "custom"])
    assert args.test == "custom"
    assert parser.parsed is not None
    assert parser.parsed.test == "custom"

  def test_argument_tuple_to_kwargs(self):
    """Test argument tuple conversion to kwargs."""
    parser = ArgParser()

    # Test basic tuple
    arg_tuple = (("--test", ), str, "default", None, "Help text")
    args, kwargs = parser.argument_tuple_to_kwargs(arg_tuple)

    assert args == ("--test", )
    assert kwargs == {"default": "default", "help": "Help text", "type": str}

  def test_argument_tuple_to_kw_argswith_action(self):
    """Test argument tuple conversion with action."""
    parser = ArgParser()

    arg_tuple = (("--flag", ), bool, True, "store_true", "Flag help")
    args, kwargs = parser.argument_tuple_to_kwargs(arg_tuple)

    assert args == ("--flag", )
    assert kwargs == {
        "default": True,
        "help": "Flag help",
        "action": "store_true"
    }

  def test_add_arguments(self):
    """Test adding multiple arguments."""
    parser = ArgParser()
    arguments = [(("--arg1", ), str, "default1", None, "Help 1"),
                 (("--arg2", ), int, 42, None, "Help 2")]

    parser.add_arguments(arguments)

    assert "arg1" in parser.info
    assert "arg2" in parser.info
    assert parser.info["arg1"] == (str, "default1")
    assert parser.info["arg2"] == (int, 42)

  def test_add_arguments_with_group(self):
    """Test adding arguments to a group."""
    parser = ArgParser()
    group = parser.add_argument_group("test_group")
    arguments = [(("--grouped1", ), str, "value1", None, "Help 1"),
                 (("--grouped2", ), str, "value2", None, "Help 2")]

    parser.add_arguments(arguments, group=group)

    assert "grouped1" in parser.info
    assert "grouped2" in parser.info

  def test_add_group(self):
    """Test adding argument group."""
    parser = ArgParser()
    arguments = [(("--group-arg", ), str, "default", None, "Group argument")]

    parser.add_group("test_group", arguments)

    assert "group_arg" in parser.info
    assert parser.info["group_arg"] == (str, "default")

  def test_add_groups(self):
    """Test adding multiple argument groups."""
    parser = ArgParser()
    groups = {
        "group1": [(("--g1-arg", ), str, "g1_default", None, "Group 1 arg")],
        "group2": [(("--g2-arg", ), int, 100, None, "Group 2 arg")]
    }

    parser.add_groups(groups)

    assert "g1_arg" in parser.info
    assert "g2_arg" in parser.info
    assert parser.info["g1_arg"] == (str, "g1_default")
    assert parser.info["g2_arg"] == (int, 100)

  def test_load_env_without_env_file(self):
    """Test load_env without environment file."""
    parser = ArgParser()
    parser.add_argument("--test", default="default")
    parser.add_argument("--env", default=".env")

    # Parse args first
    parser.parse_args(["--test", "cli_value"])

    with patch('src.utils.argparser.dotenv_values', return_value={}):
      result = parser.load_env()

      assert result.test == "cli_value"
      assert parser.origin.get("test") == "cli"

  def test_load_env_with_dotenv_file(self):
    """Test load_env with .env file values."""
    parser = ArgParser()
    parser.add_argument("--test", default="default")
    parser.add_argument("--number", default=0, type=int)
    parser.add_argument("--env", default=".env")

    parser.parse_args([])

    env_values = {"test": "env_value", "number": "42"}
    with patch('src.utils.argparser.dotenv_values', return_value=env_values):
      result = parser.load_env()

      assert result.test == "env_value"
      assert result.number == 42
      assert parser.origin.get("test") == ".env file"
      assert parser.origin.get("number") == ".env file"

  def test_load_env_with_os_environment(self):
    """Test load_env with OS environment variables."""
    parser = ArgParser()
    parser.add_argument("--test", default="default")
    parser.add_argument("--env", default=".env")

    parser.parse_args([])

    with patch('src.utils.argparser.dotenv_values', return_value={}):
      with patch.dict(env, {"test": "os_env_value"}):
        result = parser.load_env()

        assert result.test == "os_env_value"
        assert parser.origin.get("test") == "os env"

  def test_load_env_boolean_conversion(self):
    """Test load_env with boolean value conversion."""
    parser = ArgParser()
    parser.add_argument("--flag", default=False, type=bool)
    parser.add_argument("--env", default=".env")

    parser.parse_args([])

    env_values = {"flag": "true"}
    with patch('src.utils.argparser.dotenv_values', return_value=env_values):
      result = parser.load_env()

      assert result.flag is True
      assert parser.origin.get("flag") == ".env file"

  def test_load_env_with_custom_env_file(self):
    """Test load_env with custom environment file path."""
    parser = ArgParser()
    parser.add_argument("--env", default=".env")
    parser.add_argument("--test", default="default")

    parser.parse_args(["--env", "custom.env"])

    with patch('src.utils.argparser.dotenv_values') as mock_dotenv:
      mock_dotenv.return_value = {"test": "custom_env_value"}
      result = parser.load_env("custom.env")

      mock_dotenv.assert_called_with("custom.env")
      assert result.test == "custom_env_value"

  def test_pretty_output(self):
    """Test pretty output formatting."""
    parser = ArgParser()
    parser.add_argument("--test", default="value")
    parser.add_argument("--number", default=42)
    parser.add_argument("--env", default=".env")

    parser.parse_args(["--test", "custom"])
    parser.load_env()

    with patch('src.utils.argparser.prettify') as mock_prettify:
      mock_prettify.return_value = "formatted_table"
      result = parser.pretty()

      assert result == "formatted_table"
      mock_prettify.assert_called_once()
      # Check that prettify was called with correct structure
      call_args = mock_prettify.call_args
      assert call_args[1]["headers"] == ["Name", "Value", "Source"]

  def test_load_env_auto_parse_if_not_parsed(self):
    """Test that load_env automatically calls parse_args if not already parsed."""
    parser = ArgParser()
    parser.add_argument("--test", default="default")
    parser.add_argument("--env", default=".env")

    # Don't call parse_args manually, but mock sys.argv to avoid test interference
    with patch('sys.argv', ['test_program']):
      with patch('src.utils.argparser.dotenv_values', return_value={}):
        result = parser.load_env()

        # Should have auto-parsed with defaults
        assert result is not None
        assert hasattr(result, "test")

  def test_complex_type_handling(self):
    """Test handling of complex argument types."""
    parser = ArgParser()

    # Test with no type and no default (should default to str based on implementation)
    action = parser.add_argument("--no-default")
    assert action.type is str  # Implementation defaults to str when no default provided

    # Test with explicit type
    action2 = parser.add_argument("--explicit", type=float, default=1.0)
    assert action2.type is float

  def test_origin_tracking(self):
    """Test that argument value origins are correctly tracked."""
    parser = ArgParser()
    parser.add_argument("--cli-arg", default="default")
    parser.add_argument("--env-arg", default="default")
    parser.add_argument("--dotenv-arg", default="default")
    parser.add_argument("--env", default=".env")

    parser.parse_args(["--cli-arg", "from_cli"])

    env_values = {"dotenv_arg": "from_dotenv"}
    with patch('src.utils.argparser.dotenv_values', return_value=env_values):
      with patch.dict(env, {"env_arg": "from_os_env"}):
        parser.load_env()

        assert parser.origin.get("cli_arg") == "cli"
        assert parser.origin.get("env_arg") == "os env"
        assert parser.origin.get("dotenv_arg") == ".env file"
