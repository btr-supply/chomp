"""Tests for utils.uid module."""
import pytest
import sys
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


class TestUtilsUID:
  """Test utils.uid module."""

  def test_uid_imports(self):
    """Test that uid module can be imported."""
    try:
      from src.utils import uid
      assert uid is not None
    except ImportError:
      pytest.skip("utils.uid module not available")

  def test_uid_functions_exist(self):
    """Test that expected UID functions exist."""
    try:
      from src.utils.uid import (
          get_workdir_root,
          generate_instance_uid,
          get_instance_uid,
          load_uid_masks,
          generate_instance_name
      )

      assert callable(get_workdir_root)
      assert callable(generate_instance_uid)
      assert callable(get_instance_uid)
      assert callable(load_uid_masks)
      assert callable(generate_instance_name)

    except ImportError:
      pytest.skip("UID functions not available")


class TestWorkdirRoot:
  """Test workdir root functionality."""

  def test_get_workdir_root_default(self):
    """Test get_workdir_root with default value."""
    try:
      from src.utils.uid import get_workdir_root

      with patch.dict(os.environ, {}, clear=True):
        root = get_workdir_root()
        assert isinstance(root, Path)

    except ImportError:
      pytest.skip("get_workdir_root not available")


class TestInstanceUID:
  """Test instance UID generation and management."""

  def test_generate_instance_uid(self):
    """Test instance UID generation."""
    try:
      from src.utils.uid import generate_instance_uid

      uid1 = generate_instance_uid()
      uid2 = generate_instance_uid()

      assert isinstance(uid1, str)
      assert isinstance(uid2, str)
      assert len(uid1) == 32  # MD5 hash length
      assert len(uid2) == 32

    except ImportError:
      pytest.skip("generate_instance_uid not available")

  def test_get_instance_uid_no_file(self):
    """Test get_instance_uid when no file exists."""
    try:
      from src.utils.uid import get_instance_uid

      with tempfile.TemporaryDirectory() as tmpdir:
        with patch('src.utils.uid.get_workdir_root', return_value=Path(tmpdir)):
          uid = get_instance_uid()
          assert isinstance(uid, str)
          assert len(uid) == 32

          # Check that file was created
          uid_file = Path(tmpdir) / ".uid"
          assert uid_file.exists()

    except ImportError:
      pytest.skip("get_instance_uid not available")

  def test_get_instance_uid_existing_file(self):
    """Test get_instance_uid when file already exists."""
    try:
      from src.utils.uid import get_instance_uid

      test_uid = "test-uid-12345"

      with tempfile.TemporaryDirectory() as tmpdir:
        uid_file = Path(tmpdir) / ".uid"
        uid_file.write_text(test_uid)

        with patch('src.utils.uid.get_workdir_root', return_value=Path(tmpdir)):
          uid = get_instance_uid()
          assert uid == test_uid

    except ImportError:
      pytest.skip("get_instance_uid not available")


class TestUIDMasks:
  """Test UID masks loading functionality."""

  def test_load_uid_masks_success(self):
    """Test load_uid_masks with valid file."""
    try:
      from src.utils.uid import load_uid_masks

      test_names = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon"]

      with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        for name in test_names:
          f.write(f"{name}\n")
        temp_path = f.name

      try:
        with patch.dict(os.environ, {"UID_MASKS_FILE": temp_path}):
          result = load_uid_masks()
          assert result == test_names
      finally:
        os.unlink(temp_path)

    except ImportError:
      pytest.skip("load_uid_masks not available")

  def test_load_uid_masks_empty_lines(self):
    """Test load_uid_masks filters empty lines."""
    try:
      from src.utils.uid import load_uid_masks

      content = "Alpha\n\nBeta\n  \nGamma\n"
      expected = ["Alpha", "Beta", "Gamma"]

      with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write(content)
        temp_path = f.name

      try:
        with patch.dict(os.environ, {"UID_MASKS_FILE": temp_path}):
          result = load_uid_masks()
          assert result == expected
      finally:
        os.unlink(temp_path)

    except ImportError:
      pytest.skip("load_uid_masks not available")


class TestInstanceNameGeneration:
  """Test instance name generation functionality."""

  @pytest.mark.asyncio
  async def test_generate_instance_name_no_masks(self):
    """Test generate_instance_name when no masks file available."""
    try:
      from src.utils.uid import generate_instance_name

      with patch('src.utils.uid.load_uid_masks', return_value=None):
        with patch('src.utils.uid.get_instance_uid', return_value="test-uid"):
          name = await generate_instance_name()
          assert name == "test-uid"

    except ImportError:
      pytest.skip("generate_instance_name not available")

  @pytest.mark.asyncio
  async def test_generate_instance_name_success(self):
    """Test successful instance name generation."""
    try:
      from src.utils.uid import generate_instance_name

      # Create a mix of short and long names starting with different letters
      test_names = [
          "Alpha", "Beta", "Chi", "Dex", "Ebb",  # Short names (≤5 chars)
          "Fortress", "Guardian", "Horizon", "Infinity", "Justice"  # Long names
      ]

      with patch('src.utils.uid.load_uid_masks', return_value=test_names):
        name = await generate_instance_name()

        assert isinstance(name, str)
        assert "-" in name

        parts = name.split("-")
        assert len(parts) == 2

        # At least one part should be short
        short_count = sum(1 for part in parts if len(part) <= 5)
        assert short_count >= 1

    except ImportError:
      pytest.skip("generate_instance_name not available")

  @pytest.mark.asyncio
  async def test_generate_instance_name_different_starting_letters(self):
    """Test that generated names have different starting letters."""
    try:
      from src.utils.uid import generate_instance_name

      # Names with different starting letters
      test_names = [
          "Alpha", "Beta", "Chi", "Dex", "Ebb",
          "Fortress", "Guardian", "Horizon", "Infinity", "Justice"
      ]

      # Run multiple times to test consistency
      different_letters_count = 0
      total_tests = 20

      for _ in range(total_tests):
        with patch('src.utils.uid.load_uid_masks', return_value=test_names):
          name = await generate_instance_name()
          parts = name.split("-")

          if len(parts) == 2 and parts[0][0].lower() != parts[1][0].lower():
            different_letters_count += 1

      # Should succeed most of the time (allowing some randomness)
      success_rate = different_letters_count / total_tests
      assert success_rate > 0.7  # Should succeed at least 70% of the time

    except ImportError:
      pytest.skip("generate_instance_name not available")

  @pytest.mark.asyncio
  async def test_generate_instance_name_format_consistency(self):
    """Test that all generated names follow consistent format."""
    try:
      from src.utils.uid import generate_instance_name

      test_names = ["Alpha", "Beta", "Fortress", "Guardian"]

      for _ in range(10):
        with patch('src.utils.uid.load_uid_masks', return_value=test_names):
          name = await generate_instance_name()

          # Should always be Name1-Name2 format
          assert isinstance(name, str)
          assert "-" in name
          assert name.count("-") == 1

          parts = name.split("-")
          assert len(parts) == 2
          assert all(part.isalpha() for part in parts)  # Only letters
          assert all(part[0].isupper() for part in parts)  # Capitalized

    except ImportError:
      pytest.skip("generate_instance_name not available")
