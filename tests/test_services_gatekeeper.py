"""Tests for services.gatekeeeper module."""
import sys
from pathlib import Path
from unittest.mock import Mock
from fastapi import Request

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.services.gatekeeeper import requester_id, hashed_requester_id, _id_cache


class TestRequesterID:
  """Test requester_id function."""

  def test_with_state_requester_id(self):
    """Test when request state has requester_id."""
    mock_request = Mock(spec=Request)
    mock_request.state.requester_id = "custom_id"

    result = requester_id(mock_request)
    assert result == "custom_id"

  def test_with_client_host(self):
    """Test when request has client host."""
    mock_request = Mock(spec=Request)
    # Remove requester_id from state to trigger hasattr check
    del mock_request.state.requester_id
    mock_request.client.host = "192.168.1.1"

    result = requester_id(mock_request)
    assert result == "192.168.1.1"

  def test_with_no_client(self):
    """Test when request has no client."""
    mock_request = Mock(spec=Request)
    # Remove requester_id from state
    del mock_request.state.requester_id
    mock_request.client = None

    result = requester_id(mock_request)
    assert result == "unknown"

  def test_no_state_attribute(self):
    """Test when request has no state attribute."""

    class RequestWithoutState:

      def __init__(self):
        self.client = Mock()
        self.client.host = "10.0.0.1"

    mock_request = RequestWithoutState()

    result = requester_id(mock_request)
    assert result == "10.0.0.1"


class TestHashedRequesterID:
  """Test hashed_requester_id function."""

  def setup_method(self):
    """Clear cache before each test."""
    _id_cache.clear()

  def test_with_client_host(self):
    """Test hashing with client host."""
    mock_request = Mock(spec=Request)
    mock_request.client.host = "192.168.1.1"

    result = hashed_requester_id(mock_request)

    # Should be a 32-character MD5 hash
    assert len(result) == 32
    assert isinstance(result, str)

    # Test caching - should return same result
    result2 = hashed_requester_id(mock_request)
    assert result == result2

  def test_with_no_client(self):
    """Test hashing when request has no client."""
    mock_request = Mock(spec=Request)
    mock_request.client = None

    result = hashed_requester_id(mock_request)

    # Should still return a hash for "unknown"
    assert len(result) == 32
    assert isinstance(result, str)

  def test_custom_salt(self):
    """Test with custom salt."""
    mock_request1 = Mock(spec=Request)
    mock_request1.client.host = "192.168.1.10"  # Different IP to avoid cache

    mock_request2 = Mock(spec=Request)
    mock_request2.client.host = "192.168.1.11"  # Different IP to avoid cache

    result1 = hashed_requester_id(mock_request1, "salt1:")
    result2 = hashed_requester_id(mock_request2,
                                  "salt1:")  # Same salt, different IP

    # Different IPs with same salt should produce different hashes
    assert result1 != result2
    assert len(result1) == 32
    assert len(result2) == 32

  def test_cache_functionality(self):
    """Test that caching works correctly."""
    mock_request = Mock(spec=Request)
    mock_request.client.host = "192.168.1.1"

    # First call
    result1 = hashed_requester_id(mock_request)

    # Verify it's in cache
    assert "192.168.1.1" in _id_cache
    assert _id_cache["192.168.1.1"] == result1

    # Second call should use cache
    result2 = hashed_requester_id(mock_request)
    assert result1 == result2

  def test_different_ips_different_hashes(self):
    """Test different IPs produce different hashes."""
    mock_request1 = Mock(spec=Request)
    mock_request1.client.host = "192.168.1.1"

    mock_request2 = Mock(spec=Request)
    mock_request2.client.host = "192.168.1.2"

    result1 = hashed_requester_id(mock_request1)
    result2 = hashed_requester_id(mock_request2)

    assert result1 != result2

    # Both should be cached
    assert len(_id_cache) == 2

  def test_cache_persistence(self):
    """Test that cache persists between calls."""
    _id_cache.clear()

    mock_request = Mock(spec=Request)
    mock_request.client.host = "10.0.0.1"

    # Make multiple calls with same IP
    results = []
    for _ in range(3):
      results.append(hashed_requester_id(mock_request))

    # All results should be identical
    assert all(r == results[0] for r in results)

    # Cache should only have one entry
    assert len(_id_cache) == 1
